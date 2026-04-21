"""
Claude gap-finder — a scheduled job that asks Claude's knowledge base for all
food pathogen recalls worldwide in the last N days and appends any that aren't
already in Recalls or Pending.

Counterpart to pipeline/gap_finder_openai.py — same contract, same prompt,
same Pending-sheet flow. Two independent LLMs catch different gaps; whatever
either proposes goes through the 07:30 Athens URL gate and is promoted to
Recalls only if the URL is live.

Runs daily at 05:00 UTC (1h before OpenAI gap-finder at 06:00, both well
after the 17:00 Athens scrape). Single global query per run.

Model default is claude-haiku-4-5 (cheap, already configured in claude_client).
Override via ANTHROPIC_MODEL env var if a heavier model is needed — the task
is once-a-day so cost is not a constraint.
"""
from __future__ import annotations
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import Recall, normalize_pathogen, normalize_country, infer_region, assign_tier  # noqa: E402
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.claude_client import _call_claude, _strip_fences, ENABLED as CLAUDE_ENABLED  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-claude")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")


GAP_FINDER_SYSTEM = (
    "You are a senior food safety analyst with deep knowledge of global food "
    "recalls from regulators including FDA, USDA FSIS, EU RASFF, FSA (UK), FSAI "
    "(Ireland), FSANZ (Australia/NZ), CFIA (Canada), AESAN (Spain), BVL "
    "(Germany), RappelConso (France), EFET (Greece), Salute.gov.it (Italy), "
    "CFS (Hong Kong), MFDS (Korea), MHLW (Japan), ANVISA (Brazil), SENASA "
    "(Argentina), COFEPRIS (Mexico), FSSAI (India), NAFDAC (Nigeria), and "
    "others. Return ONLY strict JSON — no markdown, no prose, no commentary."
)


GAP_FINDER_PROMPT = """List EVERY food recall / public-health alert issued worldwide in the last {since_days} days whose cause is a PATHOGEN, MICROBIAL CONTAMINATION, or BIOLOGICAL TOXIN.

Today's date: {today}

In scope (pathogens): Listeria, Salmonella, E. coli / STEC / O157:H7, Clostridium
botulinum, Norovirus, Hepatitis A, Campylobacter, Cyclospora, Vibrio, Cronobacter
sakazakii, Bacillus cereus / cereulide, Aflatoxins, Ochratoxin A, Patulin, marine
biotoxins (DSP/PSP/ASP), Histamine (scombrotoxin), Shigella, Yersinia, other
mycotoxins.

OUT of scope (do NOT include): undeclared allergens, foreign objects (plastic /
metal / glass / wood), labeling errors, mechanical issues, chemical or heavy-metal
contamination unless biological in origin, pesticide residues.

For each recall return ALL fields below:
- Date       : YYYY-MM-DD, the recall / alert publication date
- Source     : agency short name, e.g. "FDA", "USDA FSIS", "RASFF", "FSA", "CFIA"
- Company    : firm / producer name
- Brand      : commercial brand name ("—" if not stated)
- Product    : full product description including size / pack where available
- Pathogen   : specific pathogen detected, e.g. "Listeria monocytogenes"
- Reason     : short cause description
- Class      : recall class ("Recall" / "Alert" / "Class I/II/III" / "Public Health Alert" etc.)
- Country    : English country name, e.g. "United States", "France"
- Outbreak   : 1 if illnesses / cases mentioned, else 0
- URL        : FULL deep-link URL to the specific recall page — NOT a homepage
               or category page. You MUST include a verifiable URL. If you cannot
               produce a specific recall-page URL, OMIT that recall entirely.
- Notes      : distribution area, lot / batch info, illness count, extra context

CRITICAL RULES:
1. Only include recalls you are confident actually happened and whose URL you are
   confident points to the real recall page. If uncertain, omit.
2. Never invent or hallucinate URLs. Every URL must be a real page that exists.
3. The URL must be specific (e.g. .../liquid-blenz-corp-recalls-product-due-..
   or .../fiche-rappel/12345), NEVER a homepage or category listing.
4. Coverage goal: worldwide — US, EU member states, UK, Canada, Australia, NZ,
   Japan, Korea, China/HK, India, Brazil, Mexico, Argentina, South Africa,
   Middle East, etc.

Return strict JSON:
{{"recalls": [{{"Date":"...","Source":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","Country":"...","Outbreak":0,"URL":"...","Notes":"..."}}]}}

If no pathogen recalls happened in the last {since_days} days, return: {{"recalls": []}}
"""


def query_claude_for_gaps(since_days: int) -> List[Dict[str, Any]]:
    """Single global query. Returns raw recall dicts (unvalidated)."""
    if not CLAUDE_ENABLED:
        log.warning("ANTHROPIC_API_KEY not set — gap-finder cannot run")
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prompt = GAP_FINDER_PROMPT.format(since_days=since_days, today=today)
    log.info("Querying Claude for pathogen recalls worldwide, last %d days", since_days)
    # max_tokens bumped to allow a long global list.
    txt = _call_claude(prompt, system=GAP_FINDER_SYSTEM, max_tokens=8000)
    if not txt:
        log.warning("Claude gap-finder returned no text")
        return []
    # Claude occasionally wraps JSON in ```json fences despite instructions.
    txt = _strip_fences(txt)
    try:
        data = json.loads(txt)
    except json.JSONDecodeError as e:
        log.warning("Gap-finder JSON parse failed: %s | text=%s", e, txt[:300])
        return []
    recalls = data.get("recalls", []) or []
    log.info("Claude proposed %d recalls", len(recalls))
    return recalls


def to_recall_objects(raw: List[Dict[str, Any]]) -> List[Recall]:
    """Convert raw Claude dicts to normalized Recall objects."""
    out: List[Recall] = []
    for row in raw:
        try:
            pathogen = normalize_pathogen(row.get("Pathogen", "") or "")
            country = normalize_country(row.get("Country", "") or "")
            outbreak = int(row.get("Outbreak", 0) or 0)
            rec = Recall(
                Date=(row.get("Date") or "")[:10],
                Source=row.get("Source", "") or "Claude-gap",
                Company=row.get("Company", "") or "",
                Brand=row.get("Brand", "") or "—",
                Product=row.get("Product", "") or "",
                Pathogen=pathogen,
                Reason=row.get("Reason", "") or "",
                Class=row.get("Class", "") or "",
                Country=country,
                Region=infer_region(country) if country else "",
                Tier=assign_tier(pathogen, outbreak),
                Outbreak=outbreak,
                URL=(row.get("URL") or "").strip(),
                Notes=row.get("Notes", "") or "",
            )
            rec = rec.normalize()
            # Hard filter: we require both Pathogen and URL. Rows without either
            # can never be promoted and would only pollute Pending.
            if not rec.Pathogen or rec.Pathogen in ("—", ""):
                continue
            if not rec.URL or not rec.URL.lower().startswith(("http://", "https://")):
                continue
            out.append(rec)
        except Exception as e:
            log.warning("Skipping malformed gap-finder row: %s (%s)", e, row)
            continue
    log.info("Gap-finder: %d raw -> %d valid Recall objects", len(raw), len(out))
    return out


def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Claude gap-finder run: %s", scraped_at)
    log.info("Data dir: %s", DATA_DIR)

    if not CLAUDE_ENABLED:
        log.error("ANTHROPIC_API_KEY missing — aborting")
        return 2

    # ---- Load current state ---------------------------------------------
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    existing_pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d existing pending", len(approved), len(existing_pending))

    # ---- Ask Claude -----------------------------------------------------
    raw = query_claude_for_gaps(SINCE_DAYS)
    if not raw:
        log.info("Nothing proposed — exiting cleanly")
        return 0

    # ---- Normalize to Recall objects ------------------------------------
    recalls = to_recall_objects(raw)
    if not recalls:
        log.info("No gap-finder rows survived validation — exiting cleanly")
        return 0

    # ---- Append to Pending ----------------------------------------------
    before = len(existing_pending)
    pending = append_to_pending(
        existing_pending=existing_pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    net_new = len(pending) - before
    log.info("Gap-finder added %d net-new rows to Pending", net_new)

    # ---- Save ------------------------------------------------------------
    save_xlsx_with_pending(approved, sort_rows(pending), XLSX_PATH)

    # ---- Commit ----------------------------------------------------------
    if not SKIP_COMMIT and net_new > 0:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx"],
            message=f"FSIS Claude gap-finder {t0.strftime('%Y-%m-%d')} (+{net_new} pending)",
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("DONE in %.1fs | +%d pending rows", elapsed, net_new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
