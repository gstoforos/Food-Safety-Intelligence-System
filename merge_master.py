"""
Claude gap-finder — a scheduled job that asks Claude for all food pathogen
recalls worldwide in the last N days and appends any that aren't already in
Recalls or Pending.

This is a SECOND safety net (alongside gap_finder_openai.py). Running two
independent LLMs with the same brief doubles the chance of catching recalls
that the deterministic scrapers missed. Claude and OpenAI have different
training-data snapshots and different failure modes, so their coverage gaps
are largely non-overlapping.

Scheduling:
  - Runs daily at 05:00 UTC (1 hour before the OpenAI gap-finder at 06:00,
    and well after the 17:00 scraper run).
  - Output goes to Pending — the 07:30 URL gate promotes rows only if the
    URL is live.

Cost: ~$0.005/run on Haiku 4.5 ($1/M input, $5/M output).
Enable by setting ANTHROPIC_API_KEY in GitHub secrets.
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

from scrapers._models import (                          # noqa: E402
    Recall, normalize_pathogen, normalize_country,
    infer_region, assign_tier,
)
from pipeline.merge_master import (                     # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.claude_client import (                      # noqa: E402
    _call_claude, ENABLED as CLAUDE_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("claude-gap-finder")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
GAP_FINDER_SYSTEM = (
    "You are a senior food safety analyst with encyclopaedic knowledge of "
    "global food recalls. You track every regulator worldwide: FDA, USDA FSIS, "
    "RASFF (EU), FSA (UK), CFIA (Canada), FSANZ (Australia/NZ), FSAI (IE), "
    "AESAN (ES), BVL (DE), RappelConso (FR), EFET (GR), Min. Salute (IT), "
    "AGES (AT), AFSCA (BE), NVWA (NL), Livsmedelsverket (SE), Mattilsynet (NO), "
    "Fødevarestyrelsen (DK), Ruokavirasto (FI), MHLW (JP), MFDS (KR), "
    "CFS (HK), SAMR (CN), SFA (SG), FSSAI (IN), ANVISA (BR), COFEPRIS (MX), "
    "ANMAT (AR), NAFDAC (NG), SFDA (SA), and dozens more. "
    "Return ONLY strict JSON — no markdown, no prose, no commentary."
)

GAP_FINDER_PROMPT = """\
List EVERY food recall, market withdrawal, or public-health alert issued
worldwide in the last {since_days} days whose cause is one of:

  (a) PATHOGENS / MICROBIAL CONTAMINATION / BIOLOGICAL TOXINS
      Listeria monocytogenes, Salmonella spp., E. coli / STEC / O157:H7,
      Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter, Cyclospora,
      Vibrio, Cronobacter sakazakii, Bacillus cereus / cereulide, Aflatoxins,
      Ochratoxin A, Patulin, marine biotoxins (DSP/PSP/ASP), Histamine
      (scombrotoxin), Shigella, Yersinia, other mycotoxins.

  (b) RODENTICIDES / RAT POISON — criminal tampering (bromadiolone,
      brodifacoum, difethialone, difenacoum, chlorophacinone).

  (c) HEAVY METALS at levels exceeding regulatory limits (lead, cadmium,
      arsenic, mercury).

  (d) PHYSICAL HAZARDS posing injury risk (glass/metal/plastic fragments,
      foreign bodies).

OUT OF SCOPE — do NOT include: undeclared allergens, labeling errors,
mechanical/packaging issues, pesticide residues (unless linked to a-d above).

Today's date: {today}

For each recall provide ALL fields:
- Date       : YYYY-MM-DD (publication / initiation date)
- Source     : agency short name, e.g. "FDA", "CFIA", "RASFF", "FSA"
- Company    : firm / producer name
- Brand      : commercial brand ("—" if not stated)
- Product    : full description incl. size/weight/lot where available
- Pathogen   : specific hazard, e.g. "Listeria monocytogenes", "Lead (Pb) contamination"
- Reason     : short cause description
- Class      : "Recall" / "Alert" / "Class I" / "Public Health Alert" / etc.
- Country    : English country name (e.g. "France", "United States")
- Outbreak   : 1 if illnesses/cases mentioned, else 0
- URL        : FULL deep-link to the specific recall page — NOT a homepage,
               search page, or category listing. You MUST provide a verifiable
               URL. If you cannot produce a specific URL, OMIT that recall.
- Notes      : distribution area, lot/batch, illness count, extra context

CRITICAL RULES:
1. Only include recalls you are CONFIDENT actually happened.
2. NEVER hallucinate URLs. Every URL must point to a real recall page.
3. Prefer official government regulator URLs over news articles.
4. Coverage: worldwide — all continents, not just US/EU.
5. If uncertain about a recall or its URL, OMIT it entirely.

Return strict JSON:
{{"recalls": [{{"Date":"...","Source":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","Country":"...","Outbreak":0,"URL":"...","Notes":"..."}}]}}

If no in-scope recalls happened in the last {since_days} days, return: {{"recalls": []}}
"""


# ---------------------------------------------------------------------------
# Query Claude
# ---------------------------------------------------------------------------
def query_claude_for_gaps(since_days: int) -> List[Dict[str, Any]]:
    """Single global query. Returns raw recall dicts (unvalidated)."""
    if not CLAUDE_ENABLED:
        log.warning("ANTHROPIC_API_KEY not set — Claude gap-finder cannot run")
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prompt = GAP_FINDER_PROMPT.format(since_days=since_days, today=today)
    log.info("Querying Claude for pathogen recalls worldwide, last %d days", since_days)

    txt = _call_claude(prompt, max_tokens=8000, system=GAP_FINDER_SYSTEM)
    if not txt:
        log.warning("Claude gap-finder returned no text")
        return []
    try:
        data = json.loads(txt)
    except json.JSONDecodeError:
        # Try stripping markdown fences
        from review.claude_client import _strip_fences
        try:
            data = json.loads(_strip_fences(txt))
        except json.JSONDecodeError as e:
            log.warning("Gap-finder JSON parse failed: %s | text=%s", e, txt[:300])
            return []
    recalls = data.get("recalls", []) or []
    log.info("Claude proposed %d recalls", len(recalls))
    return recalls


# ---------------------------------------------------------------------------
# Normalize raw dicts -> Recall objects
# ---------------------------------------------------------------------------
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
            # Hard filter: require both Pathogen and URL. Rows without either
            # can never be promoted and would only pollute Pending.
            if not rec.Pathogen or rec.Pathogen in ("—", ""):
                continue
            if not rec.URL or not rec.URL.lower().startswith(("http://", "https://")):
                continue
            out.append(rec)
        except Exception as e:
            log.warning("Skipping malformed gap-finder row: %s (%s)", e, row)
    log.info("Claude gap-finder: %d raw -> %d valid Recall objects", len(raw), len(out))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Claude gap-finder run: %s", scraped_at)
    log.info("Data dir: %s", DATA_DIR)

    if not CLAUDE_ENABLED:
        log.error("ANTHROPIC_API_KEY missing — aborting")
        return 2

    # ---- Load current state ------------------------------------------------
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    existing_pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d existing pending",
             len(approved), len(existing_pending))

    # ---- Ask Claude --------------------------------------------------------
    raw = query_claude_for_gaps(SINCE_DAYS)
    if not raw:
        log.info("Nothing proposed — exiting cleanly")
        return 0

    # ---- Normalize to Recall objects ---------------------------------------
    recalls = to_recall_objects(raw)
    if not recalls:
        log.info("No gap-finder rows survived validation — exiting cleanly")
        return 0

    # ---- Append to Pending (dedup vs Approved + existing Pending) ----------
    before = len(existing_pending)
    pending = append_to_pending(
        existing_pending=existing_pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    net_new = len(pending) - before
    log.info("Claude gap-finder added %d net-new rows to Pending", net_new)

    # ---- Save --------------------------------------------------------------
    save_xlsx_with_pending(approved, sort_rows(pending), XLSX_PATH)

    # ---- Commit ------------------------------------------------------------
    if not SKIP_COMMIT and net_new > 0:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx"],
            message=(f"FSIS Claude gap-finder {t0.strftime('%Y-%m-%d')} "
                     f"(+{net_new} pending)"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("DONE in %.1fs | +%d pending rows", elapsed, net_new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
