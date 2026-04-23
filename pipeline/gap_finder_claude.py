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
import re
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


GAP_FINDER_PROMPT = """Using your web search, find EVERY food recall, public health alert, RASFF notification, or market withdrawal issued in {region} in the last {since_days} days.

Today's date: {today}

REGULATORS TO SEARCH — go to each agency's website and check their latest postings:
{agencies}

For each agency above, search: '<agency name> food recall' and '<agency name> latest alerts'. Open the results and check publication dates within the last {since_days} days.

In scope: Listeria, Salmonella, E. coli / STEC / O157:H7, Clostridium
botulinum, Norovirus, Hepatitis A, Campylobacter, Cyclospora, Vibrio, Cronobacter
sakazakii, Bacillus cereus / cereulide, Shigella, Yersinia, Brucella,
Aflatoxins, Ochratoxin A, Patulin, marine biotoxins (DSP/PSP/ASP),
Histamine (scombrotoxin), other mycotoxins, mould/mold contamination,
foreign material (glass / metal / plastic / wood / stone fragments),
rodent / insect / pest contamination,
chemical hazards: heavy metals (lead, cadmium, mercury, arsenic), ethylene
oxide, dioxins/PCBs, mineral oil (MOAH/MOSH), pesticide residues over MRL,
Sudan dyes, melamine, chlorate, unauthorized substances.
Also include EU RASFF notifications (alerts, border rejections).

OUT of scope: undeclared allergens (unless combined with in-scope hazard),
labeling errors, quality complaints, non-food products.

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


REGION_SPECS = [
    {"region": "Europe", "agencies": (
        "RASFF (webgate.ec.europa.eu/rasff-window), RappelConso (rappel.conso.gouv.fr), "
        "BVL (lebensmittelwarnung.de), FSA UK (food.gov.uk/news-alerts), "
        "FSAI Ireland (fsai.ie), AESAN Spain, AGES Austria, NVWA Netherlands, "
        "AFSCA Belgium, EFET Greece, Min. Salute Italy, BLV Switzerland")},
    {"region": "NorthAmerica", "agencies": (
        "FDA (fda.gov/safety/recalls), USDA FSIS (fsis.usda.gov/recalls), "
        "CFIA Canada (recalls-rappels.canada.ca)")},
    {"region": "AsiaPacific", "agencies": (
        "FSANZ Australia (foodstandards.gov.au/food-recalls), MPI New Zealand, "
        "MFDS Korea, MHLW Japan, CFS Hong Kong, SFA Singapore, FSSAI India")},
    {"region": "LATAM_ME_Africa", "agencies": (
        "ANVISA Brazil, COFEPRIS Mexico, ANMAT Argentina, "
        "SFDA Saudi Arabia, NAFDAC Nigeria, NCC South Africa")},
]


def _call_claude_web(prompt: str, system: str, max_tokens: int = 4096) -> str:
    """Claude Haiku 4.5 with web search — requests.post (proven approach)."""
    import requests as _req
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        r = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
                "max_tokens": max_tokens,
                "system": system,
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=300,
        )
        if r.status_code != 200:
            log.error("Claude HTTP %d: %s", r.status_code, r.text[:500])
            return ""
        resp = r.json()
        log.info("  stop=%s blocks=%s", resp.get("stop_reason"),
                 [b.get("type") for b in resp.get("content", [])])
        texts = [b.get("text", "") for b in resp.get("content", [])
                 if b.get("type") == "text" and b.get("text", "").strip()]
        return texts[-1].strip() if texts else ""
    except Exception as e:
        log.error("Claude web call failed: %s", e)
        return ""


def query_claude_for_gaps(since_days: int) -> List[Dict[str, Any]]:
    """5-region web search sweep. Returns raw recall dicts (unvalidated)."""
    if not CLAUDE_ENABLED:
        log.warning("ANTHROPIC_API_KEY not set — gap-finder cannot run")
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_recalls: List[Dict[str, Any]] = []

    for spec in REGION_SPECS:
        region = spec["region"]
        agencies = spec["agencies"]
        log.info("→ Region %s", region)

        prompt = GAP_FINDER_PROMPT.format(
            since_days=since_days, today=today,
            region=region, agencies=agencies,
        )

        txt = _call_claude_web(prompt, system=GAP_FINDER_SYSTEM)
        if not txt:
            log.warning("  [%s] empty response", region)
            continue

        txt = _strip_fences(txt)
        
        # Extract JSON from mixed prose+JSON response
        json_match = re.search(r'\{[^{}]*"recalls"\s*:\s*\[.*\]\s*\}', txt, re.DOTALL)
        if not json_match:
            json_match = re.search(r'\{.*\}', txt, re.DOTALL)
        if not json_match:
            log.warning("  [%s] no JSON found in response: %s", region, txt[:300])
            continue
        
        try:
            data = json.loads(json_match.group(0))
        except json.JSONDecodeError as e:
            log.warning("  [%s] JSON parse failed: %s | %s", region, e, txt[:250])
            continue

        rows = data.get("recalls", []) or []
        log.info("  [%s] found %d recalls", region, len(rows))
        all_recalls.extend(rows)

    log.info("Claude gap-finder total: %d recalls across %d regions",
             len(all_recalls), len(REGION_SPECS))
    return all_recalls


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
