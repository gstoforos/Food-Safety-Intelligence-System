"""
Gemini gap-finder — scheduled job that uses Gemini 2.0 Flash with Google
Search grounding to find worldwide pathogen recalls the direct agency
scrapers missed.

Counterpart to pipeline/gap_finder_claude.py and pipeline/gap_finder_openai.py
— same output contract, same Pending-sheet flow, same URL-gate hand-off.
Three independent LLMs catch different gaps.

Why Gemini matters here
-----------------------
The other two gap-finders rely on model training data only (no web access),
so they cannot see recalls published after their training cutoff — which is
EXACTLY the window where gaps are most costly. Gemini 2.0 Flash supports
native `google_search` grounding: it runs a live Google search as part of
generating its answer, returns real URLs it found, and is FREE on the
Gemini 1,500-req/day free tier. That makes this the only gap-finder that
can actually discover recalls from last week.

Cost: $0 on Gemini free tier (1 call per run, once per day).
Model: gemini-2.5-flash (override via GEMINI_MODEL env var).
Schedule: 3×/day (Athens time):
  03:00 → PRIMARY NorthAmerica, UK
  14:00 → PRIMARY AsiaPacific, Oceania
  23:00 → PRIMARY Europe (RASFF, all 26+ EU agencies)

SDK: google-genai (new unified SDK, replaces deprecated google.generativeai).
     Install: pip install google-genai
"""
from __future__ import annotations
import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-gemini")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


# ---------------------------------------------------------------------------
# Gemini call — with google_search grounding
# ---------------------------------------------------------------------------

def _collect_gemini_keys() -> List[str]:
    """Same key-collection logic as enrichment/gemini_client.py."""
    keys: List[str] = []
    legacy = os.getenv("GEMINI_API_KEY")
    if legacy:
        keys.append(legacy.strip())
    for i in range(1, 11):
        v = os.getenv(f"GEMINI_API_KEY_{i}")
        if v and v.strip() not in keys:
            keys.append(v.strip())
    return keys


def _call_gemini_with_search(prompt: str, system: Optional[str] = None) -> Optional[str]:
    """
    Single Gemini call with Google Search grounding enabled.
    Returns the text response, or None on failure.

    Uses the new google.genai SDK (replaces deprecated google.generativeai).
    """
    try:
        from google import genai  # type: ignore
        from google.genai import types  # type: ignore
    except ImportError:
        log.error("google-genai not installed — run: pip install google-genai")
        return None

    keys = _collect_gemini_keys()
    if not keys:
        log.error("No GEMINI_API_KEY(_1..10) env var set")
        return None

    last_error: Optional[Exception] = None
    # One call — rotate through keys only on hard failure (not output).
    for api_key in keys:
        try:
            client = genai.Client(api_key=api_key)
            
            # Build config with Google Search tool
            config = types.GenerateContentConfig(
                system_instruction=system if system else None,
                tools=[types.Tool(google_search=types.GoogleSearch())],
            )
            
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=config,
            )
            
            text = (resp.text or "").strip()
            if text:
                # Log how many search queries Gemini actually fired
                try:
                    if hasattr(resp, 'candidates') and resp.candidates:
                        gm = getattr(resp.candidates[0], 'grounding_metadata', None)
                        if gm:
                            queries = getattr(gm, 'web_search_queries', []) or []
                            if queries:
                                log.info(
                                    "Gemini ran %d Google searches: %s",
                                    len(queries),
                                    " | ".join(str(q)[:60] for q in queries[:5]),
                                )
                except Exception:
                    pass
                return text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log.debug("Gemini attempt failed (%s): %s",
                      type(exc).__name__, str(exc)[:150])
            continue

    log.error("Gemini gap-finder: all keys failed. Last error: %s", last_error)
    return None


def _strip_fences(txt: str) -> str:
    """Strip ```json ... ``` fences Gemini occasionally wraps JSON in."""
    t = txt.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*\n", "", t)
        t = re.sub(r"\n```\s*$", "", t)
    return t.strip()


# ---------------------------------------------------------------------------
# Primary-region rotation — three Gemini runs/day, each targets a different
# region so every continent gets a dedicated deep sweep every 24 hours.
#
#   03:00 Athens → NorthAmerica, UK   (FDA, USDA FSIS, CFIA, FSA UK)
#   14:00 Athens → AsiaPacific, Oceania (MHLW, MFDS, CFS HK, SFA, FSANZ, MPI NZ)
#   23:00 Athens → Europe             (RASFF, RappelConso, BVL, AESAN, AGES,
#                                      Min. Salute, EFET, AFSCA, all 26+ EU)
#
# OpenAI gap-finder (12:00) covers LATAM + ME/Africa as its primary.
# Tavily (22:00) covers NorthAmerica deterministically.
# Together the four gap-finders guarantee every region has a primary.
#
# Override at runtime: GAP_PRIMARY_REGION=Europe (skips auto-rotation).
# ---------------------------------------------------------------------------

def _pick_primary_region() -> str:
    """Auto-select primary region based on Athens hour."""
    override = os.getenv("GAP_PRIMARY_REGION", "").strip()
    if override:
        return override
    try:
        from zoneinfo import ZoneInfo
        hour = datetime.now(ZoneInfo("Europe/Athens")).hour
    except ImportError:
        import pytz  # type: ignore
        hour = datetime.now(pytz.timezone("Europe/Athens")).hour
    if hour < 8:        # 03:00 run
        return "NorthAmerica, UK"
    elif hour < 18:     # 14:00 run
        return "AsiaPacific, Oceania"
    else:               # 23:00 run
        return "Europe"

PRIMARY_REGION = _pick_primary_region()


def _primary_banner(primary: str) -> str:
    return (
        f"⚑ PRIMARY-REGION DEEP SWEEP — '{primary}' is your strongest region. "
        f"Run EXTRA Google Searches in local languages and against every "
        f"regulator domain in this region. Other regions still in scope but "
        f"emphasis is on '{primary}'. ⚑\n\n"
    )


# ---------------------------------------------------------------------------
# Gap-finder prompt — same JSON contract as the Claude + OpenAI variants
# ---------------------------------------------------------------------------

GAP_FINDER_SYSTEM = (
    "You are a senior food safety analyst. Use Google Search to find real, "
    "recent food pathogen recalls worldwide. Return ONLY strict JSON — no "
    "markdown, no prose, no commentary. Never invent URLs — every URL you "
    "return must have appeared verbatim in a Google Search result you ran."
)


GAP_FINDER_PROMPT = """Using Google Search, find EVERY food recall / public-health alert issued worldwide in the last {since_days} days whose cause is a PATHOGEN, MICROBIAL CONTAMINATION, or BIOLOGICAL TOXIN.

Today's date: {today}

SEARCH STRATEGY: Run multiple Google searches to cover all major regulators. Example queries to run:
- "food recall {year} salmonella listeria" site:fda.gov OR site:fsis.usda.gov OR site:cdc.gov
- "food recall {year}" site:rappelconso.gouv.fr OR site:food.gov.uk OR site:fsai.ie
- "food recall {year}" site:inspection.canada.ca OR site:foodstandards.gov.au OR site:mpi.govt.nz
- "Lebensmittelrückruf {year}" site:lebensmittelwarnung.de OR site:ages.at
- "rappel aliment {year}" site:rappelconso.gouv.fr
- "retiro alimento {year}" site:aesan.gob.es
- Agencies to cover: FDA, USDA FSIS, EU RASFF, FSA UK, FSAI Ireland, FSANZ Australia/NZ, CFIA Canada, AESAN Spain, BVL Germany, RappelConso France, EFET Greece, Min. Salute Italy, CFS Hong Kong, MFDS Korea, MHLW Japan, ANVISA Brazil, COFEPRIS Mexico, FSSAI India, NAFDAC Nigeria, SFDA Saudi Arabia, and any others.

In scope (pathogens): Listeria, Salmonella, E. coli / STEC / O157:H7, Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter, Cyclospora, Vibrio, Cronobacter sakazakii, Bacillus cereus / cereulide, Aflatoxins, Ochratoxin A, Patulin, marine biotoxins, Histamine, Shigella, Yersinia, other mycotoxins.

OUT of scope (do NOT include): undeclared allergens, foreign objects, labeling errors, mechanical issues, chemical/heavy-metal contamination, pesticide residues.

For each recall return ALL fields below:
- Date      : YYYY-MM-DD, the publication date
- Source    : agency short name, e.g. "FDA", "USDA FSIS", "RASFF", "CFIA"
- Company   : firm / producer name
- Brand     : commercial brand name (use "—" if not stated)
- Product   : full product description including size/pack where available
- Pathogen  : specific pathogen, e.g. "Listeria monocytogenes"
- Reason    : short cause description
- Class     : recall class ("Recall", "Alert", "Class I/II/III", "Public Health Alert")
- Country   : English country name, e.g. "USA", "France", "Germany"
- Outbreak  : 1 if illnesses/cases/deaths mentioned, else 0
- URL       : FULL deep-link URL to the SPECIFIC recall detail page.
              MUST be a URL that appeared in your Google Search results.
              NEVER a homepage, category page, or invented URL.
- Notes     : distribution area, lot/batch info, illness count, extra context

CRITICAL RULES:
1. Every URL must come from a real Google Search result you ran. Do not invent.
2. The URL must be specific — a recall detail page, not a category listing.
3. If you cannot find a specific recall-page URL for a potential recall, OMIT it.
4. Coverage goal: worldwide — US, EU, UK, Canada, Australia, NZ, Japan, Korea, China, India, Brazil, Mexico, Argentina, South Africa, Middle East, etc.

Return strict JSON:
{{"recalls": [{{"Date":"...","Source":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","Country":"...","Outbreak":0,"URL":"...","Notes":"..."}}]}}

If no pathogen recalls are found, return: {{"recalls": []}}
"""


def query_gemini_for_gaps(since_days: int) -> List[Dict[str, Any]]:
    """Single global query with Google Search grounding. Returns raw recall dicts (unvalidated)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    year = datetime.now(timezone.utc).strftime("%Y")
    prompt = GAP_FINDER_PROMPT.format(since_days=since_days, today=today, year=year)
    if PRIMARY_REGION:
        prompt = _primary_banner(PRIMARY_REGION) + prompt
    log.info("Querying Gemini (with Google Search grounding) for pathogen "
             "recalls, last %d days [primary region: %s]",
             since_days, PRIMARY_REGION or "none")
    txt = _call_gemini_with_search(prompt, system=GAP_FINDER_SYSTEM)
    if not txt:
        log.warning("Gemini gap-finder returned no text")
        return []
    txt = _strip_fences(txt)
    try:
        data = json.loads(txt)
    except json.JSONDecodeError as e:
        log.warning("Gap-finder JSON parse failed: %s | text=%s", e, txt[:300])
        return []
    recalls = data.get("recalls", []) or []
    log.info("Gemini proposed %d recalls", len(recalls))
    return recalls


# ---------------------------------------------------------------------------
# Normalization — identical logic to gap_finder_claude.py
# ---------------------------------------------------------------------------

def to_recall_objects(raw: List[Dict[str, Any]]) -> List[Recall]:
    """Convert raw Gemini dicts to normalized Recall objects."""
    out: List[Recall] = []
    for row in raw:
        try:
            pathogen = normalize_pathogen(row.get("Pathogen", "") or "")
            country = normalize_country(row.get("Country", "") or "")
            outbreak = int(row.get("Outbreak", 0) or 0)
            rec = Recall(
                Date=(row.get("Date") or "")[:10],
                Source=row.get("Source", "") or "Gemini-gap",
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
                Notes=(row.get("Notes", "") or "") + "  [via Gemini gap-finder + Google Search]",
            )
            rec = rec.normalize()
            # Hard filter — same rules as Claude/OpenAI gap-finders.
            if not rec.Pathogen or rec.Pathogen in ("—", ""):
                continue
            if not rec.URL or not rec.URL.lower().startswith(("http://", "https://")):
                continue
            out.append(rec)
        except Exception as e:
            log.warning("Skipping malformed gap-finder row: %s (%s)", e, row)
            continue
    log.info("Gemini gap-finder: %d raw -> %d valid Recall objects", len(raw), len(out))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Gemini gap-finder run: %s", scraped_at)
    log.info("Data dir: %s", DATA_DIR)

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    # 1. Load existing approved + pending
    approved = load_existing(XLSX_PATH)
    pending = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # 2. Query Gemini with Google Search grounding
    raw = query_gemini_for_gaps(SINCE_DAYS)
    if not raw:
        log.info("Gemini gap-finder: nothing proposed this run.")
        return 0

    # 3. Normalize into Recall objects, filter garbage
    recalls = to_recall_objects(raw)
    if not recalls:
        log.info("Gemini gap-finder: all proposals filtered out (no URL or no pathogen).")
        return 0

    # 4. Append to Pending (dedup handled by append_to_pending)
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    log.info("Gemini gap-finder: added %d new rows to Pending (total pending=%d)",
             added, len(new_pending))

    # 5. Write back — Recalls sheet untouched, only Pending is modified
    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    # 6. Commit + push if anything changed
    if added > 0 and not SKIP_COMMIT:
        msg = f"Gemini gap-finder: +{added} rows to Pending ({scraped_at})"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    elif added == 0:
        log.info("No new rows — skipping commit.")
    else:
        log.info("SKIP_COMMIT set — not pushing.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
