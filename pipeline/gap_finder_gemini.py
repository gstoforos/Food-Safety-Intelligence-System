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

import requests as _requests

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

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "5"))
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
    last_finish_reason: Optional[str] = None
    # One call — rotate through keys only on hard failure (not output).
    for api_key in keys:
        try:
            client = genai.Client(api_key=api_key)
            
            # Build config with Google Search tool.
            # max_output_tokens=32_000 mirrors the fix in scrapers/_base.py — the
            # default cap (~8K) silently truncates Gemini's grounded JSON
            # mid-string, leaving resp.text empty and producing the misleading
            # "all keys failed. Last error: None" log line.
            config = types.GenerateContentConfig(
                system_instruction=system if system else None,
                tools=[types.Tool(google_search=types.GoogleSearch())],
                max_output_tokens=32_000,
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
            # 200 OK but empty text — capture finish_reason so the caller can
            # log a diagnostic instead of "Last error: None". Common causes:
            # MAX_TOKENS (truncated), SAFETY (filter), RECITATION (filter).
            try:
                if hasattr(resp, 'candidates') and resp.candidates:
                    fr = getattr(resp.candidates[0], 'finish_reason', None)
                    if fr is not None:
                        last_finish_reason = str(fr)
            except Exception:
                pass
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log.debug("Gemini attempt failed (%s): %s",
                      type(exc).__name__, str(exc)[:150])
            continue

    if last_error is None and last_finish_reason:
        log.error("Gemini gap-finder: all keys returned empty text. "
                  "finish_reason=%s (likely truncation, safety filter, or "
                  "recitation block — re-run; if persistent, check the prompt)",
                  last_finish_reason)
    else:
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
# vertexaisearch redirect resolution
# ---------------------------------------------------------------------------
# Gemini's google.genai SDK returns Google grounding-redirect URLs of the form
#   https://vertexaisearch.cloud.google.com/grounding-api-redirect/AbCd1234...
# These are NOT valid recall URLs — they only resolve when followed via HTTP.
# We HEAD-follow them to extract the actual destination, and reject if the
# redirect can't be resolved.

def _resolve_vertexaisearch(url: str) -> str:
    """
    Resolve a vertexaisearch.cloud.google.com redirect to the actual
    regulator URL by following HTTP redirects.

    Returns the resolved URL on success, or '' if the redirect fails or
    the URL is not a vertexaisearch redirect.

    Per spec: if redirect resolution fails, the URL is REJECTED entirely
    (caller treats empty string as "drop this recall").
    """
    if "vertexaisearch.cloud.google.com" not in url:
        return url
    try:
        resp = _requests.head(
            url, allow_redirects=True, timeout=10,
            headers={"User-Agent": "FSIS-Bot/1.0"},
        )
        if resp.status_code < 400 and resp.url and resp.url != url:
            log.info("Resolved vertexaisearch -> %s", resp.url[:100])
            return resp.url
    except Exception as exc:
        log.debug("vertexaisearch resolve failed (%s): %s",
                  type(exc).__name__, str(exc)[:120])
    # Redirect failed — reject by returning empty
    return ""


# ---------------------------------------------------------------------------
# Post-filter: remove garbage from gap-finder output before writing to Pending
# ---------------------------------------------------------------------------

def _post_filter_recalls(recalls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Strip garbage from raw Gemini output BEFORE creating Recall objects:
      • vertexaisearch redirects (resolve them, drop if unresolvable)
      • non-http URLs
      • generic / paginated / disease-info / transparency pages
      • dates before 2026-01-01

    The merge_master.validate_pending_row() gate is still the authoritative
    backstop — this just keeps the logs cleaner and avoids hitting the
    Recall normalizer with garbage.
    """
    clean: List[Dict[str, Any]] = []
    rejected = 0
    for r in recalls:
        url = str(r.get("URL", "") or "").strip()

        # Resolve vertexaisearch redirects (or reject if unresolvable)
        if "vertexaisearch" in url.lower():
            resolved = _resolve_vertexaisearch(url)
            if not resolved:
                log.warning("Rejected unresolvable Gemini redirect: %s", url[:80])
                rejected += 1
                continue
            r["URL"] = resolved
            url = resolved

        # Non-http URL
        if url and not url.lower().startswith(("http://", "https://")):
            log.warning("Rejected non-http URL: %s", url[:80])
            rejected += 1
            continue

        # Generic / listing / disease / transparency pages
        url_low = url.lower()
        bad_substrings = (
            "page=",
            "/list?",
            "/a-z/",
            "animal-disease",
            "regulatory-transparency",
            "/categorie/",
            "/rubrik/",
            "/tag/",
        )
        if any(p in url_low for p in bad_substrings):
            log.warning("Rejected generic URL: %s", url[:80])
            rejected += 1
            continue

        # Date before 2026
        d = str(r.get("Date", "") or "")[:10]
        if d and d < "2026-01-01":
            log.warning("Rejected pre-2026 recall: %s %s", d, url[:60])
            rejected += 1
            continue

        clean.append(r)

    log.info("Post-filter: %d/%d recalls passed (%d rejected)",
             len(clean), len(recalls), rejected)
    return clean


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
    "markdown, no prose, no commentary.\n\n"
    "CRITICAL RULES:\n"
    "1. Only return recalls published within the last 5 days. Older recalls "
    "are out of scope — skip them entirely.\n"
    "2. Every URL must be a DIRECT link to a regulator's website "
    "(fda.gov, fsis.usda.gov, recalls-rappels.canada.ca, "
    "rappel.conso.gouv.fr, food.gov.uk, fsai.ie, etc.). NEVER return Google "
    "redirect URLs (vertexaisearch.cloud.google.com), search-result URLs, "
    "or aggregator URLs.\n"
    "3. The Date field MUST be the date the regulator PUBLISHED the recall, "
    "extracted from the recall page itself. NEVER use today's date as a "
    "placeholder. If you cannot find the publication date, omit the row.\n"
    "4. Do NOT return investigation pages, timeline pages, disease info "
    "pages, paginated listing pages (?page=), category indices, or "
    "transparency pages. Only specific individual recall notices.\n"
    "5. Do NOT return recalls older than 2026. If a recall's URL or content "
    "shows it is from 2025 or earlier, skip it completely.\n"
    "6. Never invent URLs — every URL you return must have appeared "
    "verbatim in a Google Search result you ran."
)


GAP_FINDER_PROMPT = """Using Google Search, find EVERY food recall / public-health alert issued worldwide in the last {since_days} days whose cause is a PATHOGEN, MICROBIAL CONTAMINATION, or BIOLOGICAL TOXIN.

Goal: be COMPREHENSIVE. Return every qualifying recall you can find — downstream URL-gate + review steps will verify and reject anything questionable, so over-delivering is preferred to under-delivering. Deliver everything in clean, structured format and let the next stage decide.

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
- Date      : YYYY-MM-DD — the date the REGULATOR PUBLISHED the ORIGINAL
              recall on their website. Extract this from the recall page
              itself (datestamp, "Published", "Publication date", "Date du
              rappel", "Datum", etc.). NEVER substitute today's date.
              CRITICAL: If the page shows BOTH an original publication date
              AND a later "Update", "Clarification", "Latest update", or
              "Last modified" date, ALWAYS use the ORIGINAL publication
              date — never the update/clarification date. Example: SFA
              recall published 15 March 2026 with a "Clarification" added
              25 April 2026 → Date is 2026-03-15, NOT 2026-04-25.
              If you cannot find the original publication date, OMIT the row.
- Source    : agency short name, e.g. "FDA", "USDA FSIS", "RASFF", "CFIA"
- Company   : firm / producer name as stated on the regulator's page.
              Legitimate values include real company names AND descriptors
              like "Various brands", "Various producers", "Unbranded",
              "sans marque" (RappelConso), "—", or empty when the regulator
              itself doesn't name a single company (multi-producer recalls,
              generic raw products, bulk commodity alerts).
              What is NOT legitimate: page titles or navigation text such
              as "List of", "Food Alerts", "Recall of …", "Listeriosis",
              "Food Safety Investigation:", "Timeline of Events:".
- Brand     : commercial brand name. "—" if not stated; "Various" if many.
- Product   : full product description including size/pack where available.
              The actual product name — NOT the recall reason. Never write
              "due to Salmonella" or "due to Listeria" in this field.
- Pathogen  : specific pathogen, e.g. "Listeria monocytogenes"
- Reason    : short cause description
- Class     : recall class ("Recall", "Alert", "Class I/II/III", "Public Health Alert")
- Country   : English country name, e.g. "USA", "France", "Germany"
- Outbreak  : 1 if illnesses/cases/deaths mentioned, else 0
- URL       : FULL deep-link URL to the SPECIFIC recall detail page on the
              regulator's official domain. MUST be a URL that appeared in
              your Google Search results. NEVER:
                • a Google redirect (vertexaisearch.cloud.google.com/...)
                • a homepage, category, or paginated listing page
                • an invented or guessed URL
- Notes     : distribution area, lot/batch info, illness count, extra context

CRITICAL RULES (non-negotiable — failure on any one means OMIT the row):
1. Window: ONLY recalls published in the last {since_days} days. Skip anything
   older. Never return anything dated before 2026-01-01.
2. URL must be on the regulator's official domain — never a Google redirect,
   never a search-result URL, never an aggregator.
3. Date must be the regulator's publication date extracted from the page,
   NEVER today's date as a placeholder.
4. URL must be specific — a recall detail page, not a category listing,
   not a paginated index (?page=N), not an investigation/disease/timeline page.
5. If you cannot satisfy all of (1)–(4) for a candidate recall, OMIT it.

Beyond those four hard constraints, prefer COMPREHENSIVENESS: include borderline
recalls (unusual brand names, multi-producer alerts, niche regulators) — the
downstream URL gate and review process will verify each one.

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
    log.info("Gemini proposed %d recalls (pre-filter)", len(recalls))
    # Strip garbage before downstream normalization sees it
    recalls = _post_filter_recalls(recalls)
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
