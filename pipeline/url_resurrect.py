"""
url_resurrect.py — revive Pending rows with bad URLs.

Problem: some rows get into Pending with a URL that returns 404/5xx — either
the scraper built a URL with a stale template (e.g. the old FDA accessdata
scripts/ires path) or the agency moved the recall page after we scraped it.
Those rows sit in Pending forever with "REJECTED: URL check: HTTP 5xx" in
their Notes field, never getting promoted.

Solution: one-pass resurrection tool that:
  1. Loads Pending rows
  2. Identifies rows with dead URLs (HTTP 404/5xx + REJECTED marker)
  3. For each, asks Gemini 2.5 Flash with Google Search grounding
     "what is the correct URL on <agency site> for the recall described
     by these fields?" — Gemini fires real searches and returns URLs
     that actually appeared in search results.
  4. Probes the proposed URL (HEAD then GET)
  5. Validates the proposed URL against the regulator-domain whitelist
     (rejects news-aggregator URLs even if Gemini proposes one).
  6. If live + on whitelist -> updates row URL, clears the REJECTED
     marker from Notes
  7. If dead -> leaves row in place, logs for manual review
  8. Writes Pending back, commits

Why Gemini and not OpenAI/Claude:
  Earlier versions used OpenAI gpt-4o-mini (no web search) which
  hallucinated plausible-looking URLs that pattern-matched real recall
  pages but didn't actually exist. Gemini 2.5 Flash with native Google
  Search grounding only returns URLs from real search results.

Cost envelope: Gemini 2.5 Flash free tier (1500 req/day per key).
At ~20 rejected rows per week, well within free tier.

Invoke:
    python -m pipeline.url_resurrect              # defaults to 50 rows
    python -m pipeline.url_resurrect --limit 200
    python -m pipeline.url_resurrect --dry-run    # don't write, just print
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    sort_rows, save_xlsx_with_pending,
    STATUS_PENDING, STATUS_REJECTED,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.url_validator import check_url  # noqa: E402
from pipeline.url_gate_gemini import (  # noqa: E402
    _call_gemini_grounded, _collect_gemini_keys, _strip_fences,
)
from pipeline.regulatory_domains import (  # noqa: E402
    is_promotable_to_recalls, is_news_url, is_regulator_url,
)

GEMINI_ENABLED = bool(_collect_gemini_keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("url-resurrect")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Well-known agency site roots — hints for the LLM prompt
# ---------------------------------------------------------------------------
AGENCY_SITE_HINTS: Dict[str, str] = {
    "FDA":                  "fda.gov/safety/recalls-market-withdrawals-safety-alerts OR accessdata.fda.gov",
    "USDA FSIS":            "fsis.usda.gov/recalls",
    "CFIA":                 "recalls-rappels.canada.ca",
    "MAPAQ QC":             "quebec.ca  (search the Quebec recall system)",
    "RappelConso (FR)":     "rappelconso.gouv.fr",
    "FSA (UK)":             "food.gov.uk/news-alerts/alerts",
    "FSS (Scotland)":       "foodstandards.gov.scot/news-and-alerts",
    "FSAI (IE)":            "fsai.ie/news-and-alerts",
    "BVL (DE)":             "lebensmittelwarnung.de OR produktwarnung.eu",
    "AGES (AT)":            "ages.at",
    "AESAN (ES)":           "aesan.gob.es",
    "EFET (GR)":             "efet.gr",
    "Min. Salute (IT)":     "salute.gov.it",
    "SZPI (CZ)":            "szpi.gov.cz",
    "ŠVPS (SK)":            "svps.sk",
    "BLV (CH)":             "blv.admin.ch",
    "RASFF (EU)":           "food.ec.europa.eu OR webgate.ec.europa.eu/rasff-window",
    "FSANZ (AU)":           "foodstandards.gov.au/recalls",
    "MPI (NZ)":             "mpi.govt.nz",
    "CFS (HK)":             "cfs.gov.hk",
    "MFDS (KR)":            "mfds.go.kr",
    "MHLW (JP)":            "mhlw.go.jp",
    "ANVISA (BR)":          "gov.br/anvisa",
    "COFEPRIS (MX)":        "gob.mx/cofepris",
    "SFDA (SA)":            "sfda.gov.sa",
    "FDA (PH)":             "fda.gov.ph",
    "FSSAI (IN)":           "fssai.gov.in",
    "NAFDAC (NG)":          "nafdac.gov.ng",
}


def _site_hint(source: str) -> str:
    """Find the best site-hint string for a given scraper source name."""
    if not source:
        return "the agency's official recall page"
    # Direct match
    if source in AGENCY_SITE_HINTS:
        return AGENCY_SITE_HINTS[source]
    # Partial match (e.g. "FDA  " with trailing space, or "USDA FSIS RSS")
    for key, hint in AGENCY_SITE_HINTS.items():
        if key.split()[0].lower() in source.lower():
            return hint
    return "the agency's official recall page"


# ---------------------------------------------------------------------------
# Identify rows that need resurrection
# ---------------------------------------------------------------------------
def _needs_resurrect(row: Dict[str, Any]) -> bool:
    """Row has a URL, but the URL is flagged dead in Notes OR URL itself is empty."""
    notes = (row.get("Notes") or "").lower()
    url = (row.get("URL") or "").strip()

    # Case 1: Notes contains an explicit REJECTED marker about URLs
    if "rejected" in notes and ("url" in notes or "http" in notes):
        return True
    if "dead url" in notes or "url check" in notes:
        return True

    # Case 2: URL field is entirely empty but row otherwise has data
    if not url:
        if row.get("Company") or row.get("Product"):
            return True

    # Case 3: URL points at a known-dead pattern (stale FDA IRES template)
    if "accessdata.fda.gov/scripts/ires" in url:
        return True

    # Case 4: URL is a generic category/listing page (flagged by URL guardian)
    if "generic" in notes and "needs specific url" in notes:
        return True
    # Also catch generic URLs even if not yet flagged by guardian
    try:
        from review.url_validator import is_generic_url
    except ImportError:
        from url_validator import is_generic_url  # type: ignore
    if url and is_generic_url(url):
        return True

    return False


# ---------------------------------------------------------------------------
# Gemini: propose a correct URL via real Google Search
# ---------------------------------------------------------------------------
RESURRECT_SYSTEM = (
    "You are a food-safety-data operations specialist. Use Google Search to "
    "find the CORRECT, LIVE URL on an official regulator's website for a "
    "specific food recall given its structured fields. Return ONLY strict "
    "JSON. NEVER invent URLs — every URL you return MUST appear in your "
    "Google Search results. Verify the URL is on the agency's official "
    "domain (no news sites, no aggregators like foodsafetynews.com, "
    "produktwarnung.eu, food-safety.com, etc.). If you cannot find a live "
    "regulator URL via search, return an empty url string and strategy "
    "'give_up' — do not guess."
)


RESURRECT_PROMPT = """Use Google Search to find the correct regulator URL for this recall.

Recall fields:
  Source (agency): {source}
  Official agency site: {site_hint}
  Date:           {date}
  Company:        {company}
  Product:        {product}
  Pathogen:       {pathogen}
  Reason:         {reason}
  Country:        {country}
  Notes:          {notes}
  Current (BAD) URL: {bad_url}

Search strategy:
  1. site:<agency_domain> "<company>" <product or pathogen> <year>
  2. site:<agency_domain> <pathogen> <date>
  3. <agency> recall <company> <product> <year>
  4. If still nothing: search news outlets only to extract the agency's
     own reference number, then verify by site:<agency_domain> <ref>.

Hard rules:
  1. The URL MUST be on the agency's official domain. NO news sites,
     NO aggregators (foodsafetynews.com, food-safety.com, produktwarnung.eu,
     en.sedaily.com, capitalfm.co.ke, etc.).
  2. If you cannot cite a specific recall-detail page, return the agency's
     recall SEARCH URL with a query parameter filtered to this recall's
     company / product / recall ID — whichever is most likely to deliver a
     working result.
  3. Do NOT return the bad URL above. Do not return it with trivial changes.
  4. Do NOT invent recall IDs. If the current URL has a wrong-looking ID,
     replace with a search-URL pattern instead.
  5. If your Google Search returns no plausible regulator URL, return
     {{"url": "", "confidence": 0.0, "strategy": "give_up", "reasoning": "..."}}.

Return strict JSON:
{{"url": "https://...", "confidence": 0.0-1.0, "strategy": "specific-page | search | category", "reasoning": "one line why"}}
"""


def propose_url(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ask Gemini (with Google Search grounding) for a replacement URL.
    Returns dict with url/confidence or None.
    """
    if not GEMINI_ENABLED:
        log.warning("GEMINI_API_KEY not set — cannot propose replacement URLs")
        return None

    prompt = RESURRECT_PROMPT.format(
        source=row.get("Source", ""),
        site_hint=_site_hint(row.get("Source", "")),
        date=row.get("Date", ""),
        company=(row.get("Company") or "")[:200],
        product=(row.get("Product") or "")[:300],
        pathogen=row.get("Pathogen", ""),
        reason=(row.get("Reason") or "")[:300],
        country=row.get("Country", ""),
        notes=(row.get("Notes") or "")[:200],
        bad_url=row.get("URL", ""),
    )

    txt = _call_gemini_grounded(prompt, system=RESURRECT_SYSTEM, max_tokens=1024)
    if not txt:
        return None

    txt = _strip_fences(txt).strip()
    try:
        data = json.loads(txt)
    except json.JSONDecodeError as e:
        # Sometimes Gemini wraps a JSON object in extra prose despite
        # instructions; try to extract the first {…} block.
        m = re.search(r"\{[^{}]*\}", txt, re.S)
        if not m:
            log.warning("Gemini URL proposal: JSON parse failed (%s) | %s",
                        e, txt[:200])
            return None
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            log.warning("Gemini URL proposal: JSON parse failed (%s) | %s",
                        e, txt[:200])
            return None

    url = (data.get("url") or "").strip()
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    # Reject Gemini just echoing the bad URL back
    bad = (row.get("URL") or "").strip().lower()
    if bad and url.lower() == bad:
        log.warning("Gemini proposed same URL as bad one — rejecting")
        return None
    # Reject news/aggregator URLs even if Gemini proposes them
    if is_news_url(url):
        log.warning("Gemini proposed news-aggregator URL (%s) — rejecting "
                    "(only regulator domains allowed)", url[:80])
        return None
    if not is_regulator_url(url):
        log.warning("Gemini proposed non-regulator URL (%s) — rejecting",
                    url[:80])
        return None
    return data


# ---------------------------------------------------------------------------
# Verify a proposed URL
# ---------------------------------------------------------------------------
def verify_url(url: str) -> Tuple[bool, str]:
    """Returns (is_live, reason). Uses the same check_url helper the URL gate uses."""
    try:
        result = check_url(url)
    except Exception as e:
        return False, f"check_url error: {e}"

    # check_url returns a tuple or dict depending on version — handle both
    if isinstance(result, tuple):
        status_code, is_ok = result[0], result[1] if len(result) > 1 else None
        if isinstance(is_ok, bool):
            return (is_ok, f"HTTP {status_code}")
        # Fallback: consider 2xx / 3xx OK, 403 on gov domains OK (bot-hostile)
        return ((status_code and 200 <= status_code < 400) or status_code == 403,
                f"HTTP {status_code}")
    if isinstance(result, dict):
        ok = bool(result.get("ok"))
        code = result.get("status_code", "?")
        return ok, f"HTTP {code}"
    # Unknown shape — assume ok if truthy
    return bool(result), "check_url returned unknown shape"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50,
                    help="Max rows to attempt to resurrect this run")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print proposals but do not write xlsx or push")
    args = ap.parse_args()

    if not GEMINI_ENABLED:
        log.error("GEMINI_API_KEY not set — cannot run resurrector.")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # Find candidates
    candidates = [r for r in pending if _needs_resurrect(r)]
    log.info("Found %d Pending rows with dead/rejected URLs", len(candidates))
    if not candidates:
        log.info("Nothing to resurrect. Done.")
        return 0

    # Work within the budget
    to_try = candidates[:args.limit]
    if len(candidates) > args.limit:
        log.info("Capping this run at %d rows (budget); %d will wait for next run",
                 args.limit, len(candidates) - args.limit)

    resurrected = 0
    still_dead = 0
    no_proposal = 0

    for i, row in enumerate(to_try, 1):
        src = row.get("Source", "?")
        company = (row.get("Company") or "")[:50]
        log.info("[%d/%d] %s — %s", i, len(to_try), src, company)

        proposal = propose_url(row)
        if not proposal:
            log.info("    -> no proposal from Gemini")
            no_proposal += 1
            continue

        new_url = proposal["url"]
        confidence = proposal.get("confidence", 0.0)
        strategy = proposal.get("strategy", "?")
        log.info("    -> proposal: %s  (conf=%.2f, %s)",
                 new_url[:80], confidence, strategy)

        is_live, reason = verify_url(new_url)
        if not is_live:
            log.info("    -> URL probe FAILED: %s — leaving row alone", reason)
            still_dead += 1
            continue

        log.info("    -> URL probe PASSED (%s). Updating row.", reason)
        if args.dry_run:
            log.info("    -> DRY RUN — not writing")
        else:
            old_url = row.get("URL", "")
            row["URL"] = new_url
            # Clear REJECTED markers from Notes but keep the audit trail
            old_notes = row.get("Notes", "") or ""
            cleaned_notes = re.sub(
                r"\s*REJECTED:\s*[^|]*(?:\||$)",
                "",
                old_notes,
                flags=re.I,
            ).strip(" |")
            row["Notes"] = (
                cleaned_notes + f"  [resurrected {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: "
                f"{old_url[:40]}... -> OK via Gemini grounded search ({strategy}, conf={confidence:.2f})]"
            ).strip()
            # Reset status so URL gate picks it up again as a fresh candidate
            if "Status" in row:
                row["Status"] = STATUS_PENDING
        resurrected += 1

    log.info("=" * 60)
    log.info("RESURRECTION SUMMARY")
    log.info("  Attempted     : %d", len(to_try))
    log.info("  Resurrected   : %d  (URL fixed, back in Pending for URL gate)", resurrected)
    log.info("  Still dead    : %d  (Gemini proposal also failed probe)", still_dead)
    log.info("  No proposal   : %d  (Gemini couldn't suggest anything)", no_proposal)

    if args.dry_run:
        log.info("Dry run — no writes.")
        return 0

    if resurrected == 0:
        log.info("No rows updated. Skipping write + commit.")
        return 0

    # Write back — pending list same objects (mutated in place), just persist
    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(pending),
    )
    log.info("Saved xlsx with %d resurrected Pending rows", resurrected)

    if not SKIP_COMMIT:
        msg = f"url_resurrect: fixed {resurrected} dead URLs in Pending (Gemini-proposed, grounded)"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
