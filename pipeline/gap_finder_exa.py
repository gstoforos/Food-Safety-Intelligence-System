"""
Exa gap-finder — fallback for the Tavily gap-finder.

Runs 30 minutes after gap_finder_tavily.py (22:30 Athens). Uses Exa's
neural search index instead of Tavily. Same regulator-domain whitelist,
same deterministic field extraction (no LLM calls), same Pending-sheet
write target — only the search backend differs.

WHY THIS EXISTS
---------------
Tavily's free tier is 1,000 searches/month. We use ~270/month with
margin, but if free credits are exhausted (or Tavily is down) the
nightly gap-finder produces nothing. This module is the redundancy
layer — it queries Exa with the same canonical query set so any
recalls Tavily missed still land in Pending.

Exa free tier: 1,000 requests/month. We use ~9 queries × 1 run/day
= ~270/month, well within the free quota.

DEDUP STRATEGY
--------------
This module unconditionally runs and submits findings to Pending. The
Pending append uses URL-based dedup (existing logic in
pipeline.merge_master.append_to_pending), so on days when Tavily ran
fine, Exa's results dedupe to ~zero adds. On days Tavily failed, Exa
fills the gap. No fragile status-file branching.

Cost: $0 (Exa free tier + no LLM calls).
"""
from __future__ import annotations
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

# Reuse all deterministic helpers from the Tavily module — same domain
# whitelist, same field extraction, same recall-object construction.
# Only the search call is replaced.
from pipeline.gap_finder_tavily import (  # noqa: E402
    HOST_TO_SOURCE,
    _lookup_source,
    _detect_pathogen,
    _detect_outbreak,
    _extract_company_product,
    _parse_date,
    _is_generic_url,
    _item_to_recall,
    results_to_recalls,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-exa")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "5"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
EXA_API_KEY = os.getenv("EXA_API_KEY", "").strip()

EXA_ENDPOINT = "https://api.exa.ai/search"
EXA_MAX_RESULTS_PER_QUERY = int(os.getenv("EXA_MAX_RESULTS", "10"))


# ---------------------------------------------------------------------------
# Exa search
# ---------------------------------------------------------------------------
def _exa_search(query: str, max_results: int = 10, days: int = 7
                ) -> List[Dict[str, Any]]:
    """Single Exa search. Returns Tavily-shaped dicts on success, [] on failure.

    Exa's response schema differs from Tavily — we normalize to Tavily's
    {title, url, content, published_date} so the downstream
    _item_to_recall() helper from gap_finder_tavily.py works unchanged.
    """
    if not EXA_API_KEY:
        log.error("EXA_API_KEY not set — skipping search")
        return []

    # startPublishedDate filter — restrict to recent N days.
    start_date = (datetime.now(timezone.utc).date()
                  - timedelta(days=days)).isoformat()

    body = {
        "query":              query,
        "type":               "auto",   # neural + keyword hybrid
        "category":           "news",   # recall announcements are news
        "numResults":         max_results,
        "startPublishedDate": f"{start_date}T00:00:00.000Z",
        "contents": {
            "text": {"maxCharacters": 2000},
        },
    }
    try:
        r = requests.post(
            EXA_ENDPOINT,
            json=body,
            headers={
                "x-api-key":    EXA_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        if r.status_code == 401 or r.status_code == 403:
            log.warning("Exa %d — auth failure for: %s", r.status_code, query)
            return []
        if r.status_code == 429:
            log.warning("Exa 429 — rate limit / quota exceeded for: %s", query)
            return []
        if r.status_code != 200:
            log.warning("Exa %d: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
    except Exception as e:
        log.warning("Exa call failed: %s", e)
        return []

    raw = data.get("results", []) or []

    # Normalize Exa schema → Tavily-shaped dicts
    normalized: List[Dict[str, Any]] = []
    for item in raw:
        # Exa returns `text` (full extracted page text) and `publishedDate`
        # (camelCase). Tavily uses `content` and `published_date`.
        text_field = item.get("text") or ""
        if not text_field:
            # Sometimes Exa puts content in highlights
            highlights = item.get("highlights") or []
            text_field = " ".join(highlights) if highlights else ""

        normalized.append({
            "title":          item.get("title", "") or "",
            "url":            item.get("url", "")   or "",
            "content":        text_field,
            "published_date": item.get("publishedDate", "") or "",
        })
    return normalized


def _run_exa_queries(since_days: int) -> List[Dict[str, Any]]:
    """Run the canonical gap-finder query set against Exa.

    Same query list as gap_finder_tavily.py for parity. NorthAmerica
    runs first so if quota hits mid-run we still have full NA coverage.
    """
    queries = [
        # ── PRIMARY REGION: NorthAmerica ──
        'site:fda.gov/safety/recalls food recall pathogen',
        'site:fsis.usda.gov/recalls-alerts food recall',
        'site:recalls-rappels.canada.ca food recall',
        # ── Europe ──
        'site:rappel.conso.gouv.fr OR site:rappelconso.gouv.fr rappel alimentaire',
        'site:food.gov.uk food alert recall',
        'site:fsai.ie food recall alert',
        # ── Asia-Pacific ──
        'site:foodstandards.gov.au food recall',
        # ── Cross-region generic ──
        'food recall salmonella OR listeria OR "e. coli" OR botulism OR campylobacter',
        'food recall mould OR mold OR "foreign material" OR glass OR "ethylene oxide" '
        'OR aflatoxin OR ochratoxin OR alternaria OR mycotoxin',
    ]

    all_results: List[Dict[str, Any]] = []
    seen_urls: set = set()

    for q in queries:
        results = _exa_search(q, max_results=EXA_MAX_RESULTS_PER_QUERY,
                              days=since_days)
        log.info("  query=%r → %d raw results", q, len(results))
        for item in results:
            url = (item.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            # Filter to whitelisted regulator hosts (same gate as Tavily path)
            if _lookup_source(url) is None:
                continue
            if _is_generic_url(url):
                continue
            seen_urls.add(url)
            all_results.append(item)

    log.info("Exa total whitelisted results: %d (after dedup)", len(all_results))
    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Exa gap-finder run (Tavily fallback): %s", scraped_at)

    if not EXA_API_KEY:
        log.error("EXA_API_KEY not set — cannot run")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # 1. Run Exa searches, filter to whitelisted regulator domains
    items = _run_exa_queries(SINCE_DAYS)
    if not items:
        log.info("Exa: no regulator-whitelisted results this run.")
        return 0

    # 2. Extract structured fields deterministically (reuse Tavily helper —
    #    works because we normalized Exa response shape to Tavily's keys above)
    recalls = results_to_recalls(items)
    if not recalls:
        log.info("Exa gap-finder: no rows with detectable pathogens/hazards.")
        return 0

    # 3. Dedup-append to Pending. The append_to_pending helper drops any URL
    #    that's already in approved or pending — so on days Tavily ran fine,
    #    Exa's overlap dedupes to ~zero adds. On days Tavily failed, Exa
    #    fills the gap.
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    log.info("Exa gap-finder: added %d new rows to Pending "
             "(0 = Tavily already covered them)", added)

    if added == 0:
        log.info("No new findings — Tavily appears to have full coverage. Exiting.")
        return 0

    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    if not SKIP_COMMIT:
        msg = f"Exa gap-finder (Tavily fallback): +{added} rows to Pending ({scraped_at})"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
