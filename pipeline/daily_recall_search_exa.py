"""
Daily Recall Search — Exa fallback for daily_recall_search.py.

Runs 30 minutes after daily_recall_search.py (10:30 Athens). Reads
docs/data/.daily_search_status.json (written by the Tavily run). If
Tavily produced results without quota errors, this script exits early
and consumes ZERO Exa quota. If Tavily flagged rate-limit / auth-error
/ zero-results, this script runs the same per-region query set against
Exa instead.

This is the conditional fallback. The unconditional always-run fallback
is pipeline/gap_finder_exa.py (a separate workflow), which uses URL
dedup at the Pending-append layer to handle redundancy.

Cost: $0 most days (skipped). On Tavily failure days: ~25 Exa queries =
2.5% of Exa's free 1,000/month quota.
"""
from __future__ import annotations
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any, Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending, sort_rows,
    save_xlsx_with_pending, append_to_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

# Reuse helpers from the Tavily daily script — same status-file path,
# same per-region query templates, same Pending-append flow.
from pipeline.daily_recall_search import (  # noqa: E402
    DATA_DIR, XLSX_PATH, STATUS_FILE,
    _REGION_QUERIES,
)

# And reuse the deterministic field extractors from gap_finder_tavily
# (results_to_recalls + _item_to_recall handle pathogen/country/date/etc.)
from pipeline.gap_finder_tavily import (  # noqa: E402
    _lookup_source, _is_generic_url, results_to_recalls,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("daily-recall-search-exa")

EXA_API_KEY = os.getenv("EXA_API_KEY", "").strip()
EXA_ENDPOINT = "https://api.exa.ai/search"
EXA_MAX_RESULTS = int(os.getenv("EXA_MAX_RESULTS", "10"))
EXA_DAYS = int(os.getenv("EXA_DAYS", "3"))

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
FORCE_RUN = os.getenv("EXA_FORCE_RUN", "").lower() in ("1", "true", "yes")

# Status freshness window — if the Tavily status file is older than this,
# we treat it as missing and run defensively.
STALE_HOURS = int(os.getenv("EXA_STALE_HOURS", "6"))


# ---------------------------------------------------------------------------
# Status gate
# ---------------------------------------------------------------------------
def should_run() -> tuple[bool, str]:
    """Inspect the Tavily status file and decide whether to run.

    Returns (run, reason). Reasons are logged for the run summary.
    """
    if FORCE_RUN:
        return True, "EXA_FORCE_RUN=1 set"

    if not STATUS_FILE.exists():
        return True, f"Status file missing ({STATUS_FILE.name}) — running defensively"

    try:
        payload = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        return True, f"Status file unreadable ({e}) — running defensively"

    # Stale check
    ts_str = payload.get("ts", "")
    try:
        ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - ts
        if age > timedelta(hours=STALE_HOURS):
            return True, f"Status file stale ({age.total_seconds()/3600:.1f}h old)"
    except Exception:
        return True, "Status file timestamp unparseable — running defensively"

    if payload.get("should_fallback"):
        return True, ("Primary search flagged should_fallback=true "
                      f"(rate_limited={payload.get('exa_rate_limited', payload.get('tavily_rate_limited'))}, "
                      f"auth_error={payload.get('exa_auth_error', payload.get('tavily_auth_error'))}, "
                      f"recalls={payload.get('recalls_count')})")

    return False, (f"Tavily ran fine (recalls={payload.get('recalls_count')}, "
                   "rate_limited=False) — skipping Exa")


# ---------------------------------------------------------------------------
# Exa search (per-region)
# ---------------------------------------------------------------------------
def _exa_search_one(query: str) -> List[Dict[str, Any]]:
    """Single Exa query → Tavily-shaped result list. [] on any failure.

    Audit 2026-05-14: switched from `{"text": {"maxCharacters": 2000}}`
    to `{"text": True}` so Exa returns the full extracted page text
    rather than a 2K-char snippet. The earlier 2K cap caused the
    deterministic hazard filter (results_to_recalls → finds
    pathogen/biotoxin/mycotoxin/etc keywords in the text) to drop
    items whose hazard mention happened to fall outside the first
    2K characters — typical for press releases that lead with brand
    boilerplate and bury the actual recall reason 3K-8K chars in.
    Symptom: the 2026-05-14 07:32 UTC fallback returned 31 whitelisted
    items → 0 valid recalls, with all 31 dropped on "no detectable
    hazard". With full page text, the filter has the entire body to
    scan instead of just the lead paragraphs.

    Exa's `text` field is the post-HTML extracted body, not raw HTML —
    no parsing on our side needed. Cost is unchanged: Exa charges per
    query, not per content character. Network bytes increase modestly
    (~5-30 KB per result), well within the per-run timeout budget.
    """
    if not EXA_API_KEY:
        return []

    start_date = (datetime.now(timezone.utc).date()
                  - timedelta(days=EXA_DAYS)).isoformat()
    body = {
        "query":              query,
        "type":               "auto",
        "category":           "news",
        "numResults":         EXA_MAX_RESULTS,
        "startPublishedDate": f"{start_date}T00:00:00.000Z",
        # Full page text — see audit note above. Was {"maxCharacters": 2000}.
        "contents":           {"text": True},
    }
    try:
        r = requests.post(
            EXA_ENDPOINT, json=body, timeout=60,
            headers={"x-api-key": EXA_API_KEY,
                     "Content-Type": "application/json"},
        )
        if r.status_code != 200:
            log.warning("Exa %d: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
    except Exception as e:
        log.warning("Exa call failed: %s", e)
        return []

    out: List[Dict[str, Any]] = []
    for item in data.get("results", []) or []:
        text_field = item.get("text") or " ".join(item.get("highlights") or [])
        out.append({
            "title":          item.get("title", "")        or "",
            "url":            item.get("url", "")          or "",
            "content":        text_field,
            "published_date": item.get("publishedDate", "") or "",
        })
    return out


def run_exa_per_region() -> List[Dict[str, Any]]:
    """Run all per-region Exa queries, dedup by URL, filter to whitelisted hosts."""
    seen: set = set()
    items: List[Dict[str, Any]] = []
    total_queries = 0

    for region, queries in _REGION_QUERIES.items():
        log.info("→ Region %s (%d queries)", region, len(queries))
        for q in queries:
            total_queries += 1
            results = _exa_search_one(q)
            for r in results:
                url = (r.get("url") or "").strip()
                if not url or url in seen:
                    continue
                if _lookup_source(url) is None:
                    continue
                if _is_generic_url(url):
                    continue
                seen.add(url)
                items.append(r)

    log.info("Exa per-region sweep: %d queries → %d unique whitelisted items",
             total_queries, len(items))
    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Daily Recall Search — Exa fallback: %s", scraped_at)

    run, reason = should_run()
    log.info("Gate decision: run=%s — %s", run, reason)
    if not run:
        return 0

    if not EXA_API_KEY:
        log.error("EXA_API_KEY not set — cannot run fallback")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # 1. Run Exa per-region sweep
    items = run_exa_per_region()
    if not items:
        log.info("Exa: no whitelisted regulator results.")
        return 0

    # 2. Deterministic extraction (reuse Tavily helpers)
    recalls = results_to_recalls(items, finder_name="Exa")
    if not recalls:
        log.info("Exa: no rows with detectable pathogens/hazards.")
        return 0

    # 3. Dedup-append (URL gate inside append_to_pending handles overlap with
    #    anything Tavily already submitted)
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    # ── Gap-finder gating (audit 2026-04-29): see gap_finder_tavily.py
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("Exa fallback: added %d new rows to Pending "
             "(%d tagged Status=pending_gap)", added, tagged)

    if added == 0:
        log.info("No new findings — already covered. Exiting.")
        return 0

    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    if not SKIP_COMMIT:
        msg = f"Exa daily-recall fallback: +{added} rows to Pending ({scraped_at})"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
