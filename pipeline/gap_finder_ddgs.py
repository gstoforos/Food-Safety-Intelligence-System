"""
DuckDuckGo (DDGS) gap-finder — third deterministic finder alongside Tavily + Exa.

Runs at 02:00 Athens, after the midnight LLM cascade (Gemini-free →
Gemini-paid) and before the 06:00 morning URL gate. Provides a fourth
discovery layer using DuckDuckGo's metasearch (aggregates Bing, Yandex,
Brave's public results, and others) with NO API key, NO credit card,
and NO monthly query cap.

Same regulator-domain whitelist, same deterministic field extraction (no
LLM calls), same Pending-sheet write target — only the search backend
differs.

WHY THIS EXISTS
---------------
Pre-2026-05-09 the late-night cascade tail was paid (Claude + OpenAI +
Perplexity gap-finders). Those rarely added rows beyond what Gemini-free
already produced — expensive insurance for cases that almost never
fired.

Brave Search was originally considered as the replacement, but Brave
killed its free tier in February 2026 — replaced with $5/mo credits and
a credit card billing instrument, which is incompatible with our
"truly free, no surprises" budget target.

DDGS is the right fit:
  - No API key (zero secrets to manage in GitHub Actions)
  - No credit card (zero billing risk)
  - No monthly cap (DuckDuckGo doesn't publish hard limits)
  - Aggregates multiple search backends, including Brave's index

KNOWN RISK: DuckDuckGo rate-limits scraping clients via HTTP 202
responses (NOT 429 — counterintuitive). GitHub Actions runners share
IP pools with other scrapers, so the runner IP is sometimes already
flagged when our job starts. This module handles that gracefully:
  - Catches RatelimitException from ddgs.exceptions
  - Conservative 5s sleep between queries
  - Backend fallback (auto → lite → html)
  - Degrades gracefully — if rate-limited mid-run, returns what we got
    so far rather than crashing the workflow

PACKAGE NOTE
------------
The library was renamed in 2025 from `duckduckgo-search` to `ddgs`. We
use the new name. requirements.txt should pin: ddgs>=9.14

DEDUP STRATEGY
--------------
This module unconditionally runs and submits findings to Pending. The
Pending append uses URL-based dedup (existing logic in
pipeline.merge_master.append_to_pending), so on days Tavily + Exa
covered everything, DDGS results dedupe to ~zero adds. On days those
finders missed a regulator update, DDGS fills the gap.

Cost: $0 (DDGS is free + no LLM calls).

Author: AFTS / G. Stoforos
"""
from __future__ import annotations
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

# DDGS library — renamed from duckduckgo-search. Import lazy-checked so
# a missing dependency surfaces as a clean log line, not a stack trace
# on import.
try:
    from ddgs import DDGS
    from ddgs.exceptions import (
        DDGSException,
        RatelimitException,
        TimeoutException,
    )
    _DDGS_IMPORT_ERROR = None
except ImportError as _e:  # pragma: no cover
    DDGS = None  # type: ignore[assignment]
    DDGSException = Exception  # type: ignore[misc]
    RatelimitException = Exception  # type: ignore[misc]
    TimeoutException = Exception  # type: ignore[misc]
    _DDGS_IMPORT_ERROR = str(_e)

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
log = logging.getLogger("gap-finder-ddgs")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

DDGS_MAX_RESULTS_PER_QUERY = int(os.getenv("DDGS_MAX_RESULTS", "10"))

# Conservative pacing — DuckDuckGo doesn't publish rate limits, but
# community reports converge on 5-10 second inter-query delays as the
# safe zone for shared IP pools (GitHub Actions runners). 5s × 22
# queries = 110s, fits inside the 10-minute workflow timeout.
DDGS_INTER_QUERY_SLEEP_S = float(os.getenv("DDGS_INTER_QUERY_SLEEP_S", "5.0"))

# Backend fallback chain. "auto" tries multiple backends internally; if
# we still get rate-limited we explicitly fall through to "lite" (the
# lightweight HTML endpoint, often the last to be flagged).
DDGS_BACKEND_CHAIN = ["auto", "lite", "html"]

# Map SINCE_DAYS to DDGS's `timelimit` parameter. DDGS only supports
# preset windows (d=day, w=week, m=month, y=year), not arbitrary day
# counts — so we round up to the smallest enclosing preset.
def _ddgs_timelimit(days: int) -> str:
    if days <= 1:
        return "d"
    if days <= 7:
        return "w"
    if days <= 31:
        return "m"
    return "y"


# ---------------------------------------------------------------------------
# DDGS search
# ---------------------------------------------------------------------------
def _ddgs_search(query: str, max_results: int, timelimit: str
                 ) -> List[Dict[str, Any]]:
    """Single DDGS search with backend fallback.

    Returns Tavily-shaped dicts on success, [] on failure. Tries the
    backend chain in order — if "auto" hits a rate limit, falls through
    to "lite" on the next call. Caller is responsible for rate-limit
    bookkeeping at the run level (see _run_ddgs_queries).

    Returns empty list rather than raising — caller continues with
    whatever was collected so far.
    """
    if DDGS is None:
        log.error("ddgs package not installed: %s", _DDGS_IMPORT_ERROR)
        return []

    last_error: str = ""
    for backend in DDGS_BACKEND_CHAIN:
        try:
            with DDGS(timeout=20) as ddgs:
                raw = ddgs.text(
                    query=query,
                    region="wt-wt",            # worldwide
                    safesearch="off",          # don't filter regulator alerts
                    timelimit=timelimit,
                    backend=backend,
                    max_results=max_results,
                )
        except RatelimitException as e:
            last_error = f"ratelimit on backend={backend}: {e}"
            log.warning("DDGS %s — backend %s rate-limited, trying next",
                        query[:60], backend)
            continue
        except TimeoutException as e:
            last_error = f"timeout on backend={backend}: {e}"
            log.warning("DDGS %s — backend %s timed out, trying next",
                        query[:60], backend)
            continue
        except DDGSException as e:
            last_error = f"ddgs error on backend={backend}: {e}"
            log.warning("DDGS %s — backend %s error: %s",
                        query[:60], backend, e)
            continue
        except Exception as e:  # network / json / unexpected
            last_error = f"unexpected error on backend={backend}: {e}"
            log.warning("DDGS %s — backend %s unexpected: %s",
                        query[:60], backend, e)
            continue

        if not raw:
            # No results is valid — empty list, not failure
            return []

        # Normalize DDGS schema → Tavily-shaped dicts.
        # DDGS text() returns: {title, href, body}
        # Tavily uses:         {title, url, content, published_date}
        normalized: List[Dict[str, Any]] = []
        for item in raw:
            url = (item.get("href") or "").strip()
            if not url:
                continue
            normalized.append({
                "title":          (item.get("title") or "").strip(),
                "url":            url,
                "content":        (item.get("body") or "").strip(),
                # DDGS doesn't return a published_date on text results —
                # leave blank, _parse_date() falls back to other signals.
                "published_date": "",
            })
        return normalized

    # Exhausted backend chain
    log.warning("DDGS all backends failed for %r — %s",
                query[:60], last_error)
    return []


def _run_ddgs_queries(since_days: int) -> List[Dict[str, Any]]:
    """Run the canonical gap-finder query set against DDGS.

    Same query list as gap_finder_tavily.py / gap_finder_exa.py for parity.
    NorthAmerica runs first so if rate-limit hits mid-run we still have
    full NA coverage. Inter-query sleep paces calls to stay under the
    (undocumented) DDG rate-limit threshold.

    If a query gets globally rate-limited (all backends fail), we DON'T
    abort the run — we continue with the rest. Some queries may go
    through even when others don't, so partial coverage is better than
    none.
    """
    queries = [
        # ── PRIMARY REGION: NorthAmerica ──
        'site:fda.gov/safety/recalls food recall pathogen',
        'site:fsis.usda.gov/recalls-alerts food recall',
        'site:recalls-rappels.canada.ca food recall',
        'site:inspection.canada.ca food recall',
        # ── Europe ──
        'site:rappel.conso.gouv.fr OR site:rappelconso.gouv.fr rappel alimentaire',
        'site:food.gov.uk food alert recall',
        'site:fsai.ie food recall alert',
        'site:lebensmittelwarnung.de Lebensmittel Rückruf',
        'site:salute.gov.it richiamo alimenti',
        'site:aesan.gob.es alerta alimentaria',
        'site:ages.at Lebensmittel Warnung',
        'site:nvwa.nl voedsel terugroep',
        'site:afsca.be OR site:favv-afsca.be voedsel terugroep rappel',
        'site:livsmedelsverket.se livsmedel återkallelse',
        # ── Asia-Pacific ──
        'site:foodstandards.gov.au food recall',
        'site:mpi.govt.nz food recall',
        'site:mfds.go.kr food recall',
        'site:fssai.gov.in food recall',
        'site:cfs.gov.hk food alert recall',
        # ── EU body ──
        'site:webgate.ec.europa.eu/rasff-window notification',
        # ── Cross-region generic ──
        'food recall salmonella OR listeria OR "e. coli" OR botulism OR campylobacter',
        'food recall mould OR mold OR "foreign material" OR glass OR "ethylene oxide" '
        'OR aflatoxin OR ochratoxin OR alternaria OR mycotoxin',
    ]

    timelimit = _ddgs_timelimit(since_days)
    all_results: List[Dict[str, Any]] = []
    seen_urls: set = set()
    rate_limit_streak = 0
    queries_attempted = 0
    queries_with_results = 0

    for i, q in enumerate(queries):
        # Rate-limit pacing: sleep BEFORE each call after the first.
        if i > 0:
            time.sleep(DDGS_INTER_QUERY_SLEEP_S)

        queries_attempted += 1
        results = _ddgs_search(q, max_results=DDGS_MAX_RESULTS_PER_QUERY,
                               timelimit=timelimit)

        if results:
            queries_with_results += 1
            rate_limit_streak = 0
        else:
            rate_limit_streak += 1

        log.info("  query=%r → %d raw results", q, len(results))

        for item in results:
            url = (item.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            # Filter to whitelisted regulator hosts (same gate as Tavily/Exa)
            if _lookup_source(url) is None:
                continue
            if _is_generic_url(url):
                continue
            seen_urls.add(url)
            all_results.append(item)

        # Circuit breaker: if 5 queries in a row return nothing, the
        # runner IP is probably globally rate-limited. Stop wasting
        # workflow minutes — partial coverage is better than burning
        # 10 min for 0 results.
        if rate_limit_streak >= 5:
            log.warning("DDGS rate-limit circuit breaker tripped after "
                        "5 consecutive empty queries — aborting remaining "
                        "%d queries to save workflow minutes",
                        len(queries) - queries_attempted)
            break

    log.info("DDGS run summary: %d queries attempted, %d returned results, "
             "%d total whitelisted URLs (after dedup)",
             queries_attempted, queries_with_results, len(all_results))
    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("DDGS gap-finder run (Tavily/Exa fallback): %s", scraped_at)

    if DDGS is None:
        log.error("ddgs package not installed: %s — run "
                  "`pip install ddgs>=9.14`", _DDGS_IMPORT_ERROR)
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows",
             len(approved), len(pending))

    # 1. Run DDGS searches, filter to whitelisted regulator domains
    items = _run_ddgs_queries(SINCE_DAYS)
    if not items:
        log.info("DDGS: no regulator-whitelisted results this run "
                 "(possibly rate-limited; will retry tomorrow).")
        return 0

    # 2. Extract structured fields deterministically (reuse Tavily helper —
    #    works because we normalized DDGS response shape to Tavily's keys)
    recalls = results_to_recalls(items, finder_name="DDGS")
    if not recalls:
        log.info("DDGS gap-finder: no rows with detectable pathogens/hazards.")
        return 0

    # 3. Dedup-append to Pending. The append_to_pending helper drops any URL
    #    that's already in approved or pending — so on days Tavily+Exa
    #    covered everything, DDGS overlap dedupes to ~zero adds.
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)

    # ── Gap-finder gating (audit 2026-04-29): see gap_finder_tavily.py ──
    # Tag newly-added rows with Status=pending_gap so url_gate_gemini.py
    # advances them through the v0 → v1 → v2 ladder before claude_check
    # is consulted.
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("DDGS gap-finder: added %d new rows to Pending "
             "(%d tagged Status=pending_gap; "
             "0 = Tavily/Exa already covered them)",
             added, tagged)

    if added == 0:
        log.info("No new findings — Tavily/Exa appear to have full "
                 "coverage. Exiting.")
        return 0

    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    if not SKIP_COMMIT:
        msg = (f"DDGS gap-finder (Tavily/Exa fallback): "
               f"+{added} rows to Pending ({scraped_at})")
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
