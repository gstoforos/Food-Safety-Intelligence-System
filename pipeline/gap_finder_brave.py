"""
Brave Search gap-finder — third deterministic finder alongside Tavily + Exa.

Runs at 02:00 Athens, after the midnight LLM cascade (Gemini-free →
Gemini-paid) and before the 06:00 morning URL gate. Provides a fourth
discovery layer with an independent search index — Brave is not a
Google/Bing wrapper, so it surfaces sources the others miss, particularly
recently-published regulator pages and small regional outlets.

Same regulator-domain whitelist, same deterministic field extraction (no
LLM calls), same Pending-sheet write target — only the search backend
differs.

WHY THIS EXISTS
---------------
Pre-2026-05-09 the late-night cascade tail was paid (Claude + OpenAI +
Perplexity gap-finders). Those rarely added any rows beyond what
Gemini-free already produced — they were expensive insurance for cases
that almost never fired. Replacing them with a free deterministic
finder keeps the redundancy without the bill.

Brave free tier: 2,000 queries/month, indefinite. Our usage is
~22 queries × 1 run/day = ~660/month — comfortably within the quota
with 3× headroom.

Brave's free tier rate-limits to 1 query/second, so we sleep 1.1s
between calls. Total runtime ~30s for the canonical query set.

DEDUP STRATEGY
--------------
This module unconditionally runs and submits findings to Pending. The
Pending append uses URL-based dedup (existing logic in
pipeline.merge_master.append_to_pending), so on days Tavily + Exa
covered everything, Brave's results dedupe to ~zero adds. On days
those finders missed a regulator update or had quota issues, Brave
fills the gap.

Cost: $0 (Brave free tier + no LLM calls).

Author: AFTS / G. Stoforos
"""
from __future__ import annotations
import os
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

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
log = logging.getLogger("gap-finder-brave")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY", "").strip()

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_MAX_RESULTS_PER_QUERY = int(os.getenv("BRAVE_MAX_RESULTS", "10"))

# Brave free tier: 1 query/second. Sleep slightly more than 1s between
# calls to stay safely under the limit (HTTP 429 otherwise).
BRAVE_RATE_LIMIT_SLEEP_S = 1.1

# Map SINCE_DAYS to Brave's `freshness` parameter. Brave only supports
# preset windows (pd=past day, pw=past week, pm=past month, py=past year),
# not arbitrary day counts — so we round up to the smallest enclosing
# preset. Anything > 31 days is "py" (past year, the broadest preset).
def _brave_freshness(days: int) -> str:
    if days <= 1:
        return "pd"
    if days <= 7:
        return "pw"
    if days <= 31:
        return "pm"
    return "py"


# Brave's `description` field contains HTML strong-tags around matched
# terms (e.g. "Salmonella found in <strong>Brand X</strong>"). Strip
# these before passing to the Tavily-shaped normaliser, which expects
# plain text.
_HTML_TAG_RX = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    # Remove tags, collapse whitespace
    plain = _HTML_TAG_RX.sub("", text)
    return re.sub(r"\s+", " ", plain).strip()


# ---------------------------------------------------------------------------
# Brave search
# ---------------------------------------------------------------------------
def _brave_search(query: str, max_results: int = 10, days: int = 7
                  ) -> List[Dict[str, Any]]:
    """Single Brave search. Returns Tavily-shaped dicts on success, [] on failure.

    Brave's response schema differs from Tavily — we normalize to Tavily's
    {title, url, content, published_date} so the downstream
    _item_to_recall() helper from gap_finder_tavily.py works unchanged.
    """
    if not BRAVE_API_KEY:
        log.error("BRAVE_API_KEY not set — skipping search")
        return []

    params = {
        "q":          query,
        "count":      min(max(max_results, 1), 20),  # Brave caps at 20
        "freshness":  _brave_freshness(days),
        "safesearch": "off",         # don't filter regulator alerts
        "spellcheck": 0,             # don't auto-correct site: queries
        "extra_snippets": 1,         # request more text per result
    }
    headers = {
        "Accept":                  "application/json",
        "Accept-Encoding":         "gzip",
        "X-Subscription-Token":    BRAVE_API_KEY,
    }

    try:
        r = requests.get(BRAVE_ENDPOINT, params=params, headers=headers,
                         timeout=30)
        if r.status_code in (401, 403):
            log.warning("Brave %d — auth failure for: %s", r.status_code, query)
            return []
        if r.status_code == 422:
            log.warning("Brave 422 — bad query for: %s", query)
            return []
        if r.status_code == 429:
            log.warning("Brave 429 — rate limit / quota exceeded for: %s",
                        query)
            return []
        if r.status_code != 200:
            log.warning("Brave %d: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
    except Exception as e:
        log.warning("Brave call failed: %s", e)
        return []

    web = data.get("web") or {}
    raw = web.get("results") or []

    # Normalize Brave schema → Tavily-shaped dicts
    normalized: List[Dict[str, Any]] = []
    for item in raw:
        title = _strip_html(item.get("title") or "")
        url = (item.get("url") or "").strip()
        if not url:
            continue

        # Brave puts content in `description` (with HTML tags) and
        # sometimes in `extra_snippets` (list of plain strings).
        desc = _strip_html(item.get("description") or "")
        extras = item.get("extra_snippets") or []
        if isinstance(extras, list) and extras:
            extras_text = " ".join(_strip_html(s) for s in extras if s)
            content = (desc + " " + extras_text).strip()
        else:
            content = desc

        # Brave's `page_age` is ISO 8601 when present (e.g.
        # "2026-05-07T14:23:00"). Sometimes it's missing — leave blank
        # and let _parse_date() fall back to other signals.
        published = (item.get("page_age") or "").strip()

        normalized.append({
            "title":          title,
            "url":            url,
            "content":        content,
            "published_date": published,
        })
    return normalized


def _run_brave_queries(since_days: int) -> List[Dict[str, Any]]:
    """Run the canonical gap-finder query set against Brave.

    Same query list as gap_finder_tavily.py / gap_finder_exa.py for parity.
    NorthAmerica runs first so if quota hits mid-run we still have full
    NA coverage.
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

    all_results: List[Dict[str, Any]] = []
    seen_urls: set = set()

    for i, q in enumerate(queries):
        # Rate-limit pacing: sleep BEFORE each call after the first so
        # we never exceed Brave's 1 req/sec free-tier ceiling.
        if i > 0:
            time.sleep(BRAVE_RATE_LIMIT_SLEEP_S)

        results = _brave_search(q, max_results=BRAVE_MAX_RESULTS_PER_QUERY,
                                days=since_days)
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

    log.info("Brave total whitelisted results: %d (after dedup)",
             len(all_results))
    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Brave gap-finder run (Tavily/Exa fallback): %s", scraped_at)

    if not BRAVE_API_KEY:
        log.error("BRAVE_API_KEY not set — cannot run")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows",
             len(approved), len(pending))

    # 1. Run Brave searches, filter to whitelisted regulator domains
    items = _run_brave_queries(SINCE_DAYS)
    if not items:
        log.info("Brave: no regulator-whitelisted results this run.")
        return 0

    # 2. Extract structured fields deterministically (reuse Tavily helper —
    #    works because we normalized Brave response shape to Tavily's keys)
    recalls = results_to_recalls(items, finder_name="Brave")
    if not recalls:
        log.info("Brave gap-finder: no rows with detectable pathogens/hazards.")
        return 0

    # 3. Dedup-append to Pending. The append_to_pending helper drops any URL
    #    that's already in approved or pending — so on days Tavily+Exa
    #    covered everything, Brave's overlap dedupes to ~zero adds.
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
    # is ever consulted.
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("Brave gap-finder: added %d new rows to Pending "
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
        msg = (f"Brave gap-finder (Tavily/Exa fallback): "
               f"+{added} rows to Pending ({scraped_at})")
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
