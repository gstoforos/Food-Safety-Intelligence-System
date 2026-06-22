"""
Dated web-search supplement (audit 2026-06-22).

openFDA lags weeks and every free real-time FDA feed is either WAF-blocked or
dead, so this module runs EXPLICIT NUMERIC-DATE web searches — e.g.
"FDA recalls 6/21/2026" for each of the last N days — through Searx (the same
web search the resolver agent uses). Two payoffs:

  • zero-day coverage — surfaces recalls before the official API batches them;
  • free authority URLs — when a result is already on the authority domain
    (fda.gov / fsis.usda.gov / recalls-rappels.canada.ca) the record is stored
    WITH that URL and skips the Stage-3b resolver entirely.

Records are normalized to the same shape as official / Google-News records, so
the EXISTING Tier-1 classifier (Stage 3) decides what is accepted — nothing
here lowers the acceptance bar.

Self-gating: returns [] unless Searx is configured (SEARX_URL set), so only the
North America collector — which wires Searx — runs it.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from .base import Record
from .gnews import _has_recall_signal
from .agents import searx_search


def _numeric_dates(days_back: int):
    """Yield (date_string, date) for the last `days_back` days, in the
    no-leading-zero M/D/YYYY and M/D/YY forms people actually search
    ('6/21/2026', '6/21/26')."""
    today = datetime.now(timezone.utc).date()
    for d in range(days_back):
        dt = today - timedelta(days=d)
        yield f"{dt.month}/{dt.day}/{dt.year}", dt
        yield f"{dt.month}/{dt.day}/{str(dt.year)[2:]}", dt


def fetch_dated_search(authority_short: str, authority_domain: str,
                       country_code: str, country_name: str,
                       region: str = "North America",
                       days_back: int = 3,
                       per_query_cap: int = 8) -> list[Record]:
    if not searx_search.is_configured() or not authority_domain:
        return []

    dom = authority_domain.lower()
    records: list[Record] = []
    seen: set[str] = set()

    queries: list[tuple[str, datetime]] = []
    for ds, dt in _numeric_dates(days_back):
        for tmpl in (f"{authority_short} recalls {ds}",
                     f"{authority_short} food recall {ds}"):
            queries.append((tmpl, dt))

    print(f"  [dated-search] {len(queries)} numeric-date queries "
          f"(last {days_back}d)")
    on_auth = 0
    for q, dt in queries:
        pub = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        # one open web pass + one authority-scoped pass
        results = searx_search.search(q, max_results=per_query_cap)
        results += searx_search.search(
            q, max_results=per_query_cap, include_domains=[authority_domain])
        for r in results:
            url = (r.get("url") or "").strip()
            title = (r.get("title") or "").strip()
            if not url or not title or url in seen:
                continue
            # Same recall-signal gate as Google News: a result must read like a
            # recall/alert, not generic coverage.
            if not (_has_recall_signal(title)
                    or _has_recall_signal(r.get("content", ""))):
                continue
            seen.add(url)
            host = urlparse(url).netloc.lower()
            is_auth = host == dom or host.endswith("." + dom)
            if is_auth:
                on_auth += 1
            h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
            # Authority-domain hits get a NON-"GN-" id so Stage 3b leaves their
            # URL alone; news hits get a "GN-" id so Stage 3b resolves them.
            sid = f"DS-{h}" if is_auth else f"GN-DS-{h}"
            records.append(Record(
                source_id=sid,
                country_code=country_code,
                country_name=country_name,
                authority=authority_short,
                title=title[:160],
                company="",
                product="",
                hazard=title,
                alert_type="recall",
                region=region,
                recall_class="",
                outbreak=0,
                published=pub,
                url=url,
                raw={"dated_query": q, "_dated_search": True,
                     "_on_authority": is_auth},
            ))
    print(f"  [dated-search] {len(records)} candidate rows "
          f"({on_auth} already on {authority_domain})")
    return records
