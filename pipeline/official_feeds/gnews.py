"""
Google News supplement for the official-feed collector.

Authority websites/RSS have proven flaky (HTML scrapers break, RSS 404s,
bot-blocks). This module adds a Google News pass as insurance: it queries
the authority name + recall/pathogen terms, and — per AFTS spec — names the
last N calendar dates explicitly (today, today-1, today-2) so Google surfaces
the most recent recalls rather than historical ones. A `when:7d` operator is
also appended as a recency belt-and-suspenders.

Returns normalized Record objects (same shape as official API records), so
downstream classify/dedup/xlsx code is uniform.
"""

from __future__ import annotations

import hashlib
import urllib.parse
from datetime import datetime, timedelta, timezone

from .base import Record
from .fetch import get_rss

GN_RSS = "https://news.google.com/rss/search"


def _date_phrases(days_back: int) -> list[str]:
    """['May 28', 'May 27', 'May 26'] for days_back=3 (no leading zero)."""
    today = datetime.now(timezone.utc).date()
    out = []
    for d in range(days_back):
        dt = today - timedelta(days=d)
        # %-d is platform-dependent; build without leading zero manually
        out.append(f"{dt.strftime('%B')} {dt.day}")
    return out


def build_queries(authority: str, pathogen_terms: list[str],
                  days_back: int = 3) -> list[str]:
    """
    Build the Google News query set:
      - date-named queries: "<authority> food recall <Month Day>" for each
        of the last `days_back` days (George's explicit-date approach)
      - evergreen pathogen queries: "<authority> recall <pathogen>" with
        recency handled by when:7d
    """
    queries: list[str] = []

    # Date-named recall queries (today, -1, -2, ...)
    for phrase in _date_phrases(days_back):
        queries.append(f'{authority} food recall {phrase}')
        queries.append(f'{authority} food safety alert {phrase}')

    # Evergreen pathogen/hazard queries (recency via when:7d appended later)
    for term in pathogen_terms:
        queries.append(f'{authority} recall {term}')

    return queries


def fetch_gnews(authority: str, country_code: str, country_name: str,
                authority_short: str, pathogen_terms: list[str],
                hl: str = "en-US", gl: str = "US", ceid: str = "US:en",
                days_back: int = 3, per_query_cap: int = 10) -> list[Record]:
    records: list[Record] = []
    seen_links = set()

    for q in build_queries(authority, pathogen_terms, days_back):
        full_q = f"{q} when:7d"
        url = (f"{GN_RSS}?q={urllib.parse.quote(full_q)}"
               f"&hl={hl}&gl={gl}&ceid={ceid}")
        items = get_rss(url)
        kept = 0
        for it in items:
            link = it.get("link", "")
            title = it.get("title", "")
            if not title or link in seen_links:
                continue
            seen_links.add(link)
            sid = "GN-" + hashlib.sha1(
                (link or title).encode("utf-8", "ignore")).hexdigest()[:12]
            rec = Record(
                source_id=sid,
                country_code=country_code,
                country_name=country_name,
                authority=f"{authority_short} (via Google News)",
                title=title,
                company="",
                product="",
                hazard=title,                 # classify from headline
                alert_type="recall",
                published=it.get("published"),
                url=link,
                raw={"gnews_query": q, "description": it.get("description", "")},
            )
            records.append(rec)
            kept += 1
            if kept >= per_query_cap:
                break
    print(f"  [GNews] {authority}: {len(records)} candidate articles "
          f"across {len(build_queries(authority, pathogen_terms, days_back))} queries")
    return records
