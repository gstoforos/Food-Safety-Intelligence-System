"""
Scotland source — Food Standards Scotland (FSS).

Per AFTS spec, Scotland is tracked SEPARATELY from the rest of the UK.
FSS recalls are published through the same FSA Food Alerts API, tagged with
country=GB-SCT. We reuse the UK fetcher with include_scotland=True so the
records come out attributed to FSS / Scotland.
"""

from __future__ import annotations

from ..base import Record, FeedSource, register
from . import uk as _uk


def fetch(limit: int = 50) -> list[Record]:
    return _uk.fetch(limit=limit, include_scotland=True)


SCOTLAND = FeedSource(
    code="scotland",
    name_en="Scotland",
    authority_short="FSS",
    fetcher=fetch,
    region="Europe",
    timezone="Europe/London",
    run_local_hour=9,
    cron_utc_offsets=(8, 9),
    gnews_authority="Food Standards Scotland",
    gnews_terms=("salmonella", "listeria", "E. coli", "botulism",
                 "undeclared allergen"),
    gnews_hl="en-GB", gnews_gl="GB", gnews_ceid="GB:en",
    gnews_days_back=3,
)

register(SCOTLAND)
