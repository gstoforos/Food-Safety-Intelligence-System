"""
Ireland source — Food Safety Authority of Ireland (FSAI).

FSAI has no public RSS/JSON feed (email/SMS subscriptions only), so we
scrape the structured food-alerts listing page. FSAI helpfully states the
hazard directly in each alert title, e.g.:

  "Recall of various Manor Farm chicken products ... due to the possible
   presence of Salmonella"

so the title alone is enough to classify tier. We also fetch the listing
page's article links for source_id + url.

Listing: https://www.fsai.ie/news-and-alerts/food-alerts
Each alert URL: https://www.fsai.ie/news-and-alerts/food-alerts/<slug>
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING = "https://www.fsai.ie/news-and-alerts/food-alerts"
BASE = "https://www.fsai.ie"

_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})"
)
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def _parse_date(text: str):
    m = _DATE_RE.search(text or "")
    if not m:
        return None
    day, month, year = int(m.group(1)), _MONTHS[m.group(2)], int(m.group(3))
    return datetime(year, month, day, tzinfo=timezone.utc)


def fetch(limit: int = 50) -> list[Record]:
    records: list[Record] = []
    try:
        r = requests.get(LISTING, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSAI listing fetch failed: {e}")
        return records

    soup = BeautifulSoup(r.content, "html.parser")

    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news-and-alerts/food-alerts/" not in href:
            continue
        slug = href.rstrip("/").split("/")[-1]
        if not slug or slug == "food-alerts" or slug in seen:
            continue
        title = a.get_text(strip=True)
        # Skip nav/category links with no real title
        if not title or len(title) < 15:
            continue
        seen.add(slug)

        url = href if href.startswith("http") else BASE + href
        # Date often in a sibling/parent; fall back to None (kept by collector)
        date_text = ""
        parent = a.find_parent()
        if parent:
            date_text = parent.get_text(" ", strip=True)
        published = _parse_date(date_text)

        rec = Record(
            source_id=f"FSAI-{slug}",
            country_code="ie",
            country_name="Ireland",
            authority="FSAI",
            title=title,
            company="",          # parsed by LLM-free heuristic below
            product="",
            hazard=title,        # FSAI states hazard in the title
            alert_type="recall" if "recall" in title.lower() else "allergy",
            published=published,
            url=url,
            raw={"slug": slug, "date_text": date_text},
        )
        # Heuristic company extraction: text before " recalls" / "is recalling"
        m = re.search(r"^(?:Recall of .*?\bby\b\s+)?(.+?)\s+(?:recalls|is recalling)",
                      title, re.IGNORECASE)
        if m:
            rec.company = m.group(1).strip()
        records.append(rec)
        if len(records) >= limit:
            break
    return records


IRELAND = FeedSource(
    code="ireland",
    name_en="Ireland",
    authority_short="FSAI",
    fetcher=fetch,
    region="Europe",
    timezone="Europe/Dublin",
    run_local_hour=9,
    cron_utc_offsets=(8, 9),  # 09:00 Dublin = 08:00 UTC (IST) / 09:00 UTC (GMT)
    gnews_authority="FSAI Food Safety Authority Ireland",
    gnews_terms=("salmonella", "listeria", "E. coli", "cereulide",
                 "undeclared allergen"),
    gnews_hl="en-IE", gnews_gl="IE", gnews_ceid="IE:en",
    gnews_days_back=3,
)

register(IRELAND)
