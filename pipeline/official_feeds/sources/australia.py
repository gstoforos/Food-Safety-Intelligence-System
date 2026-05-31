"""
Australia source — Food Standards Australia New Zealand (FSANZ).

FSANZ has no public JSON/RSS recall feed. The federal recall page lists
items in HTML; we do a best-effort scrape of the listing page and rely on
the Google News supplement to carry the bulk of detections. If the HTML
structure changes or the page returns an error, the official-feed half
quietly returns 0 records and the GNews half still runs (same pattern as
the FSIS module, which has structural 403 issues against GH Actions IPs).

Federal listing: https://www.foodstandards.gov.au/food-recalls
Federal alert subpage: https://www.foodstandards.gov.au/food-recalls/recall-alert

State-level food authority pages (e.g. NSW foodauthority.nsw.gov.au) also
list recalls but FSANZ coordinates the national feed; we stay on the
federal one.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING_URLS = (
    "https://www.foodstandards.gov.au/food-recalls/recall-alert",
    "https://www.foodstandards.gov.au/food-recalls",
)
BASE = "https://www.foodstandards.gov.au"

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
    try:
        return datetime(int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)),
                        tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None


_CHROME = re.compile(
    r"^\s*(?:Home\s*[›>/]\s*)*(?:Food recalls\s*[›>/]\s*)*",
    re.IGNORECASE)


def _clean_title(t: str) -> str:
    if not t:
        return ""
    t = _CHROME.sub("", t).strip()
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t)
    return t.strip(" |·-—")


def _try_fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSANZ fetch failed: {url} — {e}")
        return None


def fetch(limit: int = 60) -> list[Record]:
    records: list[Record] = []

    html = None
    for url in LISTING_URLS:
        html = _try_fetch(url)
        if html:
            listing_url = url
            break
    if not html:
        return records

    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()

    # Recall items typically live under article/li/div containers whose anchors
    # link out to /food-recalls/recall-alert/<slug>. Be loose with selectors —
    # FSANZ has rebuilt this page multiple times. Match by URL fragment.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/food-recalls/" not in href:
            continue
        if href.rstrip("/").endswith("/food-recalls") or \
           href.rstrip("/").endswith("/recall-alert"):
            continue
        slug = href.rstrip("/").split("/")[-1]
        if not slug or slug in seen:
            continue

        title = _clean_title(a.get_text(" ", strip=True))
        if not title or len(title) < 8:
            continue
        # Skip generic navigation anchors
        if title.lower() in {"more", "next", "previous", "see all", "view all"}:
            continue
        seen.add(slug)

        url_full = href if href.startswith("http") else BASE + href

        # Try to enrich from surrounding container text (date + hazard line)
        container = a
        for _ in range(4):
            container = container.parent
            if not container or container.name in {"body", "html"}:
                break
        ctx = container.get_text(" ", strip=True) if container else title
        ctx = re.sub(r"\s+", " ", ctx)[:800]

        published = _parse_date(ctx)
        hazard = ctx

        # Extract company as the first capitalised phrase preceding " - "
        company = ""
        m = re.match(r"^([A-Z][\w &.''\-]{1,60}?)\s*[-–—]", title)
        if m:
            company = m.group(1).strip()

        rec = Record(
            source_id=f"FSANZ-{slug}",
            country_code="au",
            country_name="Australia",
            authority="FSANZ",
            title=title,
            company=company,
            product="",
            hazard=hazard,
            alert_type="recall",
            region="Oceania",
            published=published,
            url=url_full,
            raw={"slug": slug, "listing": listing_url},
        )
        records.append(rec)
        if len(records) >= limit:
            break

    return records


AUSTRALIA = FeedSource(
    code="australia",
    name_en="Australia",
    authority_short="FSANZ",
    fetcher=fetch,
    region="Oceania",
    timezone="Australia/Sydney",
    run_local_hour=9,
    # 09:00 Sydney = 23:00 UTC prev day (AEST/winter, UTC+10)
    #                22:00 UTC prev day (AEDT/summer, UTC+11)
    cron_utc_offsets=(22, 23),
    gnews_authority="FSANZ Australia food",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-AU", gnews_gl="AU", gnews_ceid="AU:en",
    gnews_days_back=3,
)

register(AUSTRALIA)
