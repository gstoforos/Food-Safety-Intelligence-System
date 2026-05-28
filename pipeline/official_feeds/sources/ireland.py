"""
Ireland source — Food Safety Authority of Ireland (FSAI).

FSAI has no public RSS/JSON feed, so we scrape the food-alerts listing AND
follow each alert link to its detail page. The LISTING titles do NOT name
the hazard (e.g. "Recall of various Manor Farm chicken products"), but the
DETAIL page states it explicitly ("Message: ... due to the possible presence
of Salmonella. Nature Of Danger: ..."). So we must enrich from the detail
page to classify correctly.

Listing: https://www.fsai.ie/news-and-alerts/food-alerts
Detail:  https://www.fsai.ie/news-and-alerts/food-alerts/<slug>
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

# Cap detail fetches to keep the run bounded
_DETAIL_CAP = 25

_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})"
)
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# Strip image alt-text prefixes the listing sometimes prepends to anchors
_ALT_PREFIX = re.compile(
    r"^(?:Image of|Picture of|Pictures of|A bag of|A packet of|Packets of|"
    r"Example of|Photo of)\b.*?(?=Recall|Withdrawal|Allergen|Alert)",
    re.IGNORECASE)


def _parse_date(text: str):
    m = _DATE_RE.search(text or "")
    if not m:
        return None
    return datetime(int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)),
                    tzinfo=timezone.utc)


def _clean_title(t: str) -> str:
    return _ALT_PREFIX.sub("", t or "").strip()


def _fetch_detail(url: str):
    """Return (title, hazard_text, published, company) from a detail page."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSAI detail fetch failed: {url} — {e}")
        return None, "", None, ""

    soup = BeautifulSoup(r.content, "html.parser")
    # Title from <h1>, fallback <title>
    h1 = soup.find("h1")
    title = _clean_title(h1.get_text(strip=True) if h1 else
                         (soup.title.get_text(strip=True) if soup.title else ""))

    # Main content text (Message + Nature Of Danger live here)
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = main.get_text(" ", strip=True) if main else ""
    # Trim to the part that carries the hazard (first ~800 chars is plenty)
    hazard = text[:800]

    published = _parse_date(text)

    company = ""
    m = re.search(r"\b(.+?)\s+is recalling\b", text, re.IGNORECASE)
    if not m:
        m = re.search(r"^(.+?)\s+recalls\b", title, re.IGNORECASE)
    if m:
        company = m.group(1).strip()[:80]
    return title, hazard, published, company


def fetch(limit: int = 50) -> list[Record]:
    records: list[Record] = []
    try:
        r = requests.get(LISTING, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSAI listing fetch failed: {e}")
        return records

    soup = BeautifulSoup(r.content, "html.parser")

    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news-and-alerts/food-alerts/" not in href:
            continue
        slug = href.rstrip("/").split("/")[-1]
        if not slug or slug == "food-alerts" or slug in seen:
            continue
        title = _clean_title(a.get_text(strip=True))
        if not title or len(title) < 12:
            continue
        seen.add(slug)
        url = href if href.startswith("http") else BASE + href
        links.append((slug, title, url))

    fetched = 0
    for slug, list_title, url in links[:limit]:
        d_title = d_hazard = ""
        d_date = None
        d_company = ""
        if fetched < _DETAIL_CAP:
            d_title, d_hazard, d_date, d_company = _fetch_detail(url)
            fetched += 1

        title = d_title or list_title
        # hazard: prefer detail body (names the pathogen); fall back to title
        hazard = d_hazard or title

        rec = Record(
            source_id=f"FSAI-{slug}",
            country_code="ie",
            country_name="Ireland",
            authority="FSAI",
            title=title,
            company=d_company,
            product="",
            hazard=hazard,
            alert_type="recall" if "recall" in title.lower() else "allergy",
            published=d_date,
            url=url,
            raw={"slug": slug, "list_title": list_title},
        )
        records.append(rec)
    return records


IRELAND = FeedSource(
    code="ireland",
    name_en="Ireland",
    authority_short="FSAI",
    fetcher=fetch,
    region="Europe",
    timezone="Europe/Dublin",
    run_local_hour=9,
    cron_utc_offsets=(8, 9),
    gnews_authority="FSAI Food Safety Authority Ireland",
    gnews_terms=("salmonella", "listeria", "E. coli", "cereulide",
                 "undeclared allergen"),
    gnews_hl="en-IE", gnews_gl="IE", gnews_ceid="IE:en",
    gnews_days_back=3,
)

register(IRELAND)
