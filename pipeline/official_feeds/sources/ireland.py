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
    r"(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})"
)
# Secondary: "Month DD, YYYY" (US order) as a fallback.
_DATE_RE_US = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})"
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
    if m:
        return datetime(int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)),
                        tzinfo=timezone.utc)
    m = _DATE_RE_US.search(text or "")
    if m:
        return datetime(int(m.group(3)), _MONTHS[m.group(1)], int(m.group(2)),
                        tzinfo=timezone.utc)
    return None


def _clean_title(t: str) -> str:
    return _ALT_PREFIX.sub("", _strip_chrome(t or "")).strip()


def _strip_chrome(s: str) -> str:
    """Remove FSAI nav/breadcrumb chrome that leaks into titles and bodies."""
    if not s:
        return s
    s = re.sub(r"^\s*Home\s+News and Alerts\s+Food Alerts\s+Current:\s*",
               "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*FSAI food alert\.?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*Food Alerts\s*", "", s, flags=re.IGNORECASE)
    return s.strip()


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
    title = _clean_title(_strip_chrome(
        h1.get_text(strip=True) if h1 else
        (soup.title.get_text(strip=True) if soup.title else "")))

    # Main content text (Message + Nature Of Danger live here)
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = _strip_chrome(main.get_text(" ", strip=True) if main else "")
    hazard = text[:800]

    published = _parse_date(text)

    # Company: capture 1-5 capitalised words immediately before "is recalling"
    # (avoids grabbing breadcrumb text the way a greedy .+? would).
    company = ""
    m = re.search(
        r"([A-Z][\w&.''\-]*(?:\s+[A-Z0-9][\w&.''\-]*){0,4})\s+is recalling",
        text)
    if not m:
        m = re.search(r"^([\w&.''\- ]{2,40}?)\s+recalls\b", title)
    if m:
        company = m.group(1).strip()[:80]
        # Drop a leading date that sometimes sits before "is recalling"
        company = re.sub(
            r"^\s*(?:\d{1,2}\s+)?(?:January|February|March|April|May|June|"
            r"July|August|September|October|November|December)\s+\d{4}\s+",
            "", company).strip()
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
        # Listing-level date fallback: FSAI prints the alert date near the
        # anchor. Parse it from the surrounding container so we still have a
        # date if the detail fetch fails or is beyond the detail cap.
        list_date = None
        node = a
        for _ in range(4):          # walk up a few parents looking for a date
            node = getattr(node, "parent", None)
            if node is None:
                break
            list_date = _parse_date(node.get_text(" ", strip=True))
            if list_date:
                break
        links.append((slug, title, url, list_date))

    fetched = 0
    for slug, list_title, url, list_date in links[:limit]:
        d_title = d_hazard = ""
        d_date = None
        d_company = ""
        if fetched < _DETAIL_CAP:
            d_title, d_hazard, d_date, d_company = _fetch_detail(url)
            fetched += 1

        # Date: prefer the detail page, fall back to the listing-level date.
        pub_date = d_date or list_date

        title = d_title or list_title
        # The detail <h1> is often a generic section banner ("Food Alerts").
        # The listing anchor text is the real alert title, so prefer it.
        def _generic(t):
            return (not t) or t.strip().lower() in (
                "food alerts", "news and alerts", "alerts", "news")
        if _generic(d_title) and not _generic(list_title):
            title = list_title
        elif _generic(list_title) and not _generic(d_title):
            title = d_title
        else:
            title = list_title or d_title
        # hazard: prefer detail body (names the pathogen); fall back to title
        hazard = d_hazard or title

        # Date-presence guard: never emit a dateless record. A missing date
        # means both the detail fetch and the listing fallback failed; writing
        # it would put a blank-date row into recalls.xlsx and the alert email.
        if pub_date is None:
            print(f"  [SKIP] FSAI record has no parseable date: {slug}")
            continue

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
            published=pub_date,
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
