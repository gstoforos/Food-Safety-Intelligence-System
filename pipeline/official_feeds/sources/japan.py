"""
Japan source — Consumer Affairs Agency (CAA) + Ministry of Health,
Labour and Welfare (MHLW).

Japan's consolidated food-recall portal is hosted by the Consumer
Affairs Agency at recall.caa.go.jp. The English entry point has a
search interface; results pages are JS-rendered, so the official feed
half degrades gracefully. MHLW publishes English food-safety alerts at
mhlw.go.jp/english/.

GNews supplement (regulator-only filter on caa.go.jp / mhlw.go.jp)
carries most of the load — similar to the FSIS pattern for the US.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

LISTING_URLS = (
    "https://www.mhlw.go.jp/english/topics/foodsafety/",          # English food-safety topics
    "https://www.caa.go.jp/en/policy/consumer_safety/recall/",    # CAA English recall page
    "https://www.recall.caa.go.jp/result/",                       # consolidated DB
)
BASE = "https://www.mhlw.go.jp"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

_DATE_RE = re.compile(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})")


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)),
                        int(m.group(3)), tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None


def _clean_title(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip(" |·-—")


def _try_fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CAA/MHLW fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CAA/MHLW detail fetch failed: {url} — {e}")
        return "", None
    soup = BeautifulSoup(r.content, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*\|\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    return (title_clean + " " + body[:400]).strip(), _parse_date(body)


def fetch(limit: int = 20) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()
    links: list[tuple] = []

    for listing_url in LISTING_URLS:
        html = _try_fetch(listing_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = _clean_title(a.get_text(" ", strip=True))
            # Looking for English food-safety/recall items
            if not text or len(text) < 12:
                continue
            tl = text.lower()
            if not any(k in tl for k in (
                    "recall", "alert", "advisory", "warning",
                    "food safety", "contamination", "withdraw")):
                continue
            slug = re.sub(r"[^A-Za-z0-9_=&-]+", "-",
                          href.split("?", 1)[0].split("#", 1)[0])[:120]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, text, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} CAA/MHLW detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"CAA-{slug}",
            country_code="jp",
            country_name="Japan",
            authority="CAA / MHLW",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


JAPAN = FeedSource(
    code="japan",
    name_en="Japan",
    authority_short="CAA / MHLW",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Tokyo",
    run_local_hour=9,
    cron_utc_offsets=(0,),         # JST UTC+9, no DST → 09:00 = 00:00 UTC
    gnews_authority="Japan",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en", gnews_gl="JP", gnews_ceid="JP:en",
    gnews_days_back=3,
    gnews_country_keywords=(
        "japan",
        "japanese",
        "tokyo",
        "mhlw",
        "caa",
        "consumer affairs",
    ),
    gnews_country_domains=(
        "caa.go.jp",
        "mhlw.go.jp",
        "recall.caa.go.jp",
        "japantimes.co.jp",          # Japan Times
        "mainichi.jp",               # Mainichi English
        "japannews.yomiuri.co.jp",   # Yomiuri English
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(JAPAN)
