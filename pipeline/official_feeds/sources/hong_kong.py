"""
Hong Kong source — Centre for Food Safety (CFS).

Two relevant listings — Food Alerts (consumer-facing recalls) and Trade
Alerts (industry-facing notifications). Both contribute to Pending.

Food Alerts:  https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/
Trade Alerts: https://www.cfs.gov.hk/english/whatsnew/whatsnew_rc/
Press:        https://www.cfs.gov.hk/english/press/

POLICY: regulator-only — Pending entries must come from cfs.gov.hk.
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
    "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html",
    "https://www.cfs.gov.hk/english/whatsnew/whatsnew_rc/whatsnew_rc.html",
)
BASE = "https://www.cfs.gov.hk"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 25

_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
    r"\s+(\d{4})"
)
_MONTHS = {
    "January":1,"February":2,"March":3,"April":4,"May":5,"June":6,
    "July":7,"August":8,"September":9,"October":10,"November":11,"December":12,
    "Jan":1,"Feb":2,"Mar":3,"Apr":4,"Jun":6,"Jul":7,"Aug":8,
    "Sep":9,"Sept":9,"Oct":10,"Nov":11,"Dec":12,
}


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), _MONTHS[m.group(2)],
                        int(m.group(1)), tzinfo=timezone.utc)
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
        print(f"  [WARN] CFS fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CFS detail fetch failed: {url} — {e}")
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
    published = _parse_date(body)
    return (title_clean + " " + body[:400]).strip(), published


def fetch(limit: int = 30) -> list[Record]:
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
            # Accept CFS food-alert pages and press-release pages
            if not ("whatsnew_fa_" in href or "whatsnew_rc_" in href
                    or "/english/press/" in href):
                continue
            if href.endswith(".html") is False and ".html?" not in href:
                continue
            slug = href.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1]
            slug = slug.replace(".html", "")
            if not slug or slug in seen:
                continue
            title = _clean_title(a.get_text(" ", strip=True))
            if not title or len(title) < 12:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, title, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} CFS detail pages "
          f"({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"CFS-{slug}",
            country_code="hk",
            country_name="Hong Kong",
            authority="CFS",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


HONG_KONG = FeedSource(
    code="hong_kong",
    name_en="Hong Kong",
    authority_short="CFS",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Hong_Kong",
    run_local_hour=9,
    cron_utc_offsets=(1,),         # HKT UTC+8, no DST → 09:00 = 01:00 UTC
    gnews_authority="Hong Kong",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-HK", gnews_gl="HK", gnews_ceid="HK:en",
    gnews_days_back=3,
    gnews_country_keywords=(),
    gnews_country_domains=("cfs.gov.hk",),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(HONG_KONG)
