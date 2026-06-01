"""
Philippines source — Food and Drug Administration (FDA Philippines).

Philippines regulatory content is published in English, so coverage
should be solid. FDA Philippines publishes food safety advisories at
fda.gov.ph/category/food-information/food-advisory-and-warning/ and
press releases at fda.gov.ph/category/food-information/.

POLICY: regulator-only — Pending entries must come from fda.gov.ph.
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
    "https://www.fda.gov.ph/category/food-information/food-advisory-and-warning/",
    "https://www.fda.gov.ph/category/food-information/",
    "https://www.fda.gov.ph/advisories/",
)
BASE = "https://www.fda.gov.ph"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 25

_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})"
)
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


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
        print(f"  [WARN] FDA-PH fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FDA-PH detail fetch failed: {url} — {e}")
        return "", None
    soup = BeautifulSoup(r.content, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*[\|–—-]\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    return (title_clean + " " + body[:500]).strip(), _parse_date(body)


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
            # FDA-PH advisory permalinks
            if not ("/advisories/" in href or "/food-information/" in href
                    or "/food-advisory-and-warning/" in href):
                continue
            slug = href.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1]
            if not slug or slug in seen or slug in {"food-information",
                    "advisories", "food-advisory-and-warning"}:
                continue
            title = _clean_title(a.get_text(" ", strip=True))
            if not title or len(title) < 12:
                continue
            if title.lower() in {"read more", "view", "see all", "next"}:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, title, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} FDA-PH detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"FDA-PH-{slug}",
            country_code="ph",
            country_name="Philippines",
            authority="FDA Philippines",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


PHILIPPINES = FeedSource(
    code="philippines",
    name_en="Philippines",
    authority_short="FDA Philippines",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Manila",
    run_local_hour=9,
    cron_utc_offsets=(1,),         # PHT UTC+8, no DST → 09:00 = 01:00 UTC
    gnews_authority="Philippines",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide", "aflatoxin",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-PH", gnews_gl="PH", gnews_ceid="PH:en",
    gnews_days_back=3,
    gnews_country_keywords=(),
    gnews_country_domains=(
        "fda.gov.ph",
        "inquirer.net",              # Philippine Daily Inquirer
        "philstar.com",              # The Philippine Star
        "rappler.com",               # Rappler
        "gmanetwork.com",            # GMA News
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(PHILIPPINES)
