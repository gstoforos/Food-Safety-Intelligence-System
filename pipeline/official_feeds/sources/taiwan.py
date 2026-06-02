"""
Taiwan source — Taiwan Food and Drug Administration (TFDA).

TFDA publishes recalls and news in English at fda.gov.tw/eng/. Coverage
is partial — many news items are Chinese-only. The collector does a
best-effort listing scrape and leans on the GNews supplement (limited
to fda.gov.tw URLs only via the regulator-only policy).
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
    "https://www.fda.gov.tw/eng/lawContent.aspx?cid=9",          # recall list (best-effort)
    "https://www.fda.gov.tw/eng/news.aspx",                       # English news index
    "https://consumer.fda.gov.tw/Eng/News.aspx",                  # consumer recalls
)
BASE = "https://www.fda.gov.tw"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

_DATE_RE = re.compile(
    r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"
)


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
        print(f"  [WARN] TFDA fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] TFDA detail fetch failed: {url} — {e}")
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


def fetch(limit: int = 25) -> list[Record]:
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
            # TFDA ASPX news/recall links
            if not (".aspx" in href.lower() and
                    ("news" in href.lower() or "recall" in href.lower()
                     or "lawcontent" in href.lower())):
                continue
            title = _clean_title(a.get_text(" ", strip=True))
            if not title or len(title) < 12:
                continue
            # Require recall/alert keyword in link text — drops generic
            # nav links like "News & Events" and tip articles
            tl = title.lower()
            if not any(k in tl for k in (
                    "recall", "withdraw", "alert", "advisory", "warning",
                    "violat", "contamination", "presence of",
                    "salmonella", "listeria", "e. coli", "e.coli", "stec",
                    "hepatitis", "bacillus", "cereulide", "undeclared",
                    "allergen", "outbreak", "non-compliant", "pesticide",
                    "ractopamine", "aflatoxin")):
                continue
            slug = href.split("?", 1)[-1] or href.split("/")[-1]
            slug = re.sub(r"[^A-Za-z0-9_-]+", "-", slug)[:120]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, title, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} TFDA detail pages "
          f"({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"TFDA-{slug}",
            country_code="tw",
            country_name="Taiwan",
            authority="TFDA",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


TAIWAN = FeedSource(
    code="taiwan",
    name_en="Taiwan",
    authority_short="TFDA",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Taipei",
    run_local_hour=9,
    cron_utc_offsets=(1,),         # CST UTC+8, no DST
    gnews_authority="Taiwan",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en", gnews_gl="TW", gnews_ceid="TW:en",
    gnews_days_back=3,
    gnews_country_keywords=(
        "taiwan",
        "taiwanese",
        "tfda",
        "taipei",
    ),
    gnews_country_domains=(
        "fda.gov.tw",
        "focustaiwan.tw",            # CNA Taiwan English service
        "taipeitimes.com",           # Taipei Times
        "taiwannews.com.tw",         # Taiwan News
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_use_description=True,
    authority_domain="fda.gov.tw",
    authority_url_pattern=r"(News|Tc|Eng)Content\.aspx",
)

register(TAIWAN)
