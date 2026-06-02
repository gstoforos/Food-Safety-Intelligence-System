"""
Singapore source — Singapore Food Agency (SFA).

POLICY (same as AU/NZ): Pending entries must come from the regulator's
own website (sfa.gov.sg). GNews supplement runs as backup; the country-
scope filter only accepts sfa.gov.sg URLs. Title denylist drops US-only
retailer/agency cross-border articles.

Listing: https://www.sfa.gov.sg/food-information/food-recalls
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
    "https://www.sfa.gov.sg/news-publications/newsroom",
    "https://www.sfa.gov.sg/news-publications/newsroom?topic=Food%20Recalls%20and%20Alerts",
)
BASE = "https://www.sfa.gov.sg"

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
        print(f"  [WARN] SFA fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] SFA detail fetch failed: {url} — {e}")
        return "", None
    soup = BeautifulSoup(r.content, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    hazard = re.split(r"\s*\|\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    published = _parse_date(body)
    return (hazard + " " + body[:400]).strip(), published


def fetch(limit: int = 25) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()
    links: list[tuple] = []

    html = None
    listing_used = ""
    for u in LISTING_URLS:
        html = _try_fetch(u)
        if html:
            listing_used = u
            break
    if not html:
        return records

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/newsroom/" not in href and "/news-publications/" not in href \
                and "/articles/" not in href:
            continue
        slug = href.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1]
        # Drop category / nav pages that share the recall keyword set
        # ("Food Alerts & Recalls" link text matches our filter but it's
        # the listing page, not an actual recall)
        if not slug or slug in {
            "food-recalls", "recall-alerts",
            "newsroom", "news-publications",
            "food-alerts-and-recalls", "food-alerts-recalls",
            "alerts-and-recalls", "alerts-recalls",
        } or slug in seen:
            continue
        title = _clean_title(a.get_text(" ", strip=True))
        if not title or len(title) < 12:
            continue
        # Drop generic category/nav titles
        if title.lower() in {
            "read more", "view", "see all", "more", "next",
            "food alerts & recalls", "food alerts and recalls",
            "alerts & recalls", "alerts and recalls",
            "food recalls", "recall alerts", "newsroom",
        }:
            continue
        # Require a recall/alert signal in the link text so we don't grab
        # generic SFA press releases that aren't recalls
        tl = title.lower()
        if not any(k in tl for k in (
                "recall", "withdraw", "alert", "advisory", "warning",
                "do not consume", "do not eat", "contamination", "presence of",
                "salmonella", "listeria", "e. coli", "e.coli", "stec",
                "hepatitis", "bacillus", "cereulide", "undeclared",
                "allergen", "outbreak")):
            continue
        seen.add(slug)
        url_full = urljoin(listing_used, href)
        links.append((slug, title, url_full))
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} SFA detail pages "
          f"({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"SFA-{slug}",
            country_code="sg",
            country_name="Singapore",
            authority="SFA",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug, "listing": listing_used},
        )
        records.append(rec)
    return records


SINGAPORE = FeedSource(
    code="singapore",
    name_en="Singapore",
    authority_short="SFA",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Singapore",
    run_local_hour=9,
    cron_utc_offsets=(1,),         # SGT UTC+8, no DST → 09:00 = 01:00 UTC
    gnews_authority="Singapore",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-SG", gnews_gl="SG", gnews_ceid="SG:en",
    gnews_days_back=3,
    gnews_country_keywords=(
        "singapore",
        "singaporean",
        "sfa",
        "singapore food agency",
    ),
    gnews_country_domains=(
        "sfa.gov.sg",
        "channelnewsasia.com",       # CNA — primary SG English news
        "straitstimes.com",          # Straits Times — SG paper of record
        "todayonline.com",           # Today — SG news
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_use_description=True,
    authority_domain="sfa.gov.sg",
    authority_url_pattern=r"(food-information/recalls|news-publications/recalls)/.+",
)

register(SINGAPORE)
