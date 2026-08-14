"""
Nigeria source — National Agency for Food and Drug Administration and
Control (NAFDAC).

NAFDAC publishes numbered "Public Alerts" at

    https://nafdac.gov.ng/public-alert-no-<NN>-<YYYY>-<slug>/

indexed on /recalls-and-alert/ and /news-and-events/, with monthly archives
at /<YYYY>/<MM>/.

WHY THIS SOURCE LEANS ON THE GNEWS SUPPLEMENT
    nafdac.gov.ng returns HTTP 403 to datacentre traffic — a direct fetch from
    a GitHub runner is refused, exactly like FDA Philippines, Korea MFDS,
    Vietnam VFA and Japan CAA. The direct listing fetch below is therefore
    best-effort: when it is blocked it returns nothing and the GNews
    supplement carries the source, with authority_domain/authority_url_pattern
    resolving each article back to the NAFDAC permalink so the stored URL is
    the regulator's own page, not the news article.

SCOPE NOTE
    NAFDAC regulates food AND drugs, cosmetics and medical devices, so its
    alert stream carries many non-food items (counterfeit tablets,
    toothpaste, antiseptic creams). Those are filtered downstream by
    classify_record / _pathogen_scope — this module does not pre-judge them,
    it only declines to invent food-ness that is not there.

    NAFDAC also republishes foreign recalls (e.g. the RappelConso Indomie
    noodles alert, the Danone Aptamil cereulide recall). Those are genuine
    NAFDAC publications about products that may circulate in Nigeria, so they
    are collected; the register's "aggregator" convention applies when the
    underlying notice belongs to another authority.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

BASE = "https://nafdac.gov.ng"
LISTING_URLS = (
    "https://nafdac.gov.ng/category/recall-and-alert/",
    "https://nafdac.gov.ng/news-and-events/",
)

_TIMEOUT = 8
_DETAIL_CAP = 20

# "Public Alert No. 08/2026 – ..." or "Public Alert No.019/2026 - ..."
_ALERT_NO_RE = re.compile(r"public[- ]alert[- ]no\.?\s*(\d{1,3})\s*[/-]\s*(\d{4})",
                          re.I)
_DATE_RE = re.compile(
    r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{4})", re.I)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), _MONTHS[m.group(2).lower()],
                        int(m.group(1)), tzinfo=timezone.utc)
    except (ValueError, KeyError):
        return None


def _try_fetch(url: str) -> str:
    """Best-effort GET. nafdac.gov.ng 403s datacentre IPs; that is expected
    and must not raise — the GNews supplement is the real path."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_TIMEOUT)
        if r.status_code != 200:
            print(f"  NAFDAC listing {url} -> HTTP {r.status_code} "
                  f"(expected for datacentre IPs; GNews supplement will run)")
            return ""
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  NAFDAC listing {url} -> {type(e).__name__} "
              f"(GNews supplement will run)")
        return ""


def _alert_id(href: str, title: str) -> str:
    m = _ALERT_NO_RE.search(title) or _ALERT_NO_RE.search(href)
    if m:
        return f"NAFDAC-{m.group(2)}-{int(m.group(1)):03d}"
    slug = href.split("?", 1)[0].rstrip("/").split("/")[-1]
    return f"NAFDAC-{slug[:60]}"


def _fetch_detail(url: str):
    html = _try_fetch(url)
    if not html:
        return "", None
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return text[:2000], _parse_date(text)


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
            if "public-alert" not in href.lower() and "recall" not in href.lower():
                continue
            slug = href.split("?", 1)[0].rstrip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            if slug in {"recall-and-alert", "news-and-events", "category"}:
                continue
            title = " ".join(a.get_text(" ", strip=True).split())
            if not title or len(title) < 12:
                continue
            if title.lower() in {"read more", "view", "see all", "next"}:
                continue
            seen.add(slug)
            links.append((slug, title, urljoin(listing_url, href)))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    if not links:
        print("  NAFDAC: no links from the official listing "
              "(site blocks runners) — relying on the GNews supplement")
        return records

    fetched = 0
    for slug, title, url_full in links:
        hazard, pub = "", None
        if fetched < _DETAIL_CAP:
            hazard, pub = _fetch_detail(url_full)
            fetched += 1
        records.append(Record(
            source_id=_alert_id(url_full, title),
            country_code="ng",
            country_name="Nigeria",
            authority="NAFDAC",
            title=title,
            company="", product="",
            hazard=hazard or title,
            alert_type="recall",
            region="Africa",
            published=pub,
            url=url_full,
            raw={"slug": slug},
        ))
    return records


NIGERIA = FeedSource(
    code="nigeria",
    name_en="Nigeria",
    authority_short="NAFDAC",
    fetcher=fetch,
    region="Africa",
    timezone="Africa/Lagos",
    run_local_hour=9,
    cron_utc_offsets=(8,),          # WAT UTC+1, no DST -> 09:00 = 08:00 UTC
    gnews_authority="NAFDAC",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "aflatoxin", "mycotoxin",          # major hazard in Nigerian grain/nuts
        "undeclared allergen", "recall", "public alert", "outbreak",
    ),
    gnews_hl="en-NG", gnews_gl="NG", gnews_ceid="NG:en",
    gnews_days_back=7,
    gnews_country_keywords=(
        "nigeria", "nigerian", "nafdac", "lagos", "abuja",
    ),
    gnews_country_domains=(
        "nafdac.gov.ng",
        "punchng.com",          # The Punch
        "businessday.ng",       # BusinessDay
        "vanguardngr.com",      # Vanguard
        "thenationonlineng.net",
        "premiumtimesng.com",
        "guardian.ng",
        "legit.ng",
        "channelstv.com",
        "dailypost.ng",
    ),
    gnews_block_title_keywords=(
        # NAFDAC alerts are frequently ABOUT a foreign recall; those are kept.
        # What must be dropped is a foreign recall with no Nigerian angle.
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
    ),
    gnews_authority_aliases=(
        "NAFDAC Nigeria",
        "National Agency for Food and Drug Administration and Control",
    ),
    gnews_use_description=True,
    authority_domain="nafdac.gov.ng",
    authority_url_pattern=r"(public-alert|recall|alert)",
    bulk_index_queries=(
        "site:nafdac.gov.ng public alert 2026",
        "site:nafdac.gov.ng recall 2026",
        "site:nafdac.gov.ng salmonella OR listeria OR aflatoxin 2026",
        "site:nafdac.gov.ng food alert 2026",
    ),
)

register(NIGERIA)
