"""
Kenya source — Kenya Bureau of Standards (KEBS).

KEBS is the market-surveillance authority: it can "issue warnings, recall
products, impose fines, revoke permits, or even prosecute", and its remit
explicitly covers food alongside chemicals, electronics and textiles.

TWO HONEST CAVEATS, recorded here so nobody is surprised by the yield:

1. NO MACHINE-READABLE RECALL INDEX.
   Unlike NAFDAC's numbered public alerts, KEBS has no dated recall listing.
   Enforcement is announced through press statements and market-surveillance
   notices, and reaches the public mainly via Kenyan media. kebs.org also
   returns HTTP 403 to datacentre traffic. So this source is GNews-led by
   design, with authority_domain resolution putting the KEBS page in the URL
   whenever one exists.

2. MOST KEBS ACTIONS ARE STANDARDS NON-COMPLIANCE, NOT PATHOGEN EVENTS.
   A typical KEBS action is "12 maize meal brands failing to meet the
   required standard" — a quality/specification failure. Those are correctly
   filtered out downstream by _pathogen_scope and will NOT reach Recalls.

   What this source is really here for is the hazard Kenya genuinely has at
   scale: AFLATOXIN in maize and groundnuts, which IS in scope. The term list
   below is weighted accordingly. Expect a low but meaningful row count —
   this is a targeted source, not a high-volume one.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

BASE = "https://www.kebs.org"
LISTING_URLS = (
    "https://www.kebs.org/media-centre/",
    "https://www.kebs.org/press-releases/",
    "https://www.kebs.org/market-surveillance/",
)

_TIMEOUT = 8
_DETAIL_CAP = 15

_DATE_RE = re.compile(
    r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{4})", re.I)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# Only keep listing links that look like enforcement, not corporate news.
_RELEVANT = re.compile(
    r"(recall|withdraw|substandard|non-?complian|unfit|ban|alert|seiz|"
    r"aflatoxin|contaminat)", re.I)


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
    """Best-effort GET. kebs.org 403s datacentre IPs; that is expected."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_TIMEOUT)
        if r.status_code != 200:
            print(f"  KEBS listing {url} -> HTTP {r.status_code} "
                  f"(expected for datacentre IPs; GNews supplement will run)")
            return ""
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  KEBS listing {url} -> {type(e).__name__} "
              f"(GNews supplement will run)")
        return ""


def _fetch_detail(url: str):
    html = _try_fetch(url)
    if not html:
        return "", None
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return text[:2000], _parse_date(text)


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
            title = " ".join(a.get_text(" ", strip=True).split())
            if not title or len(title) < 12:
                continue
            # KEBS publishes far more corporate news than enforcement; only
            # keep links whose text reads like an enforcement action.
            if not _RELEVANT.search(title) and not _RELEVANT.search(href):
                continue
            slug = href.split("?", 1)[0].rstrip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            links.append((slug, title, urljoin(listing_url, href)))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    if not links:
        print("  KEBS: no links from the official listing "
              "(site blocks runners) — relying on the GNews supplement")
        return records

    fetched = 0
    for slug, title, url_full in links:
        hazard, pub = "", None
        if fetched < _DETAIL_CAP:
            hazard, pub = _fetch_detail(url_full)
            fetched += 1
        records.append(Record(
            source_id=f"KEBS-{slug[:60]}",
            country_code="ke",
            country_name="Kenya",
            authority="KEBS",
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


KENYA = FeedSource(
    code="kenya",
    name_en="Kenya",
    authority_short="KEBS",
    fetcher=fetch,
    region="Africa",
    timezone="Africa/Nairobi",
    run_local_hour=9,
    cron_utc_offsets=(6,),          # EAT UTC+3, no DST -> 09:00 = 06:00 UTC
    gnews_authority="KEBS",
    gnews_terms=(
        # Aflatoxin first — the hazard Kenya actually has at scale, and the
        # one that is unambiguously in scope.
        "aflatoxin", "aflatoxin maize", "mycotoxin",
        "salmonella", "listeria", "E. coli",
        "contaminated", "unfit for human consumption",
        "recall", "substandard food",
    ),
    gnews_hl="en-KE", gnews_gl="KE", gnews_ceid="KE:en",
    gnews_days_back=7,
    gnews_country_keywords=(
        "kenya", "kenyan", "kebs", "nairobi", "mombasa",
    ),
    gnews_country_domains=(
        "kebs.org",
        "nation.africa",        # Daily Nation
        "standardmedia.co.ke",  # The Standard
        "the-star.co.ke",
        "businessdailyafrica.com",
        "citizen.digital",
        "kenyans.co.ke",
        "capitalfm.co.ke",
        "kenyanwallstreet.com",
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "usda", "fsis",
        "walmart", "kroger", "trader joe", "whole foods",
        "u.s.", "united states",
    ),
    gnews_authority_aliases=(
        "Kenya Bureau of Standards",
        "KEBS Kenya",
    ),
    gnews_use_description=True,
    authority_domain="kebs.org",
    authority_url_pattern=r"(press|media|surveillance|recall|alert|news)",
    bulk_index_queries=(
        "site:kebs.org recall 2026",
        "site:kebs.org substandard food 2026",
        "site:kebs.org aflatoxin",
        "site:kebs.org press release food 2026",
    ),
)

register(KENYA)
