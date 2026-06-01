"""
New Zealand source — Ministry for Primary Industries / New Zealand Food
Safety (MPI/NZFS).

POLICY (v5+): Pending entries for NZ must only come from mpi.govt.nz,
same as the EFET / AESAN country collectors in the EU pipeline.

POLICY (v6+): Detail-page enrichment. The MPI listing shows product
names only ("Emborg Emmentaler Cheese", "Nestlé Alfamino Infant
Formula") with no hazard wording, so listing-only classification
returns reject/unknown for real in-scope Listeria / cereulide recalls.
We fetch each detail page (up to _DETAIL_CAP) and use its body text as
the hazard field so the classifier can find the regulator's own
pathogen wording (e.g. "due to the possible presence of Listeria
monocytogenes").

Recalled food list: https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING_URL = (
    "https://www.mpi.govt.nz/food-safety-home/"
    "food-recalls-and-complaints/recalled-food-products/"
)
BASE = "https://www.mpi.govt.nz"

_RECALL_PATH_PREFIX = "/food-recalls-and-complaints/recalled-food-products/"

_DETAIL_CAP = 40

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
    t = re.sub(r"\s+", " ", t).strip()
    return t.strip(" |·-—")


def _try_fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] MPI/NZFS fetch failed: {url} — {e}")
        return None


def _is_recall_url(href: str) -> bool:
    if not href:
        return False
    path = href.split("?", 1)[0].split("#", 1)[0]
    if _RECALL_PATH_PREFIX not in path:
        return False
    tail = path.split(_RECALL_PATH_PREFIX, 1)[1].strip("/")
    if not tail:
        return False
    if tail.startswith("page-") or tail.isdigit():
        return False
    return True


def _fetch_detail(url: str):
    """Return (hazard_text, published, company) from an MPI detail page."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] MPI detail fetch failed: {url} — {e}")
        return "", None, ""

    soup = BeautifulSoup(r.content, "html.parser")
    main = (soup.find("main")
            or soup.find("article")
            or soup.find(class_=re.compile(r"(content|main|body)", re.I))
            or soup.body
            or soup)
    text = main.get_text(" ", strip=True) if main else ""
    text = re.sub(r"\s+", " ", text)
    hazard = text[:1000]

    published = _parse_date(text)

    # MPI detail pages typically open with "New Zealand Food Safety is
    # supporting <Company> in its recall of …" — extract company.
    company = ""
    m = re.search(
        r"(?:supporting|supports)\s+([A-Z][\w &.''\-]*"
        r"(?:\s+[A-Z0-9][\w &.''\-]*){0,5}?)\s+(?:in|with)\b",
        text)
    if not m:
        m = re.search(
            r"([A-Z][\w &.''\-]*(?:\s+[A-Z0-9][\w &.''\-]*){0,5})"
            r"\s+is\s+recalling",
            text)
    if m:
        company = m.group(1).strip()[:80]

    return hazard, published, company


def fetch(limit: int = 60) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()
    links: list[tuple] = []

    html = _try_fetch(LISTING_URL)
    if not html:
        return records

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _is_recall_url(href):
            continue
        slug = href.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1]
        if not slug or slug in seen:
            continue
        title = _clean_title(a.get_text(" ", strip=True))
        if not title or len(title) < 8:
            continue
        if title.lower() in {"read more", "view", "see all", "more",
                             "next", "previous", "recalled food products"}:
            continue
        seen.add(slug)
        url_full = href if href.startswith("http") else BASE + href
        links.append((slug, title, url_full))
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} MPI detail pages…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        d_company = ""
        if fetched < _DETAIL_CAP:
            d_hazard, d_date, d_company = _fetch_detail(url_full)
            fetched += 1

        hazard = d_hazard or list_title

        # Company: prefer detail-page; fall back to title prefix
        company = d_company
        if not company:
            m = re.match(r"^([A-Z][\w &.''\-]{1,40}?)\s+brand\b", list_title)
            if not m:
                m = re.match(r"^([A-Z][\w &.''\-]{1,40})\s+", list_title)
            if m:
                company = m.group(1).strip()

        rec = Record(
            source_id=f"NZFS-{slug}",
            country_code="nz",
            country_name="New Zealand",
            authority="MPI / NZFS",
            title=list_title,
            company=company,
            product="",
            hazard=hazard,
            alert_type="recall",
            region="Oceania",
            published=d_date,
            url=url_full,
            raw={"slug": slug, "listing": LISTING_URL},
        )
        records.append(rec)

    return records


NEW_ZEALAND = FeedSource(
    code="new_zealand",
    name_en="New Zealand",
    authority_short="MPI / NZFS",
    fetcher=fetch,
    region="Oceania",
    timezone="Pacific/Auckland",
    run_local_hour=9,
    # 09:00 Auckland = 21:00 UTC prev day (NZST/winter, UTC+12)
    #                  20:00 UTC prev day (NZDT/summer, UTC+13)
    cron_utc_offsets=(20, 21),
    gnews_authority="New Zealand",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-NZ", gnews_gl="NZ", gnews_ceid="NZ:en",
    gnews_days_back=3,
    # POLICY: regulator-only. Pending entries must come from MPI.
    gnews_country_keywords=(),
    gnews_country_domains=(
        "mpi.govt.nz",
    ),
    gnews_block_title_keywords=(
        "fda", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(NEW_ZEALAND)
