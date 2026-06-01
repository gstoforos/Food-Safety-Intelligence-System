"""
New Zealand source — Ministry for Primary Industries / New Zealand Food
Safety (MPI/NZFS).

POLICY (v5+): regulator-only. Pending entries must come from mpi.govt.nz.

POLICY (v6+): detail-page enrichment. Listing titles are product-only;
hazard wording lives on the detail page.

v7 fixes (post-2026-06-01 dry-run):
 - Switch to urllib.parse.urljoin() for proper URL construction (the
   naive BASE + href concatenation can produce malformed URLs if hrefs
   are page-relative rather than domain-absolute).
 - More tolerant text extraction (skip the strict main/article find;
   grab the whole body so we don't miss recall pages that wrap content
   differently).
 - Diagnostic prints on first 3 detail fetches so we can SEE what MPI
   is returning — status code, page size, extracted text length, and a
   short text snippet. If something's wrong, this will show it.
 - Small inter-fetch sleep to avoid tripping any rate limit.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

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


def _fetch_detail(url: str, debug: bool = False):
    """Return (hazard_text, published, company) from an MPI detail page."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        status = r.status_code
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] MPI detail fetch failed: {url} — {e}")
        return "", None, ""

    soup = BeautifulSoup(r.content, "html.parser")

    # Drop nav / footer / script / style noise before extracting text.
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()

    # Try a sequence of common Drupal/MPI content selectors. If none of
    # the named ones match, fall through to the whole body. We've seen
    # this fail silently in v6 because soup.find("main") returned an
    # element that was a sidebar/nav wrapper instead of the article body.
    main = (soup.find("article")
            or soup.find(class_=re.compile(r"^(field--name-body|main-content|"
                                           r"region-content|node__content|"
                                           r"content-main)", re.I))
            or soup.find("main")
            or soup.body
            or soup)
    text = main.get_text(" ", strip=True) if main else ""
    text = re.sub(r"\s+", " ", text)
    hazard = text[:1200]

    if debug:
        page_title = (soup.title.get_text(strip=True)
                      if soup.title else "(no <title>)")
        print(f"  [DEBUG] MPI detail fetch:")
        print(f"          url={url}")
        print(f"          status={status}  bytes={len(r.content)}")
        print(f"          page <title>: {page_title[:100]}")
        print(f"          extracted text len={len(text)}")
        print(f"          snippet: {text[:300]!r}")

    published = _parse_date(text)

    # Company extraction
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
        # urljoin handles both domain-absolute (/path) and page-relative
        # (path) hrefs correctly. The earlier naive BASE+href was
        # silently producing wrong URLs for hrefs that didn't start
        # with /, leading to detail-page fetches that succeeded against
        # a junk URL and returned a landing page with no recall info.
        url_full = urljoin(LISTING_URL, href)
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
            d_hazard, d_date, d_company = _fetch_detail(
                url_full, debug=(fetched < 3))
            fetched += 1
            time.sleep(0.3)         # be polite

        hazard = d_hazard or list_title

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
    gnews_country_keywords=(),
    gnews_country_domains=("mpi.govt.nz",),
    gnews_block_title_keywords=(
        "fda", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(NEW_ZEALAND)
