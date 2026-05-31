"""
New Zealand source — Ministry for Primary Industries / New Zealand Food
Safety (MPI/NZFS).

No public JSON/RSS recall feed. Best-effort HTML scrape of the recalled-
food-products listing page; the Google News supplement carries the bulk
of detections.

Recalled food list: https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/
Media releases:      https://www.mpi.govt.nz/news/media-releases/

Individual recall slugs appear under both namespaces. We accept both.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING_URLS = (
    "https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/",
    "https://www.mpi.govt.nz/news/media-releases/",
)
BASE = "https://www.mpi.govt.nz"

_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})"
)
# MPI also uses DD.MM.YY ("Last reviewed: 16.03.26", "Date: 16 March 2026")
_DATE_RE_DOT = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if m:
        try:
            return datetime(int(m.group(3)), _MONTHS[m.group(2)],
                            int(m.group(1)), tzinfo=timezone.utc)
        except (KeyError, ValueError):
            pass
    m = _DATE_RE_DOT.search(text)
    if m:
        try:
            year = int(m.group(3))
            if year < 100:
                year += 2000
            return datetime(year, int(m.group(2)), int(m.group(1)),
                            tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


_CHROME = re.compile(
    r"^\s*(?:Home\s*[›>/]\s*)*"
    r"(?:Food safety\s*[›>/]\s*)*"
    r"(?:News\s*[›>/]\s*)*"
    r"(?:Media releases\s*[›>/]\s*)*"
    r"(?:Recalled food products\s*[›>/]\s*)*",
    re.IGNORECASE)


def _clean_title(t: str) -> str:
    if not t:
        return ""
    t = _CHROME.sub("", t).strip()
    t = re.sub(r"\s+", " ", t)
    return t.strip(" |·-—")


# Only keep media-release anchors that look like food recalls
_FOOD_RECALL_KEYWORDS = (
    "recall", "recalled", "listeria", "salmonella", "e. coli", "e.coli",
    "stec", "allergen", "contamination", "withdrawal",
    "hepatitis", "metal", "glass", "plastic", "rubber", "foreign matter",
    "mould", "mold", "cereulide",
)


def _looks_like_food_recall(title: str, href: str) -> bool:
    """Filter MPI media releases (only some are food recalls)."""
    t = (title or "").lower()
    h = (href or "").lower()
    if "/food-recalls-and-complaints/" in h or "/recalled-food-products/" in h:
        return True
    return any(k in t for k in _FOOD_RECALL_KEYWORDS)


def _try_fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] MPI/NZFS fetch failed: {url} — {e}")
        return None


def fetch(limit: int = 60) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()

    for listing_url in LISTING_URLS:
        html = _try_fetch(listing_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Accept anchors under both recall and media-release namespaces
            if ("/recalled-food-products/" not in href
                    and "/media-releases/" not in href):
                continue
            slug = href.rstrip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            if slug in {"recalled-food-products", "media-releases"}:
                continue

            title = _clean_title(a.get_text(" ", strip=True))
            if not title or len(title) < 12:
                continue
            if title.lower() in {"more", "next", "previous",
                                 "see all", "view all", "read more"}:
                continue
            # Filter media releases that aren't food recalls
            if "/media-releases/" in href and \
                    not _looks_like_food_recall(title, href):
                continue
            seen.add(slug)

            url_full = href if href.startswith("http") else BASE + href

            # Container text for date + context
            container = a
            for _ in range(4):
                container = container.parent
                if not container or container.name in {"body", "html"}:
                    break
            ctx = container.get_text(" ", strip=True) if container else title
            ctx = re.sub(r"\s+", " ", ctx)[:800]

            published = _parse_date(ctx) or _parse_date(title)
            hazard = ctx

            # Best-effort company extraction
            company = ""
            m = re.search(
                r"(?:supporting|supports)\s+([A-Z][\w &.''\-]{1,60}?)\s+in\b",
                ctx)
            if m:
                company = m.group(1).strip()

            rec = Record(
                source_id=f"NZFS-{slug}",
                country_code="nz",
                country_name="New Zealand",
                authority="MPI / NZFS",
                title=title,
                company=company,
                product="",
                hazard=hazard,
                alert_type="recall",
                region="Oceania",
                published=published,
                url=url_full,
                raw={"slug": slug, "listing": listing_url},
            )
            records.append(rec)
            if len(records) >= limit:
                return records

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
    gnews_authority="MPI New Zealand Food Safety NZFS",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-NZ", gnews_gl="NZ", gnews_ceid="NZ:en",
    gnews_days_back=3,
)

register(NEW_ZEALAND)
