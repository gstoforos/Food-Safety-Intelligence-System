"""
New Zealand source — Ministry for Primary Industries / New Zealand Food
Safety (MPI/NZFS).

No public JSON/RSS recall feed. Best-effort HTML scrape of the recalled-
food-products listing page; the Google News supplement carries the bulk
of detections.

Recalled food list: https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/

DESIGN NOTES (v2, post-2026-05-31 dry-run):
 - We do NOT pull surrounding DOM container text for the hazard field.
   The MPI listing is a flat list with a sidebar; widening the DOM scope
   caused massive neighbour bleed (e.g. "pistachio" from one recall's
   sidebar contaminated the hazard text of every other unrelated recall,
   producing reject/allergen 'pistachio' false positives across 33 of
   60 records). Title-only is safe.
 - We only scrape the /recalled-food-products/ namespace. The earlier
   version also pulled /news/media-releases/, which mixed in non-recall
   MPI press releases. The recalled-food-products page is the canonical
   index and is sufficient.
 - Titles on this listing are product-name-only ("Emborg Emmentaler
   Cheese" — no hazard wording), so title-only classification will
   reject most as unknown. That is correct: GNews catches the same
   recalls under news headlines that DO name the pathogen ("Emborg
   Emmentaler cheese recalled due to possible presence of Listeria").
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

# Only accept anchors under this exact path prefix as recalls.
_RECALL_PATH_PREFIX = "/food-recalls-and-complaints/recalled-food-products/"

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


def fetch(limit: int = 40) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()

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
        # Generic navigation labels
        if title.lower() in {"read more", "view", "see all", "more",
                             "next", "previous", "recalled food products"}:
            continue
        seen.add(slug)

        url_full = href if href.startswith("http") else BASE + href

        # Date: try sibling <time datetime="..."> within a small DOM window.
        published = None
        parent = a.parent
        for _ in range(3):
            if not parent or parent.name in {"body", "html"}:
                break
            t = parent.find("time")
            if t and t.get("datetime"):
                try:
                    raw = t["datetime"].replace("Z", "+00:00")
                    published = datetime.fromisoformat(raw)
                    if published.tzinfo is None:
                        published = published.replace(tzinfo=timezone.utc)
                    break
                except (ValueError, KeyError):
                    pass
            parent = parent.parent
        if published is None:
            published = _parse_date(title)

        # Company: best-effort from title (e.g. "Emborg Emmentaler Cheese"
        # → "Emborg" before space, or "Pams brand Beef Lasagne" → "Pams")
        company = ""
        m = re.match(r"^([A-Z][\w &.''\-]{1,40}?)\s+brand\b", title)
        if not m:
            m = re.match(r"^([A-Z][\w &.''\-]{1,40})\s+", title)
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
            hazard=title,            # title-only — NO parent DOM bleed
            alert_type="recall",
            region="Oceania",
            published=published,
            url=url_full,
            raw={"slug": slug, "listing": LISTING_URL},
        )
        records.append(rec)
        if len(records) >= limit:
            break

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
    # SHORT authority — "MPI New Zealand Food Safety NZFS" (6 words)
    # returned 0 across 49 queries in dry-run because no NZ news headline
    # contains all those words. "New Zealand" + the en-NZ/NZ locale is
    # enough to narrow Google News results to NZ food-recall coverage.
    gnews_authority="New Zealand",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-NZ", gnews_gl="NZ", gnews_ceid="NZ:en",
    gnews_days_back=3,
    # Country-scope filter — POLICY: Pending entries for NZ must only come
    # from the regulator's own website (mpi.govt.nz), same as the EFET /
    # AESAN / ASAE / etc. country collectors in the EU pipeline. GNews
    # still runs as backup but any catch is dropped unless its URL is on
    # the MPI domain. Title-keyword matching is OFF — NZ news outlets
    # routinely syndicate US recall stories on .co.nz URLs (e.g. NZ
    # Herald covering a Kroger croutons recall) and we can't tell
    # domestic-vs-syndicated from a generic headline alone.
    gnews_country_keywords=(),
    gnews_country_domains=(
        "mpi.govt.nz",
    ),
    # Title denylist — defence in depth.
    gnews_block_title_keywords=(
        "fda", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(NEW_ZEALAND)
