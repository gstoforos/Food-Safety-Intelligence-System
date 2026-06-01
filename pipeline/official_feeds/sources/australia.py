"""
Australia source — Food Standards Australia New Zealand (FSANZ).

FSANZ has no public JSON/RSS recall feed. The federal listing page lists
recall items under /food-recalls/recall-alert/<slug>. We do a best-effort
scrape of that listing — title-only, no DOM-parent text — and rely on the
Google News supplement to carry the bulk of detections. If the HTML
structure changes or the page returns an error, the official-feed half
quietly returns 0 records and the GNews half still runs (FSIS pattern).

Federal recall-alert listing: https://www.foodstandards.gov.au/food-recalls/recall-alert
Federal recall index:          https://www.foodstandards.gov.au/food-recalls

DESIGN NOTES (v2, post-2026-05-31 dry-run):
 - We do NOT pull the surrounding container text for the hazard field —
   the FSANZ listing page is flat with sibling recall items, so any
   widening of the DOM scope causes neighbour-pathogen bleed (e.g.
   "pistachio" from one recall contaminating the hazard text of every
   nearby unrelated recall, producing reject/allergen false positives
   across the entire batch). Title-only is safe but loses hazard context;
   GNews fills that gap by re-finding the same recalls in news articles
   that DO name the pathogen in the headline.
 - We require a recall-alert URL fragment so nav links like
   "/food-recalls/templates", "/food-recalls/how-to-recall-food",
   "/food-recalls/statecontacts" are filtered out before they reach the
   classifier.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING_URLS = (
    "https://www.foodstandards.gov.au/food-recalls/recall-alert",
    "https://www.foodstandards.gov.au/food-recalls",
)
BASE = "https://www.foodstandards.gov.au"

# Only real recall items live under this exact path prefix; everything
# else (templates, statistics, how-to, faqs, contacts) is navigation.
_RECALL_PATH_PREFIX = "/food-recalls/recall-alert/"

# Belt-and-braces text-level blocklist for navigation anchors that
# sometimes link out to recall-shaped URLs.
_NAV_TITLE_RE = re.compile(
    r"^(?:food recall|food incidents|how to recall|about food|"
    r"state and territory|faqs|food industry recall|recall protocol|"
    r"recall statistics|recall templates|recall alerts)\b",
    re.IGNORECASE)

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
        print(f"  [WARN] FSANZ fetch failed: {url} — {e}")
        return None


def _is_recall_url(href: str) -> bool:
    """Only accept URLs that look like real recall items."""
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

    html = None
    listing_url = ""
    for url in LISTING_URLS:
        html = _try_fetch(url)
        if html:
            listing_url = url
            break
    if not html:
        return records

    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()

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
        if _NAV_TITLE_RE.match(title):
            continue
        seen.add(slug)

        url_full = href if href.startswith("http") else BASE + href

        # Date: try a nearby <time datetime="..."> element; if not found,
        # leave None — age filter keeps no-date records.
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

        # Company from title: text before " - " if present
        company = ""
        m = re.match(r"^([A-Z][\w &.''\-]{1,80}?)\s*[-–—]", title)
        if m:
            company = m.group(1).strip()

        rec = Record(
            source_id=f"FSANZ-{slug}",
            country_code="au",
            country_name="Australia",
            authority="FSANZ",
            title=title,
            company=company,
            product="",
            hazard=title,            # title-only — NO parent DOM bleed
            alert_type="recall",
            region="Oceania",
            published=published,
            url=url_full,
            raw={"slug": slug, "listing": listing_url},
        )
        records.append(rec)
        if len(records) >= limit:
            break

    return records


AUSTRALIA = FeedSource(
    code="australia",
    name_en="Australia",
    authority_short="FSANZ",
    fetcher=fetch,
    region="Oceania",
    timezone="Australia/Sydney",
    run_local_hour=9,
    # 09:00 Sydney = 23:00 UTC prev day (AEST/winter, UTC+10)
    #                22:00 UTC prev day (AEDT/summer, UTC+11)
    cron_utc_offsets=(22, 23),
    # SHORT authority — "FSANZ Australia food" returned 0 across 49 queries
    # in dry-run because no AU news headline contains all those words. Just
    # "Australia" + the en-AU/AU Google News locale params is enough to
    # narrow results to Australian food-recall news.
    gnews_authority="Australia",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-AU", gnews_gl="AU", gnews_ceid="AU:en",
    gnews_days_back=3,
    # Country-scope filter — drops US/UK/CA recall headlines that the
    # en-AU locale surfaces but aren't actually Australian recalls. An
    # article must mention Australia (or a state/major city) in its
    # title OR live on an Australian domain to pass.
    gnews_country_keywords=(
        "australia", "australian", "aussie",
        "nsw", "new south wales", "sydney",
        "victoria", "melbourne", "vic ",
        "queensland", "brisbane", "qld",
        "western australia", "perth", " wa ",
        "south australia", "adelaide", " sa ",
        "tasmania", "hobart", "tas ",
        "canberra", "northern territory", "darwin",
        "coles", "woolworths", "aldi australia",
        "fsanz",
        # NB: NOT "act" — too short, substring-matches react/action/fact/etc.
    ),
    gnews_country_domains=(
        ".com.au", ".org.au", ".gov.au", ".net.au", ".edu.au",
    ),
    # Title denylist — drops articles where a US-only retailer or
    # agency appears in the headline. These are AU news outlets
    # syndicating US recall stories (URL is .com.au, but content is
    # really a US/FDA/USDA recall already captured by us_fda/us_fsis).
    gnews_block_title_keywords=(
        "fda", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
)

register(AUSTRALIA)
