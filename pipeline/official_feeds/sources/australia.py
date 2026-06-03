"""
Australia source — Food Standards Australia New Zealand (FSANZ).

POLICY (v5+): Pending entries for AU must only come from the regulator's
own website (foodstandards.gov.au), same as the EFET / AESAN / ASAE
country collectors in the EU pipeline. GNews supplement still runs as
backup but its country-scope filter only accepts the regulator domain.

POLICY (v6+): Detail-page enrichment. The listing page shows each
recall as "Company - Product - Size" without hazard wording, so a
listing-only classifier returns reject/unknown for every record — even
real in-scope Listeria / Salmonella recalls. We fetch each detail page
(up to _DETAIL_CAP) and use its body text as the hazard field so the
classifier can find the pathogen wording the regulator actually
published.

Federal recall-alert listing: https://www.foodstandards.gov.au/food-recalls/recall-alert
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS, TIMEOUT

LISTING_URLS = (
    "https://www.foodstandards.gov.au/food-recalls/recall-alert",
    "https://www.foodstandards.gov.au/food-recalls",
)
BASE = "https://www.foodstandards.gov.au"

# Real recall items live under this path prefix; everything else
# (templates, statistics, how-to, faqs, contacts) is navigation.
_RECALL_PATH_PREFIX = "/food-recalls/recall-alert/"

# Detail-page enrichment cap. FSANZ listing usually has ~25-40 entries.
# We need to enrich enough to cover anything that passes the 30-day
# age filter. Worst case = _DETAIL_CAP × _DETAIL_TIMEOUT.
_DETAIL_CAP = 25
_DETAIL_TIMEOUT = 8                    # seconds per detail page (NOT TIMEOUT=30)

# Patterns that introduce the recall reason on a FSANZ detail page.
# We pull only sentences matching these patterns — that way packaging
# descriptions ("sold in plastic bag", "400g tin", "glass jar") don't
# leak into the classifier. Each pattern stops at the next period
# (sentence boundary) by using [^.] so the recall-reason sentence is
# captured cleanly and the following packaging sentence is excluded.
_REASON_PATTERNS = [
    re.compile(r"due to [^.]{1,200}", re.I),
    re.compile(r"because of [^.]{1,200}", re.I),
    re.compile(r"may contain [^.]{1,200}", re.I),
    re.compile(r"contaminated with [^.]{1,200}", re.I),
    re.compile(r"presence of [^.]{1,200}", re.I),
    re.compile(r"reason for (?:the )?recall[:\s][^.]{1,200}", re.I),
    re.compile(r"hazard[:\s][^.]{1,200}", re.I),
]


def _extract_reason_phrases(body: str) -> str:
    """Return only the recall-reason phrases from a body of text.

    This filters out product descriptions, packaging info, and
    distribution details — keeping just the sentences that name the
    hazard. Result is concatenated with spaces, capped at ~600 chars.
    """
    found: list[str] = []
    for pat in _REASON_PATTERNS:
        for m in pat.finditer(body):
            phrase = re.sub(r"\s+", " ", m.group(0)).strip()
            if phrase and phrase not in found:
                found.append(phrase)
            if len(found) >= 6:
                break
        if len(found) >= 6:
            break
    return " | ".join(found)[:600]

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
    """Return (hazard_text, published, company) from a FSANZ detail page.

    Hazard is a hybrid of:
      (a) the page <title> (chrome stripped), AND
      (b) recall-reason phrases extracted from body text via _REASON_PATTERNS
          (sentences after "due to", "may contain", "because of", etc.)

    This avoids the body-text false positives that bit v8 (packaging
    descriptions matching foreign_matter / heavy_metal lexicons) while
    still letting the classifier see the regulator's hazard wording —
    FSANZ page <title> alone is usually just the product name without
    the recall reason, unlike MPI which puts the full reason in <title>.
    """
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSANZ detail fetch failed/timeout: {url} — {e}")
        return "", None, ""

    soup = BeautifulSoup(r.content, "html.parser")

    # <title> portion
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*\|\s*", page_title, maxsplit=1)[0].strip()
    title_clean = re.sub(
        r"\s*[-–—]\s*Food Standards Australia New Zealand\s*$",
        "", title_clean).strip()

    # Body — used for reason-phrase extraction + date + company
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    main = (soup.find("article")
            or soup.find("main")
            or soup.find(class_=re.compile(r"(content|main|body)", re.I))
            or soup.body
            or soup)
    body_text = main.get_text(" ", strip=True) if main else ""
    body_text = re.sub(r"\s+", " ", body_text)

    reasons = _extract_reason_phrases(body_text)
    # hazard = title + reason phrases (newline separator preserves boundaries
    # so the classifier doesn't accidentally match across them)
    hazard = (title_clean + "\n" + reasons).strip()

    published = _parse_date(body_text)

    company = ""
    m = re.search(
        r"([A-Z][\w&.''\-]*(?:\s+[A-Z0-9][\w&.''\-]*){0,5})\s+is\s+(?:recalling|conducting)",
        body_text)
    if m:
        company = m.group(1).strip()[:80]

    return hazard, published, company


def fetch(limit: int = 25) -> list[Record]:
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
    links: list[tuple] = []

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
        # urljoin handles both domain-absolute (/path) and page-relative
        # (path) hrefs correctly.
        url_full = urljoin(listing_url or LISTING_URLS[0], href)
        links.append((slug, title, url_full))
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} FSANZ detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        d_company = ""
        if fetched < _DETAIL_CAP:
            d_hazard, d_date, d_company = _fetch_detail(url_full)
            fetched += 1

        # Title from listing; hazard from detail page (or title fallback)
        hazard = d_hazard or list_title

        # Company: prefer detail-page extraction, fall back to title prefix
        company = d_company
        if not company:
            m = re.match(r"^([A-Z][\w &.''\-]{1,80}?)\s*[-–—]", list_title)
            if m:
                company = m.group(1).strip()

        rec = Record(
            source_id=f"FSANZ-{slug}",
            country_code="au",
            country_name="Australia",
            authority="FSANZ",
            title=list_title,
            company=company,
            product="",
            hazard=hazard,
            alert_type="recall",
            region="Oceania",
            published=d_date,
            url=url_full,
            raw={"slug": slug, "listing": listing_url},
        )
        records.append(rec)

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
    gnews_authority="Australia",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-AU", gnews_gl="AU", gnews_ceid="AU:en",
    gnews_days_back=3,
    # POLICY: regulator-only. Pending entries must come from FSANZ. The
    # country-scope filter requires the URL to be on foodstandards.gov.au
    # — domestic news outlets that syndicate US recalls on .com.au URLs
    # get filtered out at the GNews stage.
    gnews_country_keywords=(),
    gnews_country_domains=(
        "foodstandards.gov.au",
    ),
    gnews_block_title_keywords=(
        "fda", "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    authority_domain="foodstandards.gov.au",
    authority_url_pattern=r"consumer/safety/recalls/[a-z0-9-]{10,}",
    authority_index_urls=(
        "https://www.foodstandards.gov.au/consumer/safety/recalls/",
    ),
)

register(AUSTRALIA)
