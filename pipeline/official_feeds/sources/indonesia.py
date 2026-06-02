"""
Indonesia source — BPOM (Badan Pengawas Obat dan Makanan).

BPOM publishes recall/warning information primarily in Bahasa Indonesia
at pom.go.id. Their public-warning sections live at:
  - pom.go.id/page/peringatan-publik (Public Warnings)
  - pom.go.id/siaran-pers/ (Press Releases)

Since Bahasa shares Latin script, the classifier lexicon (English-based)
will match some terms verbatim (salmonella, listeria, e. coli) but
miss native terms. Best-effort scrape; GNews supplement on pom.go.id
URLs catches anything indexed.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
# pom.go.id presents a cert chain that GitHub Actions runners can't
# validate (Indonesian government CA not in their bundle). We disable
# verification on this specific host only — we're scraping public
# bulletins, not auth'd content.
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

LISTING_URLS = (
    "https://www.pom.go.id/penjelasan-publik",    # Public Clarifications (recall-equivalent)
    "https://www.pom.go.id/siaran-pers",          # Press Releases
    "https://www.pom.go.id/",                     # Homepage fallback
)
BASE = "https://www.pom.go.id"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

# Indonesian + English date patterns
_MONTHS_ID = {
    "Januari":1, "Februari":2, "Maret":3, "April":4, "Mei":5, "Juni":6,
    "Juli":7, "Agustus":8, "September":9, "Oktober":10, "November":11,
    "Desember":12,
    "January":1, "February":2, "March":3, "May":5, "June":6,
    "July":7, "August":8, "October":10, "December":12,
}
_DATE_RE = re.compile(
    r"(\d{1,2})\s+(Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|"
    r"September|Oktober|November|Desember|January|February|March|"
    r"April|May|June|July|August|October|December)\s+(\d{4})"
)


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), _MONTHS_ID[m.group(2)],
                        int(m.group(1)), tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None


def _clean_title(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip(" |·-—")


def _try_fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=15, verify=False)
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] BPOM fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT, verify=False)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] BPOM detail fetch failed: {url} — {e}")
        return "", None
    soup = BeautifulSoup(r.content, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*[\|–—-]\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    return (title_clean + " " + body[:600]).strip(), _parse_date(body)


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
            text = _clean_title(a.get_text(" ", strip=True))
            if not text or len(text) < 12:
                continue
            tl = text.lower()
            # Accept anchors signalling recall / warning / safety content,
            # in Indonesian or English
            if not any(k in tl for k in (
                    "recall", "withdraw", "warning", "peringatan",
                    "alert", "advisory", "safety", "keamanan",
                    "berbahaya", "cemar", "kontaminasi", "tarik")):
                continue
            slug = href.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, text, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} BPOM detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"BPOM-{slug}",
            country_code="id",
            country_name="Indonesia",
            authority="BPOM",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


INDONESIA = FeedSource(
    code="indonesia",
    name_en="Indonesia",
    authority_short="BPOM",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Jakarta",
    run_local_hour=9,
    cron_utc_offsets=(2,),         # WIB UTC+7, no DST → 09:00 = 02:00 UTC
    gnews_authority="Indonesia",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide", "aflatoxin",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en-ID", gnews_gl="ID", gnews_ceid="ID:en",
    gnews_days_back=3,
    gnews_country_keywords=(
        "indonesia",          # strict: must be the full word, not "indo"
        "indonesian",
        "bpom",
        "badan pom",
        "jakarta,",           # comma-suffixed to avoid matching "jakartans"
        " jakarta ",
    ),
    gnews_country_domains=(
        "pom.go.id",
        "bpom.go.id",
        "thejakartapost.com",        # Jakarta Post
        "jakartaglobe.id",           # Jakarta Globe
        "antaranews.com",            # Antara News (state agency, has /en)
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_use_description=True,
)

register(INDONESIA)
