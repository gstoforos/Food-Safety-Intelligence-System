"""
Vietnam source — Vietnam Food Administration (VFA), Ministry of Health.

VFA publishes warnings primarily in Vietnamese (Latin script with
diacritics) at vfa.gov.vn. English coverage is limited. The collector
does best-effort scrape and relies on the GNews supplement (restricted
to vfa.gov.vn URLs) for the rare English-language regulator publications.
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
    "https://vfa.gov.vn/canh-bao",                # Warnings section (VN)
    "https://vfa.gov.vn/thu-hoi-san-pham",        # Product recall section
    "https://vfa.gov.vn/",
)
BASE = "https://vfa.gov.vn"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

_DATE_RE = re.compile(r"(\d{1,2})[-./](\d{1,2})[-./](\d{4})")


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), int(m.group(2)),
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
        print(f"  [WARN] VFA fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] VFA detail fetch failed: {url} — {e}")
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


def fetch(limit: int = 20) -> list[Record]:
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
            # Accept anchors signalling recall/warning in Vietnamese or English
            if not any(k in tl for k in (
                    "recall", "warning", "alert", "advisory",
                    "withdraw", "safety", "contamination",
                    "canh báo", "canh bao", "thu hồi", "thu hoi",
                    "an toàn", "an toan", "nhiễm", "nhiem")):
                continue
            slug = re.sub(r"[^A-Za-z0-9_-]+", "-",
                          href.split("?", 1)[0].split("#", 1)[0].rstrip("/")
                          .split("/")[-1])[:120]
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
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} VFA detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"VFA-{slug}",
            country_code="vn",
            country_name="Vietnam",
            authority="VFA",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


VIETNAM = FeedSource(
    code="vietnam",
    name_en="Vietnam",
    authority_short="VFA",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Ho_Chi_Minh",
    run_local_hour=9,
    cron_utc_offsets=(2,),         # ICT UTC+7, no DST → 09:00 = 02:00 UTC
    gnews_authority="Vietnam",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide", "aflatoxin",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en", gnews_gl="VN", gnews_ceid="VN:en",
    gnews_days_back=7,
    gnews_country_keywords=(
        "vietnam",
        "vietnamese",
        "hanoi",
        "ho chi minh",
        "saigon",
        "vfa",
    ),
    gnews_country_domains=(
        "vfa.gov.vn",
        "vietnamnews.vn",            # Vietnam News (state-owned, English)
        "vir.com.vn",                # Vietnam Investment Review
        "tuoitrenews.vn",            # Tuoi Tre News (English)
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_authority_aliases=(
        "VFA Vietnam",
        "Vietnam Food Administration",
    ),
    authority_domain="vfa.gov.vn",
    authority_url_pattern=r"(canh-bao|thu-hoi|tin-tuc)",
    gnews_use_description=True,
)

register(VIETNAM)
