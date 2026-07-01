"""
South Korea source — Ministry of Food and Drug Safety (MFDS) /
Food Safety Korea.

MFDS publishes recall info primarily in Korean. The English version at
mfds.go.kr/eng/ has limited recall content. Foodsafetykorea.go.kr is
the consumer-facing food safety portal with some English coverage.

Best-effort HTML scrape + GNews supplement restricted to mfds.go.kr +
foodsafetykorea.go.kr URLs.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

LISTING_URLS = (
    "https://www.mfds.go.kr/eng/brd/m_64/list.do",         # eng news
    "https://www.foodsafetykorea.go.kr/eng/board/board.do?menu_no=1869&menu_grp=MENU_NEW07",
)
BASE = "https://www.mfds.go.kr"

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

_DATE_RE = re.compile(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})")


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)),
                        int(m.group(3)), tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None


def _clean_title(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip(" |·-—")


# ── Resilient fetch (audit 2026-06-30) ──────────────────────────────────────
# These authority hosts were fetched with plain `requests` at a 15s timeout —
# no TLS impersonation, no retry — so they failed from GitHub-Actions IPs:
# connect-timeouts (mfds.go.kr, vfa.gov.vn) and WAF 403s (caa.go.jp,
# fda.gov.ph). Route through curl_cffi Chrome-131 TLS (clears many WAF 403s)
# with a longer timeout + retries, falling back to plain requests if curl_cffi
# is unavailable. A true geo/IP block may still fail — that needs an in-region
# proxy, not a header tweak.
try:
    from curl_cffi import requests as _cffi  # type: ignore
    _IMPERSONATE = "chrome131"
except Exception:  # noqa: BLE001
    _cffi = None
    _IMPERSONATE = None

_LISTING_TIMEOUT = 30
_LISTING_RETRIES = 3


def _http_get(url: str, *, timeout: int, retries: int, label: str) -> str | None:
    """GET `url` with Chrome-131 TLS impersonation + retries; None on failure."""
    last = None
    for i in range(retries):
        try:
            if _cffi is not None:
                r = _cffi.get(url, headers=DEFAULT_HEADERS, timeout=timeout,
                              impersonate=_IMPERSONATE)
            else:
                r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            if i + 1 < retries:
                time.sleep(2 * (i + 1))
    print(f"  [WARN] {label} fetch failed: {url} — {last}")
    return None


def _try_fetch(url: str) -> str | None:
    return _http_get(url, timeout=_LISTING_TIMEOUT,
                     retries=_LISTING_RETRIES, label="MFDS")


def _fetch_detail(url: str):
    html = _http_get(url, timeout=_DETAIL_TIMEOUT, retries=1,
                     label="MFDS detail")
    if not html:
        return "", None
    soup = BeautifulSoup(html, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*\|\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    return (title_clean + " " + body[:400]).strip(), _parse_date(body)


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
            if not ("view.do" in href.lower() or "boardview" in href.lower()
                    or "/eng/brd/" in href.lower()):
                continue
            slug = re.sub(r"[^A-Za-z0-9_=&-]+", "-",
                          href.split("?", 1)[-1] or href)[:120]
            if not slug or slug in seen:
                continue
            title = _clean_title(a.get_text(" ", strip=True))
            if not title or len(title) < 10:
                continue
            seen.add(slug)
            url_full = urljoin(listing_url, href)
            links.append((slug, title, url_full))
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    fetched = 0
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} MFDS detail pages "
          f"({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"MFDS-{slug}",
            country_code="kr",
            country_name="South Korea",
            authority="MFDS",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Asia", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


SOUTH_KOREA = FeedSource(
    code="south_korea",
    name_en="South Korea",
    authority_short="MFDS",
    fetcher=fetch,
    region="Asia",
    timezone="Asia/Seoul",
    run_local_hour=9,
    cron_utc_offsets=(0,),         # KST UTC+9, no DST → 09:00 = 00:00 UTC
    gnews_authority="Korea",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="en", gnews_gl="KR", gnews_ceid="KR:en",
    gnews_days_back=7,
    gnews_country_keywords=(
        "korea",
        "korean",
        "mfds",
        "seoul",
        "south korea",
    ),
    gnews_country_domains=(
        "mfds.go.kr",
        "foodsafetykorea.go.kr",
        "koreaherald.com",           # Korea Herald
        "koreatimes.co.kr",          # Korea Times
        "koreajoongangdaily.joins.com",  # JoongAng Daily English
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_authority_aliases=(
        "MFDS",
        "Korean FDA",
    ),
    authority_domain="mfds.go.kr",
    authority_url_pattern=r"(brd|board|nw)[a-z_]*/[^/]+",
    gnews_use_description=True,
)

register(SOUTH_KOREA)
