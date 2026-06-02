"""
Brazil source — ANVISA.

LATAM EFET-style: regulator websites publish primarily in Portuguese.
Best-effort listing scrape + GNews supplement restricted to regulator
domain + trusted Latin American English-language news outlets. The
gnews_authority_aliases mechanism forces Google to surface articles
explicitly naming the regulator (government-verified per EFET pattern).
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
    "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/listagem-de-noticias",
    "https://www.gov.br/anvisa/pt-br",
    "https://www.gov.br/anvisa/pt-br/assuntos/alimentos",
)

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
        print(f"  [WARN] ANVISA fetch failed: {url} — {e}")
        return None


def _fetch_detail(url: str):
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] ANVISA detail fetch failed: {url} — {e}")
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


_ANCHOR_KEYWORDS = ("recall", "recolhimento", "alerta", "alimento", "contamina", "salmonel", "listeria")


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
            if not any(k in tl for k in _ANCHOR_KEYWORDS):
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
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} ANVISA detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP:
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"ANVISA-{slug}",
            country_code="br",
            country_name="Brazil",
            authority="ANVISA",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Latin America", published=d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


BRAZIL = FeedSource(
    code="brazil",
    name_en="Brazil",
    authority_short="ANVISA",
    fetcher=fetch,
    region="Latin America",
    timezone="America/Sao_Paulo",
    run_local_hour=9,
    cron_utc_offsets=(12,),
    gnews_authority="Brazil",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide", "aflatoxin",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="pt-BR", gnews_gl="BR", gnews_ceid="BR:pt",
    gnews_days_back=7,
    gnews_country_keywords=(
        "brazil",
        "brazilian",
        "anvisa",
        "brasil",
        "sao paulo",
        "rio de janeiro",
    ),
    gnews_country_domains=(
        "anvisa.gov.br",
        "gov.br",
        "reuters.com",
        "bloomberg.com",
        "rio-times.com",
        "brazilian.report",
        "folha.uol.com.br",
    ),
    gnews_block_title_keywords=(
        "fda announces", "us fda", "u.s. fda", "fda warns shoppers",
        "usda", "fsis",
        "walmart", "kroger", "sam's club", "sams club",
        "trader joe", "whole foods", "kirkland",
        "u.s.", "united states",
    ),
    gnews_use_description=True,
    gnews_authority_aliases=(
        "ANVISA",
        "ANVISA Brazil",
    ),
)

register(BRAZIL)
