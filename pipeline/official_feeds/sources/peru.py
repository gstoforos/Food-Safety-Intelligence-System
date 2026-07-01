"""
Peru source — DIGESA.

LATAM EFET-style: regulator websites publish primarily in Spanish.
Best-effort listing scrape + GNews supplement restricted to regulator
domain + trusted Latin American English-language news outlets. The
gnews_authority_aliases mechanism forces Google to surface articles
explicitly naming the regulator (government-verified per EFET pattern).
"""

from __future__ import annotations

import re
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from ..base import Record, FeedSource, register
from ..fetch import DEFAULT_HEADERS

LISTING_URLS = (
    "https://www.digesa.minsa.gob.pe/noticias/comunicados.asp",
)

_DETAIL_TIMEOUT = 8
_DETAIL_CAP = 20

# ── Resilient fetch (audit 2026-07-01) ──────────────────────────────────────
# Route through curl_cffi Chrome-131 TLS impersonation + longer timeout +
# retries (gov hosts that 403/timeout plain requests from cloud IPs); falls
# back to plain requests when curl_cffi is unavailable.
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


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s)
                   if not unicodedata.combining(c))


_MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
# Numeric DD-MM-YYYY / DD.MM.YYYY (LatAm order) + Spanish "DD de MES de YYYY".
_DATE_RE = re.compile(r"(\d{1,2})[-./](\d{1,2})[-./](\d{4})")
_DATE_ES_RE = re.compile(
    r"(\d{1,2})\s+de\s+([A-Za-z\u00c0-\u017f]+)\s+de\s+(\d{4})")


def _parse_date(text: str):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)),
                            int(m.group(1)), tzinfo=timezone.utc)
        except (KeyError, ValueError):
            pass
    m = _DATE_ES_RE.search(text)
    if m:
        month = _MONTHS_ES.get(_strip_accents(m.group(2)).lower())
        if month:
            try:
                return datetime(int(m.group(3)), month, int(m.group(1)),
                                tzinfo=timezone.utc)
            except (KeyError, ValueError):
                pass
    return None


def _clean_title(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip(" |·-—")


def _try_fetch(url: str) -> str | None:
    return _http_get(url, timeout=_LISTING_TIMEOUT,
                     retries=_LISTING_RETRIES, label="DIGESA")


def _fetch_detail(url: str):
    html = _http_get(url, timeout=_DETAIL_TIMEOUT, retries=1,
                     label="DIGESA detail")
    if not html:
        return "", None
    soup = BeautifulSoup(html, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else ""
    title_clean = re.split(r"\s*[\|–—-]\s*", page_title, maxsplit=1)[0].strip()
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "aside"]):
        tag.decompose()
    body = (soup.find("article") or soup.find("main")
            or soup.body or soup).get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", body)
    return (title_clean + " " + body[:600]).strip(), _parse_date(body)


_ANCHOR_KEYWORDS = (
    "aciona",
    "ordena",
    "emite",
    "determina",
    "informa",
    "retira",
    "retiro",
    "recolhimento",
    "prohibe",
    "prohibe",
    "recoge",
    "alerta sanitaria",
    "alerta de",
    "alerta sobre",
    "recall",
    "ação",
    "recoge",
)

_NAV_BLOCK = (
    "ver todas",
    "consulta los",
    "consulta las",
    "consultá",
    "preguntas frecuentes",
    "registro sanitario",
    "buscador",
    "panel ",
    "controle de",
    "controle dos",
    "rotulagem",
    "programa de",
    "todas las alertas",
    "exportação",
    "importação",
    "exportaciÃ³n",
    "importaciÃ³n",
    "verifica la",
    "ingrese aquí",
)


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
            tl = _strip_accents(text.lower())
            if not any(k in tl for k in _ANCHOR_KEYWORDS):
                continue
            # Drop category/nav titles ("Ver todas las alertas", "Programa de…",
            # "Controle de Alimentos") — these match recall keywords but aren't
            # actual recall events
            if any(b in tl for b in _NAV_BLOCK):
                continue
            # Drop very short titles (likely a breadcrumb/header)
            if len(text) < 40:
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
    print(f"  enriching up to {min(len(links), _DETAIL_CAP)} DIGESA detail "
          f"pages ({_DETAIL_TIMEOUT}s timeout each)…")
    for slug, list_title, url_full in links:
        d_hazard = ""
        d_date = None
        if fetched < _DETAIL_CAP and not url_full.lower().endswith(".pdf"):
            d_hazard, d_date = _fetch_detail(url_full)
            fetched += 1
        hazard = d_hazard or list_title
        rec = Record(
            source_id=f"DIGESA-{slug}",
            country_code="pe",
            country_name="Peru",
            authority="DIGESA",
            title=list_title, company="", product="",
            hazard=hazard, alert_type="recall",
            region="Latin America", published=_parse_date(list_title) or d_date,
            url=url_full, raw={"slug": slug},
        )
        records.append(rec)
    return records


PERU = FeedSource(
    code="peru",
    name_en="Peru",
    authority_short="DIGESA",
    fetcher=fetch,
    region="Latin America",
    timezone="America/Lima",
    run_local_hour=9,
    cron_utc_offsets=(14,),
    gnews_authority="Peru",
    gnews_terms=(
        "salmonella", "listeria", "listeria monocytogenes",
        "E. coli", "STEC", "hepatitis A",
        "Bacillus cereus", "cereulide", "aflatoxin",
        "undeclared allergen", "outbreak",
    ),
    gnews_hl="es-PE", gnews_gl="PE", gnews_ceid="PE:es",
    gnews_days_back=7,
    gnews_country_keywords=(
        "peru",
        "peruvian",
        "digesa",
        "peruano",
        "lima peru",
    ),
    gnews_country_domains=(
        "digesa.minsa.gob.pe",
        "gob.pe",
        "perureports.com",
        "andina.pe",
        "elcomercio.pe",
        "larepublica.pe",
        "reuters.com",
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
        "DIGESA",
        "DIGESA Peru",
        "Direccion General de Salud Ambiental Peru",
    ),
    authority_domain="digesa.minsa.gob.pe",
    authority_url_pattern=r"(noticias|alertas|comunicados)/[^/]+",
)

register(PERU)
