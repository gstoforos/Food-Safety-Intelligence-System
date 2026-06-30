r"""
AFTS Food Safety Intelligence — Gap Finder
News article fetcher (parametric, parallel, TLS-impersonated).

Replaces the search-engine-based authority verifier. Instead of trying to
cross-reference each news candidate against a thinly-populated DDG index
of the authority's site (which only ever has 17-30 URLs available), we
fetch the news article body directly and feed it to the rule classifier
+ LLM extractor.

Why this works for the gap finder use case:
  - News articles about real recalls always contain the specific hazard
    (Salmonella, Listeria, aflatoxin, etc.), the brand, the product, and
    usually a reference to the authority's notice ID.
  - The rule classifier (rules.py) is the QC layer: news articles about
    politicians "richiamati al servizio" or recalled ambassadors won't
    contain food-hazard terms and get auto-rejected as 'unknown'.
  - We sidestep the DDG throttle entirely. Daily runs work the same
    forever, regardless of search-engine bot detection.

Architecture:
  1. For each news candidate, follow redirects (Google News → real article URL)
  2. Fetch HTML, extract main content body via BeautifulSoup heuristics
  3. Normalize whitespace, truncate to ~5000 chars (LLM-friendly)
  4. Run all fetches in parallel (5 threads) with per-domain throttling
  5. Output enriched candidates with .body field, ready for extractor

Schema-compatible with the old verified.jsonl format so extractor.py and
main.py downstream stages work unchanged.

╔════════════════════════════════════════════════════════════════════════════╗
║  2026-06-12 — TLS impersonation upgrade (proper fix for Greek 0-fetch)    ║
╠════════════════════════════════════════════════════════════════════════════╣
║ The 2026-06-12 Greek gap_finder run showed HTTP fetched fail: 11/11 ─     ║
║ every single news article fetch failed. Cause: most modern news sites     ║
║ (kathimerini, protothema, news247, skai, in.gr ...) sit behind Akamai,    ║
║ Cloudflare, or DataDome bot-detect that inspect the TLS handshake JA3/    ║
║ JA4 fingerprint. Python `requests` uses urllib3+OpenSSL with a TLS        ║
║ fingerprint that's instantly recognisable as non-browser — 403/410/      ║
║ Forbidden across the board.                                                ║
║                                                                            ║
║ Fix: route every news fetch through curl_cffi with Chrome 131 TLS        ║
║ impersonation. curl_cffi.requests is a drop-in `requests`-compatible API ║
║ that performs the handshake byte-for-byte identical to real Chrome.       ║
║ Same pattern scrapers/_akamai_fetch.py already uses successfully against ║
║ www.fda.gov + www.fsis.usda.gov.                                          ║
║                                                                            ║
║ Behaviour change vs prior version:                                        ║
║   - fetch_html() now uses curl_cffi.requests by default (with             ║
║     impersonate="chrome131").                                              ║
║   - First-call-per-host log line lets us verify routing engaged in        ║
║     production (matches _akamai_fetch.py pattern).                        ║
║   - Falls back to stdlib `requests` if curl_cffi is not installed         ║
║     (graceful degradation; warning emitted).                              ║
║                                                                            ║
║ Dependencies: curl-cffi>=0.7.0 already in requirements.txt (used by       ║
║ scrapers/ folder). No new dep, no version bump.                           ║
║                                                                            ║
║ Verified post-fix: paste a known-bot-protected URL via                    ║
║   python -m pipeline.gap_finder.article_fetcher --country gr \            ║
║       --probe "https://www.kathimerini.gr/society/.../recall-article"    ║
║ — should now return status="ok" and body > 500 chars.                    ║
╚════════════════════════════════════════════════════════════════════════════╝

CLI:
    python -m pipeline.gap_finder.article_fetcher --country it
    python -m pipeline.gap_finder.article_fetcher --country gr --probe URL
"""

from __future__ import annotations
import argparse
import json
import logging
import random
import re
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# ── curl_cffi: TLS impersonation primary path ──────────────────────────────
# Lazy-imported in fetch_html() to keep startup fast and to allow graceful
# fallback to stdlib `requests` if the package isn't installed (e.g. local
# dev environments without the prod requirements pinned).
_curl_cffi_imported: bool = False
_curl_cffi_module = None        # cf.requests once imported
_curl_cffi_import_failed: bool = False
_curl_cffi_logged_hosts: set[str] = set()  # first-call-per-host logging

try:
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder.countries import get as get_country           # type: ignore
    from gap_finder.countries.base import CountryConfig           # type: ignore

# Google News URL resolver — converts news.google.com/rss/articles/<token>
# links into the real publisher URL BEFORE we fetch the body. Without this,
# every Google-News-sourced candidate fetches the GN JS-redirect shell
# (~150 chars, no article text) and Stage 2 reports `HTTP fetched ok: 0`.
try:
    from . import gnews_resolver
except ImportError:
    import pipeline.gap_finder.gnews_resolver as gnews_resolver    # type: ignore

# Authority-URL finder: extracts the OFFICIAL regulator press-release link
# (e.g. efet.gr/...item/5396) from a news article's HTML, so the recall record
# points at the authority — never the news outlet that reported it.
try:
    from .authority_url_finder import (
        find_authority_url, resolve_authority_url_via_search,
        url_is_authority_item,
    )
except ImportError:
    from pipeline.gap_finder.authority_url_finder import (        # type: ignore
        find_authority_url, resolve_authority_url_via_search,
        url_is_authority_item,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

REQUEST_TIMEOUT = 15
# Authority (regulator) pages on slower government sites — e.g. nafdac.gov.ng,
# thencc.org.za — frequently exceed the 15s news timeout and return
# status=error_Timeout, dropping us to the news-body fallback. Give the
# authority press-release fetch more time and one retry so a real recall on a
# slow-loading page isn't lost to a transient timeout. (audit 2026-06-29)
AUTHORITY_TIMEOUT = 30
AUTHORITY_FETCH_ATTEMPTS = 2
MAX_WORKERS = 5                # concurrent HTTP fetches
PER_DOMAIN_DELAY = 0.8         # seconds between fetches to the same domain
MAX_BODY_CHARS = 5000          # truncate article body for LLM context

# curl_cffi browser-profile selection. chrome131 is current-generation and
# well-tested in curl_cffi 0.7+. Matches scrapers/_akamai_fetch.py choice
# so we don't have two different profiles to maintain.
_IMPERSONATE_PROFILE: str = "chrome131"

log = logging.getLogger(__name__)


# Tags/selectors to strip before extracting main content
STRIP_TAGS = ["script", "style", "noscript", "iframe", "svg", "form",
              "header", "footer", "nav", "aside"]
STRIP_SELECTORS = [
    ".sidebar", ".widget", ".ad", ".ads", ".advertisement", ".banner",
    ".comments", ".comment", ".related", ".social", ".share", ".newsletter",
    ".cookie", ".gdpr", ".breadcrumb", ".tags", "[role='complementary']",
    "[role='navigation']", "[role='banner']", "[role='contentinfo']",
]

# Candidate selectors for "main article content" in order of preference
CONTENT_SELECTORS = [
    # gov.pl / Polish & other gov portals: article lives in #main-content;
    # must come BEFORE generic "main" because <main> also wraps the portal
    # nav <h1>gov.pl Urzędy centralne</h1> chrome.
    "#main-content",
    ".editor-content",
    "article[itemtype*='Article']",
    "article",
    "main article",
    "[role='article']",
    "[role='main']",
    "main",
    ".article-body",
    ".article-content",
    ".entry-content",
    ".post-content",
    ".story-body",
    ".content-body",
    "#article-body",
    "#content",
]


# ─────────────────────────────────────────────────────────────────────────────
# DATA RECORD
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EnrichedCandidate:
    """News candidate with article body — schema-compatible with VerifiedRecord."""
    news_url: str
    news_title: str
    news_published: str
    news_source_domain: str
    # Compatibility fields (so extractor.py works unchanged) — populated from
    # news article instead of authority notice:
    efet_url: str            # = news_url (no separate authority URL in v2)
    efet_title: str          # = news_title
    efet_date_iso: str       # parsed from news_published
    efet_body: str           # news article body text
    match_score: float       # 1.0 — no fuzzy matching, we have the article
    matched_at: str
    resolved_url: str = ""   # post-redirect URL (for debugging)
    fetch_status: str = ""   # "ok", "timeout", "404", etc.

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# PER-DOMAIN THROTTLING
# ─────────────────────────────────────────────────────────────────────────────

_DOMAIN_LAST_FETCH: dict[str, float] = {}
_DOMAIN_LOCK = threading.Lock()


def _throttle_domain(url: str) -> None:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    with _DOMAIN_LOCK:
        last = _DOMAIN_LAST_FETCH.get(domain, 0)
        wait = (last + PER_DOMAIN_DELAY) - time.monotonic()
        if wait > 0:
            time.sleep(wait + random.uniform(0, 0.15))
        _DOMAIN_LAST_FETCH[domain] = time.monotonic()


# ─────────────────────────────────────────────────────────────────────────────
# HTTP FETCH — curl_cffi primary (Chrome 131 TLS), requests fallback
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_headers(cfg: CountryConfig) -> dict:
    """Browser-like request headers. UA is rotated; TLS handshake is the
    real bot-detect signal, handled below via curl_cffi impersonation."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": f"{cfg.language_code}-{cfg.code.upper()},"
                           f"{cfg.language_code};q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def _load_curl_cffi():
    """Lazy-import curl_cffi.requests. Returns the module or None on failure.

    Idempotent: caches both success and failure so we only pay the import
    cost (and emit any warning) once per process.
    """
    global _curl_cffi_imported, _curl_cffi_module, _curl_cffi_import_failed
    if _curl_cffi_imported:
        return _curl_cffi_module
    if _curl_cffi_import_failed:
        return None
    try:
        from curl_cffi import requests as cf  # type: ignore
        _curl_cffi_module = cf
        _curl_cffi_imported = True
        log.info("curl_cffi loaded — news fetches will use Chrome 131 TLS "
                 "impersonation (profile=%s)", _IMPERSONATE_PROFILE)
        return cf
    except ImportError:
        _curl_cffi_import_failed = True
        log.warning(
            "curl_cffi NOT installed — falling back to stdlib `requests`. "
            "Bot-protected sites (Akamai/Cloudflare/DataDome) will likely "
            "403. Install with: pip install 'curl-cffi>=0.7.0'"
        )
        return None


def _log_first_host(host: str) -> None:
    """Emit one log line the first time we route a given host via curl_cffi.
    Production sanity check that routing is engaging."""
    if host and host not in _curl_cffi_logged_hosts:
        _curl_cffi_logged_hosts.add(host)
        log.info("curl_cffi routing engaged: host=%s impersonate=%s",
                 host, _IMPERSONATE_PROFILE)


def _fetch_via_curl_cffi(
    url: str, cfg: CountryConfig, timeout: int | None = None
) -> tuple[str, str, str]:
    """Primary fetch path: curl_cffi Chrome 131 TLS impersonation.

    Returns (resolved_url, html, status). status in {ok, http_NNN,
    timeout, error_<class>, curl_cffi_unavailable}.
    """
    cf = _load_curl_cffi()
    if cf is None:
        return url, "", "curl_cffi_unavailable"

    host = urlparse(url).netloc.lower().split(":", 1)[0]
    _log_first_host(host)

    try:
        resp = cf.get(
            url,
            headers=_fetch_headers(cfg),
            timeout=(timeout or REQUEST_TIMEOUT),
            allow_redirects=True,
            impersonate=_IMPERSONATE_PROFILE,
        )
    except Exception as e:
        # curl_cffi.requests.RequestsError, ConnectionError, Timeout, etc.
        # Catch broadly so the caller sees a uniform string status.
        return url, "", f"error_{type(e).__name__}"

    status_code = getattr(resp, "status_code", 0)
    if status_code != 200:
        return url, "", f"http_{status_code}"

    return (getattr(resp, "url", url) or url,
            getattr(resp, "text", "") or "",
            "ok")


def _fetch_via_requests(
    url: str, cfg: CountryConfig, timeout: int | None = None
) -> tuple[str, str, str]:
    """Fallback fetch path: stdlib `requests`. Used only when curl_cffi
    isn't installed. Bot-protected sites will likely 403/410 here — this
    path exists to keep local-dev usable, not as a production strategy."""
    try:
        resp = requests.get(
            url,
            headers=_fetch_headers(cfg),
            timeout=(timeout or REQUEST_TIMEOUT),
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.url, resp.text, "ok"
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        return url, "", f"http_{code}"
    except requests.Timeout:
        return url, "", "timeout"
    except requests.RequestException as e:
        return url, "", f"error_{type(e).__name__}"


def fetch_html(url: str, cfg: CountryConfig, timeout: int | None = None) -> tuple[str, str, str]:
    """Fetch URL, follow redirects. Returns (resolved_url, html, status).

    Step 0: if this is a Google News redirector link, resolve it to the real
    publisher URL first (GN links serve a JS shell, not the article, when
    fetched server-side). If resolution fails we fall back to the GN URL
    itself (no worse than before).

    Then: try curl_cffi (Chrome 131 TLS); fall back to stdlib `requests`
    only when curl_cffi is unavailable.
    """
    # Step 0 — resolve Google News redirector → real article URL.
    if gnews_resolver.is_google_news_url(url):
        real = gnews_resolver.resolve(url)
        if real:
            url = real
        # if real == "" we keep the GN url and let the fetch try anyway

    _throttle_domain(url)

    resolved_url, html, status = _fetch_via_curl_cffi(url, cfg, timeout=timeout)
    if status == "ok":
        return resolved_url, html, status

    # curl_cffi unavailable → use stdlib requests as best-effort fallback.
    if status == "curl_cffi_unavailable":
        return _fetch_via_requests(url, cfg, timeout=timeout)

    # curl_cffi installed but THIS host blocked us. Returning the error
    # as-is. We deliberately do NOT try stdlib `requests` afterwards:
    # if Chrome 131 TLS got blocked, plain `requests` will be blocked
    # harder (different TLS fingerprint, no impersonation, faster ID).
    return resolved_url, html, status


# ─────────────────────────────────────────────────────────────────────────────
# CONTENT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _strip_noise(soup: BeautifulSoup) -> None:
    """Remove navigation, ads, comments, scripts, etc."""
    for tag_name in STRIP_TAGS:
        for el in soup.find_all(tag_name):
            el.decompose()
    for sel in STRIP_SELECTORS:
        for el in soup.select(sel):
            el.decompose()


def _find_main_content(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """Try common selectors; otherwise pick the densest text block.

    Selector pass first (fast, precise for EFET/AGES/news). If nothing matches,
    fall back to the container with the most text-but-fewest-links (readability-
    style). This rescues portals like gov.pl whose article sits in an unlabelled
    <div> alongside heavy nav chrome — picking <body> there returns mostly menu.
    """
    for sel in CONTENT_SELECTORS:
        try:
            el = soup.select_one(sel)
        except Exception:
            continue
        if el and len(el.get_text(strip=True)) > 200:
            return el

    # Density fallback: among div/section/article nodes, choose the one with the
    # most non-link text. Link-heavy nav blocks score low even if long.
    best = None
    best_score = 0.0
    for node in soup.find_all(["div", "section", "article", "main"]):
        txt = node.get_text(" ", strip=True)
        n = len(txt)
        if n < 300:
            continue
        link_txt = sum(len(a.get_text(" ", strip=True)) for a in node.find_all("a"))
        # Fraction of text that is NOT inside links; nav blocks → near 0.
        non_link = (n - link_txt) / n if n else 0.0
        score = n * non_link
        if score > best_score:
            best_score = score
            best = node
    if best is not None and best_score > 200:
        return best
    return soup.find("body")


def _normalize_text(text: str) -> str:
    """Collapse whitespace, decode entities, strip empty lines."""
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    # Collapse runs of whitespace to single space, preserve paragraph breaks
    paragraphs = []
    for line in text.split("\n"):
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            paragraphs.append(line)
    return "\n".join(paragraphs)


def extract_title(html: str) -> str:
    """Extract the press-release title from authority-page HTML.

    Tries, in order: <h1>, og:title meta, <title>. Returns "" if none.
    Used to title the record from the EFET press release rather than the
    news headline.
    """
    if not html:
        return ""
    import re as _re
    # Portal-chrome <h1> values that are NOT the press-release title
    # (gov.pl renders TWO chrome h1s: "gov.pl Urzędy centralne" and the
    # department name "Główny Inspektorat Sanitarny" before the article).
    _CHROME_H1 = (
        "gov.pl", "urzędy centralne", "urzedy centralne",
        "inspektorat sanitarny", "inspektoratsanitarny",
    )
    # <h1>...</h1> — iterate ALL h1s, skip portal chrome, take first real one.
    for _m in _re.finditer(r"<h1[^>]*>(.*?)</h1>", html, _re.IGNORECASE | _re.DOTALL):
        t = _re.sub(r"<[^>]+>", "", _m.group(1))
        t = _unescape_ws(t)
        if len(t) >= 8 and not any(c in t.lower() for c in _CHROME_H1):
            return t
    # og:title
    m = _re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)',
                   html, _re.IGNORECASE)
    if m:
        t = _unescape_ws(m.group(1))
        if len(t) >= 8:
            return t
    # <title> — strip portal suffixes like " - <dept> - Portal Gov.pl".
    m = _re.search(r"<title[^>]*>(.*?)</title>", html, _re.IGNORECASE | _re.DOTALL)
    if m:
        t = _unescape_ws(_re.sub(r"<[^>]+>", "", m.group(1)))
        # gov.pl: "<real title> - <dept> - Portal Gov.pl" → keep first segment.
        if " - " in t and ("portal gov.pl" in t.lower() or "gov.pl" in t.lower()):
            t = t.split(" - ")[0].strip()
        return t
    return ""


def _unescape_ws(t: str) -> str:
    from html import unescape as _u
    import re as _re
    return _re.sub(r"\s+", " ", _u(t)).strip()


def extract_body(html: str) -> str:
    """Extract main article text body from HTML."""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return ""
    _strip_noise(soup)
    main = _find_main_content(soup)
    if main is None:
        return ""
    text = main.get_text(separator="\n")
    text = _normalize_text(text)
    if len(text) > MAX_BODY_CHARS:
        text = text[:MAX_BODY_CHARS] + "…[truncated]"
    return text


# ─────────────────────────────────────────────────────────────────────────────
# DATE PARSING (for news_published → ISO)
# ─────────────────────────────────────────────────────────────────────────────

def parse_published_to_iso(published: str) -> str:
    """Best-effort: RFC 822 (Google News RSS) → ISO YYYY-MM-DD."""
    if not published:
        return ""
    formats = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(published, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# PER-CANDIDATE PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def enrich_candidate(cand: dict, cfg: CountryConfig,
                     verbose: bool = False) -> EnrichedCandidate:
    """Fetch article body for one candidate, return enriched record."""
    url = cand.get("url", "")

    # Diagnostic: show the GN→real resolution explicitly so Stage 2 failures
    # are visible per-URL instead of hidden behind an aggregate counter.
    if verbose:
        is_gn = gnews_resolver.is_google_news_url(url)
        if is_gn:
            real = gnews_resolver.resolve(url)
            tag = f"GN→{real[:70]}" if real else "GN→UNRESOLVED"
            print(f"    [fetch] {tag}", file=sys.stderr)
        else:
            print(f"    [fetch] direct→{url[:70]}", file=sys.stderr)

    resolved_url, html, status = fetch_html(url, cfg)
    body = extract_body(html) if html else ""

    # ── Tier 0: the candidate URL may ALREADY be an authority per-recall page ──
    # Some authorities (e.g. SZPI / potravinynapranyri.cz) publish each recall as
    # its own page that Google News indexes directly, so the discovered candidate
    # URL is itself the canonical authority URL. Check both the resolved URL
    # (after GN unwrap) and the raw URL; if either is an authority item page,
    # use it directly and skip the HTML scan / index search (which would fail
    # for such sites and wrongly reject a valid authority recall).
    authority_url = (url_is_authority_item(resolved_url, cfg)
                     or url_is_authority_item(url, cfg))
    if authority_url and verbose:
        print(f"    [authority-direct] candidate URL is itself an authority "
              f"page → {authority_url[:70]}", file=sys.stderr)

    # Find the OFFICIAL authority URL the article references (efet.gr/...).
    # The recall record must point at the regulator press release, NOT the
    # news outlet. If the article links to no authority page, authority_url
    # stays "" and the candidate is flagged no_authority_url so Stage 3 can
    # reject it instead of writing a news URL into Recalls.
    if not authority_url:
        authority_url = find_authority_url(html, cfg) if html else ""

    # Tier 2: Greek news outlets cite EFET by name but rarely hyperlink the
    # press release, so the HTML scan usually finds nothing. Fall back to
    # searching the authority site directly (site:efet.gr <recall terms>) for
    # the canonical press-release URL.
    if not authority_url:
        try:
            authority_url = resolve_authority_url_via_search(
                cand.get("title", ""), cfg, verbose=verbose
            )
        except Exception as e:
            if verbose:
                print(f"    [authority-search] error: {e}", file=sys.stderr)

    # ── Fetch the OFFICIAL authority page and extract data FROM IT ──────────
    # The news article is only the SIGNAL that a recall happened. The record's
    # data (brand, product, pathogen, batch, region…) must come from the
    # authority's own press release, never the news outlet. So once we have
    # the authority URL, fetch THAT page (curl_cffi clears the Joomla 409) and
    # use its body + title for extraction. If the authority page can't be
    # fetched, we keep the authority URL but fall back to the news body so the
    # reviewer still gets a record to check (flagged via fetch_status).
    efet_title = cand.get("title", "")          # default: news title (signal)
    efet_body = body                            # default: news body
    efet_status = status
    if authority_url:
        # Authority gov sites are often slow/blocked, so use a longer timeout
        # and retry once before giving up to the news-body fallback. A genuine
        # recall on a slow nafdac.gov.ng / thencc.org.za page must not be lost
        # to a single transient timeout. (audit 2026-06-29)
        au_html = au_status = au_body = ""
        for _attempt in range(AUTHORITY_FETCH_ATTEMPTS):
            try:
                au_resolved, au_html, au_status = fetch_html(
                    authority_url, cfg, timeout=AUTHORITY_TIMEOUT)
            except Exception as e:
                au_html, au_status = "", "authority_fetch_error"
                if verbose and _attempt == AUTHORITY_FETCH_ATTEMPTS - 1:
                    print(f"    [authority] {cfg.authority_short} fetch error: "
                          f"{e} — news fallback", file=sys.stderr)
            au_body = extract_body(au_html) if au_html else ""
            if au_body and len(au_body) >= 200:
                break                            # got the press release; stop
            # else: timeout / thin / blocked → try once more

        if au_body and len(au_body) >= 200:
            efet_body = au_body                       # authority press-release text
            efet_title = extract_title(au_html) or efet_title
            efet_status = f"authority_ok:{au_status}"
            if verbose:
                print(f"    [authority] fetched {cfg.authority_short} page: "
                      f"{len(au_body)} chars from {authority_url[:55]}",
                      file=sys.stderr)
        else:
            efet_status = f"authority_thin:{au_status}"
            if verbose:
                print(f"    [authority] {cfg.authority_short} page thin/blocked "
                      f"({len(au_body)} chars) — using news body as fallback",
                      file=sys.stderr)

    if verbose:
        au_tag = (f"authority={authority_url[:55]}" if authority_url
                  else "authority=NONE(will-reject)")
        print(f"    [fetch]   status={status} authority_body={len(efet_body)} chars "
              f"{au_tag}", file=sys.stderr)

    # efet_url + efet_body + efet_title now come from the AUTHORITY press
    # release (when fetchable). The news article supplied only the discovery
    # signal and the publish date.
    return EnrichedCandidate(
        news_url=url,
        news_title=cand.get("title", ""),
        news_published=cand.get("published", ""),
        news_source_domain=cand.get("source_domain", ""),
        efet_url=authority_url,                       # "" if no official link
        efet_title=efet_title,
        efet_date_iso=parse_published_to_iso(cand.get("published", "")),
        efet_body=efet_body,
        match_score=1.0,
        matched_at=datetime.now(timezone.utc).isoformat(),
        resolved_url=resolved_url,
        fetch_status=efet_status,
    )


# ─────────────────────────────────────────────────────────────────────────────
# BULK PARALLEL FETCH
# ─────────────────────────────────────────────────────────────────────────────

def enrich_all(
    candidates: list[dict],
    cfg: CountryConfig,
    verbose: bool = False,
) -> tuple[list[EnrichedCandidate], list[dict]]:
    """Parallel-fetch all candidate articles. Returns (enriched, failed)."""
    if not candidates:
        return [], []

    enriched: list[EnrichedCandidate] = []
    failed: list[dict] = []
    total = len(candidates)
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(enrich_candidate, c, cfg, verbose): c
                   for c in candidates}
        for fut in as_completed(futures):
            cand = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                failed.append({**cand, "fetch_error": str(e)})
                completed += 1
                continue

            completed += 1
            # A record is USABLE when it carries an authority (efet.gr) URL
            # AND a body long enough to extract from. The body now comes from
            # the EFET press release (fetch_status starts "authority_ok"), so
            # we key on efet_url presence + body length — NOT on the literal
            # string "ok" (which the new authority-fetch statuses don't equal).
            # Records WITHOUT an authority URL are passed through as enriched
            # too, so Stage 3's gate can reject them with the proper reason
            # (rather than being silently rebuilt here with the news URL).
            has_body = len(rec.efet_body) >= 100
            if not has_body and not rec.efet_url:
                # Nothing usable at all — genuine fetch failure.
                failed.append({**cand, "fetch_status": rec.fetch_status,
                               "body_len": len(rec.efet_body)})
                if verbose and completed % 25 == 0:
                    print(f"  [{completed}/{total}] last: skip "
                          f"({rec.fetch_status}, body={len(rec.efet_body)} chars)",
                          file=sys.stderr)
                continue

            enriched.append(rec)
            if verbose and completed % 25 == 0:
                print(f"  [{completed}/{total}] ok — body {len(rec.efet_body)} chars",
                      file=sys.stderr)

    if verbose:
        print(f"  enriched: {len(enriched)}, failed: {len(failed)}",
              file=sys.stderr)
    return enriched, failed


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def write_jsonl(records, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            if hasattr(r, "to_dict"):
                r = r.to_dict()
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def run_probe(cfg: CountryConfig, url: str) -> int:
    """Fetch and show body extraction for one URL — used for debugging."""
    print(f"Probing {url}", file=sys.stderr)
    cand = {"url": url, "title": "(probe)", "published": "", "source_domain": ""}
    rec = enrich_candidate(cand, cfg)
    print(f"  fetch_status: {rec.fetch_status}")
    print(f"  resolved_url: {rec.resolved_url}")
    print(f"  body length:  {len(rec.efet_body)}")
    print(f"  body[:500]:   {rec.efet_body[:500]}")
    print(f"  body[-300:]:  {rec.efet_body[-300:]}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Gap Finder — Article Fetcher"
    )
    parser.add_argument("--country", required=True, help="ISO2 code: gr, it, ...")
    parser.add_argument("--candidates", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--probe", default=None, help="Fetch one URL for testing")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap candidate count (for testing)")
    args = parser.parse_args()

    # Basic logging so first-call-per-host curl_cffi messages reach stderr.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO if args.verbose else logging.WARNING,
            format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        )

    cfg = get_country(args.country)

    if args.probe:
        return run_probe(cfg, args.probe)

    candidates_path = args.candidates or cfg.candidates_path
    out_path = args.out or cfg.verified_path

    candidates = read_jsonl(candidates_path)
    print(f"Loaded {len(candidates)} candidates from {candidates_path}",
          file=sys.stderr)

    if args.limit and args.limit < len(candidates):
        candidates = candidates[:args.limit]
        print(f"  capped to {len(candidates)} for testing", file=sys.stderr)

    if not candidates:
        write_jsonl([], out_path)
        return 0

    print(f"Fetching article bodies (parallel, {MAX_WORKERS} workers)...",
          file=sys.stderr)
    t0 = time.monotonic()
    enriched, failed = enrich_all(candidates, cfg, verbose=args.verbose)
    elapsed = time.monotonic() - t0
    print(f"  done in {elapsed:.1f}s — {len(enriched)} enriched, "
          f"{len(failed)} failed", file=sys.stderr)

    write_jsonl(enriched, out_path)
    write_jsonl(failed, cfg.unmatched_path)
    print(f"Wrote: {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
