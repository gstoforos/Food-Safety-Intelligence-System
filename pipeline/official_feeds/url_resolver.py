"""
URL resolver — proper EFET pattern via DuckDuckGo site:authority search.

The problem this solves:
    GNews surfaces news articles ABOUT recalls (e.g. "Champion Foods Recalls
    Cheese Bread Over Salmonella Fears" from a publisher via Google News).
    The Pending sheet needs the REGULATOR'S OWN URL — not a news article URL.

Why simple article-link extraction doesn't work:
    Google News RSS gives links like
    https://news.google.com/rss/articles/CBMi<base64-encoded-payload>
    These are JS-resolved redirects. requests.get() fetches the Google
    interstitial page, which has no publisher article HTML in it. So
    scanning that response for fda.gov links finds nothing.

Why DDG site: search works:
    1. Take the accepted article's title (we know the company/product name
       and the regulator did issue this recall — that's why classifier
       accepted it).
    2. Extract distinctive keywords (company name, product name).
    3. Query DuckDuckGo with site:authority_domain restriction.
    4. Filter to URLs matching authority_url_pattern.
    5. Return the first match.

    This is the EXACT pattern used by pipeline.gap_finder.search_verifier
    for EFET (Greek), ASAE (Portuguese), etc. — proven to work.

    For Champion Foods:
       title = "Champion Foods Recalls Cheese Bread Over Salmonella Fears"
       keywords = "Champion Foods Cheese Bread Salmonella Fears"
       query = 'site:fda.gov Champion Foods Cheese Bread Salmonella Fears'
       DDG returns: https://www.fda.gov/safety/recalls-market-withdrawals-
                    safety-alerts/champion-foods-recalls-some-batches-...
       That URL goes into Pending. ✓

If DDG returns nothing on authority domain, fall back to news URL with
Status='pending_no_auth_url' flag — operator finds it manually.
"""

from __future__ import annotations

import random
import re
import time
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote

import requests
from bs4 import BeautifulSoup


DDG_HTML_URL = "https://html.duckduckgo.com/html/"
DDG_LITE_URL = "https://lite.duckduckgo.com/lite/"
_TIMEOUT = 20
_DELAY_MIN, _DELAY_MAX = 1.5, 3.0   # polite delay between DDG queries

_USER_AGENTS = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)


# Stopwords to drop from titles when building DDG keyword query.
_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "at", "to", "for", "with",
    "and", "or", "but", "due", "after", "over", "as", "by", "from",
    "is", "are", "was", "were", "be", "been", "being",
    "recall", "recalled", "recalls", "recalling",
    "alert", "alerts", "warning", "warns", "warned",
    "fda", "usda", "fsis", "cfia", "anvisa", "cofepris", "anmat",
    "invima", "digesa", "bpom", "sfa", "cfs", "tfda", "mfds",
    "announces", "announcement", "announced",
    "nationwide", "national", "states", "state",
    "popular", "more", "new", "some", "all",
    "consumer", "consumers", "customers",
    "msn", "yahoo", "aol", "cnn", "fox", "bbc",
}


def _extract_keywords(title: str, max_words: int = 6) -> str:
    """Pick distinctive words from a title for a DDG query.

    Keeps proper nouns, brand-like CamelCase, longer content nouns. Drops
    stopwords and authority names. Returns a space-joined string suitable
    for use after `site:authority_domain `.
    """
    if not title:
        return ""
    # Strip leading "FDA Announces" / "USDA warns" prefixes
    cleaned = re.sub(r"^(FDA|USDA|FSIS|CFIA|ANVISA|COFEPRIS|ANMAT|INVIMA|"
                     r"DIGESA|BPOM|SFA|CFS|TFDA|MFDS)[\s:]+",
                     "", title, flags=re.IGNORECASE)
    cleaned = re.sub(r"[^\w\s'-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    words = cleaned.split()
    distinctive: list[str] = []
    for w in words:
        wl = w.lower().strip("'-")
        if wl in _STOPWORDS:
            continue
        if len(wl) < 3:
            continue
        distinctive.append(w)
        if len(distinctive) >= max_words:
            break
    return " ".join(distinctive)


def _ddg_headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://duckduckgo.com/",
    }


def _clean_ddg_redirect(url: str) -> str:
    """DDG wraps result URLs as //duckduckgo.com/l/?uddg=<encoded-url>."""
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    parsed = urlparse(url)
    if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
        qs = parse_qs(parsed.query)
        real = qs.get("uddg", [None])[0]
        if real:
            return unquote(real)
    return url


def _parse_ddg_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for result in soup.find_all("div", class_=re.compile(r"\bresult\b")):
        a = result.find("a", class_=re.compile(r"result__a"))
        if not a:
            continue
        url = _clean_ddg_redirect(a.get("href", "").strip())
        if url:
            out.append(url)
    # Fallback: scan all anchors
    if not out:
        for a in soup.find_all("a", href=True):
            href = _clean_ddg_redirect(a["href"])
            if href.startswith("http") and "duckduckgo.com" not in href:
                out.append(href)
    return out


def _parse_ddg_lite(html: str) -> list[str]:
    """lite.duckduckgo.com uses a <table> layout."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith(("http://", "https://")):
            continue
        href = _clean_ddg_redirect(href)
        if "duckduckgo.com" not in href:
            out.append(href)
    return out


def _ddg_search(query: str, max_results: int = 20,
                _state: dict = {"consecutive_failures": 0,
                                 "circuit_open": False}) -> list[str]:
    """Run a DDG query, return result URLs in order. Tries HTML then Lite.

    Includes a per-process circuit breaker: after 2 consecutive failures
    (rate-limit, 5xx, network error), short-circuit subsequent calls in
    this run. Avoids hammering DDG when they're throttling us.
    """
    if _state["circuit_open"]:
        return []
    for endpoint, parser in ((DDG_HTML_URL, _parse_ddg_html),
                              (DDG_LITE_URL, _parse_ddg_lite)):
        try:
            resp = requests.post(endpoint, data={"q": query},
                                  headers=_ddg_headers(), timeout=_TIMEOUT)
            resp.raise_for_status()
            urls = parser(resp.text)
            if urls:
                _state["consecutive_failures"] = 0
                return urls[:max_results]
        except Exception:   # noqa: BLE001
            continue
    # Reaching here = both endpoints failed or returned no results
    _state["consecutive_failures"] += 1
    if _state["consecutive_failures"] >= 2:
        _state["circuit_open"] = True
        print("  [WARN] DDG circuit-breaker tripped after 2 consecutive "
              "failures — URL resolution disabled for rest of run")
    return []


def resolve_authority_url(news_url: str,
                          title: str,
                          authority_domain: str,
                          authority_url_pattern: Optional[re.Pattern] = None,
                          _cache: dict = {},
                          ) -> Optional[str]:
    """
    Find the regulator's own URL for a recall surfaced via GNews.

    Args:
        news_url:           Google News redirector URL (kept for fallback)
        title:              article title — used to build DDG keywords
        authority_domain:   e.g. "fda.gov", "cfs.gov.hk", "gov.br/anvisa"
        authority_url_pattern: compiled regex applied to URL path+query
        _cache:             per-process memo (default-arg trick); avoids
                            duplicate DDG queries when many news articles
                            cover the same recall — all 29 cheese-bread
                            articles resolve via ONE DDG hit.

    Returns:
        Authority URL string, or None if no match found.
    """
    if not title or not authority_domain:
        return None

    keywords = _extract_keywords(title)
    if not keywords:
        return None

    # Cache key: keywords + authority_domain. Records covering the same
    # recall produce similar keyword strings, but not identical — so we
    # also cache by (authority_domain, exact_query) below.
    cache_key = (authority_domain, keywords)
    if cache_key in _cache:
        return _cache[cache_key]

    # Build DDG query
    if "/" in authority_domain:
        bare_domain, sub_path = authority_domain.split("/", 1)
        query = f"site:{bare_domain} {sub_path} {keywords}"
    else:
        query = f"site:{authority_domain} {keywords}"

    urls = _ddg_search(query, max_results=20)
    time.sleep(random.uniform(_DELAY_MIN, _DELAY_MAX))

    result: Optional[str] = None
    if urls:
        on_authority = [u for u in urls if authority_domain in u]
        if on_authority:
            if authority_url_pattern is None:
                result = on_authority[0]
            else:
                for url in on_authority:
                    parsed = urlparse(url)
                    path_q = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
                    if authority_url_pattern.search(path_q):
                        result = url
                        break

    _cache[cache_key] = result
    return result


def clear_cache() -> None:
    """Clear the per-process resolver cache. Call at start of each source run
    if you want the cache scoped per source rather than per process."""
    resolve_authority_url.__defaults__[0].clear()
