"""
URL resolver — the EFET method, done properly.

What this does (zero search engines, zero JS, zero guessing):

  1. ONCE per source-run: fetch the regulator's own recall listing page
     (e.g. https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts).
     Parse every <a href> on it that matches the recall-page pattern.
     Build a local index:  [(anchor_text, full_url), ...]

  2. PER ACCEPT: the GNews title gives us company/product keywords. Match
     the keywords against the local index by content-word overlap.
     Return the FDA URL with the highest match.

  3. If no match in the regulator's listing, the recall hasn't been posted
     to that page yet (FDA can lag a day or two), OR the regulator blocks
     GitHub IPs. Try a second fallback: decode the Google News URL → fetch
     publisher article → scan for authority hrefs in article body. This
     works for some Google News URL formats but not all.

  4. If both fail, return None — caller writes news URL + flags
     pending_no_auth_url for manual review.

This is the EFET pattern. The regulator is the source of truth.
"""

from __future__ import annotations

import base64
import random
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


_TIMEOUT = 20

_USER_AGENTS = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)


def _headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": ("text/html,application/xhtml+xml,application/xml;"
                   "q=0.9,*/*;q=0.8"),
        "Accept-Language": "en-US,en;q=0.9",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage A: build authority's own recall index (the reliable path)
# ─────────────────────────────────────────────────────────────────────────────

# Process-level cache so multiple records in one run share one fetch.
# Key: tuple(index_urls). Value: list of (anchor_text, full_url).
_AUTH_INDEX: dict = {}


def _fetch_authority_index(index_urls: tuple,
                            authority_domain: str,
                            authority_url_pattern: Optional[re.Pattern],
                            ) -> list[tuple]:
    """Fetch the regulator's recall listing pages once and parse all the
    individual recall URLs into a local index.

    Returns: [(anchor_text, full_url), ...]. Empty list if all fetches fail.
    """
    key = (tuple(index_urls), authority_domain)
    if key in _AUTH_INDEX:
        return _AUTH_INDEX[key]

    entries: list[tuple] = []
    for listing_url in index_urls:
        try:
            resp = requests.get(listing_url, headers=_headers(),
                                 timeout=_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200:
                print(f"  [WARN] authority index fetch {listing_url} → "
                      f"HTTP {resp.status_code}")
                continue
        except Exception as e:   # noqa: BLE001
            print(f"  [WARN] authority index fetch {listing_url} → {e}")
            continue

        soup = BeautifulSoup(resp.content, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            full = urljoin(listing_url, href)
            if authority_domain not in full:
                continue
            # Drop URL fragment, query for pattern-matching purposes
            p = urlparse(full)
            path_q = f"{p.path}?{p.query}" if p.query else p.path
            if authority_url_pattern and not authority_url_pattern.search(path_q):
                continue
            text = a.get_text(" ", strip=True)
            if not text or len(text) < 15:
                # Anchor text too short to be a real recall headline
                continue
            entries.append((text, full))

    # Dedup by URL, preserve order
    seen: set[str] = set()
    deduped: list[tuple] = []
    for t, u in entries:
        if u not in seen:
            seen.add(u)
            deduped.append((t, u))

    _AUTH_INDEX[key] = deduped
    print(f"  [authority index] {authority_domain}: "
          f"{len(deduped)} recall pages indexed from "
          f"{len(index_urls)} listing URL(s)")
    return deduped


# Words to drop from titles when scoring keyword overlap
_STOPWORDS = frozenset((
    "the","a","an","of","in","on","at","to","for","with","and","or","but",
    "due","after","over","as","by","from","is","are","was","were","be",
    "recall","recalled","recalls","recalling","alert","alerts",
    "warning","warns","warned",
    "fda","usda","fsis","cfia","anvisa","cofepris","anmat","invima",
    "digesa","bpom","sfa","cfs","tfda","mfds","sfanz","mpi",
    "announces","announcement","announced",
    "nationwide","national","states","state",
    "popular","more","new","some","all","this","that",
    "food","foods","product","products",
    "msn","yahoo","aol","cnn","fox","bbc","newsweek",
    "because","over","because","may","could","might",
))


def _title_keywords(title: str) -> set:
    """Extract distinctive content-word set from a title for matching."""
    if not title:
        return set()
    # Strip the trailing "- Publisher" attribution Google News appends
    cleaned = re.split(r"\s+[-–—]\s+[A-Z][\w. &]+$", title)[0]
    cleaned = re.sub(r"[^\w\s'-]", " ", cleaned)
    words = re.findall(r"[A-Za-z][A-Za-z0-9'-]{2,}", cleaned)
    return {w.lower() for w in words if w.lower() not in _STOPWORDS
            and len(w) >= 3}


def _match_against_index(title: str,
                          index: list,
                          min_overlap: int = 2) -> Optional[str]:
    """Return the index URL whose anchor text best matches `title`.

    Score = number of overlapping content words. Requires at least
    `min_overlap` matches to return a result (avoids false positives
    from very short titles).
    """
    title_kw = _title_keywords(title)
    if not title_kw or not index:
        return None

    best_score = 0
    best_url = None
    for anchor_text, url in index:
        anchor_kw = _title_keywords(anchor_text)
        if not anchor_kw:
            continue
        overlap = len(title_kw & anchor_kw)
        if overlap > best_score:
            best_score = overlap
            best_url = url

    if best_score >= min_overlap:
        return best_url
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Stage B (fallback): decode Google News URL → fetch publisher → scan
# ─────────────────────────────────────────────────────────────────────────────

_GN_URL_RE = re.compile(
    r"^https?://news\.google\.com/(?:rss/)?articles/([A-Za-z0-9_-]+)")


def decode_gnews_url(url: str) -> Optional[str]:
    """Best-effort extraction of the publisher URL from a Google News
    redirector URL. Works for older URL formats; newer formats are
    server-side tokens and will return None.
    """
    if not url:
        return None
    if "news.google.com" not in url:
        return url
    m = _GN_URL_RE.match(url)
    if not m:
        return None
    token = m.group(1)
    padded = token + "=" * (-len(token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded)
    except Exception:   # noqa: BLE001
        return None
    candidates = re.findall(
        rb"https?://[A-Za-z0-9\-\._~:/\?\#\[\]@!\$&'\(\)\*\+,;=%]+",
        decoded)
    for c in candidates:
        u = c.decode("utf-8", errors="ignore")
        host = (urlparse(u).hostname or "").lower()
        if any(s in host for s in ("google.", "gstatic.", "youtube.")):
            continue
        return u
    return None


def _fetch_article(url: str) -> Optional[BeautifulSoup]:
    try:
        resp = requests.get(url, headers=_headers(),
                            timeout=_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return None
        return BeautifulSoup(resp.content, "html.parser")
    except Exception:   # noqa: BLE001
        return None


def _scan_article_for_authority(soup: BeautifulSoup,
                                 authority_domain: str,
                                 pattern: Optional[re.Pattern],
                                 base: str) -> Optional[str]:
    if not soup:
        return None
    for a in soup.find_all("a", href=True):
        href = urljoin(base, a["href"].strip())
        if authority_domain not in href:
            continue
        p = urlparse(href)
        path_q = f"{p.path}?{p.query}" if p.query else p.path
        if pattern and not pattern.search(path_q):
            continue
        return href
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def resolve_authority_url(news_url: str,
                          title: str,
                          authority_domain: str,
                          authority_url_pattern: Optional[re.Pattern] = None,
                          index_urls: tuple = (),
                          _cache: dict = {},
                          ) -> Optional[str]:
    """
    Find the regulator's own URL for a recall.

    PRIMARY: match `title` keywords against the regulator's recall listing
    (if `index_urls` is set and the listing is fetchable).

    FALLBACK: decode the Google News URL to the publisher article, fetch
    it, scan for authority hrefs in its body.

    Returns the authority URL, or None.
    """
    if not authority_domain or not title:
        return None

    cache_key = (title, authority_domain)
    if cache_key in _cache:
        return _cache[cache_key]

    # ─── Stage A: regulator's own listing (preferred path) ───────────────
    if index_urls:
        index = _fetch_authority_index(index_urls, authority_domain,
                                        authority_url_pattern)
        if index:
            match = _match_against_index(title, index, min_overlap=2)
            if match:
                _cache[cache_key] = match
                return match

    # ─── Stage B: GNews decode + publisher article scan (fallback) ───────
    publisher = decode_gnews_url(news_url) if news_url else None
    if publisher and "google.com" not in (urlparse(publisher).hostname or ""):
        soup = _fetch_article(publisher)
        match = _scan_article_for_authority(soup, authority_domain,
                                             authority_url_pattern, publisher)
        if match:
            _cache[cache_key] = match
            return match

    _cache[cache_key] = None
    return None
