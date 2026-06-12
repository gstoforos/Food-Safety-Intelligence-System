"""
AFTS Gap Finder — Google News URL resolver.

PROBLEM
-------
Google News RSS feeds return article links of the form:
    https://news.google.com/rss/articles/CBMi<token>?oc=5
These are NOT direct publisher links and they do NOT HTTP-redirect when
fetched server-side. Fetching one returns a JavaScript consent/redirect
shell (~150 chars of body, no article text). That is exactly why Stage 2
shows `HTTP fetched ok: 0` — every body is the GN shell, never the article.

SOLUTION
--------
Resolve the GN URL to the real publisher URL BEFORE fetching the body.
Two strategies, tried in order:

  1. PROTOBUF DECODE (offline, instant):
     Older GN tokens are URL-safe base64 of a protobuf whose field 1 is the
     real URL as a length-prefixed UTF-8 string. Decode and extract it. No
     network call. Works for a large fraction of tokens.

  2. BATCHEXECUTE API (online, one POST):
     Newer "opaque" tokens can't be decoded offline. Google News exposes an
     internal endpoint that maps a token → real URL. We replicate the call
     the GN web app makes. One short POST per token, curl_cffi Chrome 131.

If both fail, return "" and the caller fetches the GN URL directly (which
will fail body extraction, falling back to the RSS description — the old
behaviour, no worse than before).

This module has NO side effects and no hard dependency on curl_cffi at
import time (lazy). Safe to unit-test offline (strategy 1 path).
"""

from __future__ import annotations

import base64
import json
import logging
import re
from typing import Optional
from urllib.parse import urlparse

_log = logging.getLogger(__name__)

_GN_HOSTS = {"news.google.com", "www.news.google.com"}
_ARTICLE_RE = re.compile(r"/(?:rss/)?articles/([A-Za-z0-9_\-]+)")

# curl_cffi lazy import (shared pattern with article_fetcher / news_scraper)
_cf_mod = None
_cf_state = "unloaded"
_IMPERSONATE_PROFILE = "chrome131"


def _load_curl_cffi():
    global _cf_mod, _cf_state
    if _cf_state == "ok":
        return _cf_mod
    if _cf_state == "failed":
        return None
    try:
        from curl_cffi import requests as cf  # type: ignore
        _cf_mod = cf
        _cf_state = "ok"
        return cf
    except ImportError:
        _cf_state = "failed"
        return None


def is_google_news_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host in _GN_HOSTS and "/articles/" in url


# ──────────────────────────────────────────────────────────────────────────
# Strategy 1 — offline protobuf decode
# ──────────────────────────────────────────────────────────────────────────

def _decode_token_offline(token: str) -> str:
    """Decode the real URL from a GN token via protobuf base64. '' on failure."""
    body = token
    pad = len(body) % 4
    if pad:
        body += "=" * (4 - pad)
    try:
        raw = base64.urlsafe_b64decode(body)
    except Exception:
        return ""
    # Tolerant byte scan: the real URL appears as a UTF-8 substring inside
    # the protobuf. Pick the longest non-google http(s) URL.
    try:
        text = raw.decode("latin-1")
    except Exception:
        return ""
    urls = re.findall(r'https?://[^\x00-\x1f\x7f-\xff"\s\\]+', text)
    real = [u for u in urls
            if "google.com" not in u and "gstatic" not in u
            and "googleusercontent" not in u]
    if not real:
        return ""
    return max(real, key=len)


# ──────────────────────────────────────────────────────────────────────────
# Strategy 2 — batchexecute online resolve
# ──────────────────────────────────────────────────────────────────────────

def _resolve_token_online(gn_url: str, token: str, timeout: int = 15) -> str:
    """Resolve an opaque GN token via the batchexecute endpoint. '' on failure."""
    cf = _load_curl_cffi()
    if cf is None:
        return ""

    # The GN article page embeds two values needed for the call. We fetch the
    # article page once to harvest them, then POST batchexecute.
    try:
        page = cf.get(gn_url, impersonate=_IMPERSONATE_PROFILE, timeout=timeout)
    except Exception:
        return ""
    if getattr(page, "status_code", 0) != 200:
        return ""
    html = getattr(page, "text", "") or ""

    # Harvest signature + timestamp the GN app uses for this article.
    m_sig = re.search(r'data-n-a-sg="([^"]+)"', html)
    m_ts = re.search(r'data-n-a-ts="([^"]+)"', html)
    if not (m_sig and m_ts):
        return ""
    signature, timestamp = m_sig.group(1), m_ts.group(1)

    payload = [[[
        "Fbv4je",
        json.dumps(["garturlreq",
                    [["X", "X", ["X", "X"], None, None, 1, 1,
                      "US:en", None, 1, None, None, None, None, None, 0, 1],
                     "X", "X", 1, [1, 1, 1], 1, 1, None, 0, 0, None, 0],
                    token, timestamp, signature]),
        None, "generic",
    ]]]
    data = {"f.req": json.dumps(payload)}
    try:
        resp = cf.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            data=data,
            headers={"Content-Type":
                     "application/x-www-form-urlencoded;charset=UTF-8"},
            impersonate=_IMPERSONATE_PROFILE,
            timeout=timeout,
        )
    except Exception:
        return ""
    if getattr(resp, "status_code", 0) != 200:
        return ""

    text = getattr(resp, "text", "") or ""
    # Response is the XSSI-guarded batchexecute format. Find the array that
    # contains "garturlres" then the real URL string after it.
    m = re.search(r'\["garturlres",\s*"(https?://[^"]+)"', text)
    if m:
        return m.group(1).encode().decode("unicode_escape")
    # Fallback: any non-google http URL in the response
    urls = re.findall(r'https?://[^\\"\s]+', text)
    real = [u for u in urls if "google.com" not in u and "gstatic" not in u]
    return real[0] if real else ""


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────

_CACHE: dict[str, str] = {}


def resolve(url: str) -> str:
    """Resolve a Google News URL → real publisher URL.

    Returns the original url unchanged if it isn't a GN link.
    Returns '' if it IS a GN link but cannot be resolved (caller decides
    whether to fetch the GN url directly as a last resort).
    """
    if not is_google_news_url(url):
        return url
    if url in _CACHE:
        return _CACHE[url]

    m = _ARTICLE_RE.search(url)
    if not m:
        _CACHE[url] = ""
        return ""
    token = m.group(1)

    # Strategy 1: offline decode (instant, no network)
    real = _decode_token_offline(token)
    if real:
        _CACHE[url] = real
        return real

    # Strategy 2: online batchexecute resolve
    real = _resolve_token_online(url, token)
    _CACHE[url] = real  # may be '' — cached either way
    return real


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    for arg in sys.argv[1:]:
        print(f"{arg}\n  → {resolve(arg)!r}")
