"""Akamai-protected host fetch helper — curl_cffi-based TLS impersonation.

WHY THIS MODULE EXISTS (audit 2026-05-10)
==========================================
Four regulator hosts return HTTP 403 / no-response from GitHub Actions
runner IPs even with the standard sec-fetch / sec-ch-ua headers:

    www.fda.gov          (FDA listing, RSS, datatables)
    www.fsis.usda.gov    (USDA FSIS recall API)
    www.fda.gov.ph       (Philippines FDA)
    www.gov.il           (Israel MoH)

All four sit behind Akamai bot-detection. Akamai inspects FIVE layers,
not just headers:

  1. TLS fingerprint (JA3/JA4) — Python `requests` does its TLS
     handshake completely differently from real Chrome. Distinct
     fingerprint, instantly recognised.
  2. HTTP/2 frame ordering — Real browsers send specific SETTINGS
     frames; `requests` defaults to HTTP/1.1.
  3. IP reputation — GitHub Actions runner blocks are well-known
     datacenter ranges, flagged by IP-intel feeds.
  4. Sensor cookie challenge — JS challenge sets _abck/bm_sz/ak_bmsc
     cookies (only matters on some pages).
  5. Header order/casing — Browsers send headers in specific order;
     Python alphabetises.

curl_cffi (a Python wrapper around curl-impersonate) handles layers
1, 2, and 5 automatically by performing the TLS handshake byte-for-byte
identical to real Chrome. Empirically gets through ~70% of Akamai-
protected sites without needing residential proxies.

Layers 3 and 4 are NOT addressed by this module. If a host still 403s
after this fix, the next escalation is a residential proxy (Webshare,
IPRoyal — ~$3/month) or a managed scraping API (ScrapingBee).

DESIGN DECISIONS
================
1. Per-host opt-in. Only hosts in _AKAMAI_HOSTS route through
   curl_cffi. Every other host uses the existing requests-based path
   (faster: ~100ms vs ~250ms per call, more stable, no new deps in
   the hot path).

2. Lazy import. curl_cffi is only imported on first call. If the
   package isn't installed, log a warning and return None (caller
   treats as fetch failure — graceful degradation).

3. Stateless. Each call is a fresh curl_cffi request, no session
   pooling. The four target hosts are simple GETs (one request gets
   everything we need), so connection reuse is not worth the
   complexity of mixing curl_cffi.Session + requests.Session.

4. Browser profile = chrome131. Round version, well-tested, recent
   enough (real-world Chrome is at 130-140 today). Bump if Akamai
   tightens.

5. First-call-per-host logging. Production logs explicitly show
   "FDA listing: routed via curl_cffi (chrome131 TLS impersonation)"
   the first time a host hits this path each scrape run. Lets us
   verify in production that the fix is engaging.

6. Response object compatibility. curl_cffi.requests.Response has
   the same attributes we use elsewhere: .status_code, .text,
   .content, .headers, .json(), .url, .cookies. NOT a
   requests.Response instance — code that does isinstance() checks
   would break, but no scraper in this codebase does that (verified
   by grep at refactor time).
"""
from __future__ import annotations
from typing import Any, Optional
import logging
from urllib.parse import urlparse

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# Per-host opt-in. Add a host here ONLY when production logs show 403
# / no-response with the standard Akamai header bypass. Don't pre-
# emptively route — the requests path is faster and more stable for
# normal hosts, and rotating non-broken hosts through curl_cffi adds
# no value.
# ────────────────────────────────────────────────────────────────────
_AKAMAI_HOSTS: frozenset = frozenset({
    "www.fda.gov",          # FDA Recalls listing + press RSS + datatables
    "www.fsis.usda.gov",    # USDA FSIS recall API
    "www.fda.gov.ph",       # Philippines FDA advisories
    "www.gov.il",           # Israel MoH food recall page
})

# Browser fingerprint profile. Chrome131 = current-generation, well
# supported by curl_cffi 0.15+. See _akamai_fetch.py docstring §4.
_IMPERSONATE_PROFILE: str = "chrome131"

# Track which hosts we've already logged a "routing via curl_cffi"
# message for, this Python process. Keeps log noise down — the first
# call per host per process is enough to confirm routing is engaging.
_logged_hosts: set = set()


def is_akamai_host(url: str) -> bool:
    """Return True if URL's host is in the curl_cffi opt-in list."""
    try:
        host = urlparse(url).netloc.lower()
        # Strip port if present (e.g. host:443)
        host = host.split(":", 1)[0]
        return host in _AKAMAI_HOSTS
    except Exception:
        return False


def fetch_via_curl_cffi(
    url: str,
    method: str = "GET",
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Optional[Any]:
    """Fetch an Akamai-protected URL using curl_cffi TLS impersonation.

    Returns a curl_cffi.requests.Response on success (API-compatible
    with requests.Response for the attrs we use: .status_code, .text,
    .content, .headers, .json()).

    Returns None if curl_cffi is not installed or the request fails.
    Caller treats both identically (existing fetch() callers already
    handle None as a fetch failure).

    Accepts the same kwargs as requests.get/request: headers, params,
    data, json, allow_redirects, etc. Anything else is silently
    ignored — curl_cffi has the same shape as requests for these args.
    """
    # Lazy import — pay the import cost only when we hit an Akamai host
    try:
        from curl_cffi import requests as cf
    except ImportError:
        log.warning(
            "curl_cffi not installed — Akamai host %s will likely 403. "
            "Install: pip install curl-cffi (or add curl-cffi>=0.7.0 "
            "to requirements.txt)",
            url,
        )
        return None

    # First-call logging per host (helps production diagnostics)
    host = urlparse(url).netloc.lower().split(":", 1)[0]
    if host not in _logged_hosts:
        log.info(
            "Akamai routing engaged for host=%s via curl_cffi "
            "(impersonate=%s)",
            host, _IMPERSONATE_PROFILE,
        )
        _logged_hosts.add(host)

    # Forward the kwargs that curl_cffi understands. We deliberately
    # don't pass `cookies=` or session state — these calls are stateless
    # by design (single-page fetches, no login).
    cf_kwargs: dict = {
        "timeout": timeout,
        "impersonate": _IMPERSONATE_PROFILE,
    }
    for key in ("headers", "params", "data", "json", "allow_redirects"):
        if key in kwargs and kwargs[key] is not None:
            cf_kwargs[key] = kwargs[key]

    try:
        resp = cf.request(method, url, **cf_kwargs)
        return resp
    except Exception as exc:
        # curl_cffi raises curl_cffi.requests.RequestsError on transport
        # errors — distinct from requests.RequestException. Catch broadly
        # to ensure caller sees a uniform None-on-failure interface.
        log.warning(
            "curl_cffi fetch failed for %s: %s: %s",
            url, type(exc).__name__, exc,
        )
        return None
