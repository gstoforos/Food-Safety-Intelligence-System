"""
URL validator — confirms every recall URL is reachable AND specific (not a landing page).

Key design decisions:
  - Uses browser-like headers to reduce false 403s from gov/regulatory sites
  - Distinguishes "truly broken" (404/410/5xx) from "bot-blocked" (403 on known
    allowlisted gov domains) so we don't nuke a valid rappel.conso.gouv.fr link
    just because their WAF rejects python-requests.
  - Pattern match for landing-page / category URLs is still strict.

ONE INVARIANT, ADDED 2026-09-25 (read this before touching should_blank_url)
==========================================================================
A URL is deleted from a row ONLY when a client the server answers HONESTLY
has read the server's answer and that answer was "gone".

"I could not read it" is not "it is gone". That distinction is the whole
content of this module, and it has now been learned four times:

  2026-07-29  TLS chain failure on rappel.conso.gouv.fr was classified
              "network" → real recalls archived to Weekly_Rejected.
  2026-09-01  The guardian blanked three real FDA URLs.
  2026-09-02  _provenance.py wrote it down: "www.fda.gov, www.fsis.usda.gov,
              www.fda.gov.ph and www.gov.il sit behind Akamai bot detection
              ... Plain `requests` gets HTTP 404 from them for EVERY page —
              real notices included ... With the 404 guard in check() now
              live, the same blindness would instead REJECT every real
              FDA/FSIS row."
  2026-09-25  Exactly that. All four FDA rows in Pending carried
              "REJECTED: http_error", and the Galil Importing cinnamon/lead
              recall had its URL blanked —
              "[URL-guardian 2026-09-25: blanked http_error
               https://www.fda.gov/safety/recalls-market-withdrawals-s...]"
              — leaving a URL-less twin of a row that was perfectly fine,
              because _dedup_key is URL-primary. 98 register rows sit on
              those four hosts; each was one guardian pass from the same.

The hole was never in the tolerance list. www.fda.gov and www.fsis.usda.gov
have been in BOT_HOSTILE_DOMAINS all along. The hole was that the list was
consulted for status 403 ONLY, and Akamai answers 404.

So the tolerance is no longer a list of statuses to forgive. Every result
now carries `verified_dead`, set only by a client whose verdict is
trustworthy for that host, and should_blank_url refuses to act without it.

Run as part of the 4-hour guardian pipeline, before commit.
"""
from __future__ import annotations
import re
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import requests

log = logging.getLogger(__name__)

# Rotating browser-like headers (single UA string, good enough — we're not crawling aggressively)
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,it;q=0.7,de;q=0.6",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}
TIMEOUT = 15

# Patterns indicating generic / homepage / category URLs (NOT specific recalls)
GENERIC_URL_PATTERNS = [
    r"/categorie/\d+/?$",                          # RappelConso category
    r"/categorie/0/\d+/[a-z]+/?$",
    r"/anakleiseis-cat/?$",                        # EFET landing
    r"/alertas_alimentarias/?$",                   # AESAN landing
    r"/liste/lebensmittel/bundesweit/?$",          # BVL landing
    r"/rubrik/lebensmittel/?$",                    # produktwarnung.eu category listing
    r"/portal/news/p3_2_1_3\.jsp",                 # Salute IT generic notizie
    r"/food-recalls/?$",                           # FSANZ landing
    r"/recalls?/?$",                               # generic /recalls
    r"/alerts?/?$",                                # generic /alerts
    r"/news/?$",                                   # generic news
    r"/category/[^/]+/?$",                         # generic category
    r"/tag/[^/]+/?$",                              # generic tag
    r"/search/?",                                  # search pages
    r"^https?://[^/]+/?$",                         # bare domain
    # FDA landings
    r"/safety/recalls-market-withdrawals-safety-alerts/?$",
    r"/safety/recalls/?$",
    # USDA FSIS landings
    r"/recalls-alerts/?$",
    r"/recalls-public-health-alerts/?$",
    # CFIA landings
    r"/food-recall-warnings/?$",
    r"/food-recall-warnings-and-allergy-alerts/?$",
    # FSA / FSAI landings
    r"/news-alerts/?$",
    r"/consumer/food-alerts/?$",
    # General "recall list" landing at any depth
    r"/recall-and-advice-list/?$",
    r"/list-of-recalls/?$",
    r"/food-alert-list/?$",
]

# Sites known to 403 python-requests but serve valid pages to real browsers.
# For these, a 403 response is NOT treated as a broken URL — the pattern check
# (is_generic_url) is what we trust for URL quality here.
BOT_HOSTILE_DOMAINS = {
    "rappel.conso.gouv.fr",
    "recalls-rappels.canada.ca",
    "www.food.gov.uk",
    "food.gov.uk",
    "www.fda.gov",
    "www.fsis.usda.gov",
    "www.foodstandards.gov.au",
    "www.inspection.canada.ca",
    "inspection.canada.ca",
    "www.lebensmittelwarnung.de",
    "lebensmittelwarnung.de",
    "ilfattoalimentare.it",
    "webgate.ec.europa.eu",
    "www.salute.gov.it",
    "www.efet.gr",
    "www.aesan.gob.es",
    "www.cfs.gov.hk",
}


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def is_generic_url(url: str) -> bool:
    """Pattern-based check for landing-page / category URLs. No HTTP call."""
    if not url:
        return False
    for pat in GENERIC_URL_PATTERNS:
        if re.search(pat, url, re.I):
            return True
    return False


def is_probably_valid_url(url: str) -> bool:
    """
    Fast local-only check for the weekly report's Top-5 filter.
    No network call. Excludes empty, non-HTTP, generic, and too-shallow URLs.
    """
    if not url or len(url) < 20:
        return False
    u = url.strip()
    if not u.lower().startswith(("http://", "https://")):
        return False
    if is_generic_url(u):
        return False
    try:
        p = urlparse(u)
    except Exception:
        return False
    if not p.netloc:
        return False
    # Require at least 2 meaningful path segments (e.g. /alert-recall/xyz).
    # A URL like https://www.fda.gov/ or https://www.fda.gov/recalls is too shallow.
    segments = [s for s in (p.path or "").split("/") if s]
    if len(segments) < 2:
        return False
    # Last segment should not be just a short slug like "news" or "index.html" alone
    return True


def _akamai_host(url: str) -> bool:
    """Is this a host that answers plain `requests` with a lying 404?

    Delegates to the scrapers’ own opt-in list so there is one place to
    add a host. If that module is missing we fall back to the four known
    names rather than to False — guessing "not protected" is what deletes
    URLs.
    """
    try:
        from scrapers._akamai_fetch import is_akamai_host
        return bool(is_akamai_host(url))
    except Exception:                                            # noqa: BLE001
        return _domain(url) in {"www.fda.gov", "www.fsis.usda.gov",
                                "www.fda.gov.ph", "www.gov.il"}


def _check_akamai(url: str) -> Dict[str, Any]:
    """Check an Akamai-protected URL with Chrome TLS impersonation.

    This is the same route claude_check.py, _provenance.py, the scrapers
    and recall_review_agent.py have used since 2026-05-20. The guardian
    was the last reader still on plain `requests`, which is why it was the
    one deleting URLs.

    A 404 seen THROUGH impersonation is a real 404 — verified_dead is set
    and the guardian may act on it. If curl_cffi is not installed we
    cannot tell a dead notice from a bot wall, so nothing is verified and
    the row keeps its URL.
    """
    try:
        from scrapers._akamai_fetch import fetch_via_curl_cffi
    except Exception:                                            # noqa: BLE001
        fetch_via_curl_cffi = None
    if fetch_via_curl_cffi is None:
        return {"url": url, "status": 0, "ok": True, "generic": False,
                "error": "akamai host, curl_cffi unavailable — not checked",
                "reason": "bot_blocked", "verified_dead": False}
    try:
        resp = fetch_via_curl_cffi(url)
    except Exception as e:                                       # noqa: BLE001
        return {"url": url, "status": 0, "ok": True, "generic": False,
                "error": f"akamai fetch raised {type(e).__name__} — not checked",
                "reason": "bot_blocked", "verified_dead": False}
    if resp is None:
        # Graceful degradation inside _akamai_fetch (missing package, or a
        # transport failure). Not evidence about the notice.
        return {"url": url, "status": 0, "ok": True, "generic": False,
                "error": "akamai fetch returned None — not checked",
                "reason": "bot_blocked", "verified_dead": False}
    code = int(getattr(resp, "status_code", 0) or 0)
    if 200 <= code < 400:
        return {"url": url, "status": code, "ok": True, "generic": False,
                "error": "", "reason": "ok", "verified_dead": False}
    if code in (404, 410):
        return {"url": url, "status": code, "ok": False, "generic": False,
                "error": f"HTTP {code} through Chrome impersonation",
                "reason": "http_error", "verified_dead": True}
    # 403 or 5xx even through impersonation: the wall moved or the site is
    # having a bad day. Either way we did not read the notice.
    return {"url": url, "status": code, "ok": True, "generic": False,
            "error": f"HTTP {code} through impersonation — not conclusive",
            "reason": "bot_blocked", "verified_dead": False}


def check_url(url: str, do_get_fallback: bool = True) -> Dict[str, Any]:
    """
    HEAD-check a URL. Returns {url, status, ok, generic, error, reason,
    verified_dead}.
    reason is one of: "ok", "empty", "generic", "http_error", "bot_blocked",
    "tls_error", "network".

    `verified_dead` is the only field should_blank_url is allowed to act
    on. See the module docstring.
    """
    if not url:
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": "empty", "reason": "empty", "verified_dead": True}
    if is_generic_url(url):
        return {"url": url, "status": 0, "ok": False, "generic": True,
                "error": "generic landing page", "reason": "generic",
                "verified_dead": False}

    dom = _domain(url)
    if _akamai_host(url):
        return _check_akamai(url)
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT, headers=HEADERS)
        # Many gov sites reject HEAD but serve GET — try GET with stream to avoid downloading
        if r.status_code in (403, 405, 501) and do_get_fallback:
            r = requests.get(url, allow_redirects=True, timeout=TIMEOUT,
                             headers=HEADERS, stream=True)
            r.close()
        code = r.status_code
        if 200 <= code < 400:
            return {"url": url, "status": code, "ok": True, "generic": False,
                    "error": "", "reason": "ok", "verified_dead": False}
        # A 4xx/5xx from a host on the tolerance list is not evidence. It
        # used to be forgiven for status 403 alone; the hosts that hurt us
        # answered 404, which walked straight past the exemption into
        # should_blank_url. The status is no longer what decides.
        if dom in BOT_HOSTILE_DOMAINS:
            return {"url": url, "status": code, "ok": True, "generic": False,
                    "error": f"HTTP {code} from a host that rejects "
                             f"datacentre traffic — not conclusive",
                    "reason": "bot_blocked", "verified_dead": False}
        return {"url": url, "status": code, "ok": False, "generic": False,
                "error": f"HTTP {code}", "reason": "http_error",
                "verified_dead": code in (404, 410) or 500 <= code < 600}
    except requests.exceptions.SSLError as e:
        # ── AUDIT 2026-07-29 ────────────────────────────────────────────
        # An SSL/TLS failure means OUR client could not complete a
        # handshake. It says nothing about whether the recall URL is
        # valid. Previously this fell through to the bare `except
        # Exception` below and was classified reason="network", ok=False
        # — i.e. "broken URL" — which stamped the row rejected and,
        # combined with claude_check's second look at the same failure,
        # archived real recalls to Weekly_Rejected.
        #
        # rappel.conso.gouv.fr serves an INCOMPLETE chain (missing
        # intermediate); browsers recover via the certificate's AIA
        # extension, Python does not. Tolerated exactly like
        # BOT_HOSTILE_DOMAINS: the row stays eligible and the content
        # reviewer decides on the merits.
        return {"url": url, "status": 0, "ok": True, "generic": False,
                "error": f"tls chain ({str(e)[:60]}) — tolerated",
                "reason": "tls_error", "verified_dead": False}
    except requests.Timeout:
        # Timeout on bot-hostile domain → also a tolerant pass
        if dom in BOT_HOSTILE_DOMAINS:
            return {"url": url, "status": 0, "ok": True, "generic": False,
                    "error": "timeout (tolerated)",
                    "reason": "bot_blocked", "verified_dead": False}
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": "timeout", "reason": "network",
                "verified_dead": False}
    except Exception as e:
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": str(e)[:100], "reason": "network",
                "verified_dead": False}


def validate_all(rows: List[Dict[str, Any]], max_workers: int = 10) -> List[Dict[str, Any]]:
    """Concurrently validate URLs in all rows. Returns rows with _url_check added."""
    out: List[Dict[str, Any]] = [dict(r) for r in rows]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(check_url, r.get("URL", "") or ""): i for i, r in enumerate(out)}
        for f in as_completed(futs):
            idx = futs[f]
            try:
                out[idx]["_url_check"] = f.result()
            except Exception as e:
                out[idx]["_url_check"] = {"ok": False, "error": str(e), "reason": "network"}
    bad = sum(1 for r in out if not r.get("_url_check", {}).get("ok"))
    log.info("URL validation: %d/%d bad (excluding bot-blocked passes)", bad, len(out))
    return out


def report_bad_urls(validated: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rows whose URL validation failed (404, generic, network). Excludes bot-blocked."""
    return [r for r in validated if not r.get("_url_check", {}).get("ok")]


def should_blank_url(check: Dict[str, Any]) -> bool:
    """Decide whether to delete a URL from a row.

    ONE question: did a client the server answers honestly read the
    server's answer, and was that answer "gone"? If not, the URL stays.

    Deleting a URL is not a small edit. _dedup_key is URL-primary, so a
    blanked row becomes a NEW row rather than an edited one — on
    2026-09-25 that produced two Pending rows for one Galil Importing
    recall, one of them with no URL at all and therefore unpublishable
    forever. And a row with no URL can never pass the authority-URL gate,
    which is the register's whole promise. So the bar is evidence, not a
    status code.

    Blank when:  the check set verified_dead (a real 404/410, or a
                 persistent 5xx, seen by a trusted client) or the URL was
                 empty to begin with.
    Keep when:   anything else — 403, a lying 404 from an Akamai host,
                 a TLS chain we cannot complete, a timeout, a network
                 error, or a generic landing page (url_resurrect will
                 try to sharpen that one).
    """
    if not check:
        return False
    if check.get("reason") == "empty":
        return True
    # Generic URLs are NOT blanked — a /categorie/94 link still gets the
    # user to the right neighbourhood. The guardian adds a
    # [URL-guardian … generic] note so url_resurrect picks them up.
    if check.get("reason") == "generic":
        return False
    # Absent verified_dead, a caller is working from a check built before
    # this invariant existed. Refuse rather than guess: the cost of
    # keeping a dead link is one bad click; the cost of guessing wrong is
    # a destroyed row.
    return bool(check.get("verified_dead"))
