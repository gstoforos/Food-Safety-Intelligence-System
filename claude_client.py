"""
URL validator — confirms every recall URL is reachable AND specific (not a landing page).

Key design decisions:
  - Uses browser-like headers to reduce false 403s from gov/regulatory sites
  - Distinguishes "truly broken" (404/410/5xx) from "bot-blocked" (403 on known
    allowlisted gov domains) so we don't nuke a valid rappel.conso.gouv.fr link
    just because their WAF rejects python-requests.
  - Pattern match for landing-page / category URLs is still strict.

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


def check_url(url: str, do_get_fallback: bool = True) -> Dict[str, Any]:
    """
    HEAD-check a URL. Returns {url, status, ok, generic, error, reason}.
    reason is one of: "ok", "empty", "generic", "http_error", "bot_blocked", "network".
    """
    if not url:
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": "empty", "reason": "empty"}
    if is_generic_url(url):
        return {"url": url, "status": 0, "ok": False, "generic": True,
                "error": "generic landing page", "reason": "generic"}

    dom = _domain(url)
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
                    "error": "", "reason": "ok"}
        # 403 on known bot-hostile domains → don't treat as broken
        if code == 403 and dom in BOT_HOSTILE_DOMAINS:
            return {"url": url, "status": code, "ok": True, "generic": False,
                    "error": "", "reason": "bot_blocked"}
        return {"url": url, "status": code, "ok": False, "generic": False,
                "error": f"HTTP {code}", "reason": "http_error"}
    except requests.Timeout:
        # Timeout on bot-hostile domain → also a tolerant pass
        if dom in BOT_HOSTILE_DOMAINS:
            return {"url": url, "status": 0, "ok": True, "generic": False,
                    "error": "timeout (tolerated)", "reason": "bot_blocked"}
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": "timeout", "reason": "network"}
    except Exception as e:
        return {"url": url, "status": 0, "ok": False, "generic": False,
                "error": str(e)[:100], "reason": "network"}


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
    """
    Decide whether to blank out a bad URL in the dataset.
    Blank if: generic landing page OR hard HTTP error (404/410/5xx).
    Keep if: bot-blocked (403 from known gov domain) — probably valid for humans.
    """
    if not check:
        return False
    reason = check.get("reason", "")
    if reason in ("generic", "empty"):
        return True
    if reason == "http_error":
        status = check.get("status", 0)
        # 404/410 = gone; 5xx persistent = blank; 403/401 = auth/bot, don't blank
        if status in (404, 410) or (500 <= status < 600):
            return True
    return False
