"""
URL validator: confirms every recall URL is reachable (HTTP 200) and not a generic landing page.
Run after merge_master, before commit. Flags rows for review pipeline.
"""
from __future__ import annotations
import re
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "FSIS-Bot/1.0 URL-Validator (info@advfood.tech)"}
TIMEOUT = 15

# Patterns indicating generic / homepage / category URLs (NOT specific recalls)
GENERIC_URL_PATTERNS = [
    r"/categorie/\d+$",                      # RappelConso category
    r"/categorie/0/\d+/[a-z]+$",
    r"/anakleiseis-cat/?$",                  # EFET landing
    r"/alertas_alimentarias/?$",             # AESAN landing
    r"/liste/lebensmittel/bundesweit/?$",    # BVL landing
    r"/portal/news/p3_2_1_3\.jsp",           # Salute IT generic notizie
    r"/food-recalls/?$",                     # FSANZ landing
    r"/recalls?/?$",                         # generic /recalls
    r"/news/?$",                             # generic news
    r"/category/[^/]+/?$",                   # generic category
]


def is_generic_url(url: str) -> bool:
    if not url:
        return False
    for pat in GENERIC_URL_PATTERNS:
        if re.search(pat, url, re.I):
            return True
    return False


def check_url(url: str, do_get_fallback: bool = True) -> Dict[str, Any]:
    """HEAD-check a URL. Returns {url, status, ok, generic, error}."""
    if not url:
        return {"url": url, "status": 0, "ok": False, "generic": False, "error": "empty"}
    if is_generic_url(url):
        return {"url": url, "status": 0, "ok": False, "generic": True, "error": "generic landing page"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT, headers=HEADERS)
        # Some servers reject HEAD — fall back to GET with stream=True (don't download body)
        if r.status_code in (403, 405, 501) and do_get_fallback:
            r = requests.get(url, allow_redirects=True, timeout=TIMEOUT, headers=HEADERS, stream=True)
            r.close()
        return {
            "url": url,
            "status": r.status_code,
            "ok": 200 <= r.status_code < 400,
            "generic": False,
            "error": "" if r.status_code < 400 else f"HTTP {r.status_code}",
        }
    except Exception as e:
        return {"url": url, "status": 0, "ok": False, "generic": False, "error": str(e)[:100]}


def validate_all(rows: List[Dict[str, Any]], max_workers: int = 10) -> List[Dict[str, Any]]:
    """Concurrently validate URLs in all rows. Returns rows enriched with _url_check field."""
    out: List[Dict[str, Any]] = [dict(r) for r in rows]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(check_url, r.get("URL", "")): i for i, r in enumerate(out)}
        for f in as_completed(futs):
            idx = futs[f]
            try:
                out[idx]["_url_check"] = f.result()
            except Exception as e:
                out[idx]["_url_check"] = {"ok": False, "error": str(e)}
    bad = sum(1 for r in out if not r.get("_url_check", {}).get("ok"))
    log.info("URL validation: %d/%d failed", bad, len(out))
    return out


def report_bad_urls(validated: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to only rows with bad URLs for the review pipeline."""
    return [r for r in validated if not r.get("_url_check", {}).get("ok")]
