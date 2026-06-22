"""
Generic HTTP fetch helpers shared by source modules: a JSON getter and an
RSS getter, both with retries, a real User-Agent, and tolerant parsing.

TLS impersonation (audit 2026-06-21): some official endpoints sit behind an
Akamai / WAF that 403s plain `requests` from cloud / GitHub-Actions IPs
(notably https://www.fsis.usda.gov/fsis/api/recall/v/1). We therefore route
JSON/RSS fetches through curl_cffi with Chrome-131 TLS impersonation when it
is available, and fall back to plain `requests` otherwise. FDA/CFIA endpoints
that already worked keep working; FSIS now loads.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
import xml.etree.ElementTree as ET

import requests

# Optional Chrome-TLS impersonation (curl_cffi). If unavailable we degrade to
# plain requests transparently.
try:
    from curl_cffi import requests as _cffi  # type: ignore
    _IMPERSONATE = "chrome131"
except Exception:  # noqa: BLE001
    _cffi = None
    _IMPERSONATE = None

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/html;q=0.9, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 30


def _get(url: str, params: Optional[dict] = None, *, want: str = "json"):
    """Single HTTP GET. Tries curl_cffi (Chrome-131 TLS) first when available,
    then falls back to plain requests. Returns the response object."""
    accept = ("application/json, */*;q=0.5" if want == "json"
              else "application/rss+xml, application/xml, text/xml, */*;q=0.5")
    headers = {**DEFAULT_HEADERS, "Accept": accept}
    if _cffi is not None:
        try:
            r = _cffi.get(url, params=params, headers=headers,
                          timeout=TIMEOUT, impersonate=_IMPERSONATE)
            r.raise_for_status()
            return r
        except Exception:  # noqa: BLE001 — fall back to plain requests
            pass
    r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r


def get_json(url: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    last = None
    for i in range(retries):
        try:
            return _get(url, params=params, want="json").json()
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  [WARN] json fetch failed ({i+1}/{retries}): {url} — {e}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"get_json exhausted retries for {url}: {last}")


def get_rss(url: str, retries: int = 3) -> list[dict]:
    """
    Fetch an RSS/Atom feed and return a list of dicts:
    {title, link, description, published(datetime|None)}.
    Tolerant of both RSS 2.0 <item> and Atom <entry>.
    """
    last = None
    for i in range(retries):
        try:
            r = _get(url, want="rss")
            return _parse_feed(r.content)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  [WARN] rss fetch failed ({i+1}/{retries}): {url} — {e}")
            time.sleep(2 * (i + 1))
    print(f"  [WARN] get_rss exhausted retries for {url}: {last}")
    return []


def get_text(url: str, retries: int = 3) -> str:
    """Fetch raw text/HTML (Chrome-TLS first). Empty string on failure."""
    last = None
    for i in range(retries):
        try:
            return _get(url, want="rss").text
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  [WARN] text fetch failed ({i+1}/{retries}): {url} — {e}")
            time.sleep(2 * (i + 1))
    print(f"  [WARN] get_text exhausted retries for {url}: {last}")
    return ""


def _parse_feed(content: bytes) -> list[dict]:
    out: list[dict] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  [WARN] feed parse error: {e}")
        return out

    # RSS 2.0: channel/item — Atom: entry
    for item in root.iter():
        tag = item.tag.split("}")[-1]
        if tag not in ("item", "entry"):
            continue
        d = {"title": "", "link": "", "description": "", "published": None}
        for child in item:
            ct = child.tag.split("}")[-1]
            txt = (child.text or "").strip()
            if ct == "title":
                d["title"] = txt
            elif ct == "link":
                # Atom uses href attribute; RSS uses text
                d["link"] = child.get("href") or txt or d["link"]
            elif ct in ("description", "summary", "content"):
                d["description"] = txt or d["description"]
            elif ct in ("pubDate", "published", "updated", "date"):
                d["published"] = _parse_date(txt)
        if d["title"]:
            out.append(d)
    return out


def _parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # RFC 822 (RSS)
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        pass
    # ISO 8601 / common date forms
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y",
                "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    # Last resort: ISO prefix like "2026-06-19T..." or "2026-06-19 12:00"
    try:
        dt = datetime.fromisoformat(s[:19].replace("Z", ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        return None


def parse_iso(s: str) -> Optional[datetime]:
    """Parse an ISO date string (used by JSON APIs)."""
    return _parse_date(s)
