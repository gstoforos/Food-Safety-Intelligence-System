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


# ── Hosts that must NOT be impersonated (audit 2026-08-16) ────────────
# data.food.gov.uk is an Epimorphics Linked Data API, not a browser site.
# It has now returned HTTP 400 from GitHub Actions to THREE different query
# shapes that each succeed from a different client against the identical
# URL:
#     _pageSize=250                      400 in Actions
#     _pageSize=50 & _page=0             400 in Actions
#     min-created & max-created          400 in Actions
# Three parameters blamed, three fixes shipped, same failure. The variable
# that never changed is the CLIENT: every request goes out with a spoofed
# Chrome-131 TLS fingerprint and a Chrome User-Agent, from a datacentre IP,
# with none of the other headers a real Chrome sends. That is a WAF
# signature, and a WAF is entitled to answer it with 400.
#
# TLS impersonation exists here for fsis.usda.gov, which sits behind Akamai
# and 403s plain requests. It was never needed for the FSA, and applying it
# by default made an honest API request look like a liar.
_NO_IMPERSONATE = ("data.food.gov.uk",)

# An honest identifying UA for API endpoints. A contactable string is also
# what an operator of a public data API expects to see.
API_UA = ("AFTS-FSIS/1.0 (Food Safety Intelligence System; "
          "+https://fsis.advfood.tech; contact info@advfood.tech)")


def _get(url: str, params: Optional[dict] = None, *, want: str = "json"):
    """Single HTTP GET.

    Uses curl_cffi Chrome-TLS impersonation where it is needed (Akamai-
    fronted regulator endpoints), and a plain, honestly-identified request
    for API hosts listed in _NO_IMPERSONATE.
    """
    accept = ("application/json, */*;q=0.5" if want == "json"
              else "application/rss+xml, application/xml, text/xml, */*;q=0.5")
    plain_api = any(h in url for h in _NO_IMPERSONATE)
    if plain_api:
        headers = {"User-Agent": API_UA, "Accept": accept}
    else:
        headers = {**DEFAULT_HEADERS, "Accept": accept}

    if _cffi is not None and not plain_api:
        try:
            r = _cffi.get(url, params=params, headers=headers,
                          timeout=TIMEOUT, impersonate=_IMPERSONATE)
            r.raise_for_status()
            return r
        except Exception:  # noqa: BLE001 — fall back to plain requests
            pass
    r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
    _raise_with_body(r)
    return r


def _raise_with_body(r) -> None:
    """raise_for_status(), but put the response BODY in the message.

    THIS IS THE REAL LESSON OF THE FSA EPISODE. Three wrong diagnoses were
    shipped because the only evidence available was
        "400 Client Error:  for url: ..."
    with an empty reason phrase and no body. Epimorphics returns a
    descriptive explanation in the body of a 400 — which parameter it
    objected to, or that it objected to none of them. That text would have
    ended the guessing on day one. It costs one line to keep.
    """
    if r.status_code < 400:
        return
    body = ""
    try:
        body = (r.text or "")[:600].replace("\n", " ").strip()
    except Exception:  # noqa: BLE001
        pass
    raise requests.HTTPError(
        f"{r.status_code} {r.reason or ''} for {r.url} — response body: "
        f"{body!r}", response=r)


def _is_deterministic_client_error(exc) -> bool:
    """True for a 4xx the server will answer identically next time.

    Audit 2026-08-16: a 400 was retried three times, 45 seconds apart, and
    produced three identical workflow annotations. Retrying a malformed or
    refused request cannot help — the only thing it changes is how long the
    run takes to tell you. 429 and 408 ARE worth retrying; the rest are the
    server stating a settled opinion.
    """
    r = getattr(exc, "response", None)
    code = getattr(r, "status_code", None)
    return isinstance(code, int) and 400 <= code < 500 and code not in (408, 429)


def get_json(url: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    last = None
    for i in range(retries):
        try:
            return _get(url, params=params, want="json").json()
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  [WARN] json fetch failed ({i+1}/{retries}): {url} — {e}")
            if _is_deterministic_client_error(e):
                print("  [WARN] that is a deterministic client error — not "
                      "retrying; the server will say the same thing again.")
                break
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
