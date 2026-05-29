"""
Generic HTTP fetch helpers shared by source modules: a JSON getter and an
RSS getter, both with retries, a real User-Agent, and tolerant parsing.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
import xml.etree.ElementTree as ET

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/html;q=0.9, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 30


def get_json(url: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=DEFAULT_HEADERS,
                             timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
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
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return _parse_feed(r.content)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  [WARN] rss fetch failed ({i+1}/{retries}): {url} — {e}")
            time.sleep(2 * (i + 1))
    print(f"  [WARN] get_rss exhausted retries for {url}: {last}")
    return []


def _parse_feed(content: bytes) -> list[dict]:
    out: list[dict] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  [WARN] feed parse error: {e}")
        return out

    # RSS 2.0: channel/item
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
    # RFC 822 (RSS)
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        pass
    # ISO 8601 (Atom)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def parse_iso(s: str) -> Optional[datetime]:
    """Parse an ISO date string (used by JSON APIs)."""
    return _parse_date(s)
