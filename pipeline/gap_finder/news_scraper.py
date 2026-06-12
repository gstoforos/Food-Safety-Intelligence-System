"""
AFTS Food Safety Intelligence — Gap Finder
News scraper (parametric — works for any CountryConfig).

Strategy:
  1. Try RSS feeds for each configured source. Many will 404/410 — that's fine,
     we tolerate failures and never block on a single broken feed.
  2. Always run Google News site:-restricted queries against each domain — this
     is the workhorse that produces ~200-400 candidates per run regardless of
     whether RSS feeds work.
  3. Filter by config.recall_signal_terms (drops politically-loaded "ανάκληση"
     of decisions, "richiamo" of officials, etc. that aren't food recalls).
  4. Deduplicate by URL.

Same module body for Greek, Italian, every future country — only the
CountryConfig differs.
"""

from __future__ import annotations
import argparse
import json
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlparse

import logging

import requests

try:
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder.countries import get as get_country           # type: ignore
    from gap_finder.countries.base import CountryConfig           # type: ignore

# ── curl_cffi: Chrome 131 TLS impersonation (Stage 1 fetch) ────────────────
# Greek/EU news sites + Google News reject the stdlib `requests` TLS
# fingerprint (JA3/JA4) with 403/410 from datacenter IPs. curl_cffi performs
# the handshake byte-for-byte identical to real Chrome, clearing the
# TLS-fingerprint layer. Same approach as scrapers/_akamai_fetch.py and
# pipeline/gap_finder/article_fetcher.py (Stage 2). Lazy-imported, cached,
# graceful fallback to stdlib `requests` if the package is unavailable.
_log = logging.getLogger(__name__)
_cf_mod = None
_cf_state = "unloaded"          # unloaded | ok | failed
_cf_logged_hosts: set = set()
_IMPERSONATE_PROFILE = "chrome131"


def _load_curl_cffi():
    """Lazy-import curl_cffi.requests. Cache success/failure. Return module|None."""
    global _cf_mod, _cf_state
    if _cf_state == "ok":
        return _cf_mod
    if _cf_state == "failed":
        return None
    try:
        from curl_cffi import requests as cf  # type: ignore
        _cf_mod = cf
        _cf_state = "ok"
        _log.info("curl_cffi loaded — Stage 1 fetches use Chrome 131 TLS "
                  "impersonation (profile=%s)", _IMPERSONATE_PROFILE)
        return cf
    except ImportError:
        _cf_state = "failed"
        _log.warning("curl_cffi NOT installed — Stage 1 falls back to stdlib "
                     "requests; bot-protected feeds will likely 403. "
                     "Install: pip install 'curl-cffi>=0.7.0'")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 25
GOOGLE_NEWS_BASE = "https://news.google.com/rss/search"


# ─────────────────────────────────────────────────────────────────────────────
# DATA RECORD
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candidate:
    url: str
    title: str
    published: str
    source_domain: str
    discovered_via: str   # "rss" or "google_news"
    discovered_at: str
    description: str = ""  # RSS <description> — full snippet for direct feeds,
                           # thin/empty for Google News proxy URLs

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION & FILTERING
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t)


def has_recall_signal(title: str, terms: list[str]) -> bool:
    n = _normalize(title)
    return any(_normalize(t) in n for t in terms)


# ─────────────────────────────────────────────────────────────────────────────
# RSS FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def _http_get(url: str, verbose: bool = False) -> Optional[str]:
    """Fetch a URL's text body. Tries curl_cffi (Chrome 131 TLS) first, so
    feeds that reject the stdlib `requests` fingerprint still come through;
    falls back to plain `requests` only if curl_cffi is unavailable.

    Returns the response text, or None on any failure (caller tolerates
    None — a single broken feed never blocks the run)."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;"
                  "q=0.9,*/*;q=0.8",
        "Accept-Language": "el-GR,el;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }

    # Primary: curl_cffi with Chrome 131 TLS impersonation.
    cf = _load_curl_cffi()
    if cf is not None:
        host = urlparse(url).netloc.lower().split(":", 1)[0]
        if host and host not in _cf_logged_hosts:
            _cf_logged_hosts.add(host)
            _log.info("curl_cffi routing engaged: host=%s impersonate=%s",
                      host, _IMPERSONATE_PROFILE)
        try:
            r = cf.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                impersonate=_IMPERSONATE_PROFILE,
                allow_redirects=True,
            )
            if getattr(r, "status_code", 0) == 200:
                return getattr(r, "text", "") or ""
            if verbose:
                print(f"  [WARN] fetch failed: {url} — "
                      f"HTTP {getattr(r, 'status_code', '?')}", file=sys.stderr)
            # Fall through to requests as a last resort on non-200.
        except Exception as e:
            if verbose:
                print(f"  [WARN] curl_cffi fetch error: {url} — {e}",
                      file=sys.stderr)
            # Fall through to requests.

    # Fallback: stdlib requests (used when curl_cffi missing or errored).
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        if verbose:
            print(f"  [WARN] fetch failed: {url} — {e}", file=sys.stderr)
        return None


def _clean_html(html_text: str) -> str:
    """Strip HTML tags and decode entities from an RSS description."""
    if not html_text:
        return ""
    try:
        from html import unescape
        # Strip HTML tags
        text = re.sub(r"<[^>]+>", " ", html_text)
        # Decode HTML entities (&amp;, &nbsp;, etc.)
        text = unescape(text)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return html_text


def parse_rss(xml_text: str) -> list[dict]:
    """Parse RSS 2.0 / Atom into list of {title, link, pubDate, description}."""
    if not xml_text:
        return []
    out: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    # RSS 2.0
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc_raw = (item.findtext("description") or "").strip()
        desc = _clean_html(desc_raw)
        if title and link:
            out.append({"title": title, "link": link, "published": pub,
                        "description": desc})

    # Atom (some feeds use this)
    if not out:
        ns = "{http://www.w3.org/2005/Atom}"
        for entry in root.iter(f"{ns}entry"):
            title_el = entry.find(f"{ns}title")
            link_el = entry.find(f"{ns}link")
            pub_el = entry.find(f"{ns}updated") or entry.find(f"{ns}published")
            summary_el = entry.find(f"{ns}summary") or entry.find(f"{ns}content")
            title = (title_el.text or "").strip() if title_el is not None else ""
            link = link_el.get("href", "") if link_el is not None else ""
            pub = (pub_el.text or "").strip() if pub_el is not None else ""
            desc_raw = (summary_el.text or "").strip() if summary_el is not None else ""
            desc = _clean_html(desc_raw)
            if title and link:
                out.append({"title": title, "link": link, "published": pub,
                            "description": desc})
    return out


def collect_rss(cfg: CountryConfig, verbose: bool = False) -> list[Candidate]:
    out: list[Candidate] = []
    now = datetime.now(timezone.utc).isoformat()

    for src in cfg.rss_sources:
        domain_count = 0
        for feed_url in src.feeds:
            if verbose:
                print(f"[RSS] {src.domain} ← {feed_url}", file=sys.stderr)
            xml_text = _http_get(feed_url, verbose=verbose)
            if not xml_text:
                continue
            for item in parse_rss(xml_text):
                if not has_recall_signal(item["title"], cfg.recall_signal_terms):
                    continue
                out.append(Candidate(
                    url=item["link"],
                    title=item["title"],
                    published=item["published"],
                    source_domain=src.domain,
                    discovered_via="rss",
                    discovered_at=now,
                    description=item.get("description", ""),
                ))
                domain_count += 1
        if verbose:
            print(f"  → {src.domain}: {domain_count} candidates after filter",
                  file=sys.stderr)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE NEWS (RSS-style search endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def collect_google_news(cfg: CountryConfig, verbose: bool = False) -> list[Candidate]:
    out: list[Candidate] = []
    now = datetime.now(timezone.utc).isoformat()

    for domain in cfg.google_news_domains:
        for keyword in cfg.google_news_keywords:
            # Add `when:21d` operator → only articles from last 21 days.
            # Without this, Google News returns historical articles by relevance
            # (e.g. 2017 Findus, 2022 Kinder) which pollute the candidate pool.
            # 21 days gives ~1 week buffer beyond our 14-day downstream filter.
            q = f"site:{domain} {keyword} when:21d"
            url = (
                f"{GOOGLE_NEWS_BASE}?q={quote_plus(q)}"
                f"&hl={cfg.language_code}-{cfg.code.upper()}"
                f"&gl={cfg.code.upper()}&ceid={cfg.code.upper()}:{cfg.language_code}"
            )
            if verbose:
                print(f"[GN]  {domain} ? {keyword!r}", file=sys.stderr)
            xml_text = _http_get(url, verbose=verbose)
            if not xml_text:
                continue
            items = parse_rss(xml_text)
            count = 0
            for item in items:
                if not has_recall_signal(item["title"], cfg.recall_signal_terms):
                    continue
                out.append(Candidate(
                    url=item["link"],
                    title=item["title"],
                    published=item["published"],
                    source_domain=domain,
                    discovered_via="google_news",
                    discovered_at=now,
                    description=item.get("description", ""),
                ))
                count += 1
            if verbose and count:
                print(f"  → {count} candidates", file=sys.stderr)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPE
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(candidates: list[Candidate]) -> list[Candidate]:
    seen: set[str] = set()
    out: list[Candidate] = []
    for c in candidates:
        key = c.url.split("?")[0].split("#")[0]
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def write_jsonl(records: list[Candidate], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI (mostly for direct testing — main.py is the production entrypoint)
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="AFTS Gap Finder — News Scraper")
    parser.add_argument("--country", required=True,
                        help="ISO2 country code: gr, it, es, ...")
    parser.add_argument("--out", default=None, help="Output path (default per-country)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    cfg = get_country(args.country)
    out_path = args.out or cfg.candidates_path

    print(f"News scraper — country={cfg.code} ({cfg.name_en})", file=sys.stderr)
    rss = collect_rss(cfg, verbose=args.verbose)
    gn = collect_google_news(cfg, verbose=args.verbose)
    all_cands = rss + gn
    deduped = deduplicate(all_cands)
    print(f"  RSS: {len(rss)}, Google News: {len(gn)}, deduped: {len(deduped)}",
          file=sys.stderr)
    write_jsonl(deduped, out_path)
    print(f"  Wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
