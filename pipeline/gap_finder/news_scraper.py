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

import requests

try:
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder.countries import get as get_country           # type: ignore
    from gap_finder.countries.base import CountryConfig           # type: ignore


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


def parse_rss(xml_text: str) -> list[dict]:
    """Parse RSS 2.0 / Atom into list of {title, link, pubDate}."""
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
        if title and link:
            out.append({"title": title, "link": link, "published": pub})

    # Atom (some Greek feeds)
    if not out:
        ns = "{http://www.w3.org/2005/Atom}"
        for entry in root.iter(f"{ns}entry"):
            title_el = entry.find(f"{ns}title")
            link_el = entry.find(f"{ns}link")
            pub_el = entry.find(f"{ns}updated") or entry.find(f"{ns}published")
            title = (title_el.text or "").strip() if title_el is not None else ""
            link = link_el.get("href", "") if link_el is not None else ""
            pub = (pub_el.text or "").strip() if pub_el is not None else ""
            if title and link:
                out.append({"title": title, "link": link, "published": pub})
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
            q = f"site:{domain} {keyword}"
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
