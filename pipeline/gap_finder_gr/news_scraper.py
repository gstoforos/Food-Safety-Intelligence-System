"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 2: News Scraper

Discovers food-recall candidate articles from Greek news sources:
  • Direct RSS feeds (7 sources: kathimerini, protothema, in.gr,
    troktiko, news247, protagon, skai)
  • Google News RSS site-restricted queries (fallback + extra coverage)
  • Keyword pre-filter (ΕΦΕΤ, ανάκληση, recall, etc.)
  • URL deduplication via SHA-256 short hash
  • Pathogen-hint flagging for priority routing

Output: docs/data/gap_finder_gr/candidates.jsonl (one JSON record per line).

Pure Python — no LLM, no Gemini, no Claude, no paid APIs.
Dependencies: requests, feedparser

CLI:
    python -m pipeline.gap_finder_gr.news_scraper
    python -m pipeline.gap_finder_gr.news_scraper --output ./candidates.jsonl
    python -m pipeline.gap_finder_gr.news_scraper --dry-run    # offline test
    python -m pipeline.gap_finder_gr.news_scraper --verbose
"""

from __future__ import annotations
import argparse
import hashlib
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urlparse

import requests
import feedparser


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Greek news source map — site_domain → list of RSS URLs to try in order.
# Multiple URLs per site = fallback chain; whichever returns entries wins.
RSS_FEEDS: dict[str, list[str]] = {
    "kathimerini.gr": [
        "https://www.kathimerini.gr/feed/",
        "https://www.kathimerini.gr/society/feed/",
    ],
    "protothema.gr": [
        "https://www.protothema.gr/rss/",
        "https://www.protothema.gr/ellada/rss/",
    ],
    "in.gr": [
        "https://www.in.gr/feed/",
    ],
    "troktiko.gr": [
        "https://www.troktiko.gr/feed/",
    ],
    "news247.gr": [
        "https://www.news247.gr/rss/all.xml",
        "https://www.news247.gr/rss/",
    ],
    "protagon.gr": [
        "https://www.protagon.gr/feed/",
    ],
    "skai.gr": [
        "https://www.skai.gr/feeds/news",
        "https://www.skai.gr/feed",
    ],
}

# Google News RSS — site-restricted recall queries (catches articles whose
# native RSS feeds dropped them or were never indexed).
GOOGLE_NEWS_QUERY_TEMPLATE = (
    "https://news.google.com/rss/search?"
    "q={q}+site:{site}&hl=el&gl=GR&ceid=GR:el"
)
GOOGLE_NEWS_QUERIES = [
    "ΕΦΕΤ ανάκληση",
    "ανάκληση τροφίμου",
    "ανακαλεί προϊόν",
]

# Recall-related keywords (Greek + English) — pre-filter before EFET stage.
RECALL_KEYWORDS = {
    "εφετ", "efet",
    "ανάκληση", "ανακαλεί", "ανακαλείται", "ανάκλησης",
    "απόσυρση", "αποσύρ", "αποσύρει", "αποσύρεται",
    "ανακαλούνται", "ανακάλεσε",
    "recall", "withdrawal", "withdraw",
}

# Pathogen / hazard keywords — for priority flagging (not filtering).
PATHOGEN_KEYWORDS = {
    "σαλμονέλα", "salmonella",
    "λιστέρια", "listeria",
    "e. coli", "e.coli", "ε. κολί", "κολοβακτηρίδιο",
    "campylobacter", "καμπυλοβακτηρίδιο",
    "αφλατοξίνη", "αφλατοξίνες", "aflatoxin",
    "νοροϊός", "norovirus",
    "βοτουλίνη", "botulinum",
    "παθογόνο", "pathogen",
    "bacillus cereus", "κερευλίδη",
    "cronobacter",
}

USER_AGENT = (
    "Mozilla/5.0 (compatible; AFTS-FSIS-GapFinder/1.0; "
    "+https://www.advfood.tech)"
)
REQUEST_TIMEOUT = 30
MAX_ENTRIES_PER_FEED = 50  # cap per feed; prevents pulling years of history

DEFAULT_OUTPUT = "docs/data/gap_finder_gr/candidates.jsonl"


# ─────────────────────────────────────────────────────────────────────────────
# TEXT NORMALIZATION (bilingual Greek + English, accent-stripping)
# ─────────────────────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r"\s+", " ", t)
    return t


_RECALL_KW_N = {normalize_text(k) for k in RECALL_KEYWORDS}
_PATHOGEN_KW_N = {normalize_text(k) for k in PATHOGEN_KEYWORDS}


def contains_recall_keyword(text: str) -> bool:
    n = normalize_text(text)
    return any(kw and kw in n for kw in _RECALL_KW_N)


def contains_pathogen_keyword(text: str) -> bool:
    n = normalize_text(text)
    return any(kw and kw in n for kw in _PATHOGEN_KW_N)


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE RECORD
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candidate:
    url: str
    title: str
    snippet: str
    published: str           # ISO-8601 UTC; "" if unknown
    source_domain: str
    fetch_method: str        # 'rss' | 'google_news'
    has_pathogen_hint: bool
    discovered_at: str       # ISO-8601 UTC

    def to_dict(self) -> dict:
        return asdict(self)

    def url_hash(self) -> str:
        return hashlib.sha256(self.url.encode("utf-8")).hexdigest()[:16]


def _iso_utc(ts_struct=None) -> str:
    """feedparser time.struct_time → ISO-8601 UTC string. Returns '' on failure."""
    if not ts_struct:
        return ""
    try:
        dt = datetime.fromtimestamp(time.mktime(ts_struct), tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _domain_of(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().lstrip("www.")
    except Exception:
        return ""


def _clean_snippet(text: str, max_chars: int = 400) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)          # strip HTML tags
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_chars]


# ─────────────────────────────────────────────────────────────────────────────
# FEED FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_feed(url: str, verbose: bool = False) -> list[dict]:
    """
    Fetch and parse an RSS/Atom feed. Returns list of feedparser entry dicts.
    Returns [] on network error, parse error, or empty feed.
    """
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml, */*"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        if verbose:
            print(f"  [WARN] fetch failed: {url} — {e}", file=sys.stderr)
        return []

    parsed = feedparser.parse(resp.content)
    if parsed.bozo and not parsed.entries:
        if verbose:
            print(f"  [WARN] parse failed: {url}", file=sys.stderr)
        return []

    return list(parsed.entries[:MAX_ENTRIES_PER_FEED])


def entries_to_candidates(
    entries: list[dict],
    source_domain: str,
    fetch_method: str,
) -> list[Candidate]:
    """Convert feedparser entries → Candidate records (with keyword filtering)."""
    out: list[Candidate] = []
    now = _now_utc()
    for e in entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not (title and link):
            continue

        snippet = _clean_snippet(
            e.get("summary") or e.get("description") or ""
        )
        combined = f"{title} {snippet}"

        # PRE-FILTER: must contain at least one recall keyword
        if not contains_recall_keyword(combined):
            continue

        # For Google News results, override source_domain with the real article host
        domain = source_domain
        if fetch_method == "google_news":
            d = _domain_of(link)
            if d:
                domain = d

        out.append(Candidate(
            url=link,
            title=title,
            snippet=snippet,
            published=_iso_utc(e.get("published_parsed") or e.get("updated_parsed")),
            source_domain=domain,
            fetch_method=fetch_method,
            has_pathogen_hint=contains_pathogen_keyword(combined),
            discovered_at=now,
        ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTORS
# ─────────────────────────────────────────────────────────────────────────────

def collect_rss(verbose: bool = False) -> list[Candidate]:
    """Sweep every configured RSS feed; return filtered candidates."""
    candidates: list[Candidate] = []
    for site, urls in RSS_FEEDS.items():
        site_total = 0
        for url in urls:
            if verbose:
                print(f"[RSS] {site} ← {url}", file=sys.stderr)
            entries = fetch_feed(url, verbose=verbose)
            if not entries:
                continue
            site_candidates = entries_to_candidates(entries, site, "rss")
            candidates.extend(site_candidates)
            site_total += len(site_candidates)
            if site_candidates:
                # First working URL per site is enough
                break
        if verbose:
            print(f"  → {site}: {site_total} candidates after filter", file=sys.stderr)
    return candidates


def collect_google_news(verbose: bool = False) -> list[Candidate]:
    """Query Google News RSS site-restricted per (site × query) combination."""
    candidates: list[Candidate] = []
    for site in RSS_FEEDS.keys():
        for q in GOOGLE_NEWS_QUERIES:
            url = GOOGLE_NEWS_QUERY_TEMPLATE.format(q=quote_plus(q), site=site)
            if verbose:
                print(f"[GN]  {site} ? {q!r}", file=sys.stderr)
            entries = fetch_feed(url, verbose=verbose)
            if not entries:
                continue
            site_candidates = entries_to_candidates(entries, site, "google_news")
            candidates.extend(site_candidates)
            if verbose:
                print(f"  → {len(site_candidates)} candidates", file=sys.stderr)
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(candidates: list[Candidate]) -> list[Candidate]:
    """Keep first occurrence of each URL. RSS preferred over Google News
    because RSS gives cleaner snippets and authoritative publish times."""
    candidates.sort(key=lambda c: 0 if c.fetch_method == "rss" else 1)
    seen: set[str] = set()
    out: list[Candidate] = []
    for c in candidates:
        h = c.url_hash()
        if h in seen:
            continue
        seen.add(h)
        out.append(c)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def write_jsonl(candidates: list[Candidate], output_path: str) -> None:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for c in candidates:
            f.write(json.dumps(c.to_dict(), ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# DRY-RUN (offline test with synthetic fixtures — verifies logic without network)
# ─────────────────────────────────────────────────────────────────────────────

DRY_RUN_FIXTURES = [
    # POSITIVE — should pass filter (recall keyword + pathogen hint)
    {
        "title": "ΕΦΕΤ: Ανάκληση φέτας ΒΥΤΙΝΑΣ λόγω Listeria monocytogenes",
        "link": "https://www.protothema.gr/test/recall-feta-listeria",
        "summary": "Ο ΕΦΕΤ προχώρησε σε ανάκληση παρτίδας φέτας ΒΥΤΙΝΑΣ "
                   "λόγω ανίχνευσης Listeria monocytogenes.",
        "domain": "protothema.gr",
        "expect_pass": True,
        "expect_pathogen": True,
    },
    # POSITIVE — recall keyword, no pathogen (allergen — should still pass keyword,
    # but rules.py will REJECT later)
    {
        "title": "Ανάκληση παξιμαδιών κανέλας λόγω μη δηλωμένου αλλεργιογόνου",
        "link": "https://www.kathimerini.gr/test/recall-paxim-allergen",
        "summary": "Ανακαλείται παρτίδα παξιμαδιών λόγω μη δηλωμένης παρουσίας γλουτένης.",
        "domain": "kathimerini.gr",
        "expect_pass": True,
        "expect_pathogen": False,
    },
    # NEGATIVE — no recall keyword, irrelevant news
    {
        "title": "Νέα γέφυρα στη Λάρισα από το Υπουργείο Μεταφορών",
        "link": "https://www.in.gr/test/larisa-bridge",
        "summary": "Άνοιξε νέα γέφυρα στη Λάρισα μετά από δύο χρόνια κατασκευής.",
        "domain": "in.gr",
        "expect_pass": False,
        "expect_pathogen": False,
    },
    # POSITIVE — English recall keyword (RASFF-style news echo)
    {
        "title": "Greek authorities recall peanut butter over aflatoxin levels",
        "link": "https://www.news247.gr/test/recall-peanut-aflatoxin",
        "summary": "EFET issued a recall for peanut butter exceeding aflatoxin limits.",
        "domain": "news247.gr",
        "expect_pass": True,
        "expect_pathogen": True,
    },
    # NEGATIVE — has "ανάκληση" but in unrelated context (recall of memories)
    # (Pre-filter will accept; downstream EFET + LLM will reject. Acceptable.)
    {
        "title": "Ανάκληση μνημών από τα 90s: το νέο podcast",
        "link": "https://www.protagon.gr/test/memory-recall-podcast",
        "summary": "Ένα νέο podcast για τις αναμνήσεις της δεκαετίας του '90.",
        "domain": "protagon.gr",
        "expect_pass": True,        # keyword filter is intentionally permissive
        "expect_pathogen": False,
    },
]


def run_dry_test() -> int:
    """Test filtering + dedup logic against synthetic fixtures. Returns exit code."""
    print("=" * 78)
    print("AFTS Greek Gap Finder — News Scraper Dry-Run Test")
    print("=" * 78)

    candidates: list[Candidate] = []
    passed = failed = 0

    for i, fx in enumerate(DRY_RUN_FIXTURES, 1):
        entry = {
            "title": fx["title"],
            "link": fx["link"],
            "summary": fx["summary"],
            "published_parsed": None,
        }
        results = entries_to_candidates([entry], fx["domain"], "rss")
        passed_filter = len(results) > 0
        ok = passed_filter == fx["expect_pass"]

        if ok and passed_filter:
            ok = results[0].has_pathogen_hint == fx["expect_pathogen"]

        status = "✓ PASS" if ok else "✗ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1

        print(f"\n{status}  Fixture #{i}")
        print(f"        title:           {fx['title'][:70]}")
        print(f"        passed filter:   {passed_filter} (expected {fx['expect_pass']})")
        if passed_filter:
            print(f"        pathogen hint:   {results[0].has_pathogen_hint} "
                  f"(expected {fx['expect_pathogen']})")
            candidates.append(results[0])

    # Test deduplication
    print("\n" + "-" * 78)
    print("Deduplication test: feeding identical URL twice...")
    dup = candidates + candidates
    deduped = deduplicate(dup)
    dedup_ok = len(deduped) == len(candidates)
    print(f"  Before dedup: {len(dup)}   After dedup: {len(deduped)}   "
          f"{'✓ PASS' if dedup_ok else '✗ FAIL'}")
    if dedup_ok:
        passed += 1
    else:
        failed += 1

    print("\n" + "=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 78)
    return 0 if failed == 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Greek Gap Finder — News Scraper"
    )
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT,
                        help=f"Output JSONL path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Offline test against synthetic fixtures")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose progress on stderr")
    parser.add_argument("--no-google-news", action="store_true",
                        help="Skip Google News queries (RSS only)")
    args = parser.parse_args()

    if args.dry_run:
        return run_dry_test()

    print(f"AFTS Greek Gap Finder — News Scraper", file=sys.stderr)
    print(f"Sources: {len(RSS_FEEDS)} sites, "
          f"{sum(len(v) for v in RSS_FEEDS.values())} RSS URLs",
          file=sys.stderr)

    rss_candidates = collect_rss(verbose=args.verbose)
    print(f"RSS yield: {len(rss_candidates)} candidates", file=sys.stderr)

    gn_candidates: list[Candidate] = []
    if not args.no_google_news:
        gn_candidates = collect_google_news(verbose=args.verbose)
        print(f"Google News yield: {len(gn_candidates)} candidates",
              file=sys.stderr)

    all_candidates = rss_candidates + gn_candidates
    deduped = deduplicate(all_candidates)
    print(f"After dedup: {len(deduped)} unique candidates "
          f"(removed {len(all_candidates) - len(deduped)} dupes)",
          file=sys.stderr)

    write_jsonl(deduped, args.output)
    print(f"Wrote {len(deduped)} → {args.output}", file=sys.stderr)

    # Quick summary by source for log visibility
    by_source: dict[str, int] = {}
    by_method: dict[str, int] = {}
    pathogen_hits = 0
    for c in deduped:
        by_source[c.source_domain] = by_source.get(c.source_domain, 0) + 1
        by_method[c.fetch_method] = by_method.get(c.fetch_method, 0) + 1
        if c.has_pathogen_hint:
            pathogen_hits += 1
    print(f"\nBy source: {dict(sorted(by_source.items(), key=lambda x: -x[1]))}",
          file=sys.stderr)
    print(f"By method: {by_method}", file=sys.stderr)
    print(f"Pathogen hints: {pathogen_hits}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
