"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 3 v3: EFET Fetcher (search-engine BULK index, fast)

WHY THIS VERSION:
  v2 made one DuckDuckGo search per news candidate (211 calls). With polite
  delays this exceeded GitHub Actions' 30-min workflow timeout.

  v3 inverts the architecture: do FEW broad DDG queries (5) to bulk-load
  the EFET recall index, then match each news candidate against that index
  in-memory (zero network per candidate).

  Stage 2 total: ~30 seconds (was 15-25 minutes).

  EFET's WAF still blocks direct datacenter access — we still go through
  DuckDuckGo as the search-engine proxy. Same WAF-immune principle as v2,
  just batched the right way.

Flow:
  1. fetch_efet_index() runs 5 broad DDG queries:
       site:efet.gr ΕΦΕΤ ανάκληση 2026
       site:efet.gr ΕΦΕΤ ανάκληση 2025
       site:efet.gr Δελτίο Τύπου Ανάκληση
       site:efet.gr ανακαλεί τρόφιμο
       site:efet.gr ανάκληση προϊόντος
     → builds in-memory list of ~50–150 unique EFET announcements
  2. For each news candidate, score Jaccard token overlap against every
     indexed EFET entry. Best match above MATCH_THRESHOLD wins.
  3. Write verified.jsonl + unmatched.jsonl as before.

No proxy services, no paid APIs, no LLMs. Pure DDG HTML scraping.
Same code path will work on Mac later (no architectural change needed).

CLI:
    python -m pipeline.gap_finder_gr.efet_fetcher
    python -m pipeline.gap_finder_gr.efet_fetcher --dry-run
    python -m pipeline.gap_finder_gr.efet_fetcher --verbose
    python -m pipeline.gap_finder_gr.efet_fetcher --probe "παξιμάδια κανέλας"
    python -m pipeline.gap_finder_gr.efet_fetcher --show-index   # build & print
"""

from __future__ import annotations
import argparse
import json
import random
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote

import requests
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DDG_HTML_URL = "https://html.duckduckgo.com/html/"
DDG_LITE_URL = "https://lite.duckduckgo.com/lite/"

# Broad index-building queries. Each returns 20–30 EFET URLs. After dedup
# we typically get 50–150 unique recent EFET recall announcements.
INDEX_QUERIES = [
    "site:efet.gr ΕΦΕΤ ανάκληση 2026",
    "site:efet.gr ΕΦΕΤ ανάκληση 2025",
    "site:efet.gr Δελτίο Τύπου Ανάκληση",
    "site:efet.gr ανακαλεί τρόφιμο",
    "site:efet.gr ανάκληση προϊόντος",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0",
]

REQUEST_TIMEOUT = 30
REQUEST_DELAY_MIN = 2.0   # between bulk index queries (only 5 total, can be polite)
REQUEST_DELAY_MAX = 4.0
REQUEST_DELAY = REQUEST_DELAY_MIN  # backwards-compat name for main.py

MATCH_THRESHOLD = 0.10
DATE_WINDOW_DAYS = 30      # news vs EFET date window (months apart = no match)

DEFAULT_CANDIDATES = "docs/data/gap_finder_gr/candidates.jsonl"
DEFAULT_VERIFIED = "docs/data/gap_finder_gr/verified.jsonl"
DEFAULT_UNMATCHED = "docs/data/gap_finder_gr/unmatched.jsonl"
DEFAULT_INDEX_CACHE = "docs/data/gap_finder_gr/efet_index.jsonl"

STOPWORDS = {
    "ο", "η", "το", "οι", "τα", "τον", "την", "του", "της", "των",
    "ένα", "μία", "μια", "και", "σε", "από", "για", "με", "προς",
    "που", "πως", "ως", "στο", "στη", "στην", "στα", "στους", "στις",
    "είναι", "ήταν", "έχει", "είχε", "θα", "να", "δεν", "μη", "μην",
    "δελτιο", "τυπου", "δελτίο", "τύπου",
    "ανακληση", "ανάκληση", "ανακαλει", "ανακαλείται", "ανακαλούνται",
    "προϊον", "προϊόν", "προϊοντος", "προϊόντος",
    "ασφαλους", "ασφαλούς",
    "λογω", "λόγω", "παρουσιας", "παρουσίας",
    "εφετ", "efet",
    "the", "of", "to", "in", "for", "and", "a", "an", "on", "with",
    "by", "is", "was", "be", "been",
    "recall", "recalls", "recalled", "recalling",
    "product", "products", "presence", "due", "news",
}


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION & TOKENIZATION
# ─────────────────────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r"\s+", " ", t)
    return t


_STOPWORDS_N = {normalize_text(w) for w in STOPWORDS}
_TOKEN_RE = re.compile(r"[a-zα-ω0-9]{3,}", re.UNICODE)


def tokenize(text: str) -> set[str]:
    if not text:
        return set()
    n = normalize_text(text)
    return {t for t in _TOKEN_RE.findall(n) if t not in _STOPWORDS_N}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ─────────────────────────────────────────────────────────────────────────────
# DATA RECORDS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EfetAnnouncement:
    """One EFET recall entry, indexed in memory for fast matching."""
    url: str
    title: str
    snippet: str           # search-engine snippet — used as efet_body
    date_iso: str = ""
    tokens: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["tokens"] = sorted(self.tokens)
        return d


@dataclass
class VerifiedRecord:
    news_url: str
    news_title: str
    news_published: str
    news_source_domain: str
    efet_url: str
    efet_title: str
    efet_date_iso: str
    efet_body: str
    match_score: float
    matched_at: str

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# DUCKDUCKGO HTML SEARCH (POST endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def _ddg_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "el-GR,el;q=0.9,en;q=0.7",
        "Referer": "https://duckduckgo.com/",
    }


def _polite_sleep() -> None:
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))


def _ddg_clean_redirect(url: str) -> str:
    """DuckDuckGo wraps results in /l/?uddg=<encoded>. Unwrap to real URL."""
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    parsed = urlparse(url)
    if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
        qs = parse_qs(parsed.query)
        real = qs.get("uddg", [None])[0]
        if real:
            return unquote(real)
    return url


def ddg_search(query: str, verbose: bool = False) -> list[dict]:
    """Run one DDG query; return raw hits (filtered to efet.gr only)."""
    for endpoint, parser in [
        (DDG_HTML_URL, _parse_ddg_html),
        (DDG_LITE_URL, _parse_ddg_lite),
    ]:
        try:
            if verbose:
                print(f"  [DDG] POST {endpoint}  q={query!r}", file=sys.stderr)
            resp = requests.post(
                endpoint,
                data={"q": query, "kl": "gr-el"},
                headers=_ddg_headers(),
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            hits = parser(resp.text)
            efet_hits = [h for h in hits if "efet.gr" in (urlparse(h["url"]).netloc or "")]
            if verbose:
                print(f"  [DDG] got {len(hits)} results, {len(efet_hits)} on efet.gr",
                      file=sys.stderr)
            if efet_hits:
                return efet_hits
        except requests.RequestException as e:
            if verbose:
                print(f"  [DDG] error on {endpoint}: {e}", file=sys.stderr)
            continue
    return []


def _parse_ddg_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    hits: list[dict] = []
    for result in soup.find_all("div", class_=re.compile(r"\bresult\b")):
        a = result.find("a", class_=re.compile(r"result__a"))
        if not a:
            continue
        url = _ddg_clean_redirect(a.get("href", "").strip())
        title = a.get_text(" ", strip=True)
        sn_el = result.find(class_=re.compile(r"result__snippet"))
        snippet = sn_el.get_text(" ", strip=True) if sn_el else ""
        if url and title:
            hits.append({"url": url, "title": title, "snippet": snippet})
    return hits


def _parse_ddg_lite(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    hits: list[dict] = []
    rows = soup.find_all("tr")
    pending_link = None
    pending_title = ""
    for tr in rows:
        a = tr.find("a")
        if a and a.get("href", "").startswith(("http", "//")):
            pending_link = _ddg_clean_redirect(a.get("href"))
            pending_title = a.get_text(" ", strip=True)
            continue
        if pending_link and not a:
            snippet = tr.get_text(" ", strip=True)
            if snippet and len(snippet) > 20:
                hits.append({"url": pending_link, "title": pending_title,
                             "snippet": snippet})
                pending_link = None
                pending_title = ""
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# BULK INDEX BUILD (the new fast architecture)
# ─────────────────────────────────────────────────────────────────────────────

def build_efet_index(verbose: bool = False) -> list[EfetAnnouncement]:
    """
    Run all INDEX_QUERIES against DDG, dedupe by URL, return a list of
    EfetAnnouncement with tokens pre-computed for fast in-memory matching.

    Total network: 5 DDG calls (~10-20 seconds).
    Output: ~50-150 EFET recall entries (after dedup).
    """
    all_hits: list[dict] = []
    for i, q in enumerate(INDEX_QUERIES):
        if verbose:
            print(f"[index {i + 1}/{len(INDEX_QUERIES)}] {q!r}", file=sys.stderr)
        hits = ddg_search(q, verbose=verbose)
        all_hits.extend(hits)
        if i + 1 < len(INDEX_QUERIES):
            _polite_sleep()

    # Dedupe by URL, prefer the entry with the longest snippet
    by_url: dict[str, dict] = {}
    for h in all_hits:
        existing = by_url.get(h["url"])
        if existing is None or len(h["snippet"]) > len(existing["snippet"]):
            by_url[h["url"]] = h

    index: list[EfetAnnouncement] = []
    for h in by_url.values():
        ann = EfetAnnouncement(
            url=h["url"],
            title=h["title"],
            snippet=h["snippet"],
            date_iso=extract_date(h["snippet"]),
        )
        ann.tokens = tokenize(f"{ann.title} {ann.snippet}")
        index.append(ann)

    if verbose:
        print(f"[index] built {len(index)} unique EFET entries from "
              f"{len(all_hits)} raw hits", file=sys.stderr)
    return index


# ─────────────────────────────────────────────────────────────────────────────
# DATE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

GREEK_MONTHS = {
    "ιανουαριου": 1, "φεβρουαριου": 2, "μαρτιου": 3, "απριλιου": 4,
    "μαιου": 5, "ιουνιου": 6, "ιουλιου": 7, "αυγουστου": 8,
    "σεπτεμβριου": 9, "οκτωβριου": 10, "νοεμβριου": 11, "δεκεμβριου": 12,
    "ιανουαριος": 1, "φεβρουαριος": 2, "μαρτιος": 3, "απριλιος": 4,
    "μαιος": 5, "ιουνιος": 6, "ιουλιος": 7, "αυγουστος": 8,
    "σεπτεμβριος": 9, "οκτωβριος": 10, "νοεμβριος": 11, "δεκεμβριος": 12,
}


def extract_date(text: str) -> str:
    if not text:
        return ""
    n = normalize_text(text)
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", n)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})", n)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\s+([α-ω]+)\s+(\d{4})", n)
    if m:
        d, month_name, y = int(m.group(1)), m.group(2), int(m.group(3))
        if month_name in GREEK_MONTHS:
            try:
                return datetime(y, GREEK_MONTHS[month_name], d).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return ""


def _days_between(iso_a: str, iso_b: str) -> Optional[int]:
    if not (iso_a and iso_b):
        return None
    try:
        a = datetime.fromisoformat(iso_a[:10])
        b = datetime.fromisoformat(iso_b[:10])
        return abs((a - b).days)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MATCHING (in-memory, no network)
# ─────────────────────────────────────────────────────────────────────────────

def match_in_index(
    news_title: str,
    news_published_iso: str,
    index: list[EfetAnnouncement],
) -> tuple[Optional[EfetAnnouncement], float]:
    """
    Score news_title vs every indexed EFET entry by Jaccard token overlap.
    Apply date window if both sides have a date.
    """
    news_tokens = tokenize(news_title)
    if not news_tokens or not index:
        return None, 0.0

    best: Optional[EfetAnnouncement] = None
    best_score = 0.0

    for ann in index:
        score = jaccard(news_tokens, ann.tokens)
        if score <= best_score:
            continue
        if news_published_iso and ann.date_iso:
            delta = _days_between(news_published_iso, ann.date_iso)
            if delta is not None and delta > DATE_WINDOW_DAYS:
                continue
        best = ann
        best_score = score

    if best_score < MATCH_THRESHOLD:
        return None, best_score
    return best, best_score


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_candidates(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def write_jsonl(records, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            if hasattr(r, "to_dict"):
                r = r.to_dict()
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API (called by main.py orchestrator)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_efet_index(pages: int = 0, verbose: bool = False) -> list[EfetAnnouncement]:
    """Build the EFET recall index via 5 bulk DDG queries. Total ~15-20s."""
    return build_efet_index(verbose=verbose)


def match_candidate_to_efet(
    news_title: str,
    news_published: str,
    efet_index: list[EfetAnnouncement],
):
    """Match one news candidate against pre-built index. NO network call here."""
    return match_in_index(news_title, news_published, efet_index)


def fetch_announcement_body(url: str, verbose: bool = False) -> str:
    """No-op shim — body already in the indexed snippet."""
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# PROBE — manual debugging
# ─────────────────────────────────────────────────────────────────────────────

def run_probe(query_text: str) -> int:
    print("=" * 78)
    print(f"AFTS EFET Fetcher v3 — PROBE")
    print(f"  News title: {query_text!r}")
    print("=" * 78)
    print("\nBuilding index...")
    index = build_efet_index(verbose=True)
    print(f"\nIndex has {len(index)} EFET entries.\n")
    print("Top 5 entries in index:")
    for i, ann in enumerate(index[:5], 1):
        print(f"  #{i} {ann.url}")
        print(f"      {ann.title[:90]}")
    print()
    top, score = match_in_index(query_text, "", index)
    if top:
        print(f"  BEST MATCH: score={score:.3f}")
        print(f"     url:     {top.url}")
        print(f"     title:   {top.title}")
        print(f"     snippet: {top.snippet[:200]}")
    else:
        # Show best (sub-threshold) for diagnostics
        best_score = 0.0
        best = None
        nt = tokenize(query_text)
        for ann in index:
            s = jaccard(nt, ann.tokens)
            if s > best_score:
                best_score = s
                best = ann
        print(f"  NO MATCH above threshold {MATCH_THRESHOLD}")
        print(f"  (best sub-threshold: score={best_score:.3f})")
        if best:
            print(f"     would-be url:   {best.url}")
            print(f"     would-be title: {best.title[:90]}")
    return 0


def run_show_index() -> int:
    print("Building EFET index...")
    index = build_efet_index(verbose=True)
    print(f"\n{len(index)} entries:\n")
    for i, ann in enumerate(index, 1):
        print(f"#{i:3d} {ann.url}")
        print(f"     date:    {ann.date_iso!r}")
        print(f"     title:   {ann.title[:100]}")
        print(f"     snippet: {ann.snippet[:150]}")
        print()
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# DRY-RUN (offline)
# ─────────────────────────────────────────────────────────────────────────────

def run_dry_test() -> int:
    print("=" * 78)
    print("AFTS EFET Fetcher v3 — Dry-Run Test (offline, synthetic index)")
    print("=" * 78)

    synthetic_index = []
    for url, title, snippet, date in [
        ("https://www.efet.gr/.../item/15001-strudel",
         "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (Στρουντελάκια μήλο-κανέλα)",
         "Αθήνα, 14 Μαΐου 2026. Strudito strudel μήλο/κανέλα — παρουσία κουμαρίνης.",
         "2026-05-14"),
        ("https://www.efet.gr/.../item/15000-paximadia",
         "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση παξιμαδιών κανέλας",
         "ΚΑΡΑΓΙΑΝΝΑΚΗΣ ΑΡΤΟΠΟΙΙΑ Λέσβου — μη δηλωμένο αλλεργιογόνο γλουτένη.",
         "2026-05-14"),
        ("https://www.efet.gr/.../item/14990-feta",
         "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση φέτας ΒΥΤΙΝΑΣ — Listeria",
         "9 Απριλίου 2026. Φέτα ΒΥΤΙΝΑΣ ΠΟΠ — Listeria monocytogenes.",
         "2026-04-09"),
    ]:
        ann = EfetAnnouncement(url=url, title=title, snippet=snippet, date_iso=date)
        ann.tokens = tokenize(f"{title} {snippet}")
        synthetic_index.append(ann)

    tests = [
        {
            "news_title": "ΕΦΕΤ: Ανάκληση Strudito strudel μήλο κανέλα λόγω κουμαρίνης",
            "news_published": "2026-05-14T20:00:00+03:00",
            "expected": "15001-strudel",
        },
        {
            "news_title": "Ανάκληση παξιμαδιών κανέλας ΚΑΡΑΓΙΑΝΝΑΚΗΣ λόγω γλουτένης",
            "news_published": "2026-05-14T20:00:00+03:00",
            "expected": "15000-paximadia",
        },
        {
            "news_title": "Ανάκληση φέτας ΒΥΤΙΝΑΣ Listeria",
            "news_published": "2026-04-09T18:00:00+03:00",
            "expected": "14990-feta",
        },
        {
            "news_title": "Νέα γέφυρα στη Λάρισα",
            "news_published": "2026-05-14T12:00:00+03:00",
            "expected": None,
        },
    ]

    passed = failed = 0
    for t in tests:
        top, score = match_in_index(t["news_title"], t["news_published"], synthetic_index)
        got = top.url if top else None
        ok = (t["expected"] is None and got is None) or (
            t["expected"] is not None and got is not None and t["expected"] in got
        )
        status = "✓ PASS" if ok else "✗ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"\n{status}  {t['news_title'][:70]}")
        if top:
            print(f"        match score={score:.3f}  url={top.url}")
        else:
            print(f"        no match (best sub-threshold score={score:.3f})")
        print(f"        expected fragment: {t['expected']}")

    print("\n" + "=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 78)
    return 0 if failed == 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN (standalone CLI — also processes candidates.jsonl)
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Greek Gap Finder — EFET Fetcher v3 (bulk DDG index)"
    )
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES)
    parser.add_argument("--verified", default=DEFAULT_VERIFIED)
    parser.add_argument("--unmatched", default=DEFAULT_UNMATCHED)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe", metavar="TITLE",
                        help="Run a single match test for a news title")
    parser.add_argument("--show-index", action="store_true",
                        help="Build the EFET index and print all entries")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        return run_dry_test()
    if args.probe:
        return run_probe(args.probe)
    if args.show_index:
        return run_show_index()

    candidates = read_candidates(args.candidates)
    print(f"Loaded {len(candidates)} candidates from {args.candidates}",
          file=sys.stderr)

    print("Building EFET index via bulk DDG queries...", file=sys.stderr)
    index = build_efet_index(verbose=args.verbose)
    print(f"EFET index: {len(index)} entries", file=sys.stderr)

    verified: list[dict] = []
    unmatched: list[dict] = []
    now = _now_utc()

    for cand in candidates:
        news_title = cand.get("title", "")
        news_pub = cand.get("published", "")
        top, score = match_in_index(news_title, news_pub, index)
        if not top:
            unmatched.append({**cand, "best_score": round(score, 4),
                              "checked_at": now})
            continue
        record = VerifiedRecord(
            news_url=cand.get("url", ""),
            news_title=news_title,
            news_published=news_pub,
            news_source_domain=cand.get("source_domain", ""),
            efet_url=top.url,
            efet_title=top.title,
            efet_date_iso=top.date_iso,
            efet_body=f"{top.title}\n\n{top.snippet}",
            match_score=round(score, 4),
            matched_at=now,
        )
        verified.append(record.to_dict())

    write_jsonl(verified, args.verified)
    write_jsonl(unmatched, args.unmatched)
    print(f"\nResults: {len(verified)} verified, {len(unmatched)} unmatched",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
