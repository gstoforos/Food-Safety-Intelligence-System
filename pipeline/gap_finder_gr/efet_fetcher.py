"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 3 v2: EFET Fetcher (search-engine based, WAF-immune)

WHY THIS REWRITE:
  efet.gr's WAF blocks datacenter IPs (Azure, AWS, GCP — including GitHub-hosted
  runners). Direct HTTP fetch returns 403/409 from cloud environments.

  Search engines crawl efet.gr just fine — their IPs are whitelisted. We exploit
  that by querying DuckDuckGo HTML search scoped with site:efet.gr.

  The search engine returns:
    - The EFET URL (we cite this as the authoritative source URL)
    - A content snippet from the EFET page (we use this as the EFET body)

  We never touch efet.gr directly. Zero cost. Works from any IP including
  Azure cloud runners. Works the same on Mac later (same code path).

Flow:
  1. Load candidates.jsonl from news_scraper.py
  2. For each candidate:
       a. Extract product/brand keywords from the news title
       b. DuckDuckGo HTML search: site:efet.gr <keywords> ανάκληση
       c. Best matching efet.gr result → use as the EFET URL + snippet as the body
  3. Output verified.jsonl (matched) + unmatched.jsonl (no EFET result found)

Pure Python — no LLM, no Gemini, no Claude, no paid APIs, no proxy services.
Dependencies: requests, beautifulsoup4, lxml

CLI:
    python -m pipeline.gap_finder_gr.efet_fetcher
    python -m pipeline.gap_finder_gr.efet_fetcher --dry-run
    python -m pipeline.gap_finder_gr.efet_fetcher --verbose
    python -m pipeline.gap_finder_gr.efet_fetcher --probe "παξιμάδια κανέλας"
"""

from __future__ import annotations
import argparse
import json
import random
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict
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
REQUEST_DELAY_MIN = 1.5
REQUEST_DELAY_MAX = 3.5
REQUEST_DELAY = REQUEST_DELAY_MIN  # for backwards-compat with main.py

MATCH_THRESHOLD = 0.10

DEFAULT_CANDIDATES = "docs/data/gap_finder_gr/candidates.jsonl"
DEFAULT_VERIFIED = "docs/data/gap_finder_gr/verified.jsonl"
DEFAULT_UNMATCHED = "docs/data/gap_finder_gr/unmatched.jsonl"

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
class EfetSearchHit:
    url: str
    title: str
    snippet: str
    score: float

    def to_dict(self) -> dict:
        return asdict(self)


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
# DUCKDUCKGO HTML SEARCH
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
    """DuckDuckGo wraps result URLs in /l/?uddg=<encoded-real-url>. Unwrap."""
    if not url:
        return url
    parsed = urlparse(url)
    if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
        qs = parse_qs(parsed.query)
        real = qs.get("uddg", [None])[0]
        if real:
            return unquote(real)
    if url.startswith("//"):
        return "https:" + url
    return url


def ddg_search(query: str, verbose: bool = False) -> list[EfetSearchHit]:
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
            efet_hits = [h for h in hits if "efet.gr" in (urlparse(h.url).netloc or "")]
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


def _parse_ddg_html(html: str) -> list[EfetSearchHit]:
    soup = BeautifulSoup(html, "lxml")
    hits: list[EfetSearchHit] = []
    for result in soup.find_all("div", class_=re.compile(r"\bresult\b")):
        a = result.find("a", class_=re.compile(r"result__a"))
        if not a:
            continue
        url = _ddg_clean_redirect(a.get("href", "").strip())
        title = a.get_text(" ", strip=True)
        sn_el = result.find(class_=re.compile(r"result__snippet"))
        snippet = sn_el.get_text(" ", strip=True) if sn_el else ""
        if url and title:
            hits.append(EfetSearchHit(url=url, title=title, snippet=snippet, score=0.0))
    return hits


def _parse_ddg_lite(html: str) -> list[EfetSearchHit]:
    soup = BeautifulSoup(html, "lxml")
    hits: list[EfetSearchHit] = []
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
                hits.append(EfetSearchHit(
                    url=pending_link, title=pending_title, snippet=snippet, score=0.0
                ))
                pending_link = None
                pending_title = ""
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# QUERY BUILDING & SCORING
# ─────────────────────────────────────────────────────────────────────────────

def build_query(news_title: str) -> str:
    tokens = tokenize(news_title)
    sorted_tokens = sorted(tokens, key=len, reverse=True)[:6]
    keywords = " ".join(sorted_tokens) if sorted_tokens else news_title
    return f"site:efet.gr {keywords} ανάκληση"


def best_match(news_title: str, hits: list[EfetSearchHit]) -> Optional[EfetSearchHit]:
    if not hits:
        return None
    news_tokens = tokenize(news_title)
    if not news_tokens:
        return None

    scored: list[EfetSearchHit] = []
    for h in hits:
        efet_tokens = tokenize(f"{h.title} {h.snippet}")
        h.score = jaccard(news_tokens, efet_tokens)
        scored.append(h)

    scored.sort(key=lambda h: h.score, reverse=True)
    top = scored[0]
    if top.score < MATCH_THRESHOLD:
        return None
    return top


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


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_candidates(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out = []
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


def write_jsonl(records: list[dict], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# CORE: per-candidate verification
# ─────────────────────────────────────────────────────────────────────────────

def verify_candidate(
    candidate: dict,
    verbose: bool = False,
) -> tuple[Optional[VerifiedRecord], float]:
    news_title = candidate.get("title", "")
    if not news_title:
        return None, 0.0

    query = build_query(news_title)
    hits = ddg_search(query, verbose=verbose)
    top = best_match(news_title, hits)

    if not top:
        best_score = max((h.score for h in hits), default=0.0)
        return None, best_score

    return VerifiedRecord(
        news_url=candidate.get("url", ""),
        news_title=news_title,
        news_published=candidate.get("published", ""),
        news_source_domain=candidate.get("source_domain", ""),
        efet_url=top.url,
        efet_title=top.title,
        efet_date_iso=extract_date(top.snippet),
        efet_body=f"{top.title}\n\n{top.snippet}",
        match_score=round(top.score, 4),
        matched_at=_now_utc(),
    ), top.score


# ─────────────────────────────────────────────────────────────────────────────
# BACKWARDS-COMPAT SHIMS (main.py orchestrator imports these names)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_efet_index(pages: int = 0, verbose: bool = False):
    """No-op in v2 — matching happens per-candidate via ddg_search."""
    if verbose:
        print("  [shim] fetch_efet_index is a no-op in v2 (search-based)",
              file=sys.stderr)
    return []


def match_candidate_to_efet(news_title, news_published, efet_index):
    """Called by main.py Stage 2. Ignores efet_index; runs DDG search."""
    candidate = {"title": news_title, "published": news_published}
    record, score = verify_candidate(candidate, verbose=False)
    if record is None:
        return None, score

    class _Ann:
        def __init__(self, rec):
            self.url = rec.efet_url
            self.title = rec.efet_title
            self.date_iso = rec.efet_date_iso
            self.body = rec.efet_body
    return _Ann(record), score


def fetch_announcement_body(url: str, verbose: bool = False) -> str:
    """No-op shim — body already embedded in match result from snippet."""
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# PROBE — manual one-off query for debugging
# ─────────────────────────────────────────────────────────────────────────────

def run_probe(query_text: str) -> int:
    print("=" * 78)
    print(f"AFTS EFET Fetcher — PROBE")
    print(f"  Input title: {query_text!r}")
    print("=" * 78)
    q = build_query(query_text)
    print(f"  DDG query:   {q!r}\n")
    hits = ddg_search(q, verbose=True)
    print(f"\n  Got {len(hits)} efet.gr results:\n")
    for i, h in enumerate(hits[:10], 1):
        print(f"  #{i}")
        print(f"     url:     {h.url}")
        print(f"     title:   {h.title[:90]}")
        print(f"     snippet: {h.snippet[:150]}")
        print()
    top = best_match(query_text, hits)
    if top:
        print(f"  BEST MATCH: score={top.score:.3f}")
        print(f"     url:   {top.url}")
        print(f"     title: {top.title}")
    else:
        print(f"  NO MATCH above threshold {MATCH_THRESHOLD}")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# DRY-RUN
# ─────────────────────────────────────────────────────────────────────────────

def run_dry_test() -> int:
    print("=" * 78)
    print("AFTS EFET Fetcher v2 — Dry-Run Test")
    print("=" * 78)

    synthetic_hits = [
        EfetSearchHit(
            url="https://www.efet.gr/.../anakleiseis-cat/item/15001-strudel",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (Στρουντελάκια μήλο-κανέλα)",
            snippet="Αθήνα, 14 Μαΐου 2026. Strudito strudel μήλο/κανέλα — παρουσία κουμαρίνης.",
            score=0.0,
        ),
        EfetSearchHit(
            url="https://www.efet.gr/.../anakleiseis-cat/item/15000-paximadia",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση παξιμαδιών κανέλας",
            snippet="ΚΑΡΑΓΙΑΝΝΑΚΗΣ ΑΡΤΟΠΟΙΙΑ Λέσβου — μη δηλωμένο αλλεργιογόνο γλουτένη.",
            score=0.0,
        ),
        EfetSearchHit(
            url="https://www.efet.gr/.../anakleiseis-cat/item/14990-feta",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση φέτας ΒΥΤΙΝΑΣ — Listeria",
            snippet="9 Απριλίου 2026. Φέτα ΒΥΤΙΝΑΣ ΠΟΠ — Listeria monocytogenes.",
            score=0.0,
        ),
    ]

    tests = [
        {
            "news_title": "ΕΦΕΤ: Ανάκληση Strudito strudel μήλο κανέλα λόγω κουμαρίνης",
            "expected": "15001-strudel",
        },
        {
            "news_title": "Ανάκληση παξιμαδιών κανέλας ΚΑΡΑΓΙΑΝΝΑΚΗΣ λόγω γλουτένης",
            "expected": "15000-paximadia",
        },
        {
            "news_title": "Ανάκληση φέτας ΒΥΤΙΝΑΣ Listeria",
            "expected": "14990-feta",
        },
        {
            "news_title": "Νέα γέφυρα στη Λάρισα",
            "expected": None,
        },
    ]

    passed = failed = 0
    for t in tests:
        for h in synthetic_hits:
            h.score = 0.0
        top = best_match(t["news_title"], synthetic_hits)
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
            print(f"        match score={top.score:.3f}  url={top.url}")
        else:
            print(f"        no match above threshold {MATCH_THRESHOLD}")
        print(f"        expected fragment: {t['expected']}")

    print("\n" + "-" * 78)
    q = build_query("Ανάκληση Strudito strudel μήλο κανέλα λόγω κουμαρίνης")
    has_site = "site:efet.gr" in q
    has_recall = "ανάκληση" in q
    has_product = "strudito" in q.lower() or "strudel" in q.lower()
    ok = has_site and has_recall and has_product
    print(f"Query builder: {q!r}")
    print(f"  site:efet.gr present:  {has_site}")
    print(f"  ανάκληση present:      {has_recall}")
    print(f"  product token present: {has_product}")
    print(f"  {'✓ PASS' if ok else '✗ FAIL'}")
    if ok:
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
        description="AFTS Greek Gap Finder — EFET Fetcher v2 (search-engine based)"
    )
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES)
    parser.add_argument("--verified", default=DEFAULT_VERIFIED)
    parser.add_argument("--unmatched", default=DEFAULT_UNMATCHED)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe", metavar="TITLE",
                        help="Run a single DDG search for diagnostics")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max candidates to verify (0 = no limit)")
    args = parser.parse_args()

    if args.dry_run:
        return run_dry_test()
    if args.probe:
        return run_probe(args.probe)

    candidates = read_candidates(args.candidates)
    print(f"Loaded {len(candidates)} candidates from {args.candidates}",
          file=sys.stderr)

    if args.limit:
        candidates = candidates[: args.limit]
        print(f"Limit applied: processing first {len(candidates)}", file=sys.stderr)

    verified: list[dict] = []
    unmatched: list[dict] = []
    now = _now_utc()

    for i, cand in enumerate(candidates, 1):
        if args.verbose:
            print(f"\n[{i}/{len(candidates)}] {cand.get('title', '')[:70]}",
                  file=sys.stderr)
        try:
            record, score = verify_candidate(cand, verbose=args.verbose)
        except Exception as e:
            if args.verbose:
                print(f"  ERROR: {e}", file=sys.stderr)
            unmatched.append({**cand, "best_score": 0.0, "checked_at": now,
                              "error": str(e)})
            _polite_sleep()
            continue

        if record is None:
            unmatched.append({**cand, "best_score": round(score, 4),
                              "checked_at": now})
            if args.verbose:
                print(f"  [unmatched] best_score={score:.3f}", file=sys.stderr)
        else:
            verified.append(record.to_dict())
            if args.verbose:
                print(f"  [matched]   score={record.match_score:.3f} → "
                      f"{record.efet_url}", file=sys.stderr)

        _polite_sleep()

    write_jsonl(verified, args.verified)
    write_jsonl(unmatched, args.unmatched)
    print(f"\nResults: {len(verified)} verified, {len(unmatched)} unmatched",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
