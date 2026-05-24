"""
AFTS Food Safety Intelligence — Gap Finder
Search-engine verifier (parametric — works for any CountryConfig).

Replaces the per-country `efet_fetcher` style with a generic module:
  - Takes a CountryConfig (cfg.bulk_index_queries, cfg.authority_domain, etc.)
  - Uses DuckDuckGo HTML search to bulk-load the authority's recall list
    (5 broad queries instead of one per news candidate)
  - Filters hits to cfg.authority_domain
  - Optionally tightens via cfg.authority_item_url_regex (drops portal/category
    pages that aren't real recall items)
  - Matches news candidates against the in-memory index by Jaccard token overlap
    with a date-window constraint

Total network per run: ~5 DDG calls (~15-20 seconds).
Works from any IP including Azure datacenter (WAF-immune by design — we never
touch the authority's site directly, only the search engine's snapshot).

CLI:
    python -m pipeline.gap_finder.search_verifier --country it --dry-run
    python -m pipeline.gap_finder.search_verifier --country gr --show-index
    python -m pipeline.gap_finder.search_verifier --country it --probe "richiamo Listeria"
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

try:
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder.countries import get as get_country           # type: ignore
    from gap_finder.countries.base import CountryConfig           # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION (country-independent)
# ─────────────────────────────────────────────────────────────────────────────

DDG_HTML_URL = "https://html.duckduckgo.com/html/"
DDG_LITE_URL = "https://lite.duckduckgo.com/lite/"
# Independent search engine — used as fallback when DDG throttles us.
# Mojeek has its own crawler and less aggressive bot detection.
MOJEEK_URL = "https://www.mojeek.com/search"

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
# DDG aggressively throttles after 2-3 queries from the same session.
# Bumping delays from 2-4s to 7-11s lets all 5 bulk queries succeed.
REQUEST_DELAY_MIN = 7.0
REQUEST_DELAY_MAX = 11.0
REQUEST_DELAY = REQUEST_DELAY_MIN
MATCH_THRESHOLD = 0.10
DATE_WINDOW_DAYS = 30

# Stopwords pooled from all supported languages.
STOPWORDS = {
    # Greek
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
    # Italian
    "il", "la", "lo", "gli", "le", "un", "una", "uno", "del", "della",
    "dello", "dei", "delle", "degli", "al", "alla", "allo", "ai", "alle",
    "agli", "in", "con", "per", "tra", "fra", "su", "da", "di", "che",
    "non", "se", "ma", "ed", "et",
    "richiamo", "richiamato", "richiamati", "ritiro", "ritirato",
    "allerta", "salute", "ministero", "avviso", "sicurezza", "alimentare",
    # English
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
class AuthorityAnnouncement:
    """One recall entry from the authority, indexed for fast matching."""
    url: str
    title: str
    snippet: str
    date_iso: str = ""
    tokens: set[str] = field(default_factory=set)

    @property
    def body(self) -> str:
        """main.py expects `.body` — title + snippet."""
        return f"{self.title}\n\n{self.snippet}" if self.snippet else self.title

    @body.setter
    def body(self, value: str) -> None:
        self.snippet = value or ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["tokens"] = sorted(self.tokens)
        d["body"] = self.body
        return d


# Backwards-compat alias for any external code importing the old class
EfetAnnouncement = AuthorityAnnouncement


@dataclass
class VerifiedRecord:
    news_url: str
    news_title: str
    news_published: str
    news_source_domain: str
    efet_url: str            # left named 'efet_url' for xlsx schema compat
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

def _ddg_headers(cfg: Optional[CountryConfig] = None) -> dict:
    accept_lang = "en;q=0.9"
    if cfg:
        accept_lang = f"{cfg.language_code}-{cfg.code.upper()},{cfg.language_code};q=0.9,en;q=0.7"
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": accept_lang,
        "Referer": "https://duckduckgo.com/",
    }


def _polite_sleep() -> None:
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))


def _ddg_clean_redirect(url: str) -> str:
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


def ddg_search(query: str, cfg: CountryConfig, verbose: bool = False) -> list[dict]:
    """Run one DDG query (HTML then Lite). Filter to cfg.authority_domain hits.

    Note: we tested Mojeek as a fallback but it 403s from datacenter IPs
    (Azure/Hetzner). Brave Search has a free 2000/mo API tier — usable as
    fallback if we register a key later. For now DDG is the only path.
    """
    locale = f"{cfg.code}-{cfg.language_code}"
    for endpoint, parser in [
        (DDG_HTML_URL, _parse_ddg_html),
        (DDG_LITE_URL, _parse_ddg_lite),
    ]:
        try:
            if verbose:
                print(f"  [DDG] POST {endpoint}  q={query!r}", file=sys.stderr)
            resp = requests.post(
                endpoint,
                data={"q": query, "kl": locale},
                headers=_ddg_headers(cfg),
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            hits = parser(resp.text)
            authority_hits = [
                h for h in hits
                if cfg.authority_domain in (urlparse(h["url"]).netloc or "")
            ]
            if verbose:
                print(f"  [DDG] got {len(hits)} results, "
                      f"{len(authority_hits)} on {cfg.authority_domain}",
                      file=sys.stderr)
            if authority_hits:
                return authority_hits
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


def _parse_mojeek_html(html: str) -> list[dict]:
    """Mojeek result rows: <a class="ob"> (link) inside a result <li>."""
    soup = BeautifulSoup(html, "lxml")
    hits: list[dict] = []
    # Mojeek structure: <ul class="results-standard"><li> ... <a class="ob title">title</a>
    # <p class="s">snippet</p></li></ul>
    for li in soup.find_all("li"):
        a = li.find("a", class_=re.compile(r"\bob\b"))
        if not a:
            a = li.find("a")
        if not a or not a.get("href"):
            continue
        href = a.get("href", "").strip()
        if not href.startswith(("http://", "https://")):
            continue
        title = a.get_text(" ", strip=True)
        # Snippet: <p class="s"> or general <p> sibling
        snippet = ""
        snippet_el = li.find("p", class_=re.compile(r"\bs\b")) or li.find("p")
        if snippet_el:
            snippet = snippet_el.get_text(" ", strip=True)
        if href and title:
            hits.append({"url": href, "title": title, "snippet": snippet})
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# BULK INDEX BUILD
# ─────────────────────────────────────────────────────────────────────────────

def build_index(cfg: CountryConfig, verbose: bool = False) -> list[AuthorityAnnouncement]:
    """5 broad DDG queries → dedup → in-memory list of recent recall items."""
    item_pattern = re.compile(cfg.authority_item_url_regex, re.IGNORECASE)
    all_hits: list[dict] = []
    queries = cfg.bulk_index_queries

    for i, q in enumerate(queries):
        if verbose:
            print(f"[index {i + 1}/{len(queries)}] {q!r}", file=sys.stderr)
        hits = ddg_search(q, cfg, verbose=verbose)
        all_hits.extend(hits)
        if i + 1 < len(queries):
            _polite_sleep()

    # Dedupe by URL, prefer entry with longest snippet
    by_url: dict[str, dict] = {}
    for h in all_hits:
        existing = by_url.get(h["url"])
        if existing is None or len(h["snippet"]) > len(existing["snippet"]):
            by_url[h["url"]] = h

    index: list[AuthorityAnnouncement] = []
    portal_dropped = 0
    for h in by_url.values():
        # Match URL regex against the FULL url (path + query string), so countries
        # whose authorities use query params for the recall ID (e.g. ?id=12345)
        # can require that pattern. Path-only patterns still work.
        parsed = urlparse(h["url"])
        full_url_for_match = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
        if not item_pattern.search(full_url_for_match):
            portal_dropped += 1
            continue
        ann = AuthorityAnnouncement(
            url=h["url"],
            title=h["title"],
            snippet=h["snippet"],
            date_iso=extract_date(h["snippet"]),
        )
        ann.tokens = tokenize(f"{ann.title} {ann.snippet}")
        index.append(ann)

    if verbose:
        print(f"[index] built {len(index)} unique {cfg.authority_short} items "
              f"from {len(all_hits)} raw hits "
              f"(dropped {portal_dropped} portal pages)", file=sys.stderr)
    return index


# ─────────────────────────────────────────────────────────────────────────────
# DATE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

# Greek months
GREEK_MONTHS = {
    "ιανουαριου": 1, "φεβρουαριου": 2, "μαρτιου": 3, "απριλιου": 4,
    "μαιου": 5, "ιουνιου": 6, "ιουλιου": 7, "αυγουστου": 8,
    "σεπτεμβριου": 9, "οκτωβριου": 10, "νοεμβριου": 11, "δεκεμβριου": 12,
    "ιανουαριος": 1, "φεβρουαριος": 2, "μαρτιος": 3, "απριλιος": 4,
    "μαιος": 5, "ιουνιος": 6, "ιουλιος": 7, "αυγουστος": 8,
    "σεπτεμβριος": 9, "οκτωβριος": 10, "νοεμβριος": 11, "δεκεμβριος": 12,
}

# Italian months
ITALIAN_MONTHS = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}

ALL_MONTHS = {**GREEK_MONTHS, **ITALIAN_MONTHS}


def extract_date(text: str) -> str:
    if not text:
        return ""
    n = normalize_text(text)
    # ISO YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", n)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # DD/MM/YYYY or DD-MM-YYYY or DD.MM.YYYY
    m = re.search(r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})", n)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # "23 Οκτωβρίου 2026" / "23 ottobre 2026"
    m = re.search(r"(\d{1,2})\s+([a-zα-ω]+)\s+(\d{4})", n)
    if m:
        d, month_name, y = int(m.group(1)), m.group(2), int(m.group(3))
        if month_name in ALL_MONTHS:
            try:
                return datetime(y, ALL_MONTHS[month_name], d).strftime("%Y-%m-%d")
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
    index: list[AuthorityAnnouncement],
) -> tuple[Optional[AuthorityAnnouncement], float]:
    news_tokens = tokenize(news_title)
    if not news_tokens or not index:
        return None, 0.0

    best: Optional[AuthorityAnnouncement] = None
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

def fetch_index(cfg: CountryConfig, verbose: bool = False) -> list[AuthorityAnnouncement]:
    """Public entry point — builds the authority's recall index via DDG."""
    return build_index(cfg, verbose=verbose)


# Backwards-compat shim — old code calls `fetch_efet_index`
def fetch_efet_index(pages: int = 0, verbose: bool = False):
    """DEPRECATED — use fetch_index(cfg, verbose) instead."""
    if verbose:
        print("  [shim] fetch_efet_index called without country; "
              "returning empty list. Update caller to use fetch_index(cfg).",
              file=sys.stderr)
    return []


def match_candidate(
    news_title: str,
    news_published: str,
    index: list[AuthorityAnnouncement],
):
    return match_in_index(news_title, news_published, index)


def fetch_announcement_body(url: str, verbose: bool = False) -> str:
    """No-op shim — body already in indexed snippet."""
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def run_dry_test(cfg: CountryConfig) -> int:
    """Offline test with synthetic index — language-aware."""
    print("=" * 78)
    print(f"AFTS Search Verifier — Dry-Run Test ({cfg.code} / {cfg.name_en})")
    print("=" * 78)
    if cfg.code == "gr":
        fixtures = [
            ("https://www.efet.gr/anakleiseis-cat/item/15001-strudel",
             "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση Στρουντελάκια",
             "Αθήνα, 14 Μαΐου 2026. Strudito strudel — κουμαρίνη.", "2026-05-14"),
            ("https://www.efet.gr/anakleiseis-cat/item/14990-feta",
             "Ανάκληση φέτας ΒΥΤΙΝΑΣ — Listeria",
             "9 Απριλίου 2026. Listeria monocytogenes.", "2026-04-09"),
        ]
        news = [
            ("ΕΦΕΤ: Ανάκληση Strudito strudel κουμαρίνης", "15001-strudel"),
            ("Ανάκληση φέτας ΒΥΤΙΝΑΣ Listeria", "14990-feta"),
            ("Νέα γέφυρα στη Λάρισα", None),
        ]
    else:  # it (and any future country with Italian-like terms)
        fixtures = [
            ("https://www.salute.gov.it/portale/avvisiSicurezza/item/12001-listeria",
             "Richiamo formaggio per Listeria monocytogenes",
             "14 maggio 2026. Listeria nel formaggio del produttore X.", "2026-05-14"),
            ("https://www.salute.gov.it/portale/avvisiSicurezza/item/12002-aflatossine",
             "Richiamo pistacchi per aflatossine oltre limite",
             "10 maggio 2026. Aflatossine in lotto di pistacchi.", "2026-05-10"),
        ]
        news = [
            ("Richiamo formaggio Listeria Ministero Salute", "12001-listeria"),
            ("Pistacchi richiamati per aflatossine", "12002-aflatossine"),
            ("Politica italiana", None),
        ]

    index: list[AuthorityAnnouncement] = []
    for url, title, snippet, date in fixtures:
        ann = AuthorityAnnouncement(url=url, title=title, snippet=snippet, date_iso=date)
        ann.tokens = tokenize(f"{title} {snippet}")
        index.append(ann)

    passed = failed = 0
    for news_title, expected in news:
        top, score = match_in_index(news_title, "", index)
        got = top.url if top else None
        ok = (expected is None and got is None) or (
            expected is not None and got is not None and expected in got
        )
        status = "✓ PASS" if ok else "✗ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"\n{status}  {news_title[:65]}")
        if top:
            print(f"        score={score:.3f}  url={top.url}")
        else:
            print(f"        no match (best={score:.3f})")
        print(f"        expected: {expected}")

    print("\n" + "=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 78)
    return 0 if failed == 0 else 1


def run_probe(cfg: CountryConfig, query_text: str) -> int:
    print(f"=== PROBE ({cfg.code}) — building index... ===")
    index = build_index(cfg, verbose=True)
    print(f"\nIndex: {len(index)} entries\n")
    top, score = match_in_index(query_text, "", index)
    if top:
        print(f"BEST: score={score:.3f}")
        print(f"  url:     {top.url}")
        print(f"  title:   {top.title}")
        print(f"  snippet: {top.snippet[:200]}")
    else:
        print(f"NO MATCH (best sub-threshold score={score:.3f})")
    return 0


def run_show_index(cfg: CountryConfig) -> int:
    index = build_index(cfg, verbose=True)
    print(f"\n{len(index)} entries:\n")
    for i, ann in enumerate(index, 1):
        print(f"#{i:3d} {ann.url}")
        print(f"     date:    {ann.date_iso!r}")
        print(f"     title:   {ann.title[:100]}")
        print(f"     snippet: {ann.snippet[:150]}")
        print()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Gap Finder — Search Verifier (parametric DDG bulk index)"
    )
    parser.add_argument("--country", required=True,
                        help="ISO2 country code: gr, it, ...")
    parser.add_argument("--candidates", default=None)
    parser.add_argument("--verified", default=None)
    parser.add_argument("--unmatched", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe", metavar="TITLE")
    parser.add_argument("--show-index", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    cfg = get_country(args.country)

    if args.dry_run:
        return run_dry_test(cfg)
    if args.probe:
        return run_probe(cfg, args.probe)
    if args.show_index:
        return run_show_index(cfg)

    candidates_path = args.candidates or cfg.candidates_path
    verified_path = args.verified or cfg.verified_path
    unmatched_path = args.unmatched or cfg.unmatched_path

    candidates = read_candidates(candidates_path)
    print(f"Loaded {len(candidates)} candidates from {candidates_path}",
          file=sys.stderr)

    print(f"Building {cfg.authority_short} index via bulk DDG queries...",
          file=sys.stderr)
    index = build_index(cfg, verbose=args.verbose)
    print(f"{cfg.authority_short} index: {len(index)} entries", file=sys.stderr)

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

    write_jsonl(verified, verified_path)
    write_jsonl(unmatched, unmatched_path)
    print(f"\nResults: {len(verified)} verified, {len(unmatched)} unmatched",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
