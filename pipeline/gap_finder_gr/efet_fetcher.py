"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 3: EFET Fetcher

Scrapes EFET's canonical recall announcement list (the authoritative Greek
food-safety source) and matches news_scraper.py candidates against it.

Source of truth:
    https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat

Flow:
    1. Fetch EFET recall list (1–3 pages of recent announcements)
    2. Parse announcement titles + URLs + dates
    3. For each news candidate from candidates.jsonl:
         - Tokenize news title (Greek stopword-aware, accent-stripped)
         - Score against every EFET entry using Jaccard token overlap
         - Best match if score ≥ MATCH_THRESHOLD
    4. For matched candidates, fetch EFET page → extract body text
    5. Output verified.jsonl (matched) + unmatched.jsonl (no EFET match found)

Pure Python — no LLM, no Gemini, no Claude, no paid APIs.
Dependencies: requests, beautifulsoup4, lxml

CLI:
    python -m pipeline.gap_finder_gr.efet_fetcher
    python -m pipeline.gap_finder_gr.efet_fetcher --candidates candidates.jsonl
    python -m pipeline.gap_finder_gr.efet_fetcher --dry-run
    python -m pipeline.gap_finder_gr.efet_fetcher --debug-html page.html
    python -m pipeline.gap_finder_gr.efet_fetcher --verbose
"""

from __future__ import annotations
import argparse
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

EFET_BASE = "https://www.efet.gr"
EFET_LIST_PATH = "/index.php/el/enimerosi/deltia-typou/anakleiseis-cat"
EFET_LIST_URL = EFET_BASE + EFET_LIST_PATH

# How many pages of the EFET list to fetch (Joomla paginates via ?start=N).
# Page 1 = newest 20 entries. Page 2 = next 20. 3 pages = ~60 most recent.
EFET_PAGES_TO_FETCH = 3
EFET_PAGE_STEP = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1.0  # seconds between EFET requests — be polite

# Matching threshold — Jaccard overlap of meaningful tokens.
# 0.15 = at least 15% of unique tokens overlap. Tunable based on real data.
MATCH_THRESHOLD = 0.15

# Greek stopwords + recall-formula words to exclude from match tokens
# ("ΔΕΛΤΙΟ ΤΥΠΟΥ", "Ανάκληση", etc. appear in every EFET title and would
# inflate every match score uselessly).
STOPWORDS = {
    # Articles, prepositions, common verbs
    "ο", "η", "το", "οι", "τα", "τον", "την", "του", "της", "των",
    "ένα", "μία", "μια", "και", "σε", "από", "για", "με", "προς",
    "που", "πως", "ως", "στο", "στη", "στην", "στα", "στους", "στις",
    "είναι", "ήταν", "έχει", "είχε", "θα", "να", "δεν", "μη", "μην",
    # Recall-formula words (appear in every announcement)
    "δελτιο", "τυπου", "δελτίο", "τύπου",
    "αναφορα", "ανακληση", "ανάκληση", "ανακαλει", "ανακαλείται",
    "προϊον", "προϊόν", "προϊοντος", "προϊόντος",
    "ασφαλους", "ασφαλούς", "μη",
    "λογω", "λόγω", "παρουσιας", "παρουσίας",
    "εφετ",
    # English equivalents (for English-language news)
    "the", "of", "to", "in", "for", "and", "a", "an", "on", "with",
    "by", "is", "was", "be", "been",
    "recall", "recalls", "recalled", "recalling",
    "withdrawal", "withdraw", "withdrawn",
    "product", "products", "presence", "due",
    "press", "release",
}

# Date filter window: only match candidates against EFET entries within
# ± this many days of the news article's publish date.
DATE_WINDOW_DAYS = 7

DEFAULT_CANDIDATES = "docs/data/gap_finder_gr/candidates.jsonl"
DEFAULT_VERIFIED = "docs/data/gap_finder_gr/verified.jsonl"
DEFAULT_UNMATCHED = "docs/data/gap_finder_gr/unmatched.jsonl"


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

# Token pattern: 3+ chars, Greek or Latin letters and digits
_TOKEN_RE = re.compile(r"[a-zα-ω0-9]{3,}", re.UNICODE)


def tokenize(text: str) -> set[str]:
    """Greek+English tokens, normalized, 3+ chars, stopwords removed."""
    if not text:
        return set()
    n = normalize_text(text)
    tokens = _TOKEN_RE.findall(n)
    return {t for t in tokens if t not in _STOPWORDS_N}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ─────────────────────────────────────────────────────────────────────────────
# DATA RECORDS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EfetAnnouncement:
    url: str
    title: str
    date_iso: str           # ISO-8601 if parseable; "" otherwise
    summary: str            # snippet from list page
    body: str = ""          # full body text (filled after fetch_announcement)
    tokens: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["tokens"] = sorted(self.tokens)
        return d


@dataclass
class VerifiedRecord:
    """A news candidate successfully matched to an EFET announcement."""
    news_url: str
    news_title: str
    news_published: str
    news_source_domain: str
    efet_url: str
    efet_title: str
    efet_date_iso: str
    efet_body: str
    match_score: float
    matched_at: str          # ISO-8601 UTC

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# DATE PARSING (Greek month names)
# ─────────────────────────────────────────────────────────────────────────────

GREEK_MONTHS = {
    "ιανουαριου": 1, "φεβρουαριου": 2, "μαρτιου": 3, "απριλιου": 4,
    "μαιου": 5, "ιουνιου": 6, "ιουλιου": 7, "αυγουστου": 8,
    "σεπτεμβριου": 9, "οκτωβριου": 10, "νοεμβριου": 11, "δεκεμβριου": 12,
    "ιανουαριος": 1, "φεβρουαριος": 2, "μαρτιος": 3, "απριλιος": 4,
    "μαιος": 5, "ιουνιος": 6, "ιουλιος": 7, "αυγουστος": 8,
    "σεπτεμβριος": 9, "οκτωβριος": 10, "νοεμβριος": 11, "δεκεμβριος": 12,
}


def parse_greek_date(text: str) -> str:
    """Parse a Greek date string → ISO-8601 (date-only) or '' on failure."""
    if not text:
        return ""
    n = normalize_text(text)

    # Try ISO-like patterns first (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY)
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", n)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", n)
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Greek-named-month pattern: "14 Μαΐου 2026" / "14 Μαιου, 2026"
    m = re.search(r"(\d{1,2})\s+([α-ω]+)\s+(\d{4})", n)
    if m:
        d = int(m.group(1))
        month_name = m.group(2)
        y = int(m.group(3))
        if month_name in GREEK_MONTHS:
            try:
                return datetime(y, GREEK_MONTHS[month_name], d).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return ""


def days_between(iso_a: str, iso_b: str) -> Optional[int]:
    if not (iso_a and iso_b):
        return None
    try:
        a = datetime.fromisoformat(iso_a[:10])
        b = datetime.fromisoformat(iso_b[:10])
        return abs((a - b).days)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_html(url: str, verbose: bool = False) -> str:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "el,en;q=0.8"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        if verbose:
            print(f"  [WARN] fetch failed: {url} — {e}", file=sys.stderr)
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# EFET LIST PARSING (Joomla category page)
# ─────────────────────────────────────────────────────────────────────────────

def parse_efet_list_page(html: str) -> list[EfetAnnouncement]:
    """
    Parse an EFET 'Ανακλήσεις' category page. Joomla typically wraps each
    article in <div class="item"> or similar with an <h2><a> title link.
    We try multiple selectors to be robust to template tweaks.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    announcements: list[EfetAnnouncement] = []
    seen_urls: set[str] = set()

    # Strategy: find every <a> that links to a recall article on EFET.
    # EFET (Joomla) article URLs follow the pattern:
    #   /index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/<NUM>-<slug>
    # Examples confirmed live:
    #   .../anakleiseis-cat/item/4991-anaklisi-mi-asfaloys-proiontos
    #   .../anakleiseis-cat/item/5213-deltio-typou-anaklisi-proionton
    article_links = soup.find_all("a", href=re.compile(r"anakleiseis-cat/item/\d+"))

    for a in article_links:
        href = a.get("href", "").strip()
        if not href:
            continue
        url = urljoin(EFET_BASE, href)
        if url in seen_urls:
            continue

        title = a.get_text(strip=True)
        if not title or len(title) < 10:
            # Sometimes the <a> wraps a thumbnail with no text; look for a
            # sibling header with the real title.
            parent = a.find_parent(["div", "article", "li"])
            if parent:
                header = parent.find(["h2", "h3", "h4"])
                if header:
                    title = header.get_text(strip=True)
        if not title:
            continue

        # Find a nearby date (Joomla's published meta is typically in a
        # <dd class="published"> or <time> tag within the article container)
        date_iso = ""
        parent = a.find_parent(["div", "article", "li"])
        if parent:
            # Try <time>
            t = parent.find("time")
            if t:
                dt = t.get("datetime") or t.get_text(strip=True)
                date_iso = parse_greek_date(dt) or _try_iso_attr(dt)
            # Try .published / .createdate / dd.published
            if not date_iso:
                pub = parent.find(class_=re.compile(r"publish|created|date"))
                if pub:
                    date_iso = parse_greek_date(pub.get_text(strip=True))

        # Try to grab a short summary from sibling intro paragraph
        summary = ""
        if parent:
            intro = parent.find(class_=re.compile(r"intro|preview|summary"))
            if intro:
                summary = intro.get_text(" ", strip=True)[:300]
            elif parent.find("p"):
                summary = parent.find("p").get_text(" ", strip=True)[:300]

        seen_urls.add(url)
        ann = EfetAnnouncement(
            url=url, title=title, date_iso=date_iso, summary=summary
        )
        ann.tokens = tokenize(f"{title} {summary}")
        announcements.append(ann)

    return announcements


def _try_iso_attr(text: str) -> str:
    """Some <time datetime=...> values are already ISO."""
    if not text:
        return ""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    return m.group(0) if m else ""


def fetch_efet_index(
    pages: int = EFET_PAGES_TO_FETCH,
    verbose: bool = False,
) -> list[EfetAnnouncement]:
    """Fetch N pages of the EFET recall list, merge, dedupe by URL."""
    all_anns: list[EfetAnnouncement] = []
    seen: set[str] = set()

    for i in range(pages):
        start = i * EFET_PAGE_STEP
        url = EFET_LIST_URL if start == 0 else f"{EFET_LIST_URL}?start={start}"
        if verbose:
            print(f"[EFET] fetching list page {i + 1}: {url}", file=sys.stderr)
        html = fetch_html(url, verbose=verbose)
        page_anns = parse_efet_list_page(html)
        if verbose:
            print(f"  → parsed {len(page_anns)} announcements", file=sys.stderr)

        for ann in page_anns:
            if ann.url in seen:
                continue
            seen.add(ann.url)
            all_anns.append(ann)

        if not page_anns:
            # No more entries → stop pagination
            break
        if i + 1 < pages:
            time.sleep(REQUEST_DELAY)

    return all_anns


# ─────────────────────────────────────────────────────────────────────────────
# EFET ANNOUNCEMENT BODY EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def fetch_announcement_body(url: str, verbose: bool = False) -> str:
    """Fetch a single EFET announcement page → return clean body text."""
    html = fetch_html(url, verbose=verbose)
    if not html:
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Joomla article body is typically <div itemprop="articleBody"> or
    # <div class="item-page"> or <div class="article-content">
    candidates = [
        soup.find("div", itemprop="articleBody"),
        soup.find("div", class_=re.compile(r"item-page")),
        soup.find("div", class_=re.compile(r"article-content")),
        soup.find("div", class_=re.compile(r"article-body")),
        soup.find("article"),
    ]
    body = next((c for c in candidates if c), None)
    if not body:
        # Fallback: main content area
        body = soup.find("main") or soup.find("body")
    if not body:
        return ""

    # Strip script/style/nav
    for tag in body.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = body.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# MATCHING
# ─────────────────────────────────────────────────────────────────────────────

def match_candidate_to_efet(
    news_title: str,
    news_published_iso: str,
    efet_index: list[EfetAnnouncement],
) -> tuple[Optional[EfetAnnouncement], float]:
    """
    Score the news candidate against every EFET announcement; return
    (best_match, score) if score ≥ MATCH_THRESHOLD and (if dates available)
    within DATE_WINDOW_DAYS, otherwise (None, best_score).
    """
    news_tokens = tokenize(news_title)
    if not news_tokens:
        return None, 0.0

    best: Optional[EfetAnnouncement] = None
    best_score = 0.0

    for ann in efet_index:
        score = jaccard(news_tokens, ann.tokens)
        if score <= best_score:
            continue

        # Date gate (if both sides have a date)
        if news_published_iso and ann.date_iso:
            delta = days_between(news_published_iso, ann.date_iso)
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


def write_jsonl(records: list[dict], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# DRY-RUN (offline test with synthetic fixtures)
# ─────────────────────────────────────────────────────────────────────────────

def run_dry_test() -> int:
    print("=" * 78)
    print("AFTS Greek Gap Finder — EFET Fetcher Dry-Run Test")
    print("=" * 78)

    # Synthetic EFET index — mirrors what we'd actually scrape from the live
    # recall category page (titles taken from today's real EFET listings).
    efet_index = [
        EfetAnnouncement(
            url="https://www.efet.gr/anakleiseis-cat/15001-strudel",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (Στρουντελάκια μήλο-κανέλα)",
            date_iso="2026-05-14",
            summary="Ανάκληση Strudito strudel μήλο/κανέλα λόγω παρουσίας κουμαρίνης πάνω από τα όρια.",
        ),
        EfetAnnouncement(
            url="https://www.efet.gr/anakleiseis-cat/15000-paximadia",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (παξιμαδιών)",
            date_iso="2026-05-14",
            summary="Παξιμάδια κανέλας ΚΑΡΑΓΙΑΝΝΑΚΗΣ — μη δηλωμένο αλλεργιογόνο (γλουτένη).",
        ),
        EfetAnnouncement(
            url="https://www.efet.gr/anakleiseis-cat/14990-feta",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση τυριού φέτας λόγω Listeria monocytogenes",
            date_iso="2026-04-09",
            summary="Φέτα ΒΥΤΙΝΑΣ ΠΟΠ — ανίχνευση Listeria monocytogenes.",
        ),
        EfetAnnouncement(
            url="https://www.efet.gr/anakleiseis-cat/14980-gummies",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση τροφίμων τύπου ζελεδών (gummies) με μουσκιμόλη",
            date_iso="2026-04-01",
            summary="Psillys mushroom gummies — παρουσία μουσκιμόλης.",
        ),
        EfetAnnouncement(
            url="https://www.efet.gr/anakleiseis-cat/14970-koulouria",
            title="ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση προϊόντος (κουλουριών) λόγω παρουσίας αλλεργιογόνων",
            date_iso="2026-05-10",
            summary="Κουλούρια — μη δηλωμένα αλλεργιογόνα.",
        ),
    ]
    for ann in efet_index:
        ann.tokens = tokenize(f"{ann.title} {ann.summary}")

    # Test candidates — synthetic news articles
    test_news = [
        {
            "news_title": "ΕΦΕΤ: Ανάκληση Strudito strudel μήλο κανέλα λόγω κουμαρίνης",
            "news_published": "2026-05-14T20:15:00+03:00",
            "expected_match_url": "https://www.efet.gr/anakleiseis-cat/15001-strudel",
        },
        {
            "news_title": "Ανάκληση παξιμαδιών κανέλας Καραγιαννάκης λόγω γλουτένης",
            "news_published": "2026-05-14T20:30:00+03:00",
            "expected_match_url": "https://www.efet.gr/anakleiseis-cat/15000-paximadia",
        },
        {
            "news_title": "Ανάκληση φέτας ΒΥΤΙΝΑΣ — Listeria monocytogenes",
            "news_published": "2026-04-09T18:00:00+03:00",
            "expected_match_url": "https://www.efet.gr/anakleiseis-cat/14990-feta",
        },
        {
            "news_title": "Psillys gummies μουσκιμόλη ανάκληση",
            "news_published": "2026-04-01T19:00:00+03:00",
            "expected_match_url": "https://www.efet.gr/anakleiseis-cat/14980-gummies",
        },
        # Should NOT match anything — completely unrelated recall in news
        {
            "news_title": "Νέα γέφυρα στη Λάρισα — ανάκληση παλιάς απόφασης",
            "news_published": "2026-05-14T12:00:00+03:00",
            "expected_match_url": None,
        },
    ]

    passed = failed = 0
    for tc in test_news:
        match, score = match_candidate_to_efet(
            tc["news_title"], tc["news_published"], efet_index
        )
        got_url = match.url if match else None
        ok = got_url == tc["expected_match_url"]
        status = "✓ PASS" if ok else "✗ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"\n{status}  {tc['news_title'][:70]}")
        print(f"        score:    {score:.3f}")
        print(f"        matched:  {got_url}")
        print(f"        expected: {tc['expected_match_url']}")

    # Test tokenize stopword filtering
    print("\n" + "-" * 78)
    print("Tokenize test — recall-formula stopwords must be excluded:")
    t = tokenize("ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (Στρουντελάκια μήλο-κανέλα)")
    print(f"  tokens: {sorted(t)}")
    important_tokens_present = "στρουντελακια" in t and "μηλο" in t and "κανελα" in t
    stopwords_excluded = "ανακληση" not in t and "δελτιο" not in t and "προϊοντος" not in t
    ok = important_tokens_present and stopwords_excluded
    print(f"  important tokens present: {important_tokens_present}")
    print(f"  stopwords excluded:       {stopwords_excluded}")
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
# DEBUG MODE (parse a saved EFET HTML file — useful when George tests live)
# ─────────────────────────────────────────────────────────────────────────────

def run_debug_html(path: str) -> int:
    p = Path(path)
    if not p.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 1
    html = p.read_text(encoding="utf-8")
    anns = parse_efet_list_page(html)
    print(f"Parsed {len(anns)} announcements from {path}")
    for i, a in enumerate(anns[:20], 1):
        print(f"\n#{i}")
        print(f"  url:     {a.url}")
        print(f"  title:   {a.title}")
        print(f"  date:    {a.date_iso!r}")
        print(f"  summary: {a.summary[:120]}")
        print(f"  tokens:  {sorted(a.tokens)[:15]}")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Greek Gap Finder — EFET Fetcher"
    )
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES,
                        help=f"Input JSONL (default: {DEFAULT_CANDIDATES})")
    parser.add_argument("--verified", default=DEFAULT_VERIFIED,
                        help=f"Verified output (default: {DEFAULT_VERIFIED})")
    parser.add_argument("--unmatched", default=DEFAULT_UNMATCHED,
                        help=f"Unmatched output (default: {DEFAULT_UNMATCHED})")
    parser.add_argument("--pages", type=int, default=EFET_PAGES_TO_FETCH,
                        help=f"EFET list pages to fetch (default: {EFET_PAGES_TO_FETCH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Offline test against synthetic fixtures")
    parser.add_argument("--debug-html", metavar="FILE",
                        help="Parse a saved EFET list HTML file (debugging)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose progress on stderr")
    args = parser.parse_args()

    if args.dry_run:
        return run_dry_test()

    if args.debug_html:
        return run_debug_html(args.debug_html)

    candidates = read_candidates(args.candidates)
    print(f"Loaded {len(candidates)} candidates from {args.candidates}",
          file=sys.stderr)

    print("Fetching EFET recall index...", file=sys.stderr)
    efet_index = fetch_efet_index(pages=args.pages, verbose=args.verbose)
    print(f"EFET index: {len(efet_index)} announcements", file=sys.stderr)

    verified: list[dict] = []
    unmatched: list[dict] = []
    now = _now_utc()

    for cand in candidates:
        news_title = cand.get("title", "")
        news_pub = cand.get("published", "")
        match, score = match_candidate_to_efet(news_title, news_pub, efet_index)

        if not match:
            unmatched.append({
                **cand,
                "best_score": score,
                "checked_at": now,
            })
            if args.verbose:
                print(f"[unmatched] score={score:.3f} — {news_title[:70]}",
                      file=sys.stderr)
            continue

        # Fetch full body for the matched announcement
        if not match.body:
            time.sleep(REQUEST_DELAY)
            match.body = fetch_announcement_body(match.url, verbose=args.verbose)

        record = VerifiedRecord(
            news_url=cand.get("url", ""),
            news_title=news_title,
            news_published=news_pub,
            news_source_domain=cand.get("source_domain", ""),
            efet_url=match.url,
            efet_title=match.title,
            efet_date_iso=match.date_iso,
            efet_body=match.body,
            match_score=round(score, 4),
            matched_at=now,
        )
        verified.append(record.to_dict())
        if args.verbose:
            print(f"[matched]   score={score:.3f} — {news_title[:50]} → "
                  f"{match.title[:50]}", file=sys.stderr)

    write_jsonl(verified, args.verified)
    write_jsonl(unmatched, args.unmatched)
    print(f"\nResults: {len(verified)} verified, {len(unmatched)} unmatched",
          file=sys.stderr)
    print(f"Wrote: {args.verified}", file=sys.stderr)
    print(f"Wrote: {args.unmatched}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
