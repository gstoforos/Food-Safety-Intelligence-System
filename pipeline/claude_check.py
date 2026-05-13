"""
pipeline/claude_check.py — final Claude-based verification stage.

Runs AFTER url_gate_gemini.py and BEFORE Pending → Recalls promotion.

For each row currently in Pending:
  1. Fetch the regulator URL (text-extracted, ~25K chars, 25s timeout)
  2. Ask Claude Haiku 4.5 to verify the row's Date / Company / Product /
     Pathogen against the actual page content
  3. If Claude finds the row's Date is wrong → correct it with audit trail
     in Notes: "[claude-check 2026-04-27: corrected Date 2026-04-25 →
     2026-03-15 (page header)]"
  4. If Claude finds the row otherwise consistent → mark for promotion
  5. If Claude finds the page does NOT match the claimed recall (wrong
     hazard, different company, page is a clarification of a different
     recall, page is a listing/index, etc.) → keep in Pending with
     REJECTED status

Catches the failure mode that bit us on the SFA Nature One Dairy row:
the SFA page header reads "Recall of two additional formula milk products"
with publication date 15 Mar 2026, but a "Clarification" added later in
April caused the gap-finder to stamp the row 2026-04-25. Claude reading
the page text catches the discrepancy and corrects the date back to
2026-03-15.

This stage complements the Gemini URL gate (url_gate_gemini.py):
  - Gemini gate: verifies the URL is on the right domain and points to
    a specific recall page (uses Google Search grounding for cross-check)
  - Claude check: reads the page itself and verifies the Date / Company /
    Product / Pathogen on the row actually match what the page says

Cost: Claude Haiku 4.5 (claude-haiku-4-5-20251001). With pages truncated
to ~25K chars and a few-K-token output, expect <$0.05 per run for typical
Pending sizes (5–30 rows). Cheap enough to run after every URL-gate run.

Schedule (Athens time):
  07:30 → url_gate_gemini.py            (URL verification + fixes)
  07:45 → claude_check.py               (page-content verification)
  08:00 → weekly report (Friday only)   (consumes Recalls sheet)
"""
from __future__ import annotations
import os
import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

import requests as _requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    promote_approved, sort_rows,
    save_xlsx_with_pending, mirror_json_from_xlsx,
    rebuild_daily_briefs_for_promoted,
    STATUS_REJECTED, STATUS_PENDING,
    STATUS_PENDING_GAP, STATUS_PENDING_GAP_V1, STATUS_PENDING_GAP_V2,
    STATUS_PENDING_ENRICHMENT,
    mark_rejected_with_counter,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.claude_client import (  # noqa: E402
    _call_claude, _strip_fences, ENABLED as CLAUDE_ENABLED,
)
from pipeline._url_year import is_year_mismatch  # noqa: E402
from pipeline._pathogen_scope import is_in_scope as is_tier1_pathogen  # noqa: E402
from pipeline._news_mirror_blocklist import is_news_mirror  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("claude-check")

# Audit-stamp tag prefix used in Notes. Default for the morning Claude
# pass; openrouter_check overrides this at import time so the evening
# dissent reviewer's verdicts surface as [openrouter-check ...] in the
# Thursday review email and in the Notes field. Keep this in sync with
# the regex below — both must match the same prefix string.
CHECKER_TAG = "claude-check"

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

# Page-fetch + Claude limits
# Audit 2026-05-12: bumped from 4000 → 8000. CFIA pages have the
# "Original published date" + "Date modified" in a metadata footer
# AFTER the article body. At 4000 chars that footer was being
# truncated away, leaving Haiku only the page header (which says
# "Last updated YYYY-MM-DD" — the SAME date for current recalls but
# could differ when an old recall is updated). Today's 04:15 run
# FIX'd two stale 2025 CFIA pumpkin-seeds/Camembert recalls with
# corrections instead of catching the pre-2026 dates because the
# 4000-char cap stripped the authoritative dcterms metadata block.
#
# At 8000 chars: ~2K tokens input vs. ~800 at 4K. Still under
# Haiku's 200K context; cost delta is +$0.001 per call ($0.10/M
# cached input). Set CLAUDE_CHECK_PAGE_CHARS env var to override.
HTML_TRUNCATE_CHARS = int(os.getenv("CLAUDE_CHECK_PAGE_CHARS", "8000"))
FETCH_TIMEOUT_S = 25
SLEEP_BETWEEN_ROWS_S = 0.5    # gentle on the regulator + the API
# UA + headers updated 2026-05-07 — the previous bot-style UA
# ("AFTS-FSIS-claude-check/1.0; +https://advfood.tech/fsis-home") was
# being 403-blocked by USDA FSIS (3 of 11 rows in today's claude-check).
# Same fix as the 2026-04-29 _base.py audit: use a real Chrome UA so
# regulator bot-blockers don't drop us. We're not impersonating — we
# just stop announcing ourselves as a bot.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# Full browser-fingerprint header set, mirrors scrapers/_base.py DEFAULT_HEADERS.
# USDA FSIS specifically inspects Accept + Accept-Encoding + Upgrade-Insecure-
# Requests on top of UA — minimal headers alone still get 403'd.
_FETCH_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,de;q=0.7,es;q=0.6,it;q=0.5,el;q=0.4",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Rows older than this many days are not re-verified (we trust the prior
# Claude check). 0 means verify every row every run.
SKIP_IF_VERIFIED_DAYS = int(os.getenv("CLAUDE_CHECK_SKIP_DAYS", "0"))


# ---------------------------------------------------------------------------
# HTML → text extraction (no bs4 dependency required)
# ---------------------------------------------------------------------------

_RE_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_RE_TAG = re.compile(r"<[^>]+>")
_RE_WS = re.compile(r"[ \t]+")
_RE_NL = re.compile(r"\n{3,}")


def _html_to_text(html: str) -> str:
    """
    Cheap HTML→text. Strips <script>/<style>, removes tags, collapses
    whitespace. Good enough for Claude to read regulator pages that are
    mostly prose + tables.
    """
    if not html:
        return ""
    txt = _RE_SCRIPT.sub(" ", html)
    txt = _RE_TAG.sub(" ", txt)
    # decode the most common HTML entities
    txt = (txt.replace("&nbsp;", " ")
              .replace("&amp;", "&")
              .replace("&lt;", "<")
              .replace("&gt;", ">")
              .replace("&quot;", '"')
              .replace("&#39;", "'")
              .replace("&euro;", "€"))
    # collapse whitespace
    txt = _RE_WS.sub(" ", txt)
    txt = _RE_NL.sub("\n\n", txt)
    return txt.strip()


# ── Deterministic publication-date extraction ───────────────────────
# Patterns by regulator. Audit 2026-05-12: today's 04:15 claude-check
# missed the 2025-05-10 (Hope Eco-Farm pumpkin seeds) and 2025-08-13
# (Mon Père Camembert) dates that the CFIA pages had IN THE METADATA
# in two different places (Dublin Core <meta> tags AND the body
# "Original published date:" line). Today's Haiku FIX'd Company/Brand/
# Product/Pathogen but completely missed the date → both rows advanced
# pending_gap → pending_gap_v1 (one step closer to promoting two
# stale 2025 recalls). Yesterday's run on the same URLs correctly
# returned date_pre_2026. The fix: extract these dates DETERMINISTICALLY
# from page HTML using regex BEFORE handing to Claude, so that an
# eventual pre-2026 hit short-circuits to FAIL with no LLM call.
#
# Each pattern is regulator-specific and anchored to text that only
# appears on real recall pages (not landing pages / FAQ / listings).
_PUB_DATE_PATTERNS = [
    # CFIA / Canada.ca Dublin Core meta + body
    re.compile(r'meta-dcterms\.issued:\s*(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    re.compile(r'<meta[^>]+name=["\']dcterms\.issued["\'][^>]+content=["\'](\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    re.compile(r'Original\s+published\s+date\D{0,30}(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    re.compile(r'Last\s+updated\D{0,30}(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    re.compile(r'Date\s+modified\D{0,30}(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    # FDA recall pages
    re.compile(r'<time[^>]+datetime=["\'](\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    # FSAI Ireland (day-month-year text)
    re.compile(r'Alert\s+Date\D{0,30}(\d{1,2}\s+\w+\s+\d{4})', re.IGNORECASE),
    # FSIS USDA
    re.compile(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    re.compile(r'meta-article:published_time:\s*(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    # FSANZ / MPI New Zealand
    re.compile(r'Date\s+published\D{0,30}(\d{1,2}\s+\w+\s+\d{4})', re.IGNORECASE),
    # Generic "Published: YYYY-MM-DD"
    re.compile(r'Published\D{0,30}(\d{4}-\d{2}-\d{2})', re.IGNORECASE),
    # RappelConso France
    re.compile(r'Publi[ée]\s+le\D{0,30}(\d{1,2}/\d{1,2}/\d{4})', re.IGNORECASE),
    # INVIMA Colombia (PDF text after extraction)
    re.compile(r'Bogot[áa],?\s+(\d{1,2})\s+(\w+)\s+(\d{4})', re.IGNORECASE),
]

_MONTH_NAMES = {
    # English
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,
    "sep":9,"sept":9,"oct":10,"nov":11,"dec":12,
    # Spanish
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
    "noviembre":11,"diciembre":12,
    # French
    "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,
    "novembre":11,"décembre":12,"decembre":12,
}


def _normalize_extracted_date(raw: str) -> Optional[str]:
    """Convert any of the matched date shapes to ISO YYYY-MM-DD or None."""
    if not raw:
        return None
    s = raw.strip().lower()
    # Already ISO
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        y, mo, d = (int(g) for g in m.groups())
        try:
            return datetime(y, mo, d).date().isoformat()
        except ValueError:
            return None
    # DD/MM/YYYY (RappelConso) or MM/DD/YYYY (FDA — disambiguate by value)
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        a, b, y = (int(g) for g in m.groups())
        # In European format the first number is day; if a > 12 it must be day
        if a > 12:
            d, mo = a, b
        elif b > 12:
            mo, d = a, b
        else:
            # Ambiguous — default to DD/MM (RappelConso convention)
            d, mo = a, b
        try:
            return datetime(y, mo, d).date().isoformat()
        except ValueError:
            return None
    # "12 May 2026" / "12 mayo 2026" / "12 mai 2026"
    m = re.match(r"^(\d{1,2})\s+(\w+)\s+(\d{4})$", s)
    if m:
        d, mon_name, y = m.group(1), m.group(2), m.group(3)
        mo = _MONTH_NAMES.get(mon_name.lower())
        if mo:
            try:
                return datetime(int(y), mo, int(d)).date().isoformat()
            except ValueError:
                return None
    return None


def _extract_publication_date(raw_html_or_text: str) -> Optional[str]:
    """Run the regulator patterns over raw HTML or PDF-extracted text.

    Returns the first successfully-parsed ISO date, or None. Conservative
    by design — we only short-circuit Claude when we're sure we have the
    right date. The patterns are anchored to text that appears ONLY on
    real recall pages (meta tags, "Original published date:", "Alert
    Date:", "Published:") so false positives on landing/index pages are
    rare. Even if a false positive happens, the date is then passed to
    Claude as a HINT in the page text — Claude can override.
    """
    if not raw_html_or_text:
        return None
    for pat in _PUB_DATE_PATTERNS:
        m = pat.search(raw_html_or_text)
        if not m:
            continue
        groups = m.groups()
        if len(groups) == 1:
            iso = _normalize_extracted_date(groups[0])
            if iso:
                return iso
        elif len(groups) == 3:
            # INVIMA "Bogotá, 6 mayo 2026" — day, month-name, year
            d, mon_name, y = groups
            mo = _MONTH_NAMES.get(str(mon_name).lower())
            if mo:
                try:
                    return datetime(int(y), mo, int(d)).date().isoformat()
                except ValueError:
                    pass
    return None


# ── Metadata-block extractor ────────────────────────────────────────
# Before stripping HTML for Claude, harvest the bits that almost always
# contain the authoritative publication date. Prepended to the page_text
# as a "PAGE METADATA" header so it survives the 8000-char truncate.
_META_TAG_RX = re.compile(
    r'<meta[^>]+(?:name|property)=["\']'
    r'(dcterms\.(?:issued|modified)|article:published_time|article:modified_time|'
    r'last-modified|publication-date|date|datepublished|datemodified)'
    r'["\'][^>]*content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_TITLE_RX = re.compile(r'<title[^>]*>([^<]+)</title>', re.IGNORECASE | re.DOTALL)
_H1_RX = re.compile(r'<h1[^>]*>(.*?)</h1>', re.IGNORECASE | re.DOTALL)


def _extract_metadata_block(html: str) -> str:
    """Extract HTML metadata that the truncate buffer would otherwise drop.

    Returns a short text block (≤500 chars) of key→value pairs ready to
    prepend to the page_text passed to Claude. Includes:
      • Dublin Core dcterms.issued / dcterms.modified
      • OpenGraph article:published_time / article:modified_time
      • <title>
      • First <h1>
    """
    if not html:
        return ""
    parts: List[str] = []
    seen = set()
    for m in _META_TAG_RX.finditer(html):
        key = m.group(1).lower()
        val = m.group(2).strip()
        if (key, val) in seen:
            continue
        seen.add((key, val))
        parts.append(f"  meta-{key}: {val}")
    title_m = _TITLE_RX.search(html)
    if title_m:
        t = re.sub(r"\s+", " ", title_m.group(1)).strip()[:200]
        if t:
            parts.append(f"  <title>: {t}")
    h1_m = _H1_RX.search(html)
    if h1_m:
        h1 = _RE_TAG.sub(" ", h1_m.group(1))
        h1 = re.sub(r"\s+", " ", h1).strip()[:200]
        if h1:
            parts.append(f"  <h1>: {h1}")
    if not parts:
        return ""
    return "PAGE METADATA (authoritative for dates):\n" + "\n".join(parts) + "\n\n"


# ── PDF text extraction (added 2026-05-12) ──────────────────────────
# Pre-2026-05-12 _fetch_page_text returned (None, "non-text content-type:
# application/pdf") for every PDF URL → claude-check SKIPped them
# indefinitely. INVIMA (Colombia), ANMAT (Argentina), DIGEMID (Peru),
# SENASICA (Mexico), and food.gov.uk's FSA-PRIN bulletins all publish
# recalls primarily as PDFs. Without a PDF text extractor, the entire
# Latin America regulator set was unreviewable, and two INVIMA
# pending rows (Alert 127-2026 PURIFRESK water; Alert 123-2026 BICHOTA
# sildenafil-adulterated supplement) had been SKIPping for days.
#
# Strategy: try pypdf (lightest dep), then PyPDF2 (older fork), then
# pdfplumber (heaviest, best at columnar layouts). All three are pure-
# Python; if none are installed, fall back to the pre-2026-05-12 SKIP
# behavior with a clearer log message.
def _extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Best-effort PDF → text. Returns None if no extractor is installed."""
    if not pdf_bytes:
        return None

    # Strategy 1: pypdf (modern, lightweight, pure-Python)
    try:
        from pypdf import PdfReader  # type: ignore
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages[:20]:   # cap at 20 pages for safety
            try:
                pages_text.append(page.extract_text() or "")
            except Exception:
                continue
        full = "\n".join(pages_text).strip()
        if full:
            return full
    except ImportError:
        pass
    except Exception as exc:
        log.warning("pypdf failed: %s: %s", type(exc).__name__, str(exc)[:120])

    # Strategy 2: PyPDF2 (legacy fork)
    try:
        from PyPDF2 import PdfReader as PdfReader2  # type: ignore
        import io
        reader = PdfReader2(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages[:20]:
            try:
                pages_text.append(page.extract_text() or "")
            except Exception:
                continue
        full = "\n".join(pages_text).strip()
        if full:
            return full
    except ImportError:
        pass
    except Exception as exc:
        log.warning("PyPDF2 failed: %s: %s", type(exc).__name__, str(exc)[:120])

    # Strategy 3: pdfplumber (heaviest, best at tables)
    try:
        import pdfplumber  # type: ignore
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_text = []
            for page in pdf.pages[:20]:
                try:
                    pages_text.append(page.extract_text() or "")
                except Exception:
                    continue
            full = "\n".join(pages_text).strip()
            if full:
                return full
    except ImportError:
        pass
    except Exception as exc:
        log.warning("pdfplumber failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])

    return None


def _fetch_page_text(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (text, error). Text is the page content stripped of HTML and
    truncated to HTML_TRUNCATE_CHARS, with a PAGE METADATA preamble
    containing Dublin Core / OpenGraph publication-date tags so Claude
    always sees the authoritative date even when the body truncates.

    Handles HTML, XML, plain text, AND PDF (added 2026-05-12). Returns
    error string only when (a) the URL itself fails, OR (b) PDF extraction
    isn't available, OR (c) the content-type is something we don't handle
    (image, video, etc.).
    """
    if not url or not url.lower().startswith(("http://", "https://")):
        return None, "no URL"
    try:
        resp = _requests.get(
            url, timeout=FETCH_TIMEOUT_S,
            headers=_FETCH_HEADERS,
            allow_redirects=True,
        )
    except Exception as exc:
        return None, f"fetch error: {type(exc).__name__}: {str(exc)[:120]}"

    if resp.status_code >= 400:
        return None, f"HTTP {resp.status_code}"

    ctype = (resp.headers.get("Content-Type") or "").lower()

    # PDF branch — extract text via pypdf / PyPDF2 / pdfplumber (audit 2026-05-12)
    if "pdf" in ctype or url.lower().endswith(".pdf"):
        pdf_text = _extract_pdf_text(resp.content or b"")
        if pdf_text is None:
            return None, (
                "PDF text extraction unavailable — install pypdf "
                "(pip install pypdf) to enable PDF reviewing"
            )
        # PDFs have no HTML metadata block but we can still run the
        # deterministic date extractor on the extracted text.
        date_iso = _extract_publication_date(pdf_text)
        meta_block = ""
        if date_iso:
            meta_block = (
                f"PAGE METADATA (extracted deterministically from PDF text):\n"
                f"  publication_date: {date_iso}\n\n"
            )
        if len(pdf_text) > HTML_TRUNCATE_CHARS:
            pdf_text = pdf_text[:HTML_TRUNCATE_CHARS] + "\n\n[... truncated ...]"
        return meta_block + pdf_text, None

    if "html" not in ctype and "xml" not in ctype and "text" not in ctype:
        # Image, video, or other binary — Claude can't read directly
        return None, f"non-text content-type: {ctype[:60]}"

    raw_html = resp.text or ""
    # Extract metadata BEFORE stripping tags so Dublin Core dcterms.issued
    # / OpenGraph article:published_time survive the html-to-text pass and
    # the 8000-char truncate. This is what keeps the CFIA "Original
    # published date: 2025-05-10" visible to Claude even when the article
    # body is long.
    meta_block = _extract_metadata_block(raw_html)
    text = _html_to_text(raw_html)
    if len(text) > HTML_TRUNCATE_CHARS:
        text = text[:HTML_TRUNCATE_CHARS] + "\n\n[... truncated ...]"
    return meta_block + text, None


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

CLAUDE_CHECK_SYSTEM = (
    "You are a senior food-safety analyst doing the final verification "
    "pass on a recall row before it is published on a public dashboard. "
    "You read the regulator's actual page text and answer strictly in "
    "the JSON schema requested. You never invent facts — if the page "
    "doesn't say something, leave the field blank or set the verdict "
    "to 'fail'."
)

CLAUDE_CHECK_PROMPT = """A row is in our Pending sheet, claimed to describe a real food recall on the regulator's page below. Verify it.

═══ ROW (claimed) ═══
Date     : {Date}
Source   : {Source}
Country  : {Country}
Company  : {Company}
Brand    : {Brand}
Product  : {Product}
Pathogen : {Pathogen}
Class    : {Class}
Tier     : {Tier}
Outbreak : {Outbreak}
Reason   : {Reason}
URL      : {URL}

═══ PAGE TEXT (text-extracted from the URL above) ═══
{page_text}
═══ END PAGE TEXT ═══

INSTRUCTIONS:

1. Find the ORIGINAL recall publication date on the page.
   • Look for: "Published", "Publication date", "Date du rappel",
     "Datum", a calendar-icon datestamp at the top of the article,
     or the date in the page header.
   • CRITICAL: If the page shows BOTH an original publication date
     AND a later "Update", "Clarification", "Latest update", or
     "Last modified" date, ALWAYS use the ORIGINAL publication date.
     Example: SFA "Recall of two additional formula milk products"
     published 15 Mar 2026 with a Clarification added 25 Apr 2026 →
     the date is 2026-03-15, NOT 2026-04-25.

2. Verify each field against the page:
   • date_match    — does the row's Date equal the ORIGINAL publication
                     date on the page? (±1 day tolerance for tz quirks)
   • company_match — does the page mention the row's Company?
                     "Various brands", "Various producers", "Unbranded",
                     "sans marque", or "—" are legitimate values ONLY
                     when the page itself doesn't name a single company
                     (multi-producer recalls, generic raw products, bulk
                     commodity alerts). If the page DOES name a specific
                     brand or manufacturer mid-page (typical for retail
                     recalls — RappelConso, FDA, CFIA — even when the
                     row says "Unbranded"), set company_match=false so
                     the row gets re-scraped on the next pass instead of
                     being promoted with a wrong Company value.
   • product_match — does the page describe the row's Product?
   • pathogen_match — does the page identify the row's Pathogen?
                     (e.g. "Listeria", "Salmonella", "cereulide",
                     "B. cereus toxin")
   • is_recall_page — is the page actually a recall / public-warning /
                      alert page (not a listing index, search results,
                      category page, transparency page, or unrelated
                      content)?

2b. Cross-check Date and URL (locked 2026-04-30):
   • date_pre_2026 — is the row's Date (or date_corrected) before
                     2026-01-01? If yes → fail with reason "date_pre_2026".
                     FSIS scope is 2026 onward only.
   • url_year_mismatch — does the URL clearly encode a year that:
                     (a) is < 2026, OR
                     (b) differs from the row's Date year by >1 year?
                     URL year patterns: /fiche-rappel/YYYY-MM-NNNN/,
                     /YYYY/MM/DD/, F-NNNN-YYYY, /YYYY/ with non-digit
                     boundary. CRITICAL: numeric-only fiche IDs like
                     /fiche-rappel/22019/ are sequential IDs, NOT years.
                     PDF filenames with random digits do NOT encode years.
                     If yes → fail with reason "url_year_mismatch".
   • news_mirror_domain — is the URL on a news-mirror or aggregator
                     domain (sedaily.com, reuters.com, beaconbio.org,
                     ilfattoalimentare.it, foodsafetynews.com, cnn.com,
                     bbc.com, etc.)? If yes → fail with reason
                     "news_mirror_domain". These are not official sources.

2c. Cross-check Company and Brand (locked 2026-04-30):
   • For ALL non-RASFF sources: Company must be the RECALLING FIRM
     (manufacturer / packer / "Source of the record" / brand chip on
     the page), and Brand must be the product brand chip on the page.
     - Distributor/retailer names (Carrefour, Monoprix, Intermarché,
       Grand Frais, Leclerc, Auchan) in the page Notes are RESELLERS,
       NOT the recalling firm.
     - If Company looks like it was autofilled with a distributor name
       and the actual page shows a different "Source of the record",
       fail with reason "company_is_distributor".
     - If page has no extractable recalling firm at all, fail with
       reason "empty_company_or_brand". Row stays in Pending for retry.

   • For RASFF (EU) rows ONLY (Source contains "RASFF"):
     - Company should be a 3-letter origin country code (e.g. "GRC",
       "DEU"); "UNK" allowed for unknown origin.
     - Brand should be 3-letter destination country codes
       (e.g. "FRA, DEU, ITA").
     - If Company is empty or not a 3-letter code AND not "UNK" → fail
       with reason "rasff_company_format".

2d. Cross-check Pathogen (locked 2026-04-30):
   • Pathogen must be in FSIS Tier-1 scope:
     Listeria, Salmonella, STEC/E.coli, C. botulinum, B. cereus,
     mycotoxins (aflatoxin/ochratoxin/etc.), Cronobacter, Hep A,
     Norovirus, Staphylococcus enterotoxin, Campylobacter.
   • If Pathogen is empty, "—", or anything else (Histamine, Marine
     biotoxin, Foreign body, allergens, heavy metals, sulfites,
     rodenticides, PFAS, etc.) → fail with reason
     "pathogen_out_of_scope".

2e. Extraction-garbage check (locked 2026-04-30):
   • If Company == Brand AND value is a generic landing-page word
     ("Home", "Index", "Page", "Recalls", "Alerts", "Welcome",
     "Main") → fail with reason "extraction_garbage".
   • If Product matches the Source agency name (Source="NAFDAC",
     Product="NAFDAC") → fail with reason "extraction_garbage".
   • If URL ends in /home/, /index/, /main/ → fail with reason
     "extraction_garbage".

3. If date_match is false but you can identify the correct publication
   date from the page, return it as date_corrected (YYYY-MM-DD).

4. Set verdict:
   • "pass"   — all 5 match flags true (or true after applying
                date_corrected) AND no failure from 2b/2c/2d/2e.
                Apply Tier=1 to the row (it's in scope by definition).
   • "fix"    — only date_match is false but you have a confident
                correction. Apply date_corrected. Re-check date_pre_2026
                and url_year_mismatch on the corrected date.
   • "fail"   — anything else. Row stays in Pending with reason in Notes.

4b. VERIFY THE TIER (added 2026-05-04 — see docs/fsis_reviewer_prompts.md).

   FSIS uses the HYBRID framework: regulator class first, FDA framework
   fallback, outbreak bump on top.

   Step 4b-i. Try regulator class first. If the row's "Class" matches
   one of these (case-insensitive), use the mapped Tier:
     → TIER 1: "Class I", "Class 1", "Mandatory recall (prefectural order)",
       "Public Health Alert", "Mandatory recall", "Administrative action",
       "Outbreak", or any class containing "Mandatory" / "Imperative" /
       "Impératif" / "Public Health Alert"
     → TIER 2: "Class II", "Class 2", "Voluntary", "Recall", "Alert",
       "Alert notification", "Advisory", "Conseillé", "Food Alert",
       "Product Recall Information Notice"
     → TIER 3: "Class III", "Class 3", "Information", "Border rejection",
       "Notification" (without severity qualifier)

   Step 4b-ii. If no regulator class match (or Class is empty), apply
   FDA framework:
     → TIER 1 (FDA Class I — death or serious adverse health):
       • Botulinum, marine biotoxins, cereulide → ANY food
       • Listeria, STEC, Hep A, Norovirus → ANY food
       • Salmonella → READY-TO-EAT food (peanut butter, deli meat, soft
         cheese, ice cream, prepared salads, raw nuts, sprouts, frozen
         berries, dry pet food)
       • Heavy metals at toxic levels, rodenticide if consumed
       • Foreign body in baby food / vulnerable-population food
     → TIER 2 (FDA Class II — temporary or reversible):
       • Salmonella → COOKING-REQUIRED food (raw poultry, raw beef, raw
         pork, raw eggs in shell, ground meats, fresh poultry)
       • Campylobacter, Yersinia, Vibrio, Cyclospora, Cronobacter,
         generic (non-STEC) E. coli, vegetative B. cereus, Brucella,
         Shigella, Staph enterotoxin, histamine
       • Mycotoxins above action level
       • Foreign body in adult food
     → TIER 3 (FDA Class III — unlikely to cause harm):
       • Labeling violations, net-weight discrepancies, quality issues
       • Pathogen in environmental swab only
       • Unknown pathogen / empty pathogen field (default)

   Step 4b-iii. Apply Outbreak bump:
       final_tier = max(1, base_tier - 1)   if Outbreak == 1
       final_tier = base_tier               if Outbreak == 0

   HARD RULES (never override):
     H1  Cereulide is ALWAYS Tier 1
     H2  Botulinum is ALWAYS Tier 1
     H3  STEC / E. coli O157 / VTEC / EHEC is ALWAYS Tier 1
     H4  Listeria monocytogenes in ANY food is ALWAYS Tier 1
         (overrides any regulator downgrade — likely misclassification)
     H5  Marine biotoxins are ALWAYS Tier 1
     H6  Salmonella in RTE food is Tier 1 (FDA Class I)
     H7  Salmonella in cooking-required food is Tier 2 (Class II)
     H8  Generic "E. coli" without STEC indicator is Tier 3
     H9  Bacillus cereus alone is Tier 3 (cereulide is the Tier 1 path)
     H13 Foreign body in baby food / vulnerable-population food → Tier 1
     H14 Foreign body in adult food, no injuries → Tier 3

   Output:
     tier_verified = the computed final_tier (1, 2, or 3)
     tier_method   = "regulator_class" | "fda_framework" | "hybrid"
     tier_mismatch = true if row's existing Tier ≠ tier_verified

4c. VERIFY THE OUTBREAK FLAG (STRICT rule — see docs/fsis_reviewer_prompts.md §5).

   Outbreak = 1 ONLY if the page (or a linked official health page)
   states ONE OR MORE of:
     • specific number of confirmed/probable illnesses/cases
       ("166 illnesses", "two cases", "26 hospitalised")
     • the word "outbreak" / "épidémie" / "Ausbruch" / "brote" /
       "epidemia" describing THIS hazard (not generic boilerplate)
     • epidemiological investigation triggered by reported illness
     • death(s) attributed to the hazard
     • named ongoing outbreak with published case counts

   Outbreak = 0 if:
     • notice says "no reported illnesses" / "aucun cas signalé"
     • routine sampling caught it (lab test only, product not consumed)
     • criminal tampering with no consumption (HiPP rat-poison Austria
       2026-04-18 → outbreak=0)
     • "outbreak" word only in brand name or boilerplate

   Default to 0 when in doubt.

   Output:
     outbreak_verified = 0 or 1
     outbreak_evidence = short verbatim quote (max 200 chars), "" if =0
     outbreak_mismatch = true if row's existing Outbreak ≠ outbreak_verified

OUTPUT — strict JSON, no markdown, no commentary:

{{
  "date_match": true|false,
  "date_on_page": "YYYY-MM-DD or empty",
  "date_corrected": "YYYY-MM-DD or empty",
  "company_match": true|false,
  "company_corrected": "<recalling firm from page, or empty if no correction>",
  "brand_corrected":   "<brand from page, or empty if no correction>",
  "product_corrected": "<product description from page, or empty if no correction>",
  "pathogen_corrected": "<canonical pathogen from page, or empty if no correction>",
  "product_match": true|false,
  "pathogen_match": true|false,
  "is_recall_page": true|false,
  "date_pre_2026": true|false,
  "url_year_mismatch": true|false,
  "news_mirror_domain": true|false,
  "pathogen_out_of_scope": true|false,
  "company_is_distributor": true|false,
  "extraction_garbage": true|false,
  "tier1_pathogen": true|false,
  "tier_verified": 1|2|3,
  "tier_method": "regulator_class" | "fda_framework" | "hybrid",
  "tier_mismatch": true|false,
  "outbreak_verified": 0|1,
  "outbreak_evidence": "<verbatim quote, max 200 chars>",
  "outbreak_mismatch": true|false,
  "verdict": "pass" | "fix" | "fail",
  "reason": "short single-sentence rationale"
}}

CORRECTION RULES (audit 2026-05-08):
  - When the row's Company/Brand/Product/Pathogen field is empty, "—",
    "Unbranded", "sans marque", or contains extraction garbage (page-title
    text, agency name, generic words like "Food"/"Press Release") AND the
    page CLEARLY names the correct value, populate the corresponding
    *_corrected field and set verdict="fix". The pipeline will write the
    correction back to the row and promote it.
  - For "sans marque" / unbranded products where no recalling firm is
    named on the page, leave company_corrected empty and use
    brand_corrected="Unbranded".
  - For Pathogen: only fill pathogen_corrected with a canonical name
    ("Listeria monocytogenes", "Salmonella", "Escherichia coli STEC",
    "Cereulide", etc.) — not free-text. Empty if no clear pathogen.
  - All *_corrected fields must come from the page content itself, not
    from outside knowledge or speculation.
"""


def _verify_with_claude(row: Dict[str, Any],
                         page_text: str) -> Optional[Dict[str, Any]]:
    """
    Call Claude Haiku 4.5 with the row + page text. Returns the parsed
    JSON dict, or None on failure (Claude error / parse error / etc.).
    """
    if not CLAUDE_ENABLED:
        return None

    prompt = CLAUDE_CHECK_PROMPT.format(
        Date=row.get("Date", ""),
        Source=row.get("Source", ""),
        Country=row.get("Country", ""),
        Company=row.get("Company", ""),
        Brand=row.get("Brand", "—"),
        Product=row.get("Product", ""),
        Pathogen=row.get("Pathogen", ""),
        Class=row.get("Class", ""),
        Tier=row.get("Tier", ""),
        Outbreak=row.get("Outbreak", ""),
        Reason=row.get("Reason", ""),
        URL=row.get("URL", ""),
        page_text=page_text,
    )
    try:
        raw = _call_claude(prompt=prompt, system=CLAUDE_CHECK_SYSTEM)
    except Exception as exc:
        log.warning("Claude call failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])
        return None
    if not raw:
        return None
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as exc:
        log.warning("Claude JSON parse failed: %s | %s", exc, raw[:200])
        return None


# ---------------------------------------------------------------------------
# Per-row verification
# ---------------------------------------------------------------------------

def _was_recently_verified(row: Dict[str, Any]) -> bool:
    """True if Notes contains a recent stamp from THIS reviewer.

    Matches CHECKER_TAG specifically — claude-check should re-verify
    rows that openrouter-check stamped (and vice versa), because they
    are independent dissent reviewers. Only the same reviewer's prior
    stamp triggers the cooldown skip.
    """
    if SKIP_IF_VERIFIED_DAYS <= 0:
        return False
    notes = str(row.get("Notes") or "")
    m = re.search(rf"\[{re.escape(CHECKER_TAG)} (\d{{4}}-\d{{2}}-\d{{2}})", notes)
    if not m:
        return False
    try:
        stamp = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return False
    age = (datetime.now(timezone.utc).date() - stamp).days
    return age <= SKIP_IF_VERIFIED_DAYS


# ── Audit 2026-05-08: deterministic clean-row shortcut ───────────────────
# Skip the Claude API call entirely for rows that have all required fields
# filled with non-garbage values from a regulator URL the Gemini gate
# already approved. Cuts ~25-30% of API calls at zero quality cost — these
# rows would PASS Claude check anyway.
#
# Conditions ALL must be true:
#   1. URL passes is_generic_url (already verified earlier in pipeline)
#   2. Company is non-empty AND not in known-garbage list
#   3. Brand is non-empty AND not "—" (the safe-default fallback)
#   4. Product is non-empty AND > 8 chars
#   5. Pathogen is non-empty AND not in placeholder list
#   6. Date matches YYYY-MM-DD regex
#   7. Source matches a known regulator label
#   8. Row was previously enriched by Gemini gate (Notes contains
#      "[gemini-enrich" OR "[gemini-gate-passed")
#
# The 8th condition is the strongest gate — a row that Gemini already
# verified content-wise on the same page is overwhelmingly likely to PASS
# Claude. We only skip Claude on the ones where it'd be a rubber stamp.
_KNOWN_GARBAGE_COMPANY = frozenset({
    "food", "press release", "food incident post", "what's new", "news",
    "alert", "alerts", "recall", "recalls", "advisory",
    "fda", "usda", "fsis", "cfia", "fsa", "fsai", "fsanz", "mpi",
    "aesan", "ages", "bvl", "afsca", "favv", "nvwa", "rasff", "cfs",
    "sfa", "mfds", "mhlw", "fssai", "anvisa", "efet",
})
_PATHOGEN_PLACEHOLDERS = frozenset({
    "", "unknown", "n/a", "na", "none", "—", "-", "tbd",
    "pathogen contamination", "pathogen",
})

def _is_clean_row(row: Dict[str, Any]) -> bool:
    """Deterministic clean-row check. True = skip Claude API call.

    See module-level comment for the gate criteria. Conservative —
    false positives (rows that clean-shortcut but should have been
    failed) cost data-quality regression; misses (rows that don't
    shortcut) just cost an API call. So we only shortcut when ALL
    eight conditions clearly pass.
    """
    if os.getenv("CLAUDE_NO_SHORTCUT", "").strip() in ("1", "true", "yes"):
        return False
    company = str(row.get("Company") or "").strip()
    if not company or company.lower().rstrip(".:;,") in _KNOWN_GARBAGE_COMPANY:
        return False
    brand = str(row.get("Brand") or "").strip()
    if not brand or brand == "—" or brand.lower() == "—":
        return False
    product = str(row.get("Product") or "").strip()
    if len(product) < 8:
        return False
    pathogen = str(row.get("Pathogen") or "").strip().lower()
    if pathogen in _PATHOGEN_PLACEHOLDERS:
        return False
    date = str(row.get("Date") or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return False
    source = str(row.get("Source") or "").strip()
    if not source or source.lower() in ("", "tavily-gap", "gap"):
        return False
    # Strongest condition: row was already enriched/passed by Gemini gate.
    notes = str(row.get("Notes") or "")
    if "[gemini-enrich " not in notes and "[gemini-gate-passed " not in notes:
        return False
    return True


def check_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, str], Optional[str]]:
    """
    Verify one Pending row. Returns (verdict, corrections, reason)

      verdict in {"pass", "fix", "fail", "skip"}
      corrections = dict with any of {Date, Company, Brand, Product, Pathogen}
                    populated when Claude proposed a correction. Empty
                    {} when nothing to fix.
      reason = short audit string

    "skip" = transient failure (URL fetch / Claude error). Row stays in
             Pending unchanged so the next run gets another shot. **Per
             audit 2026-05-08, skipped rows are explicitly NOT promoted —
             see main() for the parking-lot behavior.**
    """
    if _was_recently_verified(row):
        return "pass", {}, "recently verified — skipping"

    # ── Audit 2026-05-08: deterministic clean-row shortcut ───────────────
    # Skip the API call entirely if the row is provably clean — saves
    # ~25-30% of Claude calls = ~$3-5/month. See _is_clean_row comment.
    if _is_clean_row(row):
        # AUDIT 2026-05-11: shortcut must still enforce Tier-1 scope.
        # Gemini's enricher (url_gate_gemini.py:1182-1194) writes any
        # pathogen-shaped string from the page (Asbestos, Electrical
        # shock, Mechanical hazard, Nitrosamines, phthalates, etc.)
        # into the Pathogen field without scope validation. The
        # shortcut used to wave these through as "all fields non-empty";
        # the API-path branch enforces scope at line 713 but never ran.
        # Without this check, 15+ non-food RappelConso recalls (cars,
        # toys, electrical appliances) would have promoted to Recalls
        # once Bug 1 (status flip) was fixed.
        final_pathogen = str(row.get("Pathogen") or "")
        if not is_tier1_pathogen(final_pathogen):
            return ("fail", {},
                    f"pathogen_out_of_scope: {final_pathogen!r} | "
                    f"clean-row shortcut: out of Tier-1 scope")
        return "pass", {}, "clean-row shortcut (gemini-enriched + all fields valid)"

    url = str(row.get("URL") or "").strip()
    text, fetch_err = _fetch_page_text(url)
    if text is None:
        return "skip", {}, f"fetch failed ({fetch_err})"

    # ── Deterministic date pre-check (audit 2026-05-12) ───────────────
    # Before paying for the Claude/Gemini call, scan the page text for
    # an authoritative publication date via regulator-specific regex
    # (CFIA dcterms.issued, FDA <time datetime>, FSAI "Alert Date",
    # FSIS article:published_time, RappelConso "Publié le", INVIMA PDF
    # "Bogotá, DD mes YYYY", etc.). If we find a date AND it's pre-2026,
    # short-circuit FAIL — no LLM call needed, no risk of Haiku
    # stochastically missing the date and FIX-promoting a stale recall
    # (which is what happened to the Hope Eco-Farm 2025-05-10 and
    # Mon Père 2025-08-13 rows in today's 04:15 run).
    page_date_iso = _extract_publication_date(text)
    if page_date_iso:
        try:
            page_d = datetime.fromisoformat(page_date_iso).date()
            if page_d.year < 2026:
                return ("fail", {},
                        f"date_pre_2026: {page_date_iso} "
                        f"(deterministic extract, no LLM call)")
        except ValueError:
            pass

    result = _verify_with_claude(row, text)
    if result is None:
        return "skip", {}, "claude unreachable / parse failed"

    verdict = str(result.get("verdict") or "").lower()
    if verdict not in ("pass", "fix", "fail"):
        return "skip", {}, f"unknown verdict '{verdict}'"

    # ── Collect every field correction Claude proposed ────────────────────
    # Pre-2026-05-08 the FIX path only applied date corrections; broken
    # Company/Brand/Product/Pathogen rows were FAILed even when Claude
    # could read the right values off the page. The new shape carries all
    # five corrections through to main() which decides what to apply.
    corrections: Dict[str, str] = {}

    date_corr = (str(result.get("date_corrected") or "").strip() or "")
    if date_corr and re.match(r"^\d{4}-\d{2}-\d{2}$", date_corr):
        corrections["Date"] = date_corr

    for src_key, dst_field, min_len in (
        ("company_corrected",  "Company",  2),
        ("brand_corrected",    "Brand",    2),
        ("product_corrected",  "Product",  3),
        ("pathogen_corrected", "Pathogen", 3),
    ):
        v = str(result.get(src_key) or "").strip()
        if len(v) >= min_len:
            corrections[dst_field] = v

    reason_parts = [verdict]
    if not result.get("is_recall_page", True):
        reason_parts.append("not a recall page")
    if not result.get("company_match", True):
        reason_parts.append("company mismatch")
    if not result.get("product_match", True):
        reason_parts.append("product mismatch")
    if not result.get("pathogen_match", True):
        reason_parts.append("pathogen mismatch")
    if not result.get("date_match", True):
        reason_parts.append(f"date on page={result.get('date_on_page', '?')}")
    short_reason = "; ".join(reason_parts)
    if result.get("reason"):
        short_reason = f"{short_reason} | {str(result['reason'])[:140]}"

    # Refuse to apply a "fix" if Claude said "fix" but provided NO usable
    # correction at all (no date, no company, no brand, no product, no
    # pathogen). Degrade to "fail" — there's nothing to apply.
    if verdict == "fix" and not corrections:
        return "fail", {}, f"fix proposed but no corrections in response: {short_reason}"

    # Post-decision sanity (locked 2026-04-30) — hard rules that
    # override Claude's verdict if violated.
    final_url = url
    # Use corrected pathogen if Claude provided one, else the row's value
    final_pathogen = corrections.get("Pathogen") or str(row.get("Pathogen", "") or "")

    if is_news_mirror(final_url):
        return "fail", {}, f"news_mirror_domain | {short_reason}"

    # Use date_corrected if Claude proposed one, else row's date
    try:
        check_date_str = corrections.get("Date") or str(row.get("Date", ""))[:10]
        check_date_obj = (datetime.fromisoformat(check_date_str).date()
                          if check_date_str else None)
    except (TypeError, ValueError):
        check_date_obj = None

    if check_date_obj and check_date_obj.year < 2026:
        return "fail", {}, f"date_pre_2026: {check_date_obj.isoformat()} | {short_reason}"

    year_issue = is_year_mismatch(check_date_obj, final_url)
    if year_issue:
        return "fail", {}, f"url_year_mismatch: {year_issue} | {short_reason}"

    if not is_tier1_pathogen(final_pathogen):
        return "fail", {}, f"pathogen_out_of_scope: {final_pathogen!r} | {short_reason}"

    # Extraction garbage check (uses corrected values where available)
    company = (corrections.get("Company") or str(row.get("Company") or "")).strip().lower()
    brand = (corrections.get("Brand") or str(row.get("Brand") or "")).strip().lower()
    GARBAGE = {"home","index","page","recalls","alerts","alert","recall","welcome","main"}
    if company and company == brand and company in GARBAGE:
        return "fail", {}, f"extraction_garbage: Company=Brand={company!r} | {short_reason}"
    if re.search(r"/(home|index|main|welcome)/?$", final_url.lower()):
        return "fail", {}, f"extraction_garbage: URL is landing page | {short_reason}"

    # Company-placeholder rescue (audit 2026-05-08, supersedes 2026-05-06).
    #
    # Pre-2026-05-08 this branch FAILed every row where Claude saw a
    # company on the page but the row's Company field was a placeholder
    # ("", "Unbranded", "sans marque", etc.). The reasoning was sound
    # (the row IS broken), but the action was wrong: rows piled up in
    # Pending with Status=rejected for problems Claude itself had already
    # identified by name on the page.
    #
    # New behavior: if Claude provided company_corrected (or brand_corrected),
    # ESCALATE to verdict="fix" so main() applies the correction and the
    # row is promoted with the right value. Only fall through to FAIL when
    # the row genuinely has nothing to fix — placeholder Company AND no
    # correction proposed AND not a multi-producer RASFF row.
    company_match = result.get("company_match", True)
    placeholder_companies = {
        "", "—", "-", "unbranded", "sans marque",
        "various brands", "various producers", "multiple brands",
        "no brand", "marque inconnue", "n/a", "na",
    }
    is_rasff = "rasff" in str(row.get("Source") or "").lower()
    cur_company_lc = str(row.get("Company") or "").strip().lower()
    if (not company_match and not is_rasff
            and cur_company_lc in placeholder_companies):
        if "Company" in corrections or "Brand" in corrections:
            # Claude can fill it — promote via FIX path
            verdict = "fix"
            short_reason = f"placeholder_company_filled_from_page | {short_reason}"
        else:
            return ("fail", {},
                    f"placeholder_company_with_company_mismatch: "
                    f"Company={row.get('Company')!r} but Claude says page "
                    f"DOES name a company → no correction proposed | "
                    f"{short_reason}")

    return verdict, corrections, short_reason


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Claude check (final verification) started: %s",
             t0.strftime("%Y-%m-%dT%H:%M:%SZ"))

    if not CLAUDE_ENABLED:
        log.error("Claude not enabled (no ANTHROPIC_API_KEY). Aborting.")
        return 2

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending = load_pending(XLSX_PATH)
    log.info("State: %d approved + %d pending", len(approved), len(pending))

    if not pending:
        log.info("Nothing in Pending — nothing to do.")
        return 0

    # Skip rows that are already in REJECTED status — those need human
    # triage, not another Claude pass.
    rows_to_check_idx: List[int] = [
        i for i, r in enumerate(pending)
        if (r.get("Status") or "").lower() != STATUS_REJECTED
    ]
    log.info("Will verify %d rows (skipping %d already-rejected)",
             len(rows_to_check_idx), len(pending) - len(rows_to_check_idx))

    # Per-row verification
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rejected_flags: Dict[int, str] = {}
    skipped_idx: set[int] = set()
    fixed_count = 0
    pass_count = 0
    fail_count = 0
    skip_count = 0

    for n, idx in enumerate(rows_to_check_idx, 1):
        row = pending[idx]
        log.info("[%d/%d] %s | %s",
                 n, len(rows_to_check_idx),
                 str(row.get("Date", ""))[:10],
                 str(row.get("URL", ""))[:80])
        verdict, corrections, reason = check_row(row)

        # Audit trail in Notes — list every applied correction so a
        # human can trace what changed and why.
        corr_summary = ""
        if corrections:
            parts = [f"{k}→{str(v)[:40]}" for k, v in corrections.items()]
            corr_summary = "; corrections: " + ", ".join(parts)
        audit = (f"[{CHECKER_TAG} {today_iso}: {verdict}{corr_summary}; "
                 f"{reason[:120]}]")
        notes = (row.get("Notes") or "").strip()
        # Drop any prior stamp from THIS reviewer before appending. Each
        # reviewer (claude-check vs openrouter-check) only replaces its
        # own stamp so the other reviewer's verdict survives — that's
        # the whole point of the dissent-reviewer setup.
        notes = re.sub(rf"\s*\[{re.escape(CHECKER_TAG)}[^\]]*\]\s*", " ", notes).strip()
        row["Notes"] = (notes + " " + audit).strip()[:1000]

        if verdict == "fix":
            # Apply EVERY correction Claude provided (audit 2026-05-08).
            # Pre-2026-05-08 only Date was applied; broken Company/Brand/
            # Product/Pathogen rows piled up in Pending forever because
            # the FIX path didn't know how to write them in.
            for field, new_val in corrections.items():
                old_val = row.get(field, "")
                row[field] = new_val
                log.info("  → FIX %s %r → %r",
                         field, str(old_val)[:40], str(new_val)[:60])
            fixed_count += 1
            # Falls through: row will be promoted by promote_approved
        elif verdict == "fail":
            rejected_flags[idx] = f"Claude check: {reason[:280]}"
            log.info("  → FAIL: %s", reason)
            fail_count += 1
        elif verdict == "pass":
            log.info("  → PASS")
            pass_count += 1
        else:  # skip
            log.info("  → SKIP: %s (left untouched, retry next run)", reason)
            skipped_idx.add(idx)   # ← AUDIT 2026-05-08: track for parking lot
            # ── 2026-05-13 — 2nd-reviewer-confirms-rejection fix ──────────
            # Per operator design (2 reviewers, both reject → archive): if
            # the row already has Status='rejected' (set by a prior url_gate
            # pass) AND this SKIP is a URL-fetch failure (HTTP 404,
            # connection reset, timeout, etc.), that's the second reviewer
            # also confirming the URL is dead. Promote the SKIP into
            # rejected_flags so promote_approved's mark_rejected_with_counter
            # increments RejectedBy to {url_gate, claude-check} (len=2) and
            # archives the row to Weekly_Rejected. Before this fix these
            # rows sat in Pending forever as Status='rejected' /
            # RejectedBy='unknown' or similar single-reviewer state — never
            # archived because SKIP didn't count as a verdict.
            cur_status = (row.get("Status") or "").strip().lower()
            url_fetch_fail = any(s in (reason or "").lower() for s in (
                "http 4", "http 5", "fetch failed", "fetch error",
                "connection", "timeout", "http_error", "network",
            ))
            if cur_status == STATUS_REJECTED and url_fetch_fail:
                rejected_flags[idx] = (
                    f"Claude check: url-fetch-fail confirms prior rejection "
                    f"({reason[:200]})"
                )
                skipped_idx.discard(idx)
                fail_count += 1
                skip_count -= 1
                log.info("  → reclassified SKIP as FAIL (URL dead per "
                         "both reviewers — archiving via 2-reviewer counter)")
            else:
                skip_count += 1

        time.sleep(SLEEP_BETWEEN_ROWS_S)

    # ── SKIP parking lot (audit 2026-05-08, fixes silent auto-promotion) ──
    # Pre-2026-05-08 a SKIP verdict was a NO-OP — the row fell through to
    # promote_approved, which promoted any row not explicitly in
    # rejected_flags. Result: when Claude was rate-limited or unreachable,
    # every Pending row that day got auto-promoted to Recalls without
    # verification (2026-05-07T17:01 incident: 6 rows promoted, 0 reviewed).
    #
    # Fix: park skipped plain-pending rows in STATUS_PENDING_GAP_V2 (a
    # gap-gating status that promote_approved refuses to advance). They
    # stay in Pending until the next reviewer run can verify them. Skipped
    # rows that were ALREADY in a gap-gating status are left alone — they
    # were going to stay in Pending anyway.
    parked = 0
    for idx in skipped_idx:
        row = pending[idx]
        cur_status = (row.get("Status") or "").strip().lower()
        if cur_status in ("", "pending"):
            row["Status"] = STATUS_PENDING_GAP_V2
            parked += 1
    if parked:
        log.info("Parked %d skipped row(s) in pending_gap_v2 to prevent "
                 "auto-promotion (will retry next reviewer run)", parked)

    # ── Gap-finder gating state machine advance (audit 2026-04-29, fixed 2026-05-13) ──
    # Operator's design: 2 reviewers (url_gate_gemini + claude_check). Both
    # pass → promote. Pre-2026-05-13 this block only advanced pending_gap_v2,
    # which required TWO url_gate passes before claude could flip the row —
    # a third de-facto reviewer that contradicted the documented 2-reviewer
    # model. Result: rows that passed url_gate once AND passed claude_check
    # (e.g. the 2026-05-13 04:15 batch — Manor Farm / AESAN / FSA Nestlé)
    # sat at pending_gap_v1 forever even though both their reviewers had
    # said OK.
    #
    # Fixed: claude verification advances EITHER pending_gap_v1 OR
    # pending_gap_v2 to plain "pending". Failures are already in
    # rejected_flags and will be marked STATUS_REJECTED by promote_approved.
    gap_promoted_to_pending = 0
    today_iso2 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for idx in rows_to_check_idx:
        if idx in rejected_flags:
            continue  # FAIL — let promote_approved mark rejected
        if idx in skipped_idx:
            continue  # SKIP — leave in parking lot (audit 2026-05-08)
        row = pending[idx]
        cur = (row.get("Status") or "").strip()
        if cur in (STATUS_PENDING_GAP_V1, STATUS_PENDING_GAP_V2):
            row["Status"] = STATUS_PENDING
            notes = (row.get("Notes") or "").strip()
            tag = (f"[gap-gate {today_iso2}: "
                   f"{cur} → pending (Claude verified, 2nd reviewer pass)]")
            row["Notes"] = (notes + " " + tag).strip()[:1000]
            gap_promoted_to_pending += 1
    if gap_promoted_to_pending:
        log.info("Gap-gating advanced: %d rows pending_gap_v1/v2 → pending "
                 "(eligible for next merge_master)", gap_promoted_to_pending)

    # ── pending_enrichment unlock (audit 2026-05-11) ─────────────────────
    # Mirror of pending_gap_v2 → pending above. pending_enrichment rows
    # pass Claude via the clean-row shortcut once Gemini has filled the
    # Pathogen field, but pre-2026-05-11 there was no code to flip the
    # Status back. The result: rows sat in Pending forever (every run
    # rubber-stamping them via the shortcut, every promote_approved call
    # skipping them because pending_enrichment is in NON_PROMOTABLE_STATUSES).
    # Discovered 2026-05-11 — 18 rows stranded, none had ever promoted
    # despite passing Claude every run since 2026-05-09.
    #
    # Failures from the shortcut's new scope check (Patch 2) are caught
    # by rejected_flags above and don't reach this block.
    enrich_promoted_to_pending = 0
    for idx in rows_to_check_idx:
        if idx in rejected_flags:
            continue  # FAIL — let promote_approved mark rejected
        if idx in skipped_idx:
            continue  # SKIP — leave in parking lot
        row = pending[idx]
        if (row.get("Status") or "").strip() == STATUS_PENDING_ENRICHMENT:
            row["Status"] = STATUS_PENDING
            notes = (row.get("Notes") or "").strip()
            tag = (f"[enrich-gate {today_iso2}: "
                   f"pending_enrichment → pending (Claude verified)]")
            row["Notes"] = (notes + " " + tag).strip()[:1000]
            enrich_promoted_to_pending += 1
    if enrich_promoted_to_pending:
        log.info("Enrichment-gating advanced: %d rows pending_enrichment → "
                 "pending (eligible for promote_approved this run)",
                 enrich_promoted_to_pending)

    # Apply rejected_flags + dedup against Recalls via promote_approved.
    # Audit 2026-05-09: archive_immediately=True signals to promote_approved
    # that this is the FINAL reviewer (Claude/OpenRouter) — every rejection
    # archives to Rejected + Weekly_Rejected and evicts from Pending. Per
    # operator spec: Gemini = first reviewer (enrich + tag), Claude/OR =
    # second reviewer = final verdict, ~1 hour after Gemini in each session.
    # Without this kw, default behavior is the safer first-reviewer flow
    # (Status=rejected stamp, stays in Pending, 2-reviewer-counter safety net).
    new_approved, remaining, archived_rejected = promote_approved(
        pending=pending,
        approved_existing=approved,
        rejected_flags=rejected_flags,
        archive_immediately=True,
    )

    log.info("Verdicts: pass=%d, fix=%d, fail=%d, skip=%d (total checked=%d)",
             pass_count, fixed_count, fail_count, skip_count,
             pass_count + fixed_count + fail_count + skip_count)
    log.info("Promotion: %d Pending → Recalls; %d remain in Pending; "
             "%d archived to Rejected",
             len(new_approved), len(remaining), len(archived_rejected))

    # Save + commit
    final_approved = sort_rows(approved + new_approved)
    final_pending = sort_rows(remaining)
    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH,
                           newly_rejected_rows=archived_rejected)
    mirror_json_from_xlsx(XLSX_PATH, JSON_PATH)

    # Mirror promotions into the Weekly_Review sheet + refresh the
    # JSON slice the Apps Script Thursday-17:00 mailer reads.
    try:
        from pipeline.weekly_review_capture import record_promotions  # noqa: E402
        n_wr = record_promotions(new_approved, xlsx_path=XLSX_PATH)
        if n_wr:
            log.info("Weekly_Review: appended %d row(s)", n_wr)
    except Exception as _wr_err:  # never block promotion on capture failure
        log.warning("Weekly_Review capture failed: %s", _wr_err)

    # Mirror rejections into the Weekly_Rejected sheet + refresh JSON
    # slice. Same Thursday cutoff / wipe semantics as Weekly_Review.
    # Single-reviewer eviction policy (2026-05-09): archived_rejected
    # contains EVERY claude-check rejection from this run, not just
    # those that hit a 2-different-reviewer threshold.
    try:
        from pipeline.weekly_rejected_capture import record_rejections  # noqa: E402
        n_wj = record_rejections(archived_rejected, xlsx_path=XLSX_PATH)
        if n_wj:
            log.info("Weekly_Rejected: appended %d row(s)", n_wj)
    except Exception as _wj_err:  # never block run on capture failure
        log.warning("Weekly_Rejected capture failed: %s", _wj_err)

    # Rebuild daily briefs for every date that gained newly-promoted rows.
    # Without this, the dashboard's rolling 7-day display + DAILY tab stay
    # stale until the next 10:00 daily-recall-search run.
    rebuilt_briefs, brief_files = rebuild_daily_briefs_for_promoted(
        new_approved, final_approved,
    )
    if rebuilt_briefs:
        log.info("Rebuilt %d daily brief(s)", len(rebuilt_briefs))

    files_to_commit = [
        "docs/data/recalls.xlsx",
        "docs/data/recalls.json",
        "docs/data/weekly-review-latest.json",
    ]
    files_to_commit.extend(brief_files)

    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=files_to_commit,
            message=(f"FSIS Claude check {t0.strftime('%Y-%m-%d')} "
                     f"(+{len(new_approved)} promoted, "
                     f"{fixed_count} dates fixed, "
                     f"{fail_count} failed, {len(remaining)} pending"
                     + (f", rebuilt {len(rebuilt_briefs)} briefs"
                        if rebuilt_briefs else "")
                     + ")"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | +%d promoted | %d date fixes | %d failed | "
             "%d still pending | %d briefs rebuilt",
             elapsed, len(new_approved), fixed_count, fail_count,
             len(remaining), len(rebuilt_briefs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
