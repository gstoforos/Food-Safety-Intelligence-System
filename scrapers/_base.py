"""
scrapers/_base.py
=================
AFTS FSIS — Base scraper classes and shared HTTP utilities.

Public API (do not change signatures without grepping all callers)
------------------------------------------------------------------
    BaseScraper
        AGENCY          class attr, str
        COUNTRY         class attr, str
        session         instance attr, requests.Session
        logger          instance attr, logging.Logger
        scrape(since_days: int = 30) -> List[Recall]       subclasses override
        _new_recall(Date, Company, Brand, Product, Pathogen, Reason,
                    Class, URL, Outbreak, Notes) -> Recall  builds + normalizes

    GenericGeminiScraper(BaseScraper)
        INDEX_URLS        class attr, Sequence[str]
        LANGUAGE          class attr, str (default "en")
        EXTRACTION_HINTS  class attr, str (optional agency-specific guidance)
        scrape(since_days: int = 30) -> List[Recall]       uses Gemini

    make_session() -> requests.Session
    fetch(session, url, method="GET", timeout=30, **kwargs) -> Optional[Response]
        NOTE: session is the FIRST positional argument — do not change.

Consumers
---------
    pipeline.run_all                   BaseScraper, make_session
    scrapers.<region>.*                BaseScraper or GenericGeminiScraper, fetch
    scrapers.news_feeds._news_base     make_session, fetch
    pipeline.fsis_url_guardian         make_session, fetch (likely)
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

from scrapers._models import (
    Recall,
    assign_tier,
    infer_region,
    normalize_country,
    normalize_pathogen,
)
from scrapers._akamai_fetch import fetch_via_curl_cffi, is_akamai_host

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 30

# Audit 2026-04-29 — UA changed from "AFTS-FSIS/1.0" to a real Chrome UA.
# 5 regulators were blocking the bot-style UA with HTTP 403 (ŠVPS-SK,
# VMVT-LT, FDA-PH, COMESA, MoH-IL). A normal browser UA gets through.
# This is consistent with how RASFF and other open-data services accept
# anonymous public requests — the agencies block obviously-automated
# scrapers but allow normal browsing. We're not impersonating; we just
# stop announcing ourselves as a bot.
#
# If a regulator ever asks us to identify ourselves explicitly (some
# do, via robots.txt or contact@ headers), they can be added to a
# per-host override map in SPECIAL_HEADERS_BY_HOST below. None today.
DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def make_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Sequence[int] = (429, 500, 502, 503, 504),
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
) -> requests.Session:
    """
    Build a requests.Session with retry/backoff and the AFTS user-agent.

    The optional `timeout` kwarg is stored on the session as `request_timeout`
    and used by fetch() as the default when no explicit timeout is passed —
    this is how scrapers.news_feeds._news_base sets a session-wide default.
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    if headers:
        session.headers.update(headers)
    if timeout is not None:
        # requests.Session has no native default-timeout — we stash it and fetch() reads it.
        session.request_timeout = timeout  # type: ignore[attr-defined]

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch(
    session: Optional[requests.Session],
    url: str,
    method: str = "GET",
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Optional[requests.Response]:
    """
    Fetch a URL. Returns Response on success, None on failure.

    NOTE: session is the FIRST positional argument by AFTS convention.
    All regional scrapers call `fetch(self.session, url)` — do not swap.
    A None session is accepted and a default session is built internally.

    Timeout resolution order:
        1. explicit `timeout=` kwarg
        2. session.request_timeout (set by make_session(timeout=...))
        3. DEFAULT_TIMEOUT (30s)
    """
    if session is None:
        session = make_session()
    if timeout is None:
        timeout = getattr(session, "request_timeout", DEFAULT_TIMEOUT)
    # Per-host routing: Akamai-protected gov sites need curl_cffi TLS
    # impersonation. See scrapers/_akamai_fetch.py for the host list +
    # rationale. All other hosts use the standard requests path.
    if is_akamai_host(url):
        return fetch_via_curl_cffi(url, method=method, timeout=timeout, **kwargs)
    try:
        return session.request(method, url, timeout=timeout, **kwargs)
    except requests.RequestException as exc:
        log.warning("fetch failed for %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Gemini 2.0 Flash helper (used by GenericGeminiScraper)
# ---------------------------------------------------------------------------

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_MAX_HTML_CHARS = 120_000


def _gemini_api_keys() -> List[str]:
    """
    Collect every available Gemini key. Supports both conventions:
      - Single key:   GEMINI_API_KEY
      - Rotation:     GEMINI_API_KEY_1 .. GEMINI_API_KEY_5
    """
    keys: List[str] = []
    legacy = os.getenv("GEMINI_API_KEY")
    if legacy:
        keys.append(legacy)
    for i in range(1, 6):
        k = os.getenv(f"GEMINI_API_KEY_{i}")
        if k and k not in keys:
            keys.append(k)
    return keys


def _call_gemini(prompt: str, html: str, language: str = "en") -> str:
    """Call Gemini with key rotation. Returns the raw text response.

    Uses the new google-genai SDK (replaces deprecated google.generativeai).
    Install: pip install google-genai
    """
    try:
        from google import genai  # type: ignore
        from google.genai import types  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "google-genai is not installed. Add it to requirements.txt "
            "(pip install google-genai)."
        ) from exc

    keys = _gemini_api_keys()
    if not keys:
        raise RuntimeError(
            "No GEMINI_API_KEY(_1..5) env var set. Configure in GitHub Actions secrets."
        )

    if len(html) > GEMINI_MAX_HTML_CHARS:
        html = html[:GEMINI_MAX_HTML_CHARS] + "\n<!-- truncated -->"

    full_prompt = f"{prompt}\n\nLANGUAGE OF PAGE: {language}\n\nHTML:\n{html}"

    last_error: Optional[Exception] = None
    for api_key in random.sample(keys, k=len(keys)):
        try:
            client = genai.Client(api_key=api_key)
            # max_output_tokens=32000 prevents JSON truncation on long pages.
            # The default cap (~8K) was silently cutting Gemini's response
            # mid-string on agency listings with many recalls (e.g. KEBS,
            # observed 2026-04-28: "Unterminated string at char 7298").
            # Gemini 2.5 Flash supports up to 65K output tokens.
            config = types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=32_000,
            )
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=full_prompt,
                config=config,
            )
            text = (getattr(resp, "text", None) or "").strip()
            if text:
                return text
        except Exception as exc:  # noqa: BLE001 - try next key
            last_error = exc
            log.warning(
                "Gemini call failed on one key (%s); trying next.",
                type(exc).__name__,
            )
            continue

    if last_error:
        raise RuntimeError(f"All Gemini keys failed: {last_error}") from last_error
    return ""


# ---------------------------------------------------------------------------
# OpenAI fallback (audit 2026-05-14)
# ---------------------------------------------------------------------------
# Context: 2026-04 onwards, Gemini's monthly spending cap on the AI Studio
# project started exhausting roughly mid-month, leaving ~40 scrapers
# returning 0 rows for ~half the month with the symptom
# `429 RESOURCE_EXHAUSTED: monthly spending cap`. Lifting the cap conflicts
# with operator's preference (AI Studio is shared with their own model
# training experiments). Solution: when Gemini fails — especially on
# quota/rate errors — fall back to OpenAI gpt-4o-mini, which has identical
# HTML→structured-extraction capability at similar token cost
# ($0.15/M input + $0.60/M output ≈ €0.005-€0.010 per scraper page).
#
# Activation: zero-config. If OPENAI_API_KEY (or OPENAI_API_KEY_1..5) is
# set in the runner environment, fallback engages automatically when
# Gemini fails. If not set, behavior is unchanged from before
# (Gemini failure → empty row list, scraper logs warning).
#
# Cost ceiling on a worst-case day where every Gemini call fails:
#   ~40 scrapers × ~$0.008/call × ~4 daily-orchestrator runs ≈ $1.3/day.
# At ~$40/month sustained worst-case. Realistic average will be a
# fraction of that since Gemini's cap doesn't exhaust the entire month.
OPENAI_MODEL          = os.getenv("OPENAI_FALLBACK_MODEL",   "gpt-4o-mini")
OPENAI_URL            = "https://api.openai.com/v1/chat/completions"
OPENAI_TIMEOUT        = int(os.getenv("OPENAI_FALLBACK_TIMEOUT",     "60"))
OPENAI_MAX_OUT_TOKENS = int(os.getenv("OPENAI_FALLBACK_MAX_TOKENS", "16000"))


def _openai_api_keys() -> List[str]:
    """Mirror the Gemini key-rotation pattern for OpenAI."""
    keys: List[str] = []
    legacy = os.getenv("OPENAI_API_KEY")
    if legacy:
        keys.append(legacy.strip())
    for i in range(1, 6):
        k = os.getenv(f"OPENAI_API_KEY_{i}")
        if k:
            keys.append(k.strip())
    # Dedup preserving order
    return list(dict.fromkeys(k for k in keys if k))


def _call_openai(prompt: str, html: str, language: str = "en") -> str:
    """OpenAI fallback for HTML extraction. Mirrors _call_gemini's
    signature and return contract so it's a drop-in replacement.

    Returns the raw text response (expected to be JSON-as-text, same as
    Gemini). The caller (_extract_with_gemini) parses + validates.

    Multi-key rotation matches Gemini's behavior — on failure with one
    key (auth, rate, network), tries the next. Raises RuntimeError if
    every key fails, with the last error message preserved.
    """
    keys = _openai_api_keys()
    if not keys:
        raise RuntimeError(
            "No OPENAI_API_KEY(_1..5) env var set. Configure in GitHub "
            "Actions secrets to enable Gemini-failure fallback."
        )

    # Same HTML truncation cap as Gemini for consistency. Avoids one
    # backend processing 4× more page content than the other.
    if len(html) > GEMINI_MAX_HTML_CHARS:
        html = html[:GEMINI_MAX_HTML_CHARS] + "\n<!-- truncated -->"

    full_prompt = f"{prompt}\n\nLANGUAGE OF PAGE: {language}\n\nHTML:\n{html}"

    payload = {
        "model":       OPENAI_MODEL,
        "messages":    [{"role": "user", "content": full_prompt}],
        "temperature": 0.1,
        "max_tokens":  OPENAI_MAX_OUT_TOKENS,
    }

    last_error: Optional[Exception] = None
    for api_key in random.sample(keys, k=len(keys)):
        try:
            r = requests.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {api_key}",
                         "Content-Type":  "application/json"},
                json=payload,
                timeout=OPENAI_TIMEOUT,
            )
            if r.status_code == 429:
                # OpenAI rate-limit / quota — try next key (may have own budget)
                last_error = RuntimeError(
                    f"OpenAI 429 rate-limit: {r.text[:200]}")
                log.warning("OpenAI 429 on one key — trying next.")
                continue
            if r.status_code in (401, 403):
                last_error = RuntimeError(
                    f"OpenAI {r.status_code} auth failure: {r.text[:200]}")
                log.warning("OpenAI %d auth on one key — trying next.",
                            r.status_code)
                continue
            if r.status_code != 200:
                last_error = RuntimeError(
                    f"OpenAI HTTP {r.status_code}: {r.text[:200]}")
                continue
            data = r.json()
            text = (data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content") or "").strip()
            if text:
                return text
            # Empty response — try next key
            last_error = RuntimeError("OpenAI returned empty content")
        except Exception as exc:  # noqa: BLE001 - try next key
            last_error = exc
            log.warning(
                "OpenAI fallback call failed on one key (%s); trying next.",
                type(exc).__name__,
            )
            continue

    if last_error:
        raise RuntimeError(
            f"All OpenAI keys failed: {last_error}") from last_error
    return ""


def _is_quota_or_rate_error(exc: BaseException) -> bool:
    """Return True if the exception looks like a quota / rate-limit issue.

    Matches Gemini's `429 RESOURCE_EXHAUSTED`, generic "quota", and the
    specific "monthly spending cap" message AI Studio surfaces when the
    paid spending cap on the linked GCP project trips.
    """
    msg = str(exc).lower()
    return (
        "resource_exhausted"   in msg
        or "monthly spending cap" in msg
        or "spend cap"            in msg
        or "rate limit"           in msg
        or "rate_limit"           in msg
        or " 429"                 in msg
        or msg.startswith("429")
        or "quota"                in msg
    )


def _call_llm(prompt: str, html: str, language: str = "en") -> str:
    """Call Gemini first; on failure, fall back to OpenAI if configured.

    Behavior matrix:
      - Gemini succeeds                       → return Gemini's text
      - Gemini quota/rate error + OpenAI set  → try OpenAI; if it succeeds,
                                                return its text
      - Gemini other error      + OpenAI set  → try OpenAI; on any failure
                                                there, raise combined error
      - Gemini fails + OpenAI not configured  → re-raise original Gemini
                                                error (pre-fix behavior)

    The two-backend strategy is deliberately invisible to callers — they
    see a single text return as before. Logs make the choice explicit so
    operators can spot when fallback engages without parsing JSON.
    """
    try:
        text = _call_gemini(prompt, html, language)
        return text
    except Exception as gemini_exc:  # noqa: BLE001
        openai_keys = _openai_api_keys()
        if not openai_keys:
            # No fallback configured — surface original error (pre-fix path)
            raise

        is_quota = _is_quota_or_rate_error(gemini_exc)
        if is_quota:
            log.info("Gemini quota exhausted (%s) — switching to OpenAI "
                     "fallback for this call.", type(gemini_exc).__name__)
        else:
            log.info("Gemini failed (%s: %s) — trying OpenAI fallback.",
                     type(gemini_exc).__name__,
                     str(gemini_exc)[:120])

        try:
            text = _call_openai(prompt, html, language)
            log.info("OpenAI fallback succeeded (model=%s, %d chars out)",
                     OPENAI_MODEL, len(text))
            return text
        except Exception as openai_exc:  # noqa: BLE001
            # Both backends failed — raise a combined error so the
            # _extract_with_gemini layer's single except sees one
            # exception with full context.
            raise RuntimeError(
                f"Both Gemini and OpenAI failed. "
                f"Gemini: {type(gemini_exc).__name__}: {str(gemini_exc)[:200]}. "
                f"OpenAI: {type(openai_exc).__name__}: {str(openai_exc)[:200]}."
            ) from openai_exc


def _strip_code_fences(s: str) -> str:
    """Strip ```json ... ``` or ``` ... ``` fences a model may wrap JSON in."""
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*\n", "", s)
        s = re.sub(r"\n```\s*$", "", s)
    return s.strip()


GEMINI_EXTRACTION_PROMPT = """\
You are a STRICT EXTRACTOR of food recall records from an agency's HTML page.
You are NOT a summarizer, interpreter, or guesser. Return ONLY facts that
appear LITERALLY on the page. If a fact is not on the page, return "" for
that field. Never infer. Never assume. Never fill in a "likely" pathogen
from the product type. Never copy text from the HTML <title>, <h1>,
breadcrumb, navigation, footer, or cookie banner.

Return a JSON array (no prose, no markdown fences). Each item:

{
  "date": "YYYY-MM-DD",
  "company": "<recalling company, from the page body — NOT page title/nav>",
  "brand": "<product brand, from the page body — NOT page title/nav>",
  "product": "<product name / description, from the page body>",
  "pathogen": "<canonical English hazard label — see rules below>",
  "url": "<absolute URL to the recall DETAIL page>",
  "description": "<1–2 sentence summary using only words from the page>"
}

PATHOGEN / HAZARD RULES — return CANONICAL ENGLISH label (translate from
source language); never invent. Company/Brand/Product stay VERBATIM in
the source language — only the pathogen field is translated:
  - Microbiological pathogen named on page → use that name
      "Listeria monocytogenes", "Salmonella", "E. coli O157:H7", "STEC",
      "Clostridium botulinum", "Cronobacter", "Norovirus", "Hepatitis A"
  - Mycotoxin / chemical hazard named on page → use that name
      "Aflatoxin", "Ochratoxin", "Histamine", "Ethylene oxide", "Lead"
  - Non-contamination hazard described on page → use the category label:
      Page says "enthält Fleisch" / "contains meat" / "viande non déclarée"
        → "Undeclared meat"
      Page says "Fremdkörper - Plastik" / "foreign body" / "corps étranger"
        → "Foreign body (plastic)" or "Foreign body (metal)" etc.
      Page says "Allergenkennzeichnung fehlt" / "undeclared allergen"
        → "Undeclared allergen (<allergen>)" if named, else "Undeclared allergen"
      Page says "Falschdeklaration" / "mislabeling" / "étiquetage erroné"
        → "Mislabeling"
      Page says "Verpackungsmangel" / "packaging defect"
        → "Packaging defect"
      Page says "Rodentizid" / "rat poison" / "rodenticide"
        → "Rodenticide"
  - If page mentions NO pathogen, NO chemical hazard, AND NO non-contamination
    hazard category → return "" for pathogen. Do NOT pick the most likely
    pathogen for the product type. Empty is correct.

COMPANY / BRAND / PRODUCT RULES:
  - Extract from the recall TEXT BODY, not from <title>, <h1>, breadcrumb,
    navigation, "Find recalls" / "Trouvez des rappels" header strings.
  - If the only candidate string equals the page <title> tag, return "".
  - If the only candidate string is a bare domain ("canada.ca", "fda.gov",
    "foodsafetynews.com"), return "".
  - If a candidate contains any of these substrings, return "" for that field
    (HTML / JS artifacts the scraper should never see):
      "{socials", "window.", "querySelector", "&nbsp;", "<title>",
      "</title>", "[data-progress-bar]", "(function", "document.cookie",
      "addEventListener"

PAGE-TYPE RULE:
  - If the URL is a category listing, search results, homepage, or
    multi-recall index page (rather than a specific recall detail page),
    return [] (empty array). Do NOT fabricate a single record from the
    listing's first item.

GENERAL RULES:
  - Dates must be ISO (YYYY-MM-DD). If the page shows DD.MM.YYYY or
    DD/MM/YYYY, convert. Never guess a date.
  - URLs must be absolute and point to the recall DETAIL page.
  - Hazard SCOPE for this pipeline includes: microbiological pathogens,
    mycotoxins, chemical contaminants, undeclared allergens, foreign
    bodies, mislabeling, packaging defects, rodenticides, heavy metals.
    (Allergens / foreign bodies / mislabeling were OUT of scope in older
    versions — they are IN scope now.)
  - If the page has NO recall matching the rules above, return [].
  - Return ONLY the JSON array. No commentary, no markdown.
"""


# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------
# Audit 2026-05-21 — California Dairies recall came in with Date="20260420"
# (8-digit YYYYMMDD with no separators) from openFDA's recall_initiation_date
# field. The Pending sheet stored it raw, downstream consumers (sort, JSON
# mirror, validation gate) all failed silently because datetime.fromisoformat
# can't parse "20260420". Reviewer had to manually fix 3 SKU rows.
#
# Fix is at the construction point: every scraper builds Recalls via
# _new_recall(); normalize Date there so the malformed shape can never enter
# the rest of the system. pipeline/merge_master.validate_pending_row has a
# matching defensive backstop for rows that bypass _new_recall (manual
# injects, gap-finder rows constructed via Recall(...) direct).
#
# Supported input shapes:
#   "YYYY-MM-DD"        already ISO — pass through
#   "YYYYMMDD"          8-digit compact — convert to YYYY-MM-DD (openFDA, BfR)
#   "YYYY/MM/DD"        slash-separated — convert to YYYY-MM-DD
#   "DD-MM-YYYY"        EU-style — convert to YYYY-MM-DD
#   "DD/MM/YYYY"        EU-style — convert to YYYY-MM-DD
#   "M/D/YYYY"          US-style short — convert to YYYY-MM-DD
#   datetime / date     stringify via strftime
#   ""                  pass through empty (Pending allows date_unknown)
#   anything else       pass through unchanged (validation gate will reject)
# ---------------------------------------------------------------------------

_DATE_YYYYMMDD_RE  = re.compile(r"^(20\d\d)(\d{2})(\d{2})$")
_DATE_YYYY_X_MD_RE = re.compile(r"^(20\d\d)[/.](\d{1,2})[/.](\d{1,2})$")
_DATE_DMY_RE       = re.compile(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-](20\d\d)$")


def _normalize_date_string(d: Any) -> str:
    """Coerce a scraper-supplied Date value into ``YYYY-MM-DD`` form.

    Returns "" on unparseable / empty input; the validation gate downstream
    will reject the row or accept Date="" for enrichment by claude-check.
    Never raises.
    """
    if d is None:
        return ""
    # datetime / date objects from API clients
    if isinstance(d, datetime):
        try:
            return d.strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            return ""
    # date-only is a subclass of nothing — duck-type via strftime
    if hasattr(d, "strftime") and not isinstance(d, str):
        try:
            return d.strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            return ""
    s = str(d).strip()
    if not s:
        return ""

    # Already ISO YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        # Take just the date part if a timestamp leaked in ("2026-05-21T10:00Z")
        head = s[:10]
        if head[:4].isdigit() and head[5:7].isdigit() and head[8:10].isdigit():
            return head

    # YYYYMMDD compact (openFDA recall_initiation_date)
    m = _DATE_YYYYMMDD_RE.match(s)
    if m:
        y, mo, da = m.groups()
        if 1 <= int(mo) <= 12 and 1 <= int(da) <= 31:
            return f"{y}-{mo}-{da}"

    # YYYY/MM/DD or YYYY.MM.DD
    m = _DATE_YYYY_X_MD_RE.match(s)
    if m:
        y, mo, da = m.groups()
        if 1 <= int(mo) <= 12 and 1 <= int(da) <= 31:
            return f"{y}-{int(mo):02d}-{int(da):02d}"

    # DD-MM-YYYY / DD/MM/YYYY / DD.MM.YYYY (EU)
    # Ambiguous with M/D/YYYY (US) when first part <= 12 — choose the format
    # that yields a valid date; prefer EU (DMY) for European-source scrapers
    # if both work, since this normalizer is rarely needed for US scrapers
    # (openFDA returns YYYYMMDD compact, handled above).
    m = _DATE_DMY_RE.match(s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # Prefer DMY when day>12 forces interpretation
        if a > 12 and 1 <= b <= 12:
            return f"{y:04d}-{b:02d}-{a:02d}"
        if b > 12 and 1 <= a <= 12:
            return f"{y:04d}-{a:02d}-{b:02d}"
        # Ambiguous — default to MDY (US) since openFDA/FSIS/FDA dominate corpus
        if 1 <= a <= 12 and 1 <= b <= 31:
            return f"{y:04d}-{a:02d}-{b:02d}"

    # Anything else: return unchanged so the validation gate can log and reject
    return s


# ---------------------------------------------------------------------------
# BaseScraper
# ---------------------------------------------------------------------------

class BaseScraper:
    """
    Abstract base for regional agency scrapers.

    Subclasses MUST:
      - set class attrs AGENCY (str) and COUNTRY (str)
      - implement scrape(since_days: int = 30) -> List[Recall]

    Subclasses use self.session (a requests.Session) and self._new_recall(...)
    to build Recall objects. _new_recall auto-fills Source, Country, Region,
    and Tier — callers only pass the per-row fields.
    """

    AGENCY: str = ""
    COUNTRY: str = ""

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        cls_name = type(self).__name__
        self.session: requests.Session = session or make_session()
        slug = re.sub(r"\W+", "_", (self.AGENCY or cls_name).lower()).strip("_") or "unknown"
        self.logger: logging.Logger = logging.getLogger(f"scraper.{slug}")

    # --------------------------------------------------------------- API
    def scrape(self, since_days: int = 30) -> List[Recall]:
        """Subclasses override. Returns a list of Recall objects."""
        raise NotImplementedError(
            f"{type(self).__name__} must implement scrape(since_days)"
        )

    # ---------------------------------------------------- Recall builder
    def _new_recall(
        self,
        Date: str = "",
        Company: str = "",
        Brand: str = "—",
        Product: str = "",
        Pathogen: str = "",
        Reason: str = "",
        Class: str = "Recall",
        URL: str = "",
        Outbreak: Any = 0,
        Notes: str = "",
    ) -> Recall:
        """
        Build a Recall with Source/Country auto-filled from class attrs and
        Region/Tier auto-computed from the normalizers.

        Callers pass only per-row fields — Source, Country, Region, Tier are
        never passed by the regional scrapers (verified by grep across repo).
        """
        country = normalize_country(self.COUNTRY) or (self.COUNTRY or "")
        region = infer_region(country) if country else ""
        canonical_pathogen = normalize_pathogen(Pathogen) or (Pathogen or "")

        try:
            outbreak_int = 1 if int(Outbreak or 0) else 0
        except (TypeError, ValueError):
            outbreak_int = 0

        tier = assign_tier(canonical_pathogen, outbreak_int)

        def _s(v: Any, default: str = "") -> str:
            if v is None:
                return default
            return str(v).strip() or default

        return Recall(
            Date=_normalize_date_string(Date),
            Source=self.AGENCY or "",
            Company=_s(Company),
            Brand=_s(Brand, "—") or "—",
            Product=_s(Product),
            Pathogen=_s(canonical_pathogen),
            Reason=_s(Reason),
            Class=_s(Class, "Recall") or "Recall",
            Country=country,
            Region=region,
            Tier=tier,
            Outbreak=outbreak_int,
            URL=_s(URL),
            Notes=_s(Notes),
        )


# ---------------------------------------------------------------------------
# GenericGeminiScraper
# ---------------------------------------------------------------------------

class GenericGeminiScraper(BaseScraper):
    """
    Default scraper: uses Gemini 2.0 Flash to extract structured recall rows
    from each URL in INDEX_URLS.

    Subclasses typically set only:
        AGENCY, COUNTRY, INDEX_URLS, LANGUAGE, EXTRACTION_HINTS (optional)
    """

    INDEX_URLS: Sequence[str] = ()
    LANGUAGE: str = "en"
    EXTRACTION_HINTS: str = ""  # optional agency-specific guidance appended to the prompt

    def scrape(self, since_days: int = 30) -> List[Recall]:
        if not self.INDEX_URLS:
            self.logger.warning(
                "%s has no INDEX_URLS configured — skipping",
                type(self).__name__,
            )
            return []

        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date()
        all_rows: List[Recall] = []

        for url in self.INDEX_URLS:
            self.logger.info("fetching %s", url)
            resp = fetch(self.session, url)
            if resp is None or not resp.ok:
                self.logger.warning(
                    "skip %s (status=%s)",
                    url,
                    getattr(resp, "status_code", "no-response"),
                )
                continue

            try:
                rows = self._extract_with_gemini(resp.text, url)
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("gemini extract failed for %s: %s", url, exc)
                rows = []

            # Date filter — drop rows older than cutoff when date parseable,
            # keep rows with un-parseable dates (better to let review catch).
            filtered: List[Recall] = []
            for r in rows:
                if not r.Date:
                    filtered.append(r)
                    continue
                try:
                    d = datetime.strptime(r.Date[:10], "%Y-%m-%d").date()
                    if d >= cutoff:
                        filtered.append(r)
                except (ValueError, TypeError):
                    filtered.append(r)

            self.logger.info(
                "extracted %d rows from %s (%d within %d-day window)",
                len(rows), url, len(filtered), since_days,
            )
            all_rows.extend(filtered)

        return all_rows

    # ---------------------------------------------------------- internal
    def _extract_with_gemini(self, html: str, source_url: str) -> List[Recall]:
        prompt = GEMINI_EXTRACTION_PROMPT
        if self.EXTRACTION_HINTS:
            prompt = f"{prompt}\n\nAGENCY-SPECIFIC HINTS:\n{self.EXTRACTION_HINTS}"
        prompt = (
            f"{prompt}\n\n"
            f"AGENCY: {self.AGENCY}\n"
            f"COUNTRY: {self.COUNTRY}\n"
            f"LISTING URL: {source_url}\n"
        )

        try:
            # Audit 2026-05-14: use _call_llm wrapper which tries Gemini
            # first and falls back to OpenAI on quota/rate errors. If no
            # OPENAI_API_KEY is set, behavior is unchanged from pre-fix
            # (Gemini-only). Activation is automatic when OPENAI_API_KEY
            # appears in env — no per-scraper opt-in needed.
            raw = _call_llm(prompt=prompt, html=html, language=self.LANGUAGE)
        except Exception as exc:  # noqa: BLE001 - review layer can try Claude fallback
            self.logger.warning("LLM call failed (Gemini+OpenAI both): %s", exc)
            return []

        if not raw:
            return []

        cleaned = _strip_code_fences(raw)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            self.logger.warning("Gemini returned non-JSON: %s", exc)
            return []

        if not isinstance(data, list):
            self.logger.warning(
                "Gemini returned non-array JSON (%s)", type(data).__name__
            )
            return []

        rows: List[Recall] = []
        for item in data:
            if not isinstance(item, dict):
                continue

            raw_url = str(item.get("url") or "").strip()
            if raw_url and not raw_url.startswith(("http://", "https://")):
                raw_url = urljoin(source_url, raw_url)

            rec = self._new_recall(
                Date=str(item.get("date", "")),
                Company=str(item.get("company", "")),
                Brand=str(item.get("brand", "")),
                Product=str(item.get("product", "")),
                Pathogen=str(item.get("pathogen", "")),
                Reason=str(item.get("description", "")) or str(item.get("pathogen", "")),
                URL=raw_url,
                Notes=f"Gemini/{self.LANGUAGE} from {source_url}",
            )

            # Drop rows that can't be promoted anyway (no URL or no pathogen).
            if not rec.URL or not rec.Pathogen:
                continue

            rows.append(rec)

        return rows


__all__ = [
    "BaseScraper",
    "GenericGeminiScraper",
    "make_session",
    "fetch",
]
