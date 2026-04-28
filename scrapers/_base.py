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

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 30

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AFTS-FSIS/1.0; "
        "+https://advfood.tech/fsis-home)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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


def _strip_code_fences(s: str) -> str:
    """Strip ```json ... ``` or ``` ... ``` fences a model may wrap JSON in."""
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*\n", "", s)
        s = re.sub(r"\n```\s*$", "", s)
    return s.strip()


GEMINI_EXTRACTION_PROMPT = """\
You are extracting FOOD RECALL records from an agency's HTML listing page.

Return a JSON array (no prose, no markdown fences). Each item:

{
  "date": "YYYY-MM-DD",
  "company": "<recalling company or brand>",
  "product": "<product name / description>",
  "pathogen": "<contaminant — e.g. Salmonella, Listeria monocytogenes, E. coli O157:H7, Clostridium botulinum. Empty string if not pathogen-related>",
  "url": "<direct link to the recall DETAIL page; absolute URL>",
  "description": "<1–2 sentence summary>"
}

Rules:
- Include ONLY pathogen / microbiological / biotoxin / mycotoxin contamination.
- EXCLUDE: undeclared allergens, label errors, foreign material, packaging defects.
- If the page has NO recalls matching, return [].
- Dates must be ISO (YYYY-MM-DD).
- URLs MUST be absolute and point to the recall DETAIL page (not the listing page).
- Return ONLY the JSON array. No commentary, no markdown.
"""


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
            Date=_s(Date),
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
            raw = _call_gemini(prompt=prompt, html=html, language=self.LANGUAGE)
        except Exception as exc:  # noqa: BLE001 - review layer can try Claude fallback
            self.logger.warning("Gemini call failed: %s", exc)
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
