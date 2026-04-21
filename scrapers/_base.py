"""
scrapers/_base.py
=================
AFTS FSIS — Base scraper classes and shared HTTP utilities.

Exports
-------
    BaseScraper           Abstract base for all regional agency scrapers.
    GenericGeminiScraper  Default scraper that uses Gemini 2.0 Flash for HTML extraction.
    make_session          Build a configured requests.Session with retries.
    fetch                 Single-URL fetcher with timeout + retry.

Consumers
---------
    pipeline.run_all                     imports BaseScraper, make_session
    scrapers.<region>.<agency>           every regional scraper subclasses GenericGeminiScraper
    review.claude_client                 uses extract_rows() signature for HTML fallback
    pipeline.fsis_url_guardian           uses fetch() + make_session()
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from datetime import datetime, timezone
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
        "+https://advfood.tech/food-safety-intelligence)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def make_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Sequence[int] = (429, 500, 502, 503, 504),
    headers: Optional[Dict[str, str]] = None,
) -> requests.Session:
    """Build a requests.Session with retry/backoff and the AFTS user-agent."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    if headers:
        session.headers.update(headers)

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
    url: str,
    session: Optional[requests.Session] = None,
    method: str = "GET",
    timeout: int = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> Optional[requests.Response]:
    """Single-URL fetch with shared retry policy. Returns the Response or None on failure."""
    s = session or make_session()
    try:
        return s.request(method, url, timeout=timeout, **kwargs)
    except requests.RequestException as exc:
        log.warning("fetch failed for %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Gemini 2.0 Flash helper (used by GenericGeminiScraper)
# ---------------------------------------------------------------------------

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_MAX_HTML_CHARS = 120_000  # truncate very large pages to stay inside token budget


def _gemini_api_keys() -> List[str]:
    """Collect all GEMINI_API_KEY_{1..5} env vars (plus legacy GEMINI_API_KEY)."""
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
    """Call Gemini 2.0 Flash with key rotation. Returns the raw text response."""
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "google-generativeai is not installed. Add it to requirements.txt."
        ) from exc

    keys = _gemini_api_keys()
    if not keys:
        raise RuntimeError(
            "No GEMINI_API_KEY(_1..5) env var set. Configure in GitHub Actions secrets."
        )

    if len(html) > GEMINI_MAX_HTML_CHARS:
        html = html[:GEMINI_MAX_HTML_CHARS] + "\n<!-- truncated -->"

    last_error: Optional[Exception] = None
    # randomise key order so load spreads over time
    for api_key in random.sample(keys, k=len(keys)):
        try:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(GEMINI_MODEL)
            full_prompt = f"{prompt}\n\nLANGUAGE OF PAGE: {language}\n\nHTML:\n{html}"
            resp = model.generate_content(full_prompt)
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


# ---------------------------------------------------------------------------
# Extraction prompt for GenericGeminiScraper
# ---------------------------------------------------------------------------

GEMINI_EXTRACTION_PROMPT = """\
You are extracting FOOD RECALL records from an agency's HTML page.

Return a JSON array (no prose, no markdown fences). Each item:

{
  "date": "YYYY-MM-DD",
  "company": "<recalling company or brand>",
  "product": "<product name / description>",
  "pathogen": "<contaminant - e.g. Salmonella, Listeria monocytogenes, E. coli O157:H7, Clostridium botulinum. Empty string if not pathogen-related>",
  "url": "<direct link to the recall detail page; absolute URL>",
  "description": "<1-2 sentence summary>"
}

Rules:
- Include ONLY pathogen / microbiological / biotoxin / mycotoxin contamination.
- EXCLUDE: undeclared allergens, label errors, foreign material, packaging defects.
- If the page has NO recalls matching, return [].
- Dates must be ISO (YYYY-MM-DD).
- URLs MUST be absolute and point to the specific recall DETAIL page (not the listing page).
- Return ONLY the JSON array. No commentary, no markdown.
"""


# ---------------------------------------------------------------------------
# BaseScraper
# ---------------------------------------------------------------------------

class BaseScraper:
    """
    Abstract base for regional agency scrapers.

    Subclasses MUST set as class attributes:
        AGENCY       Human-readable agency name (e.g. "FDA", "RASFF").
        COUNTRY      Country name as it should appear in the Recalls sheet.
        INDEX_URLS   Iterable of listing URLs to scrape.
        LANGUAGE     Two-letter language code of the agency's pages (default "en").

    Subclasses MAY override:
        extract_rows(html, source_url) -> List[Recall]
    """

    AGENCY: str = ""
    COUNTRY: str = ""
    INDEX_URLS: Sequence[str] = ()
    LANGUAGE: str = "en"

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        if not self.AGENCY:
            raise ValueError(f"{type(self).__name__}.AGENCY is not set")
        if not self.COUNTRY:
            raise ValueError(f"{type(self).__name__}.COUNTRY is not set")
        if not self.INDEX_URLS:
            raise ValueError(f"{type(self).__name__}.INDEX_URLS is empty")

        self.session = session or make_session()
        slug = re.sub(r"\W+", "_", self.AGENCY.lower()).strip("_") or "unknown"
        self.logger = logging.getLogger(f"scraper.{slug}")

    # --------------------------------------------------------------- API
    def run(self) -> List[Recall]:
        """Iterate INDEX_URLS, fetch each, extract rows, return combined Recall list."""
        all_rows: List[Recall] = []
        for url in self.INDEX_URLS:
            self.logger.info("fetching %s", url)
            resp = fetch(url, session=self.session)
            if resp is None or not resp.ok:
                self.logger.warning(
                    "skip %s (status=%s)",
                    url,
                    getattr(resp, "status_code", "no-response"),
                )
                continue
            try:
                rows = self.extract_rows(resp.text, url)
            except Exception as exc:  # noqa: BLE001 - one bad page must not kill the run
                self.logger.exception("extract_rows failed for %s: %s", url, exc)
                rows = []
            self.logger.info("extracted %d rows from %s", len(rows), url)
            all_rows.extend(rows)
        return all_rows

    def extract_rows(self, html: str, source_url: str) -> List[Recall]:
        raise NotImplementedError(
            f"{type(self).__name__} must implement extract_rows() "
            f"or subclass GenericGeminiScraper"
        )

    # ---------------------------------------------------------- utilities
    def _build_recall(
        self,
        date: str,
        company: str,
        product: str,
        pathogen: str,
        url: str,
        description: str = "",
        source_url: str = "",
    ) -> Optional[Recall]:
        """
        Normalize raw extracted fields into a Recall.
        Returns None if the row must be dropped (no URL, not a pathogen recall).
        """
        url = (url or "").strip()
        if not url:
            return None

        canonical_pathogen = normalize_pathogen(pathogen or "")
        if not canonical_pathogen:
            return None  # not a pathogen recall — drop per FSIS whitelist rule

        country = normalize_country(self.COUNTRY)
        region = infer_region(country)
        tier = assign_tier(canonical_pathogen)

        return Recall(
            date=(date or "").strip(),
            agency=self.AGENCY,
            country=country,
            region=region,
            company=(company or "").strip(),
            product=(product or "").strip(),
            pathogen=canonical_pathogen,
            tier=tier,
            url=url,
            description=(description or "").strip(),
            source_url=source_url or "",
            scraped_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )


# ---------------------------------------------------------------------------
# GenericGeminiScraper
# ---------------------------------------------------------------------------

class GenericGeminiScraper(BaseScraper):
    """
    Default scraper: uses Gemini 2.0 Flash to extract structured rows from HTML.

    Most regional scrapers only need to set AGENCY, COUNTRY, INDEX_URLS, LANGUAGE
    as class attributes and inherit this class unchanged.
    """

    EXTRA_PROMPT: str = ""  # subclasses may append agency-specific guidance

    def extract_rows(self, html: str, source_url: str) -> List[Recall]:
        prompt = GEMINI_EXTRACTION_PROMPT
        if self.EXTRA_PROMPT:
            prompt = f"{prompt}\n\nADDITIONAL AGENCY CONTEXT:\n{self.EXTRA_PROMPT}"
        prompt = (
            f"{prompt}\n\n"
            f"AGENCY: {self.AGENCY}\n"
            f"COUNTRY: {self.COUNTRY}\n"
            f"LISTING URL: {source_url}\n"
        )

        try:
            raw = _call_gemini(prompt=prompt, html=html, language=self.LANGUAGE)
        except Exception as exc:  # noqa: BLE001 - caller can fall back to Claude
            self.logger.warning("Gemini extraction failed: %s", exc)
            return []

        if not raw:
            return []

        cleaned = _strip_code_fences(raw)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            self.logger.warning("Gemini returned non-JSON response: %s", exc)
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
            raw_url = (item.get("url") or "").strip()
            if raw_url and not raw_url.startswith(("http://", "https://")):
                raw_url = urljoin(source_url, raw_url)
            rec = self._build_recall(
                date=str(item.get("date", "")),
                company=str(item.get("company", "")),
                product=str(item.get("product", "")),
                pathogen=str(item.get("pathogen", "")),
                url=raw_url,
                description=str(item.get("description", "")),
                source_url=source_url,
            )
            if rec is not None:
                rows.append(rec)
        return rows


__all__ = [
    "BaseScraper",
    "GenericGeminiScraper",
    "make_session",
    "fetch",
]
