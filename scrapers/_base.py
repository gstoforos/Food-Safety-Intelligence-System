"""
Shared base for all scrapers:
- Robust HTTP client with retry, timeout, rate limiting
- BaseScraper abstract class
- GenericGeminiScraper: fetches a URL and uses Gemini to extract structured Recall rows
  (used by ~47 of 57 scrapers that lack clean APIs)
"""
from __future__ import annotations
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ._models import Recall, infer_region, parse_date, normalize_pathogen, normalize_country

log = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "FSIS-Bot/1.0 (Food Safety Intelligence System; Advanced Food-Tech Solutions; "
        "info@advfood.tech) Mozilla/5.0"
    ),
    "Accept-Language": "en,fr;q=0.8,de;q=0.7,es;q=0.6,it;q=0.5,el;q=0.4",
}


def make_session(timeout: int = 30) -> requests.Session:
    """Build requests session with retry + timeout."""
    s = requests.Session()
    retry = Retry(
        total=4, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(DEFAULT_HEADERS)
    s.request_timeout = timeout  # custom attribute, used in fetch()
    return s


def fetch(session: requests.Session, url: str, **kwargs) -> Optional[requests.Response]:
    """GET with timeout + retry. Returns None on failure (logged)."""
    try:
        timeout = kwargs.pop("timeout", getattr(session, "request_timeout", 30))
        r = session.get(url, timeout=timeout, **kwargs)
        if r.status_code >= 400:
            log.warning("fetch %s -> %d", url, r.status_code)
            return None
        return r
    except Exception as e:
        log.warning("fetch %s failed: %s", url, e)
        return None


class BaseScraper(ABC):
    """Abstract scraper. Subclasses implement scrape() returning List[Recall]."""

    AGENCY: str = ""             # e.g. "FDA", "RappelConso (FR)"
    COUNTRY: str = ""            # e.g. "USA", "France"
    BASE_URL: str = ""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or make_session()

    @abstractmethod
    def scrape(self, since_days: int = 30) -> List[Recall]:
        """Return new recalls from the last `since_days` days. Pathogens-only filter applied."""
        ...

    def _new_recall(self, **kwargs) -> Recall:
        """Helper: create a Recall with Source/Country/Region pre-filled."""
        kwargs.setdefault("Source", self.AGENCY)
        kwargs.setdefault("Country", self.COUNTRY)
        r = Recall(**kwargs)
        if not r.Region and r.Country:
            r.Region = infer_region(r.Country)
        return r.normalize()


class GenericGeminiScraper(BaseScraper):
    """
    Default scraper for sources without clean APIs. Workflow:
      1. Fetch one or more index/listing URLs
      2. Send raw HTML (truncated) to Gemini with structured-extraction prompt
      3. Parse JSON response into Recall objects
      4. Filter to pathogen-related recalls only

    Subclass and set: AGENCY, COUNTRY, BASE_URL, INDEX_URLS, LANGUAGE
    """
    INDEX_URLS: List[str] = []      # listing pages to scrape
    LANGUAGE: str = "en"            # hint to Gemini for prompt

    # Optional: subclass can override to provide a cleaner extraction prompt
    EXTRACTION_HINTS: str = ""

    def scrape(self, since_days: int = 30) -> List[Recall]:
        from enrichment.gemini_client import extract_recalls_from_html

        out: List[Recall] = []
        seen_urls = set()
        for url in self.INDEX_URLS:
            r = fetch(self.session, url)
            if not r:
                continue
            html = r.text[:120_000]  # cap to ~30k tokens
            try:
                rows = extract_recalls_from_html(
                    html=html,
                    source_url=url,
                    agency=self.AGENCY,
                    country=self.COUNTRY,
                    language=self.LANGUAGE,
                    extra_hints=self.EXTRACTION_HINTS,
                    since_days=since_days,
                )
            except Exception as e:
                log.warning("Gemini extract failed for %s: %s", url, e)
                continue
            for row in rows:
                u = (row.get("URL") or url).strip()
                if u in seen_urls:
                    continue
                seen_urls.add(u)
                rec = self._new_recall(
                    Date=row.get("Date", ""),
                    Company=row.get("Company", ""),
                    Brand=row.get("Brand", ""),
                    Product=row.get("Product", ""),
                    Pathogen=row.get("Pathogen", ""),
                    Reason=row.get("Reason", ""),
                    Class=row.get("Class", ""),
                    URL=u,
                    Outbreak=int(row.get("Outbreak", 0) or 0),
                    Notes=row.get("Notes", ""),
                )
                # Only keep pathogen-related recalls
                if rec.Pathogen and rec.Pathogen.strip() and rec.Pathogen != "—":
                    out.append(rec)
            time.sleep(1.0)  # politeness between index fetches
        return out
