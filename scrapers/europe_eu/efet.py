"""EFET (GR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class EFETScraper(GenericGeminiScraper):
    AGENCY = "EFET (GR)"
    COUNTRY = "Greece"
    INDEX_URLS = ['https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat']
    LANGUAGE = "el"
