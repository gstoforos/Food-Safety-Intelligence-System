"""MAST (IS) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MASTScraper(GenericGeminiScraper):
    AGENCY = "MAST (IS)"
    COUNTRY = "Iceland"
    INDEX_URLS = ['https://www.mast.is/is/frettir/innkollun']
    LANGUAGE = "is"
