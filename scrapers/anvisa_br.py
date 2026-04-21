"""COFEPRIS (MX) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class COFEPRISScraper(GenericGeminiScraper):
    AGENCY = "COFEPRIS (MX)"
    COUNTRY = "Mexico"
    INDEX_URLS = ['https://www.gob.mx/cofepris/es/archivo/prensa']
    LANGUAGE = "es"
