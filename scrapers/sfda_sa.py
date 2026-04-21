"""ANMAT (AR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ANMATScraper(GenericGeminiScraper):
    AGENCY = "ANMAT (AR)"
    COUNTRY = "Argentina"
    INDEX_URLS = ['https://www.argentina.gob.ar/anmat/regulados/alimentos/alertas']
    LANGUAGE = "es"
