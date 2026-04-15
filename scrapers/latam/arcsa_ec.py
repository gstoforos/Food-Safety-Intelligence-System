"""ARCSA (EC) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ARCSAScraper(GenericGeminiScraper):
    AGENCY = "ARCSA (EC)"
    COUNTRY = "Ecuador"
    INDEX_URLS = ['https://www.controlsanitario.gob.ec/category/noticias/']
    LANGUAGE = "es"
