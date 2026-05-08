"""DIGESA (PE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class DIGESAScraper(GenericGeminiScraper):
    AGENCY = "DIGESA (PE)"
    COUNTRY = "Peru"
    INDEX_URLS = ['https://www.digesa.minsa.gob.pe/noticias/comunicados.asp']
    LANGUAGE = "es"
