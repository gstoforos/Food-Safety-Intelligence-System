"""ANMAT (AR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ANMATScraper(GenericLLMScraper):
    AGENCY = "ANMAT (AR)"
    COUNTRY = "Argentina"
    INDEX_URLS = ['https://www.argentina.gob.ar/anmat/regulados/alimentos/alertas']
    LANGUAGE = "es"
