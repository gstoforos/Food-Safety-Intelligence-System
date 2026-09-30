"""COFEPRIS (MX) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class COFEPRISScraper(GenericLLMScraper):
    AGENCY = "COFEPRIS (MX)"
    COUNTRY = "Mexico"
    INDEX_URLS = ['https://www.gob.mx/cofepris/es/archivo/prensa']
    LANGUAGE = "es"
