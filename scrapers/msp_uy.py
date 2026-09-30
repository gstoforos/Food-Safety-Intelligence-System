"""DIGESA (PE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class DIGESAScraper(GenericLLMScraper):
    AGENCY = "DIGESA (PE)"
    COUNTRY = "Peru"
    INDEX_URLS = ['http://www.digesa.minsa.gob.pe/noticias.asp']
    LANGUAGE = "es"
