"""DIGESA (PE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class DIGESAScraper(GenericLLMScraper):
    AGENCY = "DIGESA (PE)"
    COUNTRY = "Peru"
    INDEX_URLS = ['https://www.digesa.minsa.gob.pe/noticias/comunicados.asp']
    LANGUAGE = "es"
