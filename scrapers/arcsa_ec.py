"""ARCSA (EC) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ARCSAScraper(GenericLLMScraper):
    AGENCY = "ARCSA (EC)"
    COUNTRY = "Ecuador"
    INDEX_URLS = ['https://www.controlsanitario.gob.ec/category/noticias/']
    LANGUAGE = "es"
