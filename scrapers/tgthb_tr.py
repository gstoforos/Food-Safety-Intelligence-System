"""MSP (UY) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MSPScraper(GenericLLMScraper):
    AGENCY = "MSP (UY)"
    COUNTRY = "Uruguay"
    INDEX_URLS = ['https://www.gub.uy/ministerio-salud-publica/comunicacion/noticias']
    LANGUAGE = "es"
