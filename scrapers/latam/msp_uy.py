"""MSP (UY) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MSPScraper(GenericGeminiScraper):
    AGENCY = "MSP (UY)"
    COUNTRY = "Uruguay"
    INDEX_URLS = ['https://www.gub.uy/ministerio-salud-publica/comunicacion/noticias']
    LANGUAGE = "es"
