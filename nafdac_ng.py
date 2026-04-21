"""ISP (CL) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ISPScraper(GenericGeminiScraper):
    AGENCY = "ISP (CL)"
    COUNTRY = "Chile"
    INDEX_URLS = ['https://www.minsal.cl/category/alertas-alimentarias/']
    LANGUAGE = "es"
