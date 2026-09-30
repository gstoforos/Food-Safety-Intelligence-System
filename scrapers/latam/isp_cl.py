"""ISP (CL) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ISPScraper(GenericLLMScraper):
    AGENCY = "ISP (CL)"
    COUNTRY = "Chile"
    INDEX_URLS = ['https://www.minsal.cl/category/alertas-alimentarias/']
    LANGUAGE = "es"
