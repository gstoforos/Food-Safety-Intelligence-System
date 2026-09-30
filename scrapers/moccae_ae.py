"""INVIMA (CO) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class INVIMAScraper(GenericLLMScraper):
    AGENCY = "INVIMA (CO)"
    COUNTRY = "Colombia"
    INDEX_URLS = ['https://www.invima.gov.co/sala-de-prensa']
    LANGUAGE = "es"
