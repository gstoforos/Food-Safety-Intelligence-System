"""INVIMA (CO) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class INVIMAScraper(GenericGeminiScraper):
    AGENCY = "INVIMA (CO)"
    COUNTRY = "Colombia"
    INDEX_URLS = ['https://www.invima.gov.co/sala-de-prensa']
    LANGUAGE = "es"
