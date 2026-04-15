"""NCC (ZA) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NCCScraper(GenericGeminiScraper):
    AGENCY = "NCC (ZA)"
    COUNTRY = "South Africa"
    INDEX_URLS = ['https://thencc.org.za/category/product-recalls/']
    LANGUAGE = "en"
