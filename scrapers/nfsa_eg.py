"""NCC (ZA) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class NCCScraper(GenericLLMScraper):
    AGENCY = "NCC (ZA)"
    COUNTRY = "South Africa"
    INDEX_URLS = ['https://thencc.org.za/category/product-recalls/']
    LANGUAGE = "en"
