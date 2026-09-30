"""FDA (GH) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class FDAGHScraper(GenericLLMScraper):
    AGENCY = "FDA (GH)"
    COUNTRY = "Ghana"
    INDEX_URLS = ['https://fdaghana.gov.gh/category/product-recalls-alerts/']
    LANGUAGE = "en"
