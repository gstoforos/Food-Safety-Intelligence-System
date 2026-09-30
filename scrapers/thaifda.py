"""FSSAI (IN) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class FSSAIScraper(GenericLLMScraper):
    AGENCY = "FSSAI (IN)"
    COUNTRY = "India"
    INDEX_URLS = ['https://www.fssai.gov.in/cms/recall.php']
    LANGUAGE = "en"
