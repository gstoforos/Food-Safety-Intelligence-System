"""UVHVVR (SI) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class UVHVVRScraper(GenericLLMScraper):
    AGENCY = "UVHVVR (SI)"
    COUNTRY = "Slovenia"
    INDEX_URLS = ['https://www.gov.si/podrocja/podjetnistvo-in-gospodarstvo/varstvo-potrosnikov-in-konkurence/nevarni-in-neskladni-izdelki/']
    LANGUAGE = "sl"
