"""SFDA (SA) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class SFDAScraper(GenericLLMScraper):
    AGENCY = "SFDA (SA)"
    COUNTRY = "Saudi Arabia"
    INDEX_URLS = ['https://www.sfda.gov.sa/en/news-list']
    LANGUAGE = "en"
