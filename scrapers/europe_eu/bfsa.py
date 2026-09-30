"""BFSA (BG) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class BFSAScraper(GenericLLMScraper):
    AGENCY = "BFSA (BG)"
    COUNTRY = "Bulgaria"
    INDEX_URLS = ['https://www.babh.government.bg/bg/Page/news/index/news']
    LANGUAGE = "bg"
