"""BFSA (BG) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class BFSAScraper(GenericGeminiScraper):
    AGENCY = "BFSA (BG)"
    COUNTRY = "Bulgaria"
    INDEX_URLS = ['https://www.babh.government.bg/bg/Page/news/index/news']
    LANGUAGE = "bg"
