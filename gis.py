"""FSAI (IE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FSAIScraper(GenericGeminiScraper):
    AGENCY = "FSAI (IE)"
    COUNTRY = "Ireland"
    INDEX_URLS = ['https://www.fsai.ie/news-and-alerts/food-alerts']
    LANGUAGE = "en"
