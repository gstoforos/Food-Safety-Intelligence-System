"""SFDA (SA) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SFDAScraper(GenericGeminiScraper):
    AGENCY = "SFDA (SA)"
    COUNTRY = "Saudi Arabia"
    INDEX_URLS = ['https://www.sfda.gov.sa/en/news-list']
    LANGUAGE = "en"
