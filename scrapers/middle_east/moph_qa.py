"""MoPH (QA) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MoPHQAScraper(GenericGeminiScraper):
    AGENCY = "MoPH (QA)"
    COUNTRY = "Qatar"
    INDEX_URLS = ['https://www.moph.gov.qa/english/Pages/news.aspx']
    LANGUAGE = "en"
