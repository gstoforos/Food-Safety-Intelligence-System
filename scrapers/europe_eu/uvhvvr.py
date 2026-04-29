"""UVHVVR (SI) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class UVHVVRScraper(GenericGeminiScraper):
    AGENCY = "UVHVVR (SI)"
    COUNTRY = "Slovenia"
    INDEX_URLS = ['https://www.gov.si/podrocja/podjetnistvo-in-gospodarstvo/varstvo-potrosnikov-in-konkurence/nevarni-in-neskladni-izdelki/']
    LANGUAGE = "sl"
