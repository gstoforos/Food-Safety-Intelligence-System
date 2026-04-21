"""HAH (HR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class HAHScraper(GenericGeminiScraper):
    AGENCY = "HAH (HR)"
    COUNTRY = "Croatia"
    INDEX_URLS = ['https://www.hah.hr/category/obavijesti/']
    LANGUAGE = "hr"
