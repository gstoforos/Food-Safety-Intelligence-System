"""COMESA food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class COMESAScraper(GenericGeminiScraper):
    AGENCY = "COMESA"
    COUNTRY = "Kenya"
    INDEX_URLS = ['https://www.comesa.int/category/news/']
    LANGUAGE = "en"
