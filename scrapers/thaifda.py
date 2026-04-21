"""FSSAI (IN) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FSSAIScraper(GenericGeminiScraper):
    AGENCY = "FSSAI (IN)"
    COUNTRY = "India"
    INDEX_URLS = ['https://www.fssai.gov.in/cms/recall.php']
    LANGUAGE = "en"
