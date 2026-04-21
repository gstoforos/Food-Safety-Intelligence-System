"""FDA (PH) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FDAPHScraper(GenericGeminiScraper):
    AGENCY = "FDA (PH)"
    COUNTRY = "Philippines"
    INDEX_URLS = ['https://www.fda.gov.ph/food-advisories/']
    LANGUAGE = "en"
