"""NAFDAC (NG) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NAFDACScraper(GenericGeminiScraper):
    AGENCY = "NAFDAC (NG)"
    COUNTRY = "Nigeria"
    INDEX_URLS = ['https://nafdac.gov.ng/category/recalls-and-alerts/']
    LANGUAGE = "en"
