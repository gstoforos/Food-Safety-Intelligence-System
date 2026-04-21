"""SFA (SG) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SFAScraper(GenericGeminiScraper):
    AGENCY = "SFA (SG)"
    COUNTRY = "Singapore"
    INDEX_URLS = ['https://www.sfa.gov.sg/food-information/recall-faq']
    LANGUAGE = "en"
