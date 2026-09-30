"""SFA (SG) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class SFAScraper(GenericLLMScraper):
    AGENCY = "SFA (SG)"
    COUNTRY = "Singapore"
    INDEX_URLS = ['https://www.sfa.gov.sg/food-information/recall-faq']
    LANGUAGE = "en"
