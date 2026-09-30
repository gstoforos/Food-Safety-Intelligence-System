"""MoH (IL) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MoHILScraper(GenericLLMScraper):
    AGENCY = "MoH (IL)"
    COUNTRY = "Israel"
    INDEX_URLS = ['https://www.gov.il/he/departments/ministry_of_health/govil-landing-page']
    LANGUAGE = "he"
