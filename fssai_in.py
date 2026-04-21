"""SAMR (CN) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SAMRScraper(GenericGeminiScraper):
    AGENCY = "SAMR (CN)"
    COUNTRY = "China"
    INDEX_URLS = ['https://www.samr.gov.cn/spcjs/tzgg/']
    LANGUAGE = "zh"
