"""CFS (HK) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class CFSHKScraper(GenericGeminiScraper):
    AGENCY = "CFS (HK)"
    COUNTRY = "Hong Kong"
    INDEX_URLS = ['https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html']
    LANGUAGE = "en"
