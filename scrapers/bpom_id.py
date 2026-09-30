"""CFS (HK) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class CFSHKScraper(GenericLLMScraper):
    AGENCY = "CFS (HK)"
    COUNTRY = "Hong Kong"
    INDEX_URLS = ['https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html']
    LANGUAGE = "en"
