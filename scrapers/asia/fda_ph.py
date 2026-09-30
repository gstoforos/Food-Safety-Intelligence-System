"""FDA (PH) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class FDAPHScraper(GenericLLMScraper):
    AGENCY = "FDA (PH)"
    COUNTRY = "Philippines"
    INDEX_URLS = ['https://www.fda.gov.ph/advisories/']
    LANGUAGE = "en"
