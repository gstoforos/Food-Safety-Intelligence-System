"""NFSA (EG) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class NFSAScraper(GenericLLMScraper):
    AGENCY = "NFSA (EG)"
    COUNTRY = "Egypt"
    INDEX_URLS = ['http://www.nfsa.gov.eg/ar/News/AdvisoriesAndAlerts']
    LANGUAGE = "ar"
