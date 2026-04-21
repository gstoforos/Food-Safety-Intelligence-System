"""NFSA (EG) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NFSAScraper(GenericGeminiScraper):
    AGENCY = "NFSA (EG)"
    COUNTRY = "Egypt"
    INDEX_URLS = ['http://www.nfsa.gov.eg/ar/News/AdvisoriesAndAlerts']
    LANGUAGE = "ar"
