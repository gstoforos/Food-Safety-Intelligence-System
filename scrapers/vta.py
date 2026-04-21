"""VTA (EE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class VTAScraper(GenericGeminiScraper):
    AGENCY = "VTA (EE)"
    COUNTRY = "Estonia"
    INDEX_URLS = ['https://pta.agri.ee/uudised']
    LANGUAGE = "et"
