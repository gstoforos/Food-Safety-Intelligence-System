"""UVHVVR (SI) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class UVHVVRScraper(GenericGeminiScraper):
    AGENCY = "UVHVVR (SI)"
    COUNTRY = "Slovenia"
    INDEX_URLS = ['https://www.gov.si/teme/odpoklici-in-opozorila-zivila/']
    LANGUAGE = "sl"
