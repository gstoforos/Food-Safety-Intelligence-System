"""BPOM (ID) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class BPOMScraper(GenericGeminiScraper):
    AGENCY = "BPOM (ID)"
    COUNTRY = "Indonesia"
    INDEX_URLS = ['https://www.pom.go.id/siaran-pers']
    LANGUAGE = "id"
