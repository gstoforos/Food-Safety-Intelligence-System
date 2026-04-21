"""NVWA (NL) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NVWAScraper(GenericGeminiScraper):
    AGENCY = "NVWA (NL)"
    COUNTRY = "Netherlands"
    INDEX_URLS = ['https://www.nvwa.nl/onderwerpen/waarschuwingen-voedsel']
    LANGUAGE = "nl"
