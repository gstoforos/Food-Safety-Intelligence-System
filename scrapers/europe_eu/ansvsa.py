"""ANSVSA (RO) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ANSVSAScraper(GenericGeminiScraper):
    AGENCY = "ANSVSA (RO)"
    COUNTRY = "Romania"
    INDEX_URLS = ['https://www.ansvsa.ro/categorie/comunicate/alerte-alimentare/']
    LANGUAGE = "ro"
