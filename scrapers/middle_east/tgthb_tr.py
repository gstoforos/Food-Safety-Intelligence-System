"""TGTHB (TR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class TGTHBScraper(GenericGeminiScraper):
    AGENCY = "TGTHB (TR)"
    COUNTRY = "Turkey"
    INDEX_URLS = ['https://www.tarimorman.gov.tr/Duyuru']
    LANGUAGE = "tr"
