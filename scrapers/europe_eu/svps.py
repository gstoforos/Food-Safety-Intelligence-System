"""ŠVPS (SK) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SVPSScraper(GenericGeminiScraper):
    AGENCY = "ŠVPS (SK)"
    COUNTRY = "Slovakia"
    INDEX_URLS = ['https://www.svps.sk/zakladne_info/upozornenia.asp']
    LANGUAGE = "sk"
