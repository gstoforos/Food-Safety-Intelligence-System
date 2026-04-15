"""SZPI (CZ) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SZPIScraper(GenericGeminiScraper):
    AGENCY = "SZPI (CZ)"
    COUNTRY = "Czech Republic"
    INDEX_URLS = ['https://www.szpi.gov.cz/clanky-bezpecne-potraviny.aspx']
    LANGUAGE = "cs"
