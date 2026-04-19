"""AGES (AT) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class AGESScraper(GenericGeminiScraper):
    AGENCY = "AGES (AT)"
    COUNTRY = "Austria"
    INDEX_URLS = ['https://www.ages.at/mensch/produktwarnungen-produktrueckrufe']
    LANGUAGE = "de"
