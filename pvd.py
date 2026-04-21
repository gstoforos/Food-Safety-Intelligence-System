"""AFSCA (BE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class AFSCAScraper(GenericGeminiScraper):
    AGENCY = "AFSCA (BE)"
    COUNTRY = "Belgium"
    INDEX_URLS = ['https://www.favv-afsca.be/professionnels/publications/communiques/rappel/']
    LANGUAGE = "fr"
