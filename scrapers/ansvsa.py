"""AFSCA (BE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class AFSCAScraper(GenericLLMScraper):
    AGENCY = "AFSCA (BE)"
    COUNTRY = "Belgium"
    INDEX_URLS = ['https://www.favv-afsca.be/professionnels/publications/communiques/rappel/']
    LANGUAGE = "fr"
