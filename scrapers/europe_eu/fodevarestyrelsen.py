"""Fødevarestyrelsen (DK) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FodevarestyrelsenScraper(GenericGeminiScraper):
    AGENCY = "Fødevarestyrelsen (DK)"
    COUNTRY = "Denmark"
    INDEX_URLS = ['https://www.foedevarestyrelsen.dk/kost-og-foedevarer/foedevaresikkerhed/tilbagetrukne-foedevarer']
    LANGUAGE = "da"
