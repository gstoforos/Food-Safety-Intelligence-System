"""GIS (PL) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class GISScraper(GenericGeminiScraper):
    AGENCY = "GIS (PL)"
    COUNTRY = "Poland"
    INDEX_URLS = ['https://www.gov.pl/web/gis/ostrzezenia-publiczne-dotyczace-zywnosci']
    LANGUAGE = "pl"
