"""GIS (PL) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class GISScraper(GenericLLMScraper):
    AGENCY = "GIS (PL)"
    COUNTRY = "Poland"
    INDEX_URLS = ['https://www.gov.pl/web/gis/ostrzezenia-publiczne-dotyczace-zywnosci']
    LANGUAGE = "pl"
