"""Fødevarestyrelsen (DK) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class FodevarestyrelsenScraper(GenericLLMScraper):
    AGENCY = "Fødevarestyrelsen (DK)"
    COUNTRY = "Denmark"
    INDEX_URLS = ['https://www.foedevarestyrelsen.dk/kost-og-foedevarer/foedevaresikkerhed/tilbagetrukne-foedevarer']
    LANGUAGE = "da"
