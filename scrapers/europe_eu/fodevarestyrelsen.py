"""Fødevarestyrelsen (DK) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class FodevarestyrelsenScraper(GenericLLMScraper):
    AGENCY = "Fødevarestyrelsen (DK)"
    COUNTRY = "Denmark"
    INDEX_URLS = ['https://foedevarestyrelsen.dk/kost-og-foedevarer/foedevaresikkerhed/foedevareberedskab/soeg-i-tilbagekaldte-foedevarer']
    LANGUAGE = "da"
