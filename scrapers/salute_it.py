"""UVHVVR (SI) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class UVHVVRScraper(GenericLLMScraper):
    AGENCY = "UVHVVR (SI)"
    COUNTRY = "Slovenia"
    INDEX_URLS = ['https://www.gov.si/teme/odpoklici-in-opozorila-zivila/']
    LANGUAGE = "sl"
