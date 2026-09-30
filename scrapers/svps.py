"""NVWA (NL) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class NVWAScraper(GenericLLMScraper):
    AGENCY = "NVWA (NL)"
    COUNTRY = "Netherlands"
    INDEX_URLS = ['https://www.nvwa.nl/onderwerpen/waarschuwingen-voedsel']
    LANGUAGE = "nl"
