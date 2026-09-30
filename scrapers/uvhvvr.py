"""ANSVSA (RO) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ANSVSAScraper(GenericLLMScraper):
    AGENCY = "ANSVSA (RO)"
    COUNTRY = "Romania"
    INDEX_URLS = ['https://www.ansvsa.ro/categorie/comunicate/alerte-alimentare/']
    LANGUAGE = "ro"
