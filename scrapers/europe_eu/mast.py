"""MAST (IS) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MASTScraper(GenericLLMScraper):
    AGENCY = "MAST (IS)"
    COUNTRY = "Iceland"
    INDEX_URLS = ['https://www.mast.is/is/neytendur/innkallanir']
    LANGUAGE = "is"
