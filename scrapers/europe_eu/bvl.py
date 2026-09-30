"""BVL (DE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class BVLScraper(GenericLLMScraper):
    AGENCY = "BVL (DE)"
    COUNTRY = "Germany"
    INDEX_URLS = ['https://www.lebensmittelwarnung.de/', 'https://www.produktwarnung.eu/rubrik/lebensmittel']
    LANGUAGE = "de"
