"""EFET (GR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class EFETScraper(GenericLLMScraper):
    AGENCY = "EFET (GR)"
    COUNTRY = "Greece"
    INDEX_URLS = ['https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat']
    LANGUAGE = "el"
