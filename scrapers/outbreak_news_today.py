"""TGTHB (TR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class TGTHBScraper(GenericLLMScraper):
    AGENCY = "TGTHB (TR)"
    COUNTRY = "Turkey"
    INDEX_URLS = ['https://www.tarimorman.gov.tr/Duyuru']
    LANGUAGE = "tr"
