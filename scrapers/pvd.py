"""ŠVPS (SK) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class SVPSScraper(GenericLLMScraper):
    AGENCY = "ŠVPS (SK)"
    COUNTRY = "Slovakia"
    INDEX_URLS = ['https://www.svps.sk/zakladne_info/upozornenia.asp']
    LANGUAGE = "sk"
