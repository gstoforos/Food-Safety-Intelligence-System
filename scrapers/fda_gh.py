"""COMESA food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class COMESAScraper(GenericLLMScraper):
    AGENCY = "COMESA"
    COUNTRY = "Kenya"
    INDEX_URLS = ['https://www.comesa.int/category/news/']
    LANGUAGE = "en"
