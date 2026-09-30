"""NAFDAC (NG) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class NAFDACScraper(GenericLLMScraper):
    AGENCY = "NAFDAC (NG)"
    COUNTRY = "Nigeria"
    INDEX_URLS = ['https://www.nafdac.gov.ng/category/news/recalls/']
    LANGUAGE = "en"
