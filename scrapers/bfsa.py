"""VTA (EE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class VTAScraper(GenericLLMScraper):
    AGENCY = "VTA (EE)"
    COUNTRY = "Estonia"
    INDEX_URLS = ['https://pta.agri.ee/uudised']
    LANGUAGE = "et"
