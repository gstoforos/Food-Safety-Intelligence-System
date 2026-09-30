"""PVD (LV) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class PVDScraper(GenericLLMScraper):
    AGENCY = "PVD (LV)"
    COUNTRY = "Latvia"
    INDEX_URLS = ['https://www.pvd.gov.lv/lv/aktualitates']
    LANGUAGE = "lv"
