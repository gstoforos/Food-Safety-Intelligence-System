"""PVD (LV) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class PVDScraper(GenericGeminiScraper):
    AGENCY = "PVD (LV)"
    COUNTRY = "Latvia"
    INDEX_URLS = ['https://www.pvd.gov.lv/lv/aktualitates']
    LANGUAGE = "lv"
