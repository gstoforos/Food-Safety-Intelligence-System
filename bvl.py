"""VMVT (LT) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class VMVTScraper(GenericGeminiScraper):
    AGENCY = "VMVT (LT)"
    COUNTRY = "Lithuania"
    INDEX_URLS = ['https://vmvt.lt/maisto-sauga/aktualijos']
    LANGUAGE = "lt"
