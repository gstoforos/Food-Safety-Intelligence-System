"""VMVT (LT) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class VMVTScraper(GenericLLMScraper):
    AGENCY = "VMVT (LT)"
    COUNTRY = "Lithuania"
    INDEX_URLS = ['https://vmvt.lt/maisto-sauga/aktualijos']
    LANGUAGE = "lt"
