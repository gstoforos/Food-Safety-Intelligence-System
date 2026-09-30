"""Nébih (HU) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class NebihScraper(GenericLLMScraper):
    AGENCY = "Nébih (HU)"
    COUNTRY = "Hungary"
    INDEX_URLS = ['https://portal.nebih.gov.hu/termekvisszahivas']
    LANGUAGE = "hu"
