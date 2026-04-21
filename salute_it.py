"""Nébih (HU) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NebihScraper(GenericGeminiScraper):
    AGENCY = "Nébih (HU)"
    COUNTRY = "Hungary"
    INDEX_URLS = ['https://portal.nebih.gov.hu/elelmiszerlanc-felugyelet/elelmiszerek-visszahivasa']
    LANGUAGE = "hu"
