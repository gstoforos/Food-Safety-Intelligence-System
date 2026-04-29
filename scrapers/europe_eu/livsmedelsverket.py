"""Livsmedelsverket (SE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class LivsmedelsverketScraper(GenericGeminiScraper):
    AGENCY = "Livsmedelsverket (SE)"
    COUNTRY = "Sweden"
    INDEX_URLS = ['https://www.livsmedelsverket.se/om-oss/press/aterkallanden']
    LANGUAGE = "sv"
