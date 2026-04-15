"""VFA (VN) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class VFAScraper(GenericGeminiScraper):
    AGENCY = "VFA (VN)"
    COUNTRY = "Vietnam"
    INDEX_URLS = ['https://vfa.gov.vn/canh-bao/']
    LANGUAGE = "vi"
