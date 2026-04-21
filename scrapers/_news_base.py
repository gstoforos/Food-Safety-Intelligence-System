"""MoCCAE (AE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MOCCAEScraper(GenericGeminiScraper):
    AGENCY = "MoCCAE (AE)"
    COUNTRY = "UAE"
    INDEX_URLS = ['https://www.moccae.gov.ae/en/media-center/news.aspx']
    LANGUAGE = "en"
