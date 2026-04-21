"""TFDA (TW) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class TFDATWScraper(GenericGeminiScraper):
    AGENCY = "TFDA (TW)"
    COUNTRY = "Taiwan"
    INDEX_URLS = ['https://www.fda.gov.tw/TC/news.aspx?cid=4']
    LANGUAGE = "zh"
