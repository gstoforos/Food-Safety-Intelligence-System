"""TFDA (TW) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class TFDATWScraper(GenericLLMScraper):
    AGENCY = "TFDA (TW)"
    COUNTRY = "Taiwan"
    INDEX_URLS = ['https://www.fda.gov.tw/TC/news.aspx?cid=4']
    LANGUAGE = "zh"
