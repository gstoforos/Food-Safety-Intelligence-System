"""MoCCAE (AE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MOCCAEScraper(GenericLLMScraper):
    AGENCY = "MoCCAE (AE)"
    COUNTRY = "UAE"
    INDEX_URLS = ['https://www.moccae.gov.ae/en/media-center/news.aspx']
    LANGUAGE = "en"
