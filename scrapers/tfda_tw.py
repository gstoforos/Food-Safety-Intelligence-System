"""Thai FDA food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ThaiFDAScraper(GenericLLMScraper):
    AGENCY = "Thai FDA"
    COUNTRY = "Thailand"
    INDEX_URLS = ['https://www.fda.moph.go.th/sites/food/SitePages/news.aspx']
    LANGUAGE = "th"
