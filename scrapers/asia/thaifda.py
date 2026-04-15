"""Thai FDA food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ThaiFDAScraper(GenericGeminiScraper):
    AGENCY = "Thai FDA"
    COUNTRY = "Thailand"
    INDEX_URLS = ['https://www.fda.moph.go.th/sites/food/SitePages/news.aspx']
    LANGUAGE = "th"
