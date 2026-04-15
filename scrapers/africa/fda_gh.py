"""FDA (GH) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FDAGHScraper(GenericGeminiScraper):
    AGENCY = "FDA (GH)"
    COUNTRY = "Ghana"
    INDEX_URLS = ['https://fdaghana.gov.gh/index.php/news/recall-notices/']
    LANGUAGE = "en"
