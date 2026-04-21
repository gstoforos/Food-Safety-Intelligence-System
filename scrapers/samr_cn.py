"""KKM (MY) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class KKMScraper(GenericGeminiScraper):
    AGENCY = "KKM (MY)"
    COUNTRY = "Malaysia"
    INDEX_URLS = ['https://www.moh.gov.my/index.php/pages/view/4019']
    LANGUAGE = "ms"
