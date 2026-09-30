"""KKM (MY) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class KKMScraper(GenericLLMScraper):
    AGENCY = "KKM (MY)"
    COUNTRY = "Malaysia"
    INDEX_URLS = ['https://www.moh.gov.my/index.php/pages/view/4019']
    LANGUAGE = "ms"
