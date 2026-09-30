"""HAH (HR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class HAHScraper(GenericLLMScraper):
    AGENCY = "HAH (HR)"
    COUNTRY = "Croatia"
    INDEX_URLS = ['https://www.hapih.hr/kategorija/obavijesti-za-potrosace/']
    LANGUAGE = "hr"
