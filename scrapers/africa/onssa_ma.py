"""ONSSA (MA) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ONSSAScraper(GenericLLMScraper):
    AGENCY = "ONSSA (MA)"
    COUNTRY = "Morocco"
    INDEX_URLS = ['http://www.onssa.gov.ma/index.php/fr/communiques-de-presse']
    LANGUAGE = "fr"
