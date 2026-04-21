"""ONSSA (MA) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ONSSAScraper(GenericGeminiScraper):
    AGENCY = "ONSSA (MA)"
    COUNTRY = "Morocco"
    INDEX_URLS = ['http://www.onssa.gov.ma/index.php/fr/communiques-de-presse']
    LANGUAGE = "fr"
