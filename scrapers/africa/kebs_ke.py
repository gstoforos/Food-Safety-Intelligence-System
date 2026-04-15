"""KEBS (KE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class KEBSScraper(GenericGeminiScraper):
    AGENCY = "KEBS (KE)"
    COUNTRY = "Kenya"
    INDEX_URLS = ['https://www.kebs.org/index.php?option=com_content&view=category&id=29']
    LANGUAGE = "en"
