"""KEBS (KE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class KEBSScraper(GenericLLMScraper):
    AGENCY = "KEBS (KE)"
    COUNTRY = "Kenya"
    INDEX_URLS = ['https://www.kebs.org/index.php?option=com_content&view=category&id=29']
    LANGUAGE = "en"
