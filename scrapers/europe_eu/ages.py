"""AGES (AT) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class AGESScraper(GenericLLMScraper):
    AGENCY = "AGES (AT)"
    COUNTRY = "Austria"
    INDEX_URLS = ['https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/']
    LANGUAGE = "de"
