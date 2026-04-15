"""Ruokavirasto (FI) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class RuokavirastoScraper(GenericGeminiScraper):
    AGENCY = "Ruokavirasto (FI)"
    COUNTRY = "Finland"
    INDEX_URLS = ['https://www.ruokavirasto.fi/yritykset/elintarvikeala/elintarvikkeiden-takaisinvedot/']
    LANGUAGE = "fi"
