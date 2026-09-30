"""Ruokavirasto (FI) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class RuokavirastoScraper(GenericLLMScraper):
    AGENCY = "Ruokavirasto (FI)"
    COUNTRY = "Finland"
    INDEX_URLS = ['https://www.ruokavirasto.fi/henkiloasiakkaat/tietoa-elintarvikkeista/takaisinvedot/']
    LANGUAGE = "fi"
