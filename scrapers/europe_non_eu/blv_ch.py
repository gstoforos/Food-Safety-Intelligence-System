"""BLV (CH) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class BLVScraper(GenericLLMScraper):
    AGENCY = "BLV (CH)"
    COUNTRY = "Switzerland"
    INDEX_URLS = ['https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen.html']
    LANGUAGE = "de"
