"""BLV (CH) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class BLVScraper(GenericGeminiScraper):
    AGENCY = "BLV (CH)"
    COUNTRY = "Switzerland"
    INDEX_URLS = ['https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen.html']
    LANGUAGE = "de"
