"""BVL (DE) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class BVLScraper(GenericGeminiScraper):
    AGENCY = "BVL (DE)"
    COUNTRY = "Germany"
    INDEX_URLS = ['https://www.lebensmittelwarnung.de/bvl-lmw-de/liste/lebensmittel/bundesweit', 'https://www.produktwarnung.eu/rubrik/lebensmittel']
    LANGUAGE = "de"
