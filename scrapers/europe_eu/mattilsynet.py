"""Mattilsynet (NO) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MattilsynetScraper(GenericGeminiScraper):
    AGENCY = "Mattilsynet (NO)"
    COUNTRY = "Norway"
    INDEX_URLS = ['https://www.mattilsynet.no/tilbakekallinger']
    LANGUAGE = "no"
