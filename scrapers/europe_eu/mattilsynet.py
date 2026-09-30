"""Mattilsynet (NO) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MattilsynetScraper(GenericLLMScraper):
    AGENCY = "Mattilsynet (NO)"
    COUNTRY = "Norway"
    INDEX_URLS = ['https://www.mattilsynet.no/tilbakekallinger']
    LANGUAGE = "no"
