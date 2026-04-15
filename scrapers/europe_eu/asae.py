"""ASAE (PT) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ASAEScraper(GenericGeminiScraper):
    AGENCY = "ASAE (PT)"
    COUNTRY = "Portugal"
    INDEX_URLS = ['https://www.asae.gov.pt/seguranca-alimentar/alertas-alimentares.aspx']
    LANGUAGE = "pt"
