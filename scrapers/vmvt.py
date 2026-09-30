"""ASAE (PT) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ASAEScraper(GenericLLMScraper):
    AGENCY = "ASAE (PT)"
    COUNTRY = "Portugal"
    INDEX_URLS = ['https://www.asae.gov.pt/seguranca-alimentar/alertas-alimentares.aspx']
    LANGUAGE = "pt"
