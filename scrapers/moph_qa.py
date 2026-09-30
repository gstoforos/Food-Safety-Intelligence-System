"""ANVISA (BR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class ANVISAScraper(GenericLLMScraper):
    AGENCY = "ANVISA (BR)"
    COUNTRY = "Brazil"
    INDEX_URLS = ['https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa']
    LANGUAGE = "pt"
