"""ANVISA (BR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ANVISAScraper(GenericGeminiScraper):
    AGENCY = "ANVISA (BR)"
    COUNTRY = "Brazil"
    INDEX_URLS = ['https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa']
    LANGUAGE = "pt"
