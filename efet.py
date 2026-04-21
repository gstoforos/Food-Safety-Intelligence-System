"""AESAN (ES) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class AESANScraper(GenericGeminiScraper):
    AGENCY = "AESAN (ES)"
    COUNTRY = "Spain"
    INDEX_URLS = ['https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/listado/alertas.htm']
    LANGUAGE = "es"
