"""AESAN (ES) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class AESANScraper(GenericLLMScraper):
    AGENCY = "AESAN (ES)"
    COUNTRY = "Spain"
    INDEX_URLS = ['https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/ampliacion/listado_alertas_general.htm']
    LANGUAGE = "es"
