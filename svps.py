"""Min. Salute (IT) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class SaluteITScraper(GenericGeminiScraper):
    AGENCY = "Min. Salute (IT)"
    COUNTRY = "Italy"
    INDEX_URLS = ['https://www.salute.gov.it/portale/news/p3_2_1_3_1.jsp?lingua=italiano&menu=notizie&p=avvisi&tipo=richiamo']
    LANGUAGE = "it"
