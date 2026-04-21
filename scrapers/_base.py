"""
MAPAQ QC — Québec food recall warnings.

Ministère de l'Agriculture, des Pêcheries et de l'Alimentation du Québec
publishes food safety warnings (mises en garde) on québec.ca. These
complement CFIA (federal Canada) with provincial-level Québec recalls
that often appear here first before the federal notice ships.

Source: https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/agriculture/industrie-alimentaire/mises-garde-alimentaires
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MAPAQScraper(GenericGeminiScraper):
    AGENCY = "MAPAQ (QC)"
    COUNTRY = "Canada"
    BASE_URL = "https://www.quebec.ca"
    INDEX_URLS = [
        "https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/agriculture/industrie-alimentaire/mises-garde-alimentaires",
    ]
    LANGUAGE = "fr"
    EXTRACTION_HINTS = """
MAPAQ warnings are in French. Each warning ("mise en garde") typically contains:
- Company/establishment name
- Product description
- Reason for recall (pathogen, contamination)
- Date of warning
- Province: always set Country to "Canada" and add "Quebec" in Notes.
URLs should be the individual mise-en-garde page links, not the listing page.
"""
