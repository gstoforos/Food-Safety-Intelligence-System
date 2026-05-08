"""DIGESA (PE) food safety scraper — uses Gemini for HTML extraction.

URL update + region cleanup (2026-05-08):

The previous URL `http://www.digesa.minsa.gob.pe/noticias.asp` returns
HTTP 404. The working URL is `https://www.digesa.minsa.gob.pe/noticias/comunicados.asp`
(also an HTTPS upgrade), but that lived in a misfiled
`scrapers/north_america/digesa_pe.py` (Peru was incorrectly under North
America). Both files defined a `DIGESAScraper` class with the same
AGENCY tag, so each cycle ran DIGESA twice — once with each URL.

This consolidates: working URL moved to the correct LATAM canonical,
misfiled `north_america/digesa_pe.py` deleted in the same commit.

URL `/noticias/comunicados.asp` returned HTTP 200 in the 2026-05-08 run
(extraction yield was 0 — could be no recalls in 7-day window or
extraction limitations on the legacy ASP page; see HINTS below).
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class DIGESAScraper(GenericGeminiScraper):
    AGENCY = "DIGESA (PE)"
    COUNTRY = "Peru"
    INDEX_URLS = ['https://www.digesa.minsa.gob.pe/noticias/comunicados.asp']
    LANGUAGE = "es"
    EXTRACTION_HINTS = (
        "Focus on official communications ('comunicados') about food "
        "safety alerts, prohibitions ('prohibición'), recalls ('retiro "
        "del mercado'), or sanitary actions involving specific food "
        "products. Pathogen-related entries may mention 'Listeria', "
        "'Salmonella', 'Bacillus cereus', 'cereulida', 'E. coli', "
        "'patógeno', or 'contaminación microbiológica'. Skip entries "
        "about regulatory training, administrative procedures, "
        "drinking water quality reports without recall actions, or "
        "general consumer guidance — those are not recalls."
    )
