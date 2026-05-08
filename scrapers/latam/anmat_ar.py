"""ANMAT (AR) food safety scraper — uses Gemini for HTML extraction.

URL update + region cleanup (2026-05-08):

The previous URL `/anmat/regulados/alimentos/alertas` returns HTTP 404 —
that path no longer exists on argentina.gob.ar. The working URL was
`/anmat/alertas/alimentos`, but that lived in a misfiled
`scrapers/north_america/anmat_ar.py` (Argentina was incorrectly under
North America). Both files defined an `ANMATScraper` class with the same
AGENCY tag, so each cycle ran ANMAT twice — once with each URL.

This consolidates: working URL moved to the correct LATAM canonical,
misfiled `north_america/anmat_ar.py` deleted in the same commit.

URL `/anmat/alertas/alimentos` returned HTTP 200 in the 2026-05-08 run
(extraction yield was 0 — likely no recalls in 7-day window or page
needs stronger extraction hints; see HINTS below).
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class ANMATScraper(GenericGeminiScraper):
    AGENCY = "ANMAT (AR)"
    COUNTRY = "Argentina"
    INDEX_URLS = ['https://www.argentina.gob.ar/anmat/alertas/alimentos']
    LANGUAGE = "es"
    EXTRACTION_HINTS = (
        "Focus on entries about ANMAT 'Disposiciones' that prohibit "
        "commercialization ('prohibición de comercialización'), recalls "
        "('retiro del mercado'), or sanitary alerts ('alerta sanitaria'). "
        "Pathogen-related entries may mention 'Listeria', 'Listeria "
        "monocytogenes', 'Salmonella', 'Bacillus cereus', 'cereulida', "
        "'E. coli', 'EHEC', or 'STEC'. Skip entries about cosmetic "
        "products, medical devices ('dispositivos médicos'), drugs "
        "('medicamentos'), and herbal supplements without pathogen "
        "involvement — those are out of FSIS scope."
    )
