"""MAST (IS) food safety scraper — uses Gemini for HTML extraction.

URL update (2026-05-08):
The previous URL `/is/frettir/innkollun` started returning HTTP 404 — that
page no longer exists on mast.is. The site has been reorganized.

Replacement URLs (verified 2026-05-08 against live MAST.is content):

1. `/is/um-mast/frettir/frettir` — MAST's full news feed. Individual
   recall articles live at `/is/um-mast/frettir/frettir/<slug>`. Recent
   examples confirmed via web search:
     - "Aðskotahlutir í kexi fyrir börn" (Feb 2026, foreign objects)
     - "Salmonella í kökum" (Aug 2024, Salmonella in cakes)
     - "Listeria í sveppum" (Listeria in mushrooms)
     - "Ólöglegt bleikiefni í hveiti" (illegal bleach in flour)

2. `/is/neytendur/innkallanir` — the consumer-facing recalls page (this
   was previously in the deprecated europe_eu/mast.py duplicate). Returns
   HTTP 200 but extraction yield was 0 in the 2026-05-08 run; kept as a
   secondary source in case the news-feed page stops carrying recalls.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MASTScraper(GenericGeminiScraper):
    AGENCY = "MAST (IS)"
    COUNTRY = "Iceland"
    INDEX_URLS = [
        # Primary: full news feed where recall articles are published.
        'https://www.mast.is/is/um-mast/frettir/frettir',
        # Secondary: consumer-facing recall page.
        'https://www.mast.is/is/neytendur/innkallanir',
    ]
    LANGUAGE = "is"
    EXTRACTION_HINTS = (
        "Focus on news entries about food recalls ('innköllun'), "
        "warnings ('vörun', 'aðvörun'), or sales stoppage ('sölustöðvun'). "
        "Pathogen-related entries may mention 'Listeria', 'Salmonella', "
        "'Bacillus cereus', 'cereulide', 'E. coli', 'kampýlóbakter' "
        "(Campylobacter), or 'sýklar' (pathogens). Skip news about "
        "animal welfare, fishery management, regulatory consultations, "
        "or staff appointments — those are not recalls."
    )
