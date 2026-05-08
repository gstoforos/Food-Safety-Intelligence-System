"""Mattilsynet (NO) food safety scraper — uses Gemini for HTML extraction.

URL update (2026-05-08):
The previous URL `/mat-og-drikke/tilbakekalte-matvarer` started returning
HTTP 404 — Mattilsynet reorganized the recall section to a top-level
`/tilbakekallinger` path.

Replacement URL (verified 2026-05-08 against live Mattilsynet.no):

`/tilbakekallinger` — Mattilsynet's official recall listing. Page header
text: "Bruk søkefeltet eller filtrer på tema for å finne tilbakekallingen
du leter etter." (Use the search field or filter by topic to find the
recall you're looking for.) Individual recall articles live at
`/tilbakekallinger/<slug>`. Recent examples confirmed via web search:
  - "Norfersk AS tilbakekaller flere typer kjøttdeig... Salmonella"
    (Sep 2025, ground meat / Salmonella)
  - "Smalahovetunet Voss... mistanke om listeria"
    (Jan 2026, traditional sheep meats / Listeria, 6 confirmed cases)
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MattilsynetScraper(GenericGeminiScraper):
    AGENCY = "Mattilsynet (NO)"
    COUNTRY = "Norway"
    INDEX_URLS = ['https://www.mattilsynet.no/tilbakekallinger']
    LANGUAGE = "no"
    EXTRACTION_HINTS = (
        "All entries on this page are food/feed recalls. Each item "
        "describes a 'tilbakekalling' (recall) with company, product, "
        "and reason. Pathogen-related entries may mention 'Listeria', "
        "'Salmonella', 'Bacillus cereus', 'cereulide', 'E. coli', "
        "'EHEC', 'Campylobacter', or 'patogen'. Skip non-pathogen "
        "recalls (allergens, foreign objects, illegal additives like "
        "E 127) — those are out of FSIS scope."
    )
