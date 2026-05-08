"""ANVISA (BR) food safety scraper — uses Gemini for HTML extraction.

URL retargeting (Batch 3, 2026-05-08):
The previous URL `/assuntos/noticias-anvisa` is the parent news landing
page that consistently returned 0 rows from Gemini — likely because the
page is more nav/teaser than article content (it's where users *navigate
into* news, not where individual stories are listed inline).

The year-archive URL `/noticias-anvisa/<YEAR>` is a flat chronological
listing where each item shows title + date + lede. ANVISA's recall items
("Anvisa determina recolhimento de...", "Proibida venda de fórmula
infantil...") appear inline with extractable structure.

YEAR ROTATION: This URL is year-specific. Update the `2026` slug each
January, or implement dynamic year via `datetime.now().year`.
"""
from __future__ import annotations
from datetime import datetime, timezone
from scrapers._base import GenericGeminiScraper


class ANVISAScraper(GenericGeminiScraper):
    AGENCY = "ANVISA (BR)"
    COUNTRY = "Brazil"
    # Dynamic year so we don't have to manually rotate every January.
    # Fall back to current Athens-time year (orchestrator's reference TZ).
    _year = datetime.now(timezone.utc).year
    INDEX_URLS = [
        f'https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/{_year}',
    ]
    LANGUAGE = "pt"
    EXTRACTION_HINTS = (
        "Focus on items with these recall/withdrawal keywords in titles: "
        "'recolhimento', 'suspensão', 'proibição', 'retirada', 'apreensão'. "
        "Pathogen-related entries often mention 'Listeria', 'Salmonella', "
        "'Bacillus cereus', 'cereulida', 'Cronobacter', 'E. coli'. "
        "Skip items about regulations, public consultations, or "
        "administrative changes — those are not recalls."
    )
