"""INVIMA (CO) food safety scraper — uses Gemini for HTML extraction.

URL retargeting (Batch 3, 2026-05-08):
The previous URL `/sala-de-prensa` was the generic INVIMA press hall
which mixes all categories (drugs, devices, cosmetics, food). It
consistently returned 0 rows from Gemini.

The dedicated food-and-beverages alerts subdomain
`app.invima.gov.co/alertas/alertas-alimentos-bebidas` is a structured
list of food-specific recall alerts with dates and product details.
Recent entries include "FALSIFICACIÓN DE LECHE EN POLVO ENTERA
FORTIFICADA" and similar food-specific items — exactly the
extraction-friendly format Gemini handles well.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class INVIMAScraper(GenericGeminiScraper):
    AGENCY = "INVIMA (CO)"
    COUNTRY = "Colombia"
    INDEX_URLS = [
        'https://app.invima.gov.co/alertas/alertas-alimentos-bebidas',
    ]
    LANGUAGE = "es"
    EXTRACTION_HINTS = (
        "All entries on this page are food/beverage alerts. Extract each "
        "'Alerta sanitaria sobre:' entry with its date and product details. "
        "Pathogen-related entries may mention 'Listeria', 'Salmonella', "
        "'Bacillus cereus', 'cereulida', 'E. coli', or 'contaminación "
        "microbiológica'. Falsification ('FALSIFICACIÓN') alerts that "
        "do not involve a pathogen are not in scope for FSIS."
    )
