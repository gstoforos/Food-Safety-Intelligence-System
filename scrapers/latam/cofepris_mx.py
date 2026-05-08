"""COFEPRIS (MX) food safety scraper — uses Gemini for HTML extraction.

COFEPRIS publishes alerts across multiple categories (medicamentos,
dispositivos, alimentos, etc.). The two official COFEPRIS URLs are
preserved below as the authoritative source.

Mirror URL added (Batch 3, 2026-05-08):
The two official `gob.mx/cofepris/...` URLs consistently returned 0 rows
from Gemini in the daily orchestrator runs — likely because the gob.mx
Drupal pages are JS-rendered and individual alerts live behind PDF links,
leaving Gemini with no inline text to extract.

The Nuevo León state government site mirrors COFEPRIS's publications with
the FULL text of each alert inline, plus dates. Gemini can extract from
this format. The state site is technically a third-party mirror, but it
republishes COFEPRIS's official communicados verbatim, attributing each
alert to "La Comisión Federal para la Protección contra Riesgos
Sanitarios (COFEPRIS)". URLs of individual alerts in the extracted rows
still point to canonical COFEPRIS pages where available.

YEAR ROTATION: The state-mirror URL is year-specific. Update the `2026`
slug each January, or use dynamic year as below.
"""
from __future__ import annotations
from datetime import datetime, timezone
from scrapers._base import GenericGeminiScraper


class COFEPRISScraper(GenericGeminiScraper):
    AGENCY = "COFEPRIS (MX)"
    COUNTRY = "Mexico"
    _year = datetime.now(timezone.utc).year
    INDEX_URLS = [
        # Official COFEPRIS — kept for canonicity (often JS-rendered, may
        # return 0 rows but inexpensive to attempt).
        'https://www.gob.mx/cofepris/documentos/alertas-sanitarias-de-alimentos',
        'https://www.gob.mx/cofepris/acciones-y-programas/alertas-sanitarias-336947',
        # State-government mirror with full inline text. This is where
        # most rows actually come from.
        f'https://saludnl.gob.mx/regulacion-sanitaria/index.php/comunicados-y-alertas-sanitarias-{_year}-2/',
    ]
    LANGUAGE = "es"
    EXTRACTION_HINTS = (
        "COFEPRIS issues alerts in several categories: only food-related "
        "alerts matter here. Look for entries mentioning 'retiro del "
        "mercado', 'alerta sanitaria sobre el alimento', 'fórmula infantil', "
        "'suplemento alimenticio', 'producto alimenticio', or specific "
        "pathogens ('Listeria', 'Salmonella', 'Bacillus cereus', "
        "'cereulida', 'Cronobacter', 'E. coli'). Skip alerts about "
        "medicamentos (drugs), dispositivos médicos (medical devices), "
        "cosméticos, or robos (theft of pharmaceuticals)."
    )
