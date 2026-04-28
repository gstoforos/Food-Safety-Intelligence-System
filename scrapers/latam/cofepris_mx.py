"""COFEPRIS (MX) food safety scraper — uses Gemini for HTML extraction.

COFEPRIS publishes alerts across multiple categories (medicamentos,
dispositivos, alimentos, etc.). The previous index URL pointed at the
generic press archive (`/es/archivo/prensa`), which returns ALL press
releases — not just food alerts — and frequently misses the dedicated
food alerts page maintained by the Subsecretaría de Regulación y Fomento
Sanitario.

The two URLs below cover food alerts specifically, where Gemini
extraction can find the actual product recalls without wading through
unrelated press releases. The PDF-attachment pattern
`/cms/uploads/attachment/file/<id>/Alerta_Sanitaria_<slug>.pdf` is the
canonical link format for individual alerts; Gemini picks those up
automatically when scanning the index pages.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class COFEPRISScraper(GenericGeminiScraper):
    AGENCY = "COFEPRIS (MX)"
    COUNTRY = "Mexico"
    INDEX_URLS = [
        'https://www.gob.mx/cofepris/documentos/alertas-sanitarias-de-alimentos',
        'https://www.gob.mx/cofepris/acciones-y-programas/alertas-sanitarias-336947',
    ]
    LANGUAGE = "es"
