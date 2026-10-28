"""AESAN (ES) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class AESANScraper(GenericLLMScraper):
    AGENCY = "AESAN (ES)"
    COUNTRY = "Spain"
    # 2026-10-28: the previous index,
    #   /AECOSAN/web/seguridad_alimentaria/ampliacion/listado_alertas_general.htm
    # was read today and lists NO alerts at all — it is a landing page about
    # risk management on the old AECOSAN site. AESAN moved its alert index to
    # the path below, which lists every live alert with its own /alertas/<id>
    # address. The cost of the dead index is measurable: the register held 1
    # AESAN row in the 30 days to 2026-10-28, while aesan.gob.es published
    # five alerts in October alone (2026_72 … 2026_76). The four this scraper
    # never saw reached Pending only as search-engine page copies, one per
    # language version, and three of them were rejected as junk.
    INDEX_URLS = ['https://www.aesan.gob.es/alertas/alertas-alimentarias']
    LANGUAGE = "es"
