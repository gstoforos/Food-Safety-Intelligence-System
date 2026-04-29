"""
RASFF Window — EU Rapid Alert System for Food and Feed.

DISABLED 2026-04-29. Audit history:

  - Original approach: scrape https://webgate.ec.europa.eu/rasff-window/screen/search
    with GenericGeminiScraper (HTML → Gemini → rows).
  - Verified 2026-04-29: RASFF Window is a Vue/Angular SPA. The static HTML
    served by /screen/search and /screen/notification/{id} contains only the
    string "Application name" — no recall data, no SSR fallback, no metadata
    that Gemini can extract. The previous scraper has therefore been silently
    producing zero rows (or unreliable Gemini hallucinations) since RASFF
    Window's last refactor.

Why we don't have a working alternative:

  - BVL daily RASFF mirror (https://www.bvl.bund.de/...): Germany's national
    contact point published an anonymized daily mirror of all RASFF notifications
    for years. DISCONTINUED 2026-02-01 — BVL now redirects users back to the
    SPA. No replacement national mirror has emerged.
  - EU Open Data Portal (data.europa.eu/data/datasets/restored_rasff):
    same SPA framework as RASFF Window; metadata page is also an empty shell.
  - api.store / Apitalks third-party RASFF API: returned HTTP 500 on
    verification 2026-04-29; not production-grade in any case.
  - WUR Wageningen archive (bigdata-wfsr.wur.nl): historical only, no live feed.

Coverage we still have without RASFF:

  Country scrapers cover all major EU members directly — AESAN-ES, BVL-DE
  (consumer side, lebensmittelwarnung.de), AGES-AT, AFSCA-BE, NVWA-NL,
  NEBIH-HU, UVHVVR-SI, VTA-EE, plus the four Nordics (Fødevarestyrelsen-DK,
  Livsmedelsverket-SE, Mattilsynet-NO, Ruokavirasto-FI) and the EU group
  files. A Salmonella-in-Polish-poultry notification typically surfaces on
  both the notifying country's portal AND the receiving country's portal,
  so most consumer-facing recall traffic is captured at the national level.

Coverage gap accepted:

  Border rejection notifications (3rd-country goods stopped at EU ports of
  entry) are RASFF-exclusive — they don't surface on member state consumer
  portals. Same for some "information for attention" notifications that
  never trigger a national recall. AFTS customers — food businesses
  monitoring consumer-facing recall risk — care less about these than
  about the consumer alerts the country scrapers already capture.

Forward path:

  If full RASFF coverage becomes a customer requirement, the right fix is
  a Playwright-based scraper variant added to scrapers/_base.py — load the
  SPA, wait for hydration, extract from the rendered DOM. Adds a chromium
  binary dependency to the GitHub Actions runner; deferred until justified
  by demand.
"""
from __future__ import annotations
from typing import List
import logging
from scrapers._base import BaseScraper
from scrapers._models import Recall

log = logging.getLogger(__name__)


class RASFFScraper(BaseScraper):
    AGENCY = "RASFF (EU)"
    COUNTRY = ""
    LANGUAGE = "en"

    def scrape(self, since_days: int = 30) -> List[Recall]:
        log.info(
            "RASFF (EU): disabled — RASFF Window is a SPA with no public data feed; "
            "BVL mirror discontinued 2026-02-01. EU coverage relies on country scrapers. "
            "See module docstring for full audit and forward path."
        )
        return []
