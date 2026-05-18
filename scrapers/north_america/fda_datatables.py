"""FDA datatables — DISABLED (audit 2026-05-18).

WHY THIS SCRAPER IS DISABLED
============================
Two consecutive days of production logs (2026-05-17 and 2026-05-18) showed
all three candidate endpoints failing in identical patterns:

    https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts?_format=json
        → HTTP 200 but body is HTML, not JSON (Drupal serves the
          interactive page instead of the format-aliased download).
          ~73 KB of HTML returned, parser rejects.

    https://www.fda.gov/datatables/json/recalls-market-withdrawals-safety-alerts
        → HTTP 404. The internal Views REST endpoint hypothesised by
          the audit 2026-05-09 docstring either never existed or was
          removed by FDA. No replacement path discovered.

    https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/datatables-data?_format=json
        → HTTP 406 Not Acceptable. The endpoint exists (the listing
          page's XLSX download button hits it) but rejects the JSON
          Accept header. XLSX-only.

The two-day reproducibility was the green light to disable. The original
"Drupal datatables widget exposes machine-readable variants" hypothesis
from the audit 2026-05-09 docstring is empirically wrong for fda.gov's
current Drupal configuration.

COVERAGE WITHOUT THIS SCRAPER
=============================
FDA recall coverage continues through:

    - scrapers/north_america/fda_press.py  → RSS feed + HTML fallback
      with Akamai bot-detection bypass. Produces ~10-20 candidate URLs
      per run, needs claude-check enrichment for Date+Pathogen.

    - scrapers/north_america/fda.py         → openFDA enforcement API.
      Authoritative but lags publication by 5-30 days.

Both still load at AGENCIES_FILTER and run on schedule.

WHY KEEP THE FILE
=================
Returning [] from a registered scraper class is cheaper than removing
the class from the discovery tree (which would require touching the
AGENCIES_FILTER count expectations and the scraper-health audit
manifest). The single INFO log per run leaves a breadcrumb in
production logs so we don't forget this exists.

RE-ENABLING
===========
If FDA restores a working JSON endpoint (or someone discovers a working
one), git history has the full implementation. `git log --follow
scrapers/north_america/fda_datatables.py` → find the commit before
this disable, `git show <commit>:scrapers/north_america/fda_datatables.py`
to recover the scrape() body and helpers. The scrape signature is
unchanged, so swapping the body back in is a one-step restore.
"""
from __future__ import annotations

import logging
from typing import List

from scrapers._base import BaseScraper
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FDADatatablesScraper(BaseScraper):
    """DISABLED — see module docstring for rationale and re-enable path."""

    AGENCY = "FDA"
    COUNTRY = "USA"

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # One INFO line per run — quiet but auditable. No HTTP calls,
        # no warnings, no enrichment placeholders. Returns empty list
        # immediately. Coverage of FDA recalls flows through fda_press.py
        # (RSS + HTML fallback) and fda.py (openFDA enforcement).
        log.info(
            "FDA datatables: scraper disabled 2026-05-18 — all 3 candidate "
            "endpoints fail consistently (non-JSON / 404 / 406). Coverage "
            "via fda_press.py + fda.py. See module docstring."
        )
        return []
