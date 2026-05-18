"""FDA listing HTML — DISABLED (audit 2026-05-18).

WHY THIS SCRAPER IS DISABLED
============================
Two consecutive days of production logs (2026-05-17 and 2026-05-18) showed
the same failure pattern:

    [WARNING] scrapers.north_america.fda_listing: FDA listing: no
              dataTable found in HTML response (page structure may
              have changed)

The original implementation (audit 2026-05-10) located recall URLs by
parsing the Drupal Views DataTables markup on the listing page —
specifically the `<table>` element with class `dataTable` containing
`<td.views-field-brand-name>` cells with `<a href>` slugs. The current
page structure does not include that table on the initial HTML render;
the DataTables.js library populates it client-side via XHR after the
page loads. The scraper sees the empty pre-hydration HTML.

This was a viable strategy in May 2026 when the page was server-side
rendered. FDA's Drupal upgrade or DataTables.js configuration change
moved table population to a JS-side XHR, breaking the static-HTML parse.

The XHR target hit by DataTables.js is the same /datatables-data
endpoint that fda_datatables.py probes — and that endpoint also fails
(HTTP 406 to JSON Accept, XLSX-only via the listing page's Download
button). So there's no clean fallback. A JS-execution scraper
(Playwright / Selenium) would work but is overkill for the marginal
coverage this layer provides.

COVERAGE WITHOUT THIS SCRAPER
=============================
FDA recall coverage continues through:

    - scrapers/north_america/fda_press.py  → RSS feed + HTML fallback
      with Akamai bot-detection bypass. The HTML fallback hits a
      DIFFERENT page (the RSS-fed list) that does render slugs in
      static HTML.

    - scrapers/north_america/fda.py         → openFDA enforcement API.
      Authoritative but lags publication by 5-30 days.

Both still load at AGENCIES_FILTER and run on schedule.

WHY KEEP THE FILE
=================
Same reason as fda_datatables.py: returning [] from a registered class
is cheaper than removing the class from discovery. One INFO log per
run leaves a breadcrumb.

RE-ENABLING
===========
Three paths to re-enable, in order of effort:

    1. If FDA reverts to server-side table rendering, the original
       BeautifulSoup parse will work again. Recover from git:
       `git show <pre-disable-commit>:scrapers/north_america/fda_listing.py`

    2. Find a different fda.gov page that publishes recall URLs in
       static HTML. Candidate: the press-announcements page
       (/news-events/press-announcements/) sometimes carries
       FDA-issued recall press releases with direct slugs.

    3. Move to Playwright-based JS execution. Adds a 200 MB browser
       dependency and a ~5 s render delay per run. Only worth it if
       fda_press.py RSS + HTML fallback proves insufficient over a
       sustained period (≥30 days of zero direct FDA captures).
"""
from __future__ import annotations

import logging
from typing import List

from scrapers._base import BaseScraper
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FDAListingScraper(BaseScraper):
    """DISABLED — see module docstring for rationale and re-enable paths."""

    AGENCY = "FDA"
    COUNTRY = "USA"

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # One INFO line per run. No HTTP call. Returns empty list
        # immediately. The original 348-line implementation lives in
        # git history; see module docstring's RE-ENABLING section.
        log.info(
            "FDA listing: scraper disabled 2026-05-18 — listing page no "
            "longer renders the DataTable in static HTML (client-side JS "
            "hydration). Coverage via fda_press.py + fda.py. See module "
            "docstring."
        )
        return []
