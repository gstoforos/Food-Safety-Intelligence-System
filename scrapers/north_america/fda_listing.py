"""FDA Recalls Listing HTML — Layer 1 canonical source for FDA recalls.

WHY THIS SCRAPER EXISTS (audit 2026-05-10)
==========================================
The 2026-05-09 production run had ZERO US FDA coverage. All three layers
of the existing FDA stack returned empty:

  - fda_press.py RSS (Layer 2)       → 404 from Akamai
  - fda_press.py HTML fallback       → 404 from Akamai
  - fda.py openFDA enforcement (L3)  → 0 rows in 35-day window
                                       (publication lag still in effect)

Triage of what's actually live on fda.gov found:
  1. The listing page at /safety/recalls-market-withdrawals-safety-alerts
     IS publicly reachable from a normal browser — Drupal Views renders
     a DataTables.js table with ~925 entries in the archive.
  2. The Network tab capture shows the table fetches its data via an
     XHR to /datatables-data?_format=xlsx (Solr-backed search index).
     We can hit that endpoint, but the XLSX it returns has NO URLs to
     individual recall pages — Drupal strips the <a> markup on export.
  3. The listing-page HTML itself, however, contains the URL slugs
     directly inside <td.views-field-brand-name> as plain <a href>
     elements (e.g.
       href="/safety/recalls-market-withdrawals-safety-alerts/jonco-...")

Per the project's NO-SYNTHESIS rule, we do not synthesise URLs from
headlines or company names. URLs must come from the source. The HTML
listing is the only remaining surface that publishes them, so this is
where we go.

Operationally, the listing's "Show 10 entries" default + sorted-newest-
first behaviour means the first page already covers our SINCE_DAYS=7
window in typical operation (5–20 fresh entries per week vs 925 in full
archive). For backfill or anomaly investigation, the operator can
manually pass a higher since_days; this scraper still parses whatever
rows are visible in the listing-page HTML.

Layered architecture (after this scraper lands)
-----------------------------------------------
    Layer 1 (NEW, this file) → listing HTML       → fresh, structured,
                                                    URLs guaranteed
    Layer 2 (fda_press.py)   → RSS + HTML fallback → fresh, redundant
    Layer 3 (fda.py)         → openFDA enforcement → 5–30d lag,
                                                    classification field

merge_master URL-dedupes across all three. If L1 ever gets bot-blocked
again, L2 and L3 still cover (with their respective trade-offs).

DESIGN DECISIONS
================
1. Column-order parsing, not class-name parsing.
   The Brand column's CSS class (views-field-brand-name) is verified
   live, but the other six columns may have Drupal-config suffixes
   (_1, _2) that drift with content model edits. Walking <td> children
   in order avoids that fragility. Column order is locked by the
   visible page header: Date | Brand | Product | Type | Reason |
   Company | Terminated.

2. Akamai bypass headers — same set as fda_press.py.
   sec-ch-ua, sec-ch-ua-platform, sec-fetch-* headers make the request
   indistinguishable from a Chrome 127 navigation. Without these,
   Akamai returns HTTP 404 (yes, 404, not 403) with
   server=AkamaiNetStorage. Verified pattern in fda_press.py docstring.

3. Pathogen + food filter — same vocab as fda_press.py.
   for_languages("en") gives the bilingual single-source vocab used
   by every English-speaking regulator scraper. _FOOD_CONTEXT_TOKENS
   only used as a fallback when Product Type column is unexpectedly
   empty; normally we filter on the Product Type cell directly.

4. Date format — MM/DD/YYYY (US-format strings).
   Verified from the XLSX export (same Solr index → same source data),
   e.g. "05/08/2026" = 2026-05-08. Consistent across the archive
   (oldest entry 2017-10-19).

5. Terminated recalls are KEPT in scope.
   "Terminated" only means FDA is satisfied with corrective action;
   the recall itself is still a valid historical food safety event.
   Within a 7-day window, almost no entries will be terminated yet.
   merge_master + claude-check decide what to surface.

6. URL filter — uses fda_press._ACCEPTABLE_URL_PREFIXES inline copy
   for parity. If FDA reshuffles URL structure (rare), both scrapers
   need updating in lockstep.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


# Outbreak signal tokens — mirror fda_press.py exactly so both scrapers
# emit the same Outbreak verdict for the same recall.
_OUTBREAK_TOKENS = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses",
)

# Product-Type cell substring → "this row is food".
# The Solr-indexed Product Type values are comma-separated category
# tags, e.g. "Food & Beverages, Foodborne Illness, Cheese/Cheese Product".
# A simple substring check on "Food & Beverages" catches all 30+ subcategory
# variants without false positives (verified across the 925-row archive).
_FOOD_PRODUCT_TYPE_TOKEN = "food & beverages"

# Defensive fallback when Product Type cell is empty (rare): scan the
# combined text for food-context keywords. Same list as fda_press.py.
_FOOD_CONTEXT_TOKENS = (
    "food", "beverage", "beverages", "drink", "drinks",
    "milk", "dairy", "cheese", "yogurt", "yoghurt", "ice cream",
    "meat", "poultry", "chicken", "beef", "pork", "turkey", "lamb",
    "fish", "seafood", "shrimp", "oyster", "salmon", "tuna",
    "produce", "vegetable", "vegetables", "fruit", "fruits",
    "salad", "spinach", "lettuce", "onion", "tomato", "carrot",
    "snack", "snacks", "chips", "crisps", "crackers", "biscuit",
    "cereal", "granola", "oats", "rice", "pasta", "noodle",
    "bakery", "bread", "cake", "pastry", "muffin",
    "infant formula", "baby food",
    "supplement", "dietary supplement", "powder", "drink mix",
    "spice", "spices", "herb", "herbs", "seasoning",
    "sauce", "dressing", "soup", "stew",
    "candy", "chocolate", "confection",
    "frozen", "ready to eat", "rte", "deli",
)

# Acceptable URL path prefixes for FDA recall pages. Anything else (a
# stray nav link, a generic landing page) is rejected.
_ACCEPTABLE_URL_PREFIXES = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/",
    "https://www.fda.gov/news-events/press-announcements/",
    "https://www.fda.gov/food/alerts-advisories-safety-information/",
)


def _detect_outbreak(text_lower: str) -> int:
    return 1 if any(t in text_lower for t in _OUTBREAK_TOKENS) else 0


def _matched_pathogen_keyword(
    text_lower: str, keywords: Tuple[str, ...]
) -> Optional[str]:
    for kw in keywords:
        if kw in text_lower:
            return kw
    return None


def _parse_date(s: str) -> Optional[datetime]:
    """FDA listing dates are 'MM/DD/YYYY' strings (verified)."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y")
    except ValueError:
        return None


class FDAListingScraper(BaseScraper):
    """Layer 1 — parses the FDA listing-page HTML directly.

    Single GET to the listing URL. URLs come from <a href> in the
    Brand column; structured fields from the visible <td> cells.
    No XLSX, no XHR chasing — we work with what the browser gets.
    """

    AGENCY = "FDA"
    COUNTRY = "USA"

    LISTING_URL = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
    )

    PATHOGEN_KEYWORDS = for_languages("en")

    # Akamai bot-detection bypass — copy of fda_press.py's set, with
    # sec-fetch-* tuned for an HTML page navigation (document/navigate/none)
    # rather than an RSS XHR (empty/cors/same-origin). Without these,
    # Akamai returns HTTP 404 with server=AkamaiNetStorage. With them,
    # the request is indistinguishable from a real Chrome 127 navigation.
    _AKAMAI_BYPASS_HEADERS = {
        "sec-ch-ua": (
            '"Not)A;Brand";v="99", "Google Chrome";v="127", '
            '"Chromium";v="127"'
        ),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "Cache-Control": "max-age=0",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
    }

    def scrape(self, since_days: int = 30) -> List[Recall]:
        r = fetch(
            self.session, self.LISTING_URL,
            headers=self._AKAMAI_BYPASS_HEADERS,
        )
        if not r:
            log.warning("FDA listing: no response from %s", self.LISTING_URL)
            return []
        if r.status_code != 200:
            log.warning(
                "FDA listing: HTTP %d from %s — Akamai may have tightened. "
                "fda_press.py and fda.py still cover at L2/L3.",
                r.status_code, self.LISTING_URL,
            )
            return []

        try:
            from bs4 import BeautifulSoup
        except ImportError as exc:
            log.error("BeautifulSoup not available: %s", exc)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        # The DataTables widget renders one <table> with class 'dataTable'
        # plus 'no-footer' once initialised by JS. On server-rendered HTML
        # the class may just be on the inner table; pick the first match.
        table = (
            soup.select_one("table.dataTable")
            or soup.select_one("table.no-footer")
            or soup.select_one("table.views-table")
        )
        if table is None:
            log.warning("FDA listing: no dataTable found in HTML response "
                        "(page structure may have changed)")
            return []

        rows_total = 0
        skipped_no_link = 0
        skipped_bad_url = 0
        skipped_old = 0
        skipped_unparseable_date = 0
        skipped_non_food = 0
        skipped_non_pathogen = 0

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen_urls: set = set()

        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 6:
                # Some Drupal renders include a footer pseudo-row; ignore.
                continue
            rows_total += 1

            # Column order (verified from screenshot 2026-05-10):
            #   0: Date           e.g. "05/08/2026"
            #   1: Brand-Names    contains the <a> with the recall URL
            #   2: Product-Description
            #   3: Product-Types  comma-separated tags
            #   4: Recall-Reason-Description
            #   5: Company-Name
            #   6: Terminated-Recall  ("Terminated" or empty)
            date_str = cells[0].get_text(" ", strip=True)
            brand_a = cells[1].find("a")
            product = cells[2].get_text(" ", strip=True)
            ptype = cells[3].get_text(" ", strip=True)
            reason = cells[4].get_text(" ", strip=True)
            company = cells[5].get_text(" ", strip=True)
            # cells[6] is Terminated — kept in scope per design note 5.

            if brand_a is None:
                skipped_no_link += 1
                continue
            href = (brand_a.get("href") or "").strip()
            brand_text = brand_a.get_text(" ", strip=True) or "—"
            if not href:
                skipped_no_link += 1
                continue
            url = href if href.startswith("http") else f"https://www.fda.gov{href}"
            if not any(url.startswith(p) for p in _ACCEPTABLE_URL_PREFIXES):
                skipped_bad_url += 1
                continue
            if url in seen_urls:
                continue

            d = _parse_date(date_str)
            if d is None:
                skipped_unparseable_date += 1
                continue
            if d < cutoff:
                skipped_old += 1
                continue

            # Food filter: prefer Product Type, fall back to keyword scan.
            ptype_lc = ptype.lower()
            haystack = (
                reason + " " + product + " " + brand_text
            ).lower()
            if ptype_lc:
                if _FOOD_PRODUCT_TYPE_TOKEN not in ptype_lc:
                    skipped_non_food += 1
                    continue
            else:
                if not any(t in haystack for t in _FOOD_CONTEXT_TOKENS):
                    skipped_non_food += 1
                    continue

            matched_kw = _matched_pathogen_keyword(
                haystack, self.PATHOGEN_KEYWORDS,
            )
            if not matched_kw:
                skipped_non_pathogen += 1
                continue

            outbreak = _detect_outbreak(haystack)

            out.append(self._new_recall(
                Date=d.strftime("%Y-%m-%d"),
                Company=(company or brand_text)[:150],
                Brand=brand_text[:120],
                Product=(product or brand_text)[:300],
                Pathogen=matched_kw,           # canonicalised by _new_recall
                Reason=(reason or product)[:400],
                Class="Recall",
                URL=url,
                Outbreak=outbreak,
                Notes="FDA listing HTML (Layer 1)",
            ))
            seen_urls.add(url)

        log.info(
            "FDA listing: %d pathogen recalls in %d-day window (rows scanned=%d, "
            "skipped: no_link=%d bad_url=%d unparseable_date=%d old=%d "
            "non_food=%d non_pathogen=%d)",
            len(out), since_days, rows_total,
            skipped_no_link, skipped_bad_url, skipped_unparseable_date,
            skipped_old, skipped_non_food, skipped_non_pathogen,
        )
        return out
