"""FDA Recalls Listing HTML — Layer 1 canonical source for FDA recalls.

REWRITE 2026-05-19 — FOR LIVE FDA STRUCTURE
============================================
The previous parser (`soup.select_one("table.dataTable")`) intermittently
failed with "no dataTable found in HTML response" even when the listing
DID contain a valid table. Triage of a saved snapshot of the live page
(operator capture 2026-05-19) revealed:

  1. The table now carries class `lcds-datatable table table-bordered
     cols-8 responsive-enabled dataTable no-footer dtr-inline` AND
     `id="datatable"`. The `id` is the only single-attribute selector
     that's guaranteed unique on the page.

  2. Cell semantics are encoded in Drupal views-field-* classes, which
     are stable across Drupal config edits. Column ORDER is also stable,
     but cell CLASS NAMES carry the meaning. We use both: class-name as
     primary, position as fallback.

  3. Drupal view-config quirks add `-1` / `-2` suffixes to some
     views-field classes (e.g. `views-field-field-change-date-2`,
     `views-field-field-product-description-1`). We match the PREFIX
     to insulate against future drift.

  4. Date cells contain `<time datetime="2026-05-14T04:00:00Z">05/14/2026</time>`.
     The ISO `datetime` attribute is the most reliable parse target;
     the visible MM/DD/YYYY text is a fallback.

  5. Akamai sometimes serves a different/stripped HTML to non-browser
     fetches. When parsing fails, we now log RAW HTML STATS (size, count
     of <table>, count of <tr>) so we can diagnose without re-running.

LIVE HTML STRUCTURE (verified 2026-05-19, operator-captured snapshot)
=====================================================================
```
<table class="lcds-datatable ... dataTable no-footer dtr-inline"
       id="datatable" role="grid">
  <thead>
    <tr>
      <th>Date</th><th>Brand Name(s)</th><th>Product Description</th>
      <th>Product Type</th><th>Recall Reason Description</th>
      <th>Company Name</th><th>Terminated Recall</th><th>Excerpt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td class="... views-field-field-change-date-2 ...">
        <time datetime="2026-05-14T04:00:00Z">05/14/2026</time>
      </td>
      <td class="... views-field-brand-name ...">
        <a href="https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/
                 terra-medi-llc-recalls-..." >Hellas Meze</a>
      </td>
      <td class="... views-field-field-product-description-1 ...">
        Hellas Meze Golden Smoked Whole Herring
      </td>
      <td class="... views-field-field-regulated-product-field ...">
        Food &amp; Beverages, Foodborne Illness
      </td>
      <td class="... views-field-field-recall-reason-description-1 ...">
        Product is an uneviscerated fish with potential for
        Clostridium botulinum contamination
      </td>
      <td class="... views-field-company-name ...">Terra Medi LLC</td>
      <td class="... views-field-field-terminated-recall ...">[empty]</td>
      <td class="all priority-low views-field-field-terminated...">[empty]</td>
    </tr>
    ...
  </tbody>
</table>
```

Layered architecture
--------------------
    Layer 1 (THIS FILE)      → listing HTML       → 10 most-recent recalls
                                                    with full structured data
    Layer 2 (fda_press.py)   → RSS + HTML fallback → fresh, redundant
    Layer 3 (fda.py)         → openFDA enforcement → 5-30d lag, has
                                                    classification field

DESIGN DECISIONS (revised)
==========================
1. Multi-selector cascade for the table:
     a) `table#datatable` (most stable — unique ID)
     b) `table.dataTable` (legacy fallback)
     c) `table.views-table` (Drupal-views fallback)
     d) `table.lcds-datatable` (newest class prefix)
   First match wins.

2. Cell extraction by views-field PREFIX, with positional fallback:
   For each row we try class-name match first, fall back to column index
   if the views-field-* class isn't found. Both fail → row skipped with
   `skipped_unparseable_row` counter incremented.

3. Date parsing prefers `<time datetime="...">` attribute (ISO 8601),
   falls back to MM/DD/YYYY text. The `datetime` attribute is timezone-
   aware (Z suffix); we strip the timezone for our naive cutoff compare.

4. Defensive HTML stats logging when table not found, so we don't
   silently lose a day of FDA coverage.

5. Akamai bypass headers unchanged — Chrome 127 navigation profile.
   Verified to return HTTP 200 with full content. If FDA ever tightens
   to require JS execution, fall back to L2/L3 (this scraper logs a
   warning rather than crashing).

6. Pathogen + food filter — same vocab as fda_press.py.
   for_languages("en") gives the bilingual single-source vocab used
   by every English-speaking regulator scraper.

7. Terminated recalls are KEPT in scope.
   "Terminated" only means FDA is satisfied with corrective action;
   the recall itself is still a valid food safety event.
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
# A simple substring check on "food & beverages" catches all 30+
# subcategory variants without false positives.
_FOOD_PRODUCT_TYPE_TOKEN = "food & beverages"

# Defensive fallback when Product Type cell is empty (rare): scan the
# combined text for food-context keywords. Same list as fda_press.py.
_FOOD_CONTEXT_TOKENS = (
    "food", "beverage", "beverages", "drink", "drinks",
    "milk", "dairy", "cheese", "yogurt", "yoghurt", "ice cream",
    "meat", "poultry", "chicken", "beef", "pork", "turkey", "lamb",
    "fish", "seafood", "shrimp", "oyster", "salmon", "tuna", "herring",
    "produce", "vegetable", "vegetables", "fruit", "fruits",
    "salad", "spinach", "lettuce", "onion", "tomato", "carrot",
    "mushroom", "mushrooms", "enoki",
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

# Drupal views-field CSS class PREFIXES (we match by prefix to survive
# config-suffix drift like `-1`, `-2`). Order = visible column order.
_FIELD_CLASS_PREFIXES = {
    "date":      "views-field-field-change-date",
    "brand":     "views-field-brand-name",
    "product":   "views-field-field-product-description",
    "type":      "views-field-field-regulated-product-field",
    "reason":    "views-field-field-recall-reason-description",
    "company":   "views-field-company-name",
    "terminated":"views-field-field-terminated-recall",
}

# Positional fallback if class-name match fails — verified column order.
_POSITIONAL_INDEX = {
    "date": 0, "brand": 1, "product": 2, "type": 3,
    "reason": 4, "company": 5, "terminated": 6,
}


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
    """FDA listing dates: prefer ISO from <time datetime="...">,
    fall back to MM/DD/YYYY text."""
    if not s:
        return None
    s = s.strip()
    # Try ISO 8601 first (strip 'Z' or +00:00 for naive compare)
    iso = s.replace("Z", "").split("+")[0].split(".")[0]
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(iso, fmt)
        except ValueError:
            pass
    # Fall back to US-format
    try:
        return datetime.strptime(s, "%m/%d/%Y")
    except ValueError:
        return None


def _cell_by_class_prefix(tr, prefix: str):
    """Find first <td> in <tr> whose class attribute contains a token
    starting with the given Drupal views-field prefix. Returns the
    BeautifulSoup Tag or None."""
    for td in tr.find_all("td"):
        classes = td.get("class") or []
        for c in classes:
            if c.startswith(prefix):
                return td
    return None


def _get_cell(tr, field: str, all_cells):
    """Return the cell for a logical field — by class prefix first,
    by positional fallback second. `all_cells` is the cached td list."""
    prefix = _FIELD_CLASS_PREFIXES[field]
    cell = _cell_by_class_prefix(tr, prefix)
    if cell is not None:
        return cell
    idx = _POSITIONAL_INDEX[field]
    if 0 <= idx < len(all_cells):
        return all_cells[idx]
    return None


def _extract_date(cell) -> Optional[datetime]:
    """Date cell: prefer <time datetime="..."> attribute, fall back to text."""
    if cell is None:
        return None
    time_tag = cell.find("time")
    if time_tag is not None:
        iso = (time_tag.get("datetime") or "").strip()
        if iso:
            d = _parse_date(iso)
            if d is not None:
                return d
    return _parse_date(cell.get_text(" ", strip=True))


def _extract_brand_and_url(cell) -> Tuple[str, str]:
    """Brand cell: text inside the first <a>, plus href."""
    if cell is None:
        return "", ""
    a = cell.find("a")
    if a is None:
        return cell.get_text(" ", strip=True), ""
    return a.get_text(" ", strip=True), (a.get("href") or "").strip()


def _log_html_stats(html: str, marker: str) -> None:
    """When parsing fails, log enough about the response to diagnose
    without needing to re-fetch and stare at headers."""
    size = len(html)
    n_table = len(re.findall(r"<table\b", html, re.IGNORECASE))
    n_tr = len(re.findall(r"<tr\b", html, re.IGNORECASE))
    n_td = len(re.findall(r"<td\b", html, re.IGNORECASE))
    has_id = 'id="datatable"' in html
    has_class = "dataTable" in html
    has_views = "views-field" in html
    log.warning(
        "FDA listing: %s — size=%d bytes, <table>=%d, <tr>=%d, <td>=%d, "
        "id=datatable=%s, class=dataTable=%s, views-field=%s",
        marker, size, n_table, n_tr, n_td,
        has_id, has_class, has_views,
    )


class FDAListingScraper(BaseScraper):
    """Layer 1 — parses the FDA listing-page HTML directly.

    Single GET to the listing URL. URLs come from <a href> in the
    Brand column; structured fields from views-field-* cells.
    No XLSX, no XHR chasing — we work with what the browser gets.
    """

    AGENCY = "FDA"
    COUNTRY = "USA"

    LISTING_URL = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
    )

    PATHOGEN_KEYWORDS = for_languages("en")

    # Akamai bot-detection bypass — Chrome 127 navigation profile.
    # Without these, Akamai returns HTTP 404 with server=AkamaiNetStorage.
    # With them, indistinguishable from a real Chrome 127 navigation.
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

        # Multi-selector cascade: try most-stable first.
        # `id="datatable"` is the only single-attribute selector
        # guaranteed unique on the page.
        table = (
            soup.select_one("table#datatable")
            or soup.select_one("table.dataTable")
            or soup.select_one("table.lcds-datatable")
            or soup.select_one("table.views-table")
            or soup.select_one("table.no-footer")
        )
        if table is None:
            _log_html_stats(r.text, "no recall table found in HTML")
            return []

        # tbody might not be present in some Drupal renders — fall back
        # to selecting <tr>s directly within the table, skipping any
        # that are inside <thead>.
        tbody = table.find("tbody")
        if tbody is not None:
            trs = tbody.find_all("tr")
        else:
            thead = table.find("thead")
            head_trs = set(id(t) for t in (thead.find_all("tr") if thead else []))
            trs = [tr for tr in table.find_all("tr") if id(tr) not in head_trs]

        if not trs:
            _log_html_stats(r.text, "table found but contains no data rows")
            return []

        rows_total = 0
        skipped_no_link = 0
        skipped_bad_url = 0
        skipped_old = 0
        skipped_unparseable_date = 0
        skipped_unparseable_row = 0
        skipped_non_food = 0
        skipped_non_pathogen = 0

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen_urls: set = set()

        for tr in trs:
            cells = tr.find_all("td")
            if len(cells) < 6:
                # Footer pseudo-rows or malformed entries.
                skipped_unparseable_row += 1
                continue
            rows_total += 1

            date_cell    = _get_cell(tr, "date",    cells)
            brand_cell   = _get_cell(tr, "brand",   cells)
            product_cell = _get_cell(tr, "product", cells)
            type_cell    = _get_cell(tr, "type",    cells)
            reason_cell  = _get_cell(tr, "reason",  cells)
            company_cell = _get_cell(tr, "company", cells)
            # terminated_cell unused — design decision 7 keeps them in scope.

            d = _extract_date(date_cell)
            if d is None:
                skipped_unparseable_date += 1
                continue
            if d < cutoff:
                skipped_old += 1
                continue

            brand_text, href = _extract_brand_and_url(brand_cell)
            if not href:
                skipped_no_link += 1
                continue
            url = (
                href if href.startswith("http")
                else f"https://www.fda.gov{href}"
            )
            if not any(url.startswith(p) for p in _ACCEPTABLE_URL_PREFIXES):
                skipped_bad_url += 1
                continue
            if url in seen_urls:
                continue

            product = product_cell.get_text(" ", strip=True) if product_cell else ""
            ptype   = type_cell.get_text(" ", strip=True) if type_cell else ""
            reason  = reason_cell.get_text(" ", strip=True) if reason_cell else ""
            company = company_cell.get_text(" ", strip=True) if company_cell else ""

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
            "FDA listing: %d pathogen recalls in %d-day window "
            "(rows scanned=%d, skipped: no_link=%d bad_url=%d "
            "unparseable_date=%d unparseable_row=%d old=%d "
            "non_food=%d non_pathogen=%d)",
            len(out), since_days, rows_total,
            skipped_no_link, skipped_bad_url,
            skipped_unparseable_date, skipped_unparseable_row,
            skipped_old, skipped_non_food, skipped_non_pathogen,
        )
        return out
