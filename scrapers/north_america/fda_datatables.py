"""FDA datatables JSON — Layer 1 canonical source for FDA recalls.

WHY THIS SCRAPER EXISTS (audit 2026-05-09)
==========================================
The FDA recalls page at /safety/recalls-market-withdrawals-safety-alerts
is rendered from a Drupal datatables widget. The same data is exposed at
multiple machine-readable URLs (?_format=json, ?_format=xml, ?_format=csv —
these are the same URLs the page's "Download" button hits when a user
clicks it in a browser).

This is the **canonical source** ("hero" source-of-truth) for the live
recall list. Three reasons it's better than RSS or the HTML scrape:

  1. **Bot-detection surface.** Akamai's bot rules fingerprint browsing
     (Accept: text/html, sec-fetch-dest: document, etc). The download
     endpoints are typically EXCLUDED from the same fingerprint heuristics
     because they're served as binary/structured downloads — different
     code path on FDA's CDN. We've seen this gap before with other
     Akamai sites (CDC, USDA NASS): the page is bot-blocked, the
     download URL isn't.

  2. **Structured fields.** RSS gives us title + description + pubDate.
     The datatables endpoint gives us discrete columns: Date, Brand
     Name, Product Description, Product Type, Recall Reason Description,
     Company Name, Terminated, Excerpt — same shape as the visible
     table on the page. No regex needed to recover Company name from
     a title string.

  3. **Sortable, complete.** The datatables endpoint returns ALL active
     recalls (not just the 25 most recent that the RSS shows). For
     SINCE_DAYS=7 windows we still see everything in scope.

Layered architecture
--------------------
This scraper sits at Layer 1. The full FDA architecture after this lands:

    Layer 1 (NEW, this file)  → datatables JSON  → fresh, structured, fast
    Layer 2 (fda_press.py)    → RSS feed         → fresh, redundant, smaller
    Layer 3 (fda.py)          → openFDA API     → 5–30d lag, brings classification

merge_master URL-dedupes across all three. If L1 ever breaks, L2 covers
in real time and L3 covers within a month. No single-point failure can
silently hide a recall for >24h.

Endpoint discovery
------------------
The exact endpoint URL behind the FDA datatables widget is not officially
documented and has changed over the years. We probe a list of CANDIDATE
endpoints in order, take the first one that returns valid JSON.

If FDA changes the endpoint shape, this scraper will log a clear
"unknown shape" warning and return 0 rows. The other two layers stay
intact, so worst case is reduced redundancy (still 2 of 3 layers live).

Operator: validate the response shape on first run by checking workflow
logs for the "FDA datatables: shape=" line. If shape is "unknown",
inspect the JSON manually and add a parser branch in _parse_payload().
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Sequence
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


# Akamai bypass — same set as scrapers/north_america/fda_press.py.
# We duplicate (not import) so the two scrapers can be edited independently
# if Akamai's fingerprinting changes; both will need to evolve together
# but the deployment cadence shouldn't be coupled.
_AKAMAI_BYPASS_HEADERS = {
    "sec-ch-ua": (
        '"Not)A;Brand";v="99", "Google Chrome";v="127", '
        '"Chromium";v="127"'
    ),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",        # different from page-fetch (was 'document')
    "sec-fetch-mode": "cors",         # download is XHR, not navigation
    "sec-fetch-site": "same-origin",
    "Cache-Control": "no-cache",
}

# Outbreak token set — same as fda_press.py.
_OUTBREAK_TOKENS = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses",
)

# Food-context filter — copy of fda_press._FOOD_CONTEXT_TOKENS for parity.
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
    "recalls", "recalled", "recall of", "recalls because",
    "voluntarily recalls", "voluntary recall",
    "issues recall", "issues alert",
)

# URL prefixes that count as legitimate FDA recall links — match fda_press.py
_ACCEPTABLE_URL_PREFIXES = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/",
    "https://www.fda.gov/news-events/press-announcements/",
    "https://www.fda.gov/food/alerts-advisories-safety-information/",
)

# Field-name candidates for each logical column. The datatables payload
# may use different keys depending on the Drupal view config; we try each
# in order. First non-empty match wins.
#
# These are based on FDA's published openFDA enforcement schema + the
# column headings visible on the recalls page itself. If the actual
# response uses different keys, _parse_payload's "unknown shape" warning
# will fire and the operator can extend these tuples.
_FIELD_CANDIDATES = {
    "date":       ("field_change_date_2", "date", "Date", "field_recall_date",
                   "date_initiated", "field_date_posted"),
    "brand":      ("field_brand_name", "brand", "Brand Name", "brand_name"),
    "product":    ("field_product_description", "product",
                   "Product Description", "product_description"),
    "reason":     ("field_recall_reason_descrip", "reason",
                   "Recall Reason Description", "reason_for_recall"),
    "company":    ("field_company_name", "company", "Company Name",
                   "recalling_firm"),
    "url_slug":   ("field_recall_url", "url", "URL", "view_node"),
    "excerpt":    ("excerpt", "Excerpt", "field_summary"),
    "product_type": ("field_regulated_product_field", "product_type",
                     "Product Type"),
    "terminated": ("field_terminated_recall", "terminated", "Terminated"),
}


def _first(rec: dict, keys: Sequence[str], default: str = "") -> str:
    """Return the first non-empty stringified value from any candidate key."""
    for k in keys:
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def _strip_html(s: str) -> str:
    """FDA datatables fields sometimes contain wrapping <a> or <time> tags."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_date(s: str) -> Optional[datetime]:
    """Parse the assortment of date formats FDA emits.

    Observed in production: '05/08/2026', '2026-05-08', 'May 8, 2026',
    '2026-05-08T00:00:00', and the occasional '<time datetime="2026-05-08">'
    wrapper (which _strip_html will already have removed by the time we get
    here, but the inner formatted text remains).
    """
    if not s:
        return None
    s = s.strip()
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(s[:len(fmt)+5], fmt).replace(
                hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            continue
    return None


def _detect_outbreak(text_lower: str) -> int:
    return 1 if any(t in text_lower for t in _OUTBREAK_TOKENS) else 0


class FDADatatablesScraper(BaseScraper):
    """Layer 1 — reads FDA's datatables-format recall list directly.

    Fast (single GET), structured (no HTML regex), and not subject to the
    Akamai page-rendering fingerprint that broke fda_press.py for ~3 weeks
    in April–May 2026.
    """

    AGENCY = "FDA"
    COUNTRY = "USA"

    # Candidate endpoints, tried in order. The first that returns valid JSON
    # with a recognisable shape wins.
    #
    # The query-string variants (?_format=json) are Drupal's standard
    # format-alias routing; the bare /datatables/json/ path is an internal
    # Views REST endpoint observed on similar fda.gov pages. We probe both
    # so a reshuffle on FDA's side doesn't take us down.
    _CANDIDATE_ENDPOINTS = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts?_format=json",
        "https://www.fda.gov/datatables/json/recalls-market-withdrawals-safety-alerts",
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/datatables-data?_format=json",
    )

    PATHOGEN_KEYWORDS = for_languages("en")

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = datetime.utcnow() - timedelta(days=since_days)

        payload = self._fetch_first_working_endpoint()
        if payload is None:
            log.warning(
                "FDA datatables: no candidate endpoint returned valid JSON "
                "(tried %d) — returning 0 rows. fda_press.py + fda.py "
                "still cover at L2/L3.",
                len(self._CANDIDATE_ENDPOINTS),
            )
            return []

        records = self._records_from_payload(payload)
        if not records:
            log.warning(
                "FDA datatables: payload had no records, shape=%s. "
                "Operator: inspect logs and extend _parse_payload() if shape changed.",
                self._payload_shape(payload),
            )
            return []

        log.info("FDA datatables: %d total records in payload (shape=%s)",
                 len(records), self._payload_shape(payload))

        out: List[Recall] = []
        skipped_non_food = 0
        skipped_non_pathogen = 0
        skipped_old = 0
        skipped_no_url = 0

        for rec in records:
            if not isinstance(rec, dict):
                continue
            row = self._row_from_record(rec, cutoff)
            if row is None:
                continue
            kind, recall = row
            if kind == "non_food":
                skipped_non_food += 1
            elif kind == "non_pathogen":
                skipped_non_pathogen += 1
            elif kind == "old":
                skipped_old += 1
            elif kind == "no_url":
                skipped_no_url += 1
            elif kind == "ok":
                out.append(recall)

        log.info(
            "FDA datatables: %d pathogen recalls in window (since_days=%d). "
            "Skipped: non_food=%d non_pathogen=%d old=%d no_url=%d",
            len(out), since_days,
            skipped_non_food, skipped_non_pathogen, skipped_old, skipped_no_url,
        )
        return out

    # ------------------------------------------------------------------
    def _fetch_first_working_endpoint(self) -> Optional[object]:
        for url in self._CANDIDATE_ENDPOINTS:
            log.debug("FDA datatables: trying %s", url)
            r = fetch(self.session, url, headers=self._json_headers())
            if r is None:
                log.warning("FDA datatables: no response from %s", url)
                continue
            if r.status_code != 200:
                log.warning("FDA datatables: %s returned HTTP %d", url, r.status_code)
                continue
            try:
                payload = r.json()
            except ValueError:
                # Some endpoints return JSON-compatible bodies as text; try anyway.
                import json as _json
                try:
                    payload = _json.loads(r.text)
                except Exception:
                    log.warning("FDA datatables: %s returned non-JSON (len=%d)",
                                url, len(r.text))
                    continue
            log.info("FDA datatables: hit %s (HTTP 200, %d bytes)",
                     url, len(r.content))
            return payload
        return None

    @classmethod
    def _json_headers(cls) -> dict:
        h = dict(_AKAMAI_BYPASS_HEADERS)
        h["Accept"] = "application/json, text/javascript, */*; q=0.01"
        h["X-Requested-With"] = "XMLHttpRequest"
        return h

    # ------------------------------------------------------------------
    @staticmethod
    def _payload_shape(payload: object) -> str:
        """Compact shape descriptor for log lines."""
        if isinstance(payload, list):
            inner = type(payload[0]).__name__ if payload else "empty"
            return f"list[{inner}]"
        if isinstance(payload, dict):
            keys = sorted(payload.keys())
            return f"dict[{','.join(keys[:5])}{'...' if len(keys) > 5 else ''}]"
        return type(payload).__name__

    @staticmethod
    def _records_from_payload(payload: object) -> List[dict]:
        """Normalise the various Drupal/datatables payload shapes to a list[dict].

        Known shapes:
          1. Bare list:           [{...}, {...}]
          2. {"data": [...]}      DataTables server-side format
          3. {"rows": [...]}      Some Drupal Views REST endpoints
          4. {"results": [...]}   openFDA-style (unlikely here but cheap to check)
        """
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("data", "rows", "results", "items", "records"):
                v = payload.get(key)
                if isinstance(v, list):
                    return [r for r in v if isinstance(r, dict)]
        return []

    # ------------------------------------------------------------------
    def _row_from_record(self, rec: dict, cutoff: datetime):
        """Convert one source record to (kind, Recall|None).

        kind ∈ {ok, non_food, non_pathogen, old, no_url}. Used by the caller
        only for skip-counter logging; non-ok rows are discarded.
        """
        date_raw    = _strip_html(_first(rec, _FIELD_CANDIDATES["date"]))
        brand       = _strip_html(_first(rec, _FIELD_CANDIDATES["brand"]))
        product     = _strip_html(_first(rec, _FIELD_CANDIDATES["product"]))
        reason      = _strip_html(_first(rec, _FIELD_CANDIDATES["reason"]))
        company     = _strip_html(_first(rec, _FIELD_CANDIDATES["company"]))
        excerpt     = _strip_html(_first(rec, _FIELD_CANDIDATES["excerpt"]))
        prod_type   = _strip_html(_first(rec, _FIELD_CANDIDATES["product_type"])).lower()
        url_slug    = _first(rec, _FIELD_CANDIDATES["url_slug"])

        # 1. URL — required. Slug may be relative (/safety/...) or absolute.
        url = url_slug.strip()
        if url and url.startswith("/"):
            url = "https://www.fda.gov" + url
        if not url or not any(url.startswith(p) for p in _ACCEPTABLE_URL_PREFIXES):
            return ("no_url", None)

        # 2. Product type — drop drugs/devices/cosmetics/tobacco fast.
        # `product_type` is FDA's own classification. When present, trust it.
        # When absent, fall back to the food-keyword scan below.
        if prod_type and "food" not in prod_type and "biolog" not in prod_type:
            return ("non_food", None)

        # 3. Pathogen filter — at least one keyword in reason+product+excerpt.
        haystack = (reason + " " + product + " " + excerpt).lower()
        matched_kw = next(
            (kw for kw in self.PATHOGEN_KEYWORDS if kw in haystack),
            None,
        )
        if not matched_kw:
            return ("non_pathogen", None)

        # 4. Food-context fallback — if product_type was empty, ensure a
        # food token appears somewhere. Same defensive pattern as fda_press.py.
        if not prod_type:
            if not any(t in haystack for t in _FOOD_CONTEXT_TOKENS):
                return ("non_food", None)

        # 5. Date — required AND must be within window.
        d = _parse_date(date_raw)
        if d is None:
            log.debug("FDA datatables: unparseable date %r for %s", date_raw, url)
            return ("old", None)  # treat unparseable as out-of-window
        if d < cutoff:
            return ("old", None)

        # 6. Outbreak detection
        outbreak = _detect_outbreak(haystack)

        # 7. Build Recall — same _new_recall contract as fda_press.py.
        return (
            "ok",
            self._new_recall(
                Date=d.strftime("%Y-%m-%d"),
                Company=(company or brand or "")[:150],
                Brand=(brand or "—")[:120],
                Product=(product or excerpt or "")[:300],
                Pathogen=matched_kw,           # canonicalised by _new_recall
                Reason=(reason or excerpt or product)[:400],
                Class="Recall",
                URL=url,
                Outbreak=outbreak,
                Notes="FDA datatables (Layer 1 canonical source)",
            ),
        )
