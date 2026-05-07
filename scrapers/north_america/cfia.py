"""CFIA Canada food recall scraper — open-data first, RSS, HTML.

WHY THIS REPLACES THE PREVIOUS VERSION (audits 2026-05-06, 2026-05-07)
======================================================================
Production data showed zero CFIA rows captured between 2026-04-28 and
2026-05-07 across every shape of the previous code. Root causes, in
order of damage:

1. **All three RSS URLs were 404.** The previous list (/en/rss.xml,
   /en/rss, /rss.xml) was a guess inherited from older versions —
   none of those paths has ever existed on recalls-rappels.canada.ca.
   The canonical food RSS feed is published at
   ``/en/feed/cfia-alerts-recalls`` (see ``/en/rss-feeds`` for the
   directory). Verified 2026-05-07.

2. **HTML fallback rejected every slug on date filter.** With the
   orchestrator running ``since_days=2``, the cutoff sits inside the
   homepage's "past 7 days" widget but the widget's items often lag
   the actual feed by 5-15 days, so no slug ever passed the date gate.

3. **Namespace blindness / date-format brittleness / no HTTP status
   check.** The pre-2026-05-06 version had these three failure modes
   too — see git history for that audit.

THIS VERSION
============
Three-layer fallback, mirroring the RappelConso pattern:

  L1 — **Open-data bulk JSON** (preferred; daily-refreshed structured feed)
       https://recalls-rappels.canada.ca/sites/default/files/
         opendata-donneesouvertes/HCRSAMOpenData.json
       Same dataset that powers the recalls-rappels.canada.ca site.
       Documented at open.canada.ca dataset GUID
       d38de914-c94c-429b-8ab1-8776c31643e3, license OGL-Canada.
       Carries structured ``Organization``, ``Issue``, ``Recall class``,
       ``Last updated``, ``Archived`` fields → no text parsing required
       to filter for CFIA food + pathogen + active + date window.

  L2 — **RSS feed** (canonical, descriptive slug, format auto-detect)
       Falls back if open-data fetch fails (CDN issue, JSON shape
       drift, etc.). RSS 2.0 ``<item>`` and Atom 1.0 ``<entry>``
       both supported.

  L3 — **HTML listing** (last resort)
       Parses /en?page=%2C1 for ``/en/alert-recall/<slug>`` hrefs.
       Used only when both L1 and L2 are dead — claude-check is
       expected to enrich each row's date and pathogen.

All three layers carry the same downstream contract: List[Recall]
with ``Pathogen`` set to a CORE keyword (canonicalised by
``_new_recall``), ``Outbreak`` detected bilingually, Company normalised.

Loud logging at every layer transition. Operator can grep workflow
logs for ``CFIA:`` to see exactly which path executed.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages
from scrapers._company_normalise import normalise_company_brand

log = logging.getLogger(__name__)


_OUTBREAK_TOKENS_EN = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses",
)
_OUTBREAK_TOKENS_FR = (
    "éclosion", "eclosion", "personnes malades",
    "lié à des malad", "liée à des malad", "cas de malad",
)


def _detect_outbreak(merged_lower: str) -> int:
    if any(t in merged_lower for t in _OUTBREAK_TOKENS_EN):
        return 1
    if any(t in merged_lower for t in _OUTBREAK_TOKENS_FR):
        return 1
    return 0


# Words that ``for_languages("en","fr")`` returns as "keywords" but which
# are NOT actual pathogens — they're recall meta-vocabulary (verbs, alert
# nouns, regulator-status words). The pathogen vocab includes these so
# the regex layer can detect "this is a recall notice" generically.
# For our purposes — picking a pathogen value to put in the Pathogen
# field — they're noise. Filter them out.
_PATHOGEN_META_NOISE = frozenset({
    "recall", "recalled", "recalls", "recalling",
    "rappel", "rappels", "retrait", "retiré",
    "alert", "alerts", "warning", "warnings",
    "alerta", "alertas", "alertă",
    "avertizare",
    "advisory", "advisories",
    "withdrawal", "withdrawals",
    "notice", "notices", "notification", "notifications",
})


def _matched_pathogen_keyword(text_lower: str,
                              keywords: Tuple[str, ...]) -> Optional[str]:
    """Find the first PATHOGEN keyword in text. Skips meta-vocabulary
    (recall/alert/rappel/etc.) which is in the vocab for regex-detection
    purposes but is not a real pathogen."""
    for kw in keywords:
        if kw in _PATHOGEN_META_NOISE:
            continue
        if kw in text_lower:
            return kw
    return None


def _split_company_brand_from_title(title: str) -> Tuple[str, str]:
    """Extract (Company, Brand) from a CFIA-style recall title.

    CFIA titles follow a small set of templates. In rough order of frequency:

      1. "<BRAND> brand <PRODUCT> recalled due to <PATHOGEN>"
         e.g. "Auricchio brand Taleggio D.O.P. Cheese recalled due to Listeria"
              "7-Eleven brand Sandwiches, Subs, and Wraps recalled..."
         → Brand = token(s) before " brand ", Company = ""

      2. "<COMPANY> recalls <PRODUCT> ..." / "... recalled by <COMPANY>"
         e.g. "Fresh Start Foods brand salads recalled..." (overlaps #1)
         → Company = first chunk

      3. "Various brands of <CATEGORY> recalled due to <PATHOGEN>"
         e.g. "Various brands of cheese products recalled due to Listeria"
         → Brand = "Various", Company = ""

      4. "<PRODUCT> recalled due to <PATHOGEN>" (no brand/company token)
         e.g. "Pistachio Kernel recalled due to Salmonella"
         → leave both empty; downstream enrichment can fetch detail page

    The result is then passed through ``normalise_company_brand`` to apply
    the project-wide normalisation (suffixes, casing, brand-vs-company
    disambiguation).
    """
    if not title:
        return ("", "—")
    t = title.strip()

    # Template 3: "Various brands of ..."
    m = re.match(r"^Various\s+brands?\s+of\b", t, re.I)
    if m:
        return normalise_company_brand("", "Various")

    # Template 1: "<BRAND> brand <PRODUCT> ..."
    m = re.match(r"^(.+?)\s+brand\s+", t, re.I)
    if m:
        brand_chunk = m.group(1).strip()[:80]
        return normalise_company_brand("", brand_chunk)

    # Template 2: "<COMPANY> recalls ..."
    m = re.match(r"^(.+?)\s+recalls?\s+", t, re.I)
    if m:
        company_chunk = m.group(1).strip()[:100]
        return normalise_company_brand(company_chunk, "—")

    # Template 4 / unknown — leave empty, claude-check enriches
    return normalise_company_brand("", "—")


_GENERIC_URL_SUBSTRINGS = (
    "/search/site",
    "/recherche?",
    "/recherche/",
    "/page/",
    "page=",
)


def _is_generic_url(url: str) -> bool:
    if not url:
        return True
    u = url.lower()
    if any(p in u for p in _GENERIC_URL_SUBSTRINGS):
        return True
    # Bare landing pages
    bare = u.rstrip("/").split("?", 1)[0]
    if bare in (
        "https://recalls-rappels.canada.ca/en",
        "https://recalls-rappels.canada.ca/fr",
        "https://recalls-rappels.canada.ca",
    ):
        return True
    return False


def _parse_pubdate(s: str) -> Optional[datetime]:
    """Try every plausible date format CFIA might emit, return naive UTC.

    Audit 2026-05-06: previous version handled 2 formats. Now handles 7,
    covering RSS 2.0 (RFC-822), Atom 1.0 (RFC-3339 / ISO 8601), and a
    date-only fallback for HTML listing scrape.
    """
    if not s:
        return None
    s = s.strip()
    formats = (
        "%a, %d %b %Y %H:%M:%S %z",     # RFC-822 with offset
        "%a, %d %b %Y %H:%M:%S GMT",    # RFC-822 GMT literal
        "%a, %d %b %Y %H:%M:%S",        # RFC-822 no zone
        "%Y-%m-%dT%H:%M:%S%z",          # ISO 8601 with offset (+00:00)
        "%Y-%m-%dT%H:%M:%SZ",           # ISO 8601 with Z literal
        "%Y-%m-%dT%H:%M:%S",            # ISO 8601 no zone
        "%Y-%m-%d",                     # date-only
    )
    for fmt in formats:
        try:
            d = datetime.strptime(s, fmt)
            if d.tzinfo is not None:
                # Strip tzinfo for naive UTC comparison
                d = d.replace(tzinfo=None)
            return d
        except ValueError:
            continue
    # Last resort — peel off timezone abbreviation if present
    m = re.match(r"^(.+?)\s+([A-Z]{2,4})$", s)
    if m:
        try:
            return datetime.strptime(m.group(1).strip(),
                                     "%a, %d %b %Y %H:%M:%S")
        except ValueError:
            pass
    return None


# Realistic CFIA pathogen + food-context filters
_PATHOGEN_KEYWORDS = for_languages("en", "fr")

_FOOD_CONTEXT_TOKENS = (
    "food", "aliment", "recall", "rappel",
    # Pathogens double as food signals
    "salmon", "listeria", "e. coli", "stec", "botulin",
    # Bilingual food categories
    "viande", "fromage", "poisson", "lait", "produit laitier",
    "meat", "cheese", "fish", "milk", "dairy",
    "chicken", "beef", "pork", "turkey",
    "vegetable", "vegetables", "fruit", "fruits",
    "spinach", "lettuce", "tomato", "onion",
    "snack", "snacks", "chips", "crackers",
    "bakery", "bread", "cake", "biscuit",
    "candy", "chocolate", "confection",
    "spice", "spices", "herb", "herbs",
    "sauce", "soup", "stew",
    "infant", "baby food",
    "rte", "ready to eat", "ready-to-eat",
    "frozen", "deli",
    "supplement", "powder",
    "egg", "œuf", "oeuf",
    "rice", "pasta", "noodle", "cereal", "granola",
    "pistachio", "almond", "peanut", "nut",
)


# ---------------------------------------------------------------------------
# Open-data JSON — pathogen filtering on the structured `Issue` field
# ---------------------------------------------------------------------------
# The HCRSAMOpenData.json feed gives us the recall's hazard category as a
# typed value, no NLP needed. The dataset's tag taxonomy uses prefixed
# forms ("Microbiological Listeria", "Microbiological E. coli", etc.) but
# the per-row `Issue` field uses simpler values ("Listeria", "Salmonella",
# "E. coli", "Extraneous Material", "Non harmful (quality or spoilage)").
# We accept either by lower-cased substring match.
#
# Excluded on purpose:
#   - "Allergen *"            → not a microbial pathogen
#   - "Extraneous Material"   → glass / insects / metal etc. — not pathogen
#   - "Non harmful (quality or spoilage)" → mould / staleness — not pathogen
#   - "Chemical" / "Labelling" / "Tampering" → not pathogen
#
# Genus-level "Microbiological" (with no specific organism) is INCLUDED —
# CFIA occasionally uses it for early notifications before speciation.
_PATHOGEN_ISSUE_TOKENS = (
    "listeria",
    "salmonella",
    "e. coli", "e.coli", "escherichia coli", "stec",
    "clostridium", "botulin",
    "staphylococcus",
    "bacillus cereus", "cereulide",
    "cronobacter",
    "marine biotoxin", "biotoxin",
    "norovirus",
    "hepatitis",
    "vibrio",
    "cyclospora",
    "shigella",
    "campylobacter",
    "yersinia",
    "microbiological",   # generic — accept
)


def _is_pathogen_issue(issue: str) -> bool:
    """True if the open-data ``Issue`` value indicates a microbial /
    biological food-safety hazard (vs allergen, extraneous material,
    quality, chemical, labelling)."""
    if not issue:
        return False
    issue_lower = issue.lower()
    return any(tok in issue_lower for tok in _PATHOGEN_ISSUE_TOKENS)


class CFIAScraper(BaseScraper):
    """CFIA Canada food-recall scraper (open-data → RSS → HTML)."""

    AGENCY = "CFIA"
    COUNTRY = "Canada"

    # ── Layer 1 ── Open-data bulk JSON. Daily-refreshed by Health Canada.
    # Documented at open.canada.ca dataset GUID
    # d38de914-c94c-429b-8ab1-8776c31643e3 (license: OGL-Canada).
    # Schema (per row, observed 2026-05-07):
    #   NID            unique numeric ID
    #   Title          full descriptive title
    #   URL            canonical https://recalls-rappels.canada.ca/en/...
    #   Organization   "CFIA" | "Consumer product safety" | "Medical devices" |
    #                  "Drugs and health products" — we keep CFIA only
    #   Product        product name
    #   Issue          structured hazard category (see _PATHOGEN_ISSUE_TOKENS)
    #   Category       finer taxonomy (e.g., "Dairy", "Multiple food items")
    #   Recall class   "Class 1" | "Class 2" | "Class 3" (food)
    #                  or "Type I" | "Type II" | "Type III" (devices) — we
    #                  only see Class * here because we filter Org=CFIA
    #   Last updated   "YYYY-MM-DD"
    #   Archived       "0" (active) | "1" (archived)
    OPEN_DATA_JSON_URL = (
        "https://recalls-rappels.canada.ca/sites/default/files/"
        "opendata-donneesouvertes/HCRSAMOpenData.json"
    )

    # ── Layer 2 ── RSS feed.
    #
    # 2026-05-07 audit: the previous URL list (/en/rss.xml, /en/rss,
    # /rss.xml) was a set of guesses inherited from older versions of
    # this scraper. None of those paths has ever existed on
    # recalls-rappels.canada.ca — every run since at least 2026-04-28
    # got HTTP 404 from all three, then dropped to the HTML fallback,
    # which itself produces 0 rows when since_days < ~10 because the
    # homepage's "past 7 days" widget often lags real dates.
    #
    # The canonical RSS feeds are listed at
    # https://recalls-rappels.canada.ca/en/rss-feeds — Canada.ca uses
    # descriptive slugs (not generic "rss.xml"), one feed per category.
    # For food recalls the feed is `cfia-alerts-recalls`. Returns
    # `application/rss+xml; charset=utf-8`. Verified 2026-05-07.
    #
    # We list English first; French is a defensive secondary. Recall
    # items themselves are bilingual on Canada.ca, so EN is sufficient
    # for normal operation.
    #
    # If Canada.ca ever migrates again, the open-data dataset at
    # https://open.canada.ca/data/en/dataset/d38de914-c94c-429b-8ab1-8776c31643e3
    # carries the same data as daily-updated CSV/JSON — preferred path
    # for a future rewrite.
    FEED_URLS = (
        "https://recalls-rappels.canada.ca/en/feed/cfia-alerts-recalls",
        "https://recalls-rappels.canada.ca/fr/fil-de-nouvelles/acia-rappels-avis-securite",
    )

    # ── Layer 3 ── HTML listing — the page operators visit. Last resort.
    LISTING_URL = "https://recalls-rappels.canada.ca/en?page=%2C1"

    PATHOGEN_KEYWORDS = _PATHOGEN_KEYWORDS

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # ── L1 — open-data bulk JSON ──
        # Returns None on fetch/parse failure (fall through);
        # returns [] on legit-empty (don't fall through — empty is real);
        # returns [recall, ...] on success.
        log.info("CFIA: L1 open-data %s", self.OPEN_DATA_JSON_URL)
        rows = self._try_open_data_json(since_days)
        if rows is not None:
            log.info("CFIA: L1 open-data returned %d rows — done", len(rows))
            return rows
        log.warning("CFIA: L1 open-data unavailable; falling through to RSS")

        # ── L2 — RSS feeds ──
        for feed_url in self.FEED_URLS:
            log.info("CFIA: L2 trying feed %s", feed_url)
            rows = self._scrape_feed(feed_url, since_days)
            if rows:
                log.info("CFIA: L2 feed %s returned %d rows", feed_url, len(rows))
                return rows
            log.warning("CFIA: L2 feed %s returned 0 rows", feed_url)

        # ── L3 — HTML listing ──
        log.warning("CFIA: all RSS feeds returned 0 rows; trying L3 HTML listing")
        return self._scrape_html_listing(since_days)

    # ------------------------------------------------------------------
    # Layer 1 — open-data bulk JSON
    # ------------------------------------------------------------------
    def _try_open_data_json(self, since_days: int) -> Optional[List[Recall]]:
        """Fetch and filter the Health Canada open-data recalls JSON.

        Returns:
          - ``None`` on transport / parse / schema-shape failure
            (caller should fall through to next layer)
          - ``[]`` when the fetch succeeded but no row matches our filter
            (legit-empty — caller should NOT fall through)
          - ``[Recall, ...]`` on success
        """
        # The full file is ~20 MB (≈18,900 active records). Fetching every
        # run is consistent with how RappelConso L3 operates (~17.5 K
        # records). dedup_master dedupes downstream by URL so re-fetches
        # are free.
        r = fetch(self.session, self.OPEN_DATA_JSON_URL)
        if r is None:
            log.warning("CFIA L1: fetch returned None")
            return None
        if r.status_code != 200:
            log.warning("CFIA L1: HTTP %d", r.status_code)
            return None
        try:
            data = r.json()
        except Exception as e:
            log.warning("CFIA L1: JSON parse failed: %s", e)
            return None
        if not isinstance(data, list):
            log.warning("CFIA L1: unexpected schema (top-level type=%s)",
                        type(data).__name__)
            return None
        log.info("CFIA L1: %d total records in open-data feed", len(data))

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        skipped_org = skipped_archived = 0
        skipped_pathogen = skipped_date = skipped_url = 0

        for row in data:
            # Filter 1 — CFIA food only (drops Consumer product safety,
            # Medical devices, Drugs and health products, Cannabis, etc.)
            if (row.get("Organization") or "").strip() != "CFIA":
                skipped_org += 1
                continue

            # Filter 2 — active recalls only (Archived="0")
            if str(row.get("Archived", "0")).strip() != "0":
                skipped_archived += 1
                continue

            # Filter 3 — structured pathogen Issue
            issue = (row.get("Issue") or "").strip()
            if not _is_pathogen_issue(issue):
                skipped_pathogen += 1
                continue

            # Filter 4 — date cutoff
            date_str = (row.get("Last updated") or "").strip()
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                skipped_date += 1
                continue
            if d < cutoff:
                skipped_date += 1
                continue

            # URL is the dedup key; reject empty / generic
            url = (row.get("URL") or "").strip()
            if not url or _is_generic_url(url):
                skipped_url += 1
                continue

            title = (row.get("Title") or "").strip()
            product = (row.get("Product") or "").strip()
            recall_class = (row.get("Recall class") or "").strip() or "Recall"
            nid = (row.get("NID") or "").strip()

            # Outbreak detection — open-data has no structured field for it,
            # so we run our existing bilingual token detector against the
            # title (the only narrative field that's reliably populated
            # for every row; "What you should do" is empty for many CFIA
            # rows and is consumer guidance, not investigation status).
            merged_lower = title.lower()
            outbreak = _detect_outbreak(merged_lower)

            # Company / brand from title. CFIA titles use either:
            #   "<COMPANY> brand <PRODUCT> recalled due to <PATHOGEN>"
            #   "<COMPANY> recalls <PRODUCT> ..."
            #   "Various brands of <CATEGORY> recalled due to <PATHOGEN>"
            # _split_company_brand_from_title handles all three.
            co, br = _split_company_brand_from_title(title)

            out.append(self._new_recall(
                Date=d.strftime("%Y-%m-%d"),
                Company=co,
                Brand=br,
                Product=(product or title)[:300],
                Pathogen=issue,         # canonicalised by _new_recall
                Reason=title[:400],
                Class=recall_class,
                URL=url,
                Outbreak=outbreak,
                Notes=f"CFIA open-data NID={nid}",
            ))

        log.info(
            "CFIA L1: %d pathogen recalls kept "
            "(skipped: %d non-CFIA, %d archived, %d non-pathogen, "
            "%d outside date window, %d bad/generic url)",
            len(out), skipped_org, skipped_archived,
            skipped_pathogen, skipped_date, skipped_url,
        )
        return out

    # ------------------------------------------------------------------
    def _scrape_feed(self, feed_url: str, since_days: int) -> List[Recall]:
        r = fetch(self.session, feed_url)
        if r is None:
            log.warning("CFIA: %s — fetch returned None", feed_url)
            return []
        if r.status_code != 200:
            log.warning("CFIA: %s — HTTP %d", feed_url, r.status_code)
            return []

        body = r.content
        # Body sanity check — reject HTML masquerading as XML (WAF redirect,
        # error page, etc.). A real RSS/Atom feed starts with `<?xml` or
        # `<rss` or `<feed`.
        head = body[:200].lstrip().lower()
        if not (head.startswith(b"<?xml") or head.startswith(b"<rss")
                or head.startswith(b"<feed") or head.startswith(b"<atom")):
            log.warning("CFIA: %s — body is not XML (first bytes: %r)",
                        feed_url, body[:60])
            return []

        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            log.warning("CFIA: %s — XML parse failed: %s", feed_url, e)
            return []

        # Format auto-detect:
        #   RSS 2.0 → root tag "rss", entries in <item> (no namespace)
        #   Atom 1.0 → root tag "{...Atom}feed", entries in <entry>
        items = list(root.iter("item"))
        atom_ns = "{http://www.w3.org/2005/Atom}"
        if not items:
            items = list(root.iter(f"{atom_ns}entry"))
            if items:
                log.info("CFIA: %s is Atom 1.0 (%d entries)",
                         feed_url, len(items))
                return self._parse_atom_entries(items, since_days, atom_ns)
        else:
            log.info("CFIA: %s is RSS 2.0 (%d items)", feed_url, len(items))
            return self._parse_rss_items(items, since_days)

        log.warning("CFIA: %s — unknown feed format (root=%s)",
                    feed_url, root.tag)
        return []

    # ------------------------------------------------------------------
    def _parse_rss_items(self, items, since_days: int) -> List[Recall]:
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen: set = set()
        skipped_date = skipped_filter = skipped_url = 0

        for item in items:
            try:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                desc = (item.findtext("description") or "").strip()
                pub = (item.findtext("pubDate") or "").strip()

                if not link or _is_generic_url(link) or link in seen:
                    skipped_url += 1
                    continue

                d = _parse_pubdate(pub)
                if d is None:
                    log.warning("CFIA RSS: unparseable pubDate %r — keeping "
                                "row (claude-check will fix Date)", pub)
                    d = datetime.utcnow()  # don't drop; let downstream review
                if d < cutoff:
                    skipped_date += 1
                    continue

                rec = self._build_recall(title, link, desc, d)
                if rec is None:
                    skipped_filter += 1
                    continue

                seen.add(link)
                out.append(rec)
            except Exception as e:
                log.warning("CFIA RSS item parse failed: %s", e)

        log.info("CFIA RSS: %d kept, %d skipped (filter=%d url=%d date=%d)",
                 len(out), skipped_date + skipped_filter + skipped_url,
                 skipped_filter, skipped_url, skipped_date)
        return out

    # ------------------------------------------------------------------
    def _parse_atom_entries(self, entries, since_days: int,
                            ns: str) -> List[Recall]:
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen: set = set()

        for entry in entries:
            try:
                title = (entry.findtext(f"{ns}title") or "").strip()
                # Atom links live in <link href="..."/> attributes
                link_elem = entry.find(f"{ns}link")
                link = ""
                if link_elem is not None:
                    link = (link_elem.get("href") or "").strip()
                summary = (entry.findtext(f"{ns}summary")
                           or entry.findtext(f"{ns}content")
                           or "").strip()
                pub = (entry.findtext(f"{ns}published")
                       or entry.findtext(f"{ns}updated")
                       or "").strip()

                if not link or _is_generic_url(link) or link in seen:
                    continue

                d = _parse_pubdate(pub)
                if d is None:
                    log.warning("CFIA Atom: unparseable date %r — keeping row",
                                pub)
                    d = datetime.utcnow()
                if d < cutoff:
                    continue

                rec = self._build_recall(title, link, summary, d)
                if rec is None:
                    continue
                seen.add(link)
                out.append(rec)
            except Exception as e:
                log.warning("CFIA Atom entry parse failed: %s", e)

        log.info("CFIA Atom: %d entries → %d recalls", len(entries), len(out))
        return out

    # ------------------------------------------------------------------
    def _build_recall(self, title: str, link: str, desc: str,
                      d: datetime) -> Optional[Recall]:
        """Apply pathogen + food-context filters and build a Recall."""
        # Strip HTML from desc (some feeds embed <p>, <a>, etc.)
        desc_text = re.sub(r"<[^>]+>", " ", desc or "")
        desc_text = re.sub(r"\s+", " ", desc_text).strip()
        merged = (title + " " + desc_text).lower()

        # Pathogen filter
        matched_kw = _matched_pathogen_keyword(merged, self.PATHOGEN_KEYWORDS)
        if not matched_kw:
            return None

        # Food-context filter
        if not any(tok in merged for tok in _FOOD_CONTEXT_TOKENS):
            return None

        # Company / brand extraction (bilingual verb match)
        m = re.match(
            r"^(?:Recall\s*[-–]\s*|Rappel\s*[-–]\s*"
            r"|Health\s+hazard\s+alert\s*[-–]\s*"
            r"|Allergy\s+alert\s*[-–]\s*"
            r"|Avis\s+[-–]\s*)?"
            r"(.+?)\s+(?:recalls?|brand|recalled|may contain|"
            r"rappelle|rappelés?|marque|peut contenir).*",
            title, re.I,
        )
        raw_company = (m.group(1).strip() if m
                       else title.split(" - ")[0].split(" – ")[0]).strip()
        co, br = normalise_company_brand(raw_company[:100], "—")

        outbreak = _detect_outbreak(merged)

        return self._new_recall(
            Date=d.strftime("%Y-%m-%d"),
            Company=co,
            Brand=br,
            Product=title[:300],
            Pathogen=matched_kw,
            Reason=desc_text[:400] or title[:400],
            Class="Recall",
            URL=link,
            Outbreak=outbreak,
            Notes="CFIA RSS",
        )

    # ------------------------------------------------------------------
    def _scrape_html_listing(self, since_days: int) -> List[Recall]:
        """Last-resort: parse the HTML listing page for recall slugs.

        Used when every RSS endpoint is dead/empty. Extracts every
        ``/en/alert-recall/<slug>`` href from the listing HTML and the
        inline metadata. claude-check will then fetch each detail page
        and fill in the structured fields.
        """
        r = fetch(self.session, self.LISTING_URL)
        if r is None or r.status_code != 200:
            log.error("CFIA: HTML listing fetch failed (status=%s)",
                      r.status_code if r else None)
            return []

        html = r.text
        # Find every recall-detail href. The listing renders cards like:
        #   <a href="/en/alert-recall/<slug>">...title...</a>
        slugs = re.findall(
            r'href="(/(?:en|fr)/alert-recall/[^"#?]+)"',
            html,
        )
        if not slugs:
            log.error("CFIA: HTML listing — no /alert-recall/ slugs found "
                      "(WAF block? page-structure change?)")
            return []

        # Dedup, cap at 50 most-recent (listing is reverse-chronological)
        seen_slugs: set = set()
        unique = []
        for slug in slugs:
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            unique.append(slug)
            if len(unique) >= 50:
                break

        # Match each slug to surrounding HTML context to pull title +
        # date if visible. Pattern: card containers usually carry the
        # date in a <time datetime="2026-05-05"> or similar element.
        # If we can't find a date, use today and let claude-check fix it.
        today = datetime.utcnow()
        cutoff = today - timedelta(days=since_days)
        out: List[Recall] = []

        for slug in unique:
            url = f"https://recalls-rappels.canada.ca{slug}"

            # Try to extract title: look for the slug in href, then walk
            # back to find the anchor's text content.
            title_match = re.search(
                rf'href="{re.escape(slug)}"[^>]*>\s*([^<]+?)\s*<',
                html,
            )
            title = (title_match.group(1).strip() if title_match
                     else slug.rsplit("/", 1)[-1].replace("-", " ").title())

            # Try to find a date attribute near the slug. CFIA listings
            # render dates in a few different patterns; we look in a
            # 1500-char window around the href.
            slug_pos = html.find(f'href="{slug}"')
            window_start = max(0, slug_pos - 750)
            window_end = min(len(html), slug_pos + 750)
            window = html[window_start:window_end]
            date_match = re.search(
                r'datetime="(\d{4}-\d{2}-\d{2})"', window
            )
            if not date_match:
                date_match = re.search(
                    r'(\d{4}-\d{2}-\d{2})', window
                )
            if date_match:
                d = _parse_pubdate(date_match.group(1)) or today
            else:
                d = today  # claude-check will fix

            if d < cutoff:
                continue

            # Build a candidate row; pathogen filter applied to title.
            # If pathogen isn't in title, send it to Pending anyway with
            # an empty Pathogen — claude-check fetches the detail page
            # and enriches.
            merged = title.lower()
            matched_kw = _matched_pathogen_keyword(
                merged, self.PATHOGEN_KEYWORDS,
            ) or ""

            co, br = normalise_company_brand(title.split(" recalls")[0][:100],
                                             "—")

            out.append(self._new_recall(
                Date=d.strftime("%Y-%m-%d"),
                Company=co,
                Brand=br,
                Product=title[:300],
                Pathogen=matched_kw,
                Reason=title[:400],
                Class="Recall",
                URL=url,
                Outbreak=_detect_outbreak(merged),
                Notes="CFIA HTML listing fallback — claude-check needs to "
                      "enrich Date+Pathogen",
            ))

        log.warning("CFIA: HTML listing fallback produced %d candidate rows "
                    "(claude-check must enrich)", len(out))
        return out
