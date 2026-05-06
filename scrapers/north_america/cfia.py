"""CFIA Canada food recall scraper — RSS-first with HTML listing fallback.

WHY THIS REPLACES THE PREVIOUS VERSION (audit 2026-05-06)
==========================================================
Production data shows zero CFIA rows captured between 2026-04-28 and
2026-05-06, while CFIA's listing page at
https://recalls-rappels.canada.ca/en?page=%2C1 shows 4-5 new recalls in
that window. The previous scraper silently returned zero rows. Three
known failure modes for the previous code, all silent:

1. **Namespace blindness.** ``ET.fromstring(...)`` returns a root tag
   like ``{http://www.w3.org/2005/Atom}feed`` if CFIA migrates to Atom.
   ``root.iter("item")`` returns 0 elements (RSS 2.0 only) — every
   entry is invisible. No error logged.

2. **Date-format brittleness.** Previous code tried only RFC-822
   ``%a, %d %b %Y %H:%M:%S %z`` and ``GMT`` literal. Any feed change
   to ISO 8601 (``2026-05-05T14:00:00Z``) → every item silently
   skipped at ``log.debug`` level (workflow doesn't surface debug).

3. **No HTTP status check.** ``fetch()`` returns 4xx/5xx responses
   the same as 200. WAF blocks (403 + HTML "Access Denied" body) get
   passed to ``ET.fromstring`` which raises ParseError → 1 WARNING
   log line, then return [] silently.

THIS VERSION
============
- HTTP status check first; non-200 → WARNING + try fallback.
- Content-type / first-byte check; HTML body → fall through to HTML
  listing scraper instead of trying to parse HTML as XML.
- Format auto-detect: try RSS 2.0 ``<item>``, then Atom 1.0 ``<entry>``
  with namespace-aware iteration, then HTML listing.
- Date parser supports 7 formats including ISO 8601 with Z and offset.
- HTML listing fallback parses
  https://recalls-rappels.canada.ca/en?page=%2C1 — the URL CFIA
  actually serves to humans. Extracts ``/en/alert-recall/<slug>``
  hrefs + the inline metadata table.
- Loud logging at every step. Operator can grep workflow logs for
  ``CFIA:`` to see exactly which path executed and how many items
  came out.
- Same downstream contract as before: returns List[Recall] with
  ``Pathogen`` set to a CORE keyword (canonicalised by ``_new_recall``),
  ``Outbreak`` detected bilingually, Company normalised.
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


class CFIAScraper(BaseScraper):
    """CFIA Canada food-recall scraper (RSS-first, HTML fallback)."""

    AGENCY = "CFIA"
    COUNTRY = "Canada"

    # RSS feed first. CFIA has historically served RSS 2.0 here.
    FEED_URLS = (
        "https://recalls-rappels.canada.ca/en/rss.xml",
        "https://recalls-rappels.canada.ca/en/rss",
        "https://recalls-rappels.canada.ca/rss.xml",
    )

    # HTML listing — the page operators visit. Used when RSS is dead
    # or returns zero items.
    LISTING_URL = "https://recalls-rappels.canada.ca/en?page=%2C1"

    PATHOGEN_KEYWORDS = _PATHOGEN_KEYWORDS

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # Try every RSS endpoint first
        for feed_url in self.FEED_URLS:
            log.info("CFIA: trying feed %s", feed_url)
            rows = self._scrape_feed(feed_url, since_days)
            if rows:
                log.info("CFIA: feed %s returned %d rows", feed_url, len(rows))
                return rows
            log.warning("CFIA: feed %s returned 0 rows", feed_url)

        # All RSS feeds dead → HTML fallback
        log.warning("CFIA: all RSS feeds returned 0 rows; trying HTML listing")
        return self._scrape_html_listing(since_days)

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
