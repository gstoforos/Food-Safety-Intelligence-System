"""FDA Press-Release Recalls RSS — fast path for fresh FDA recalls.

WHY THIS SCRAPER EXISTS (audit 2026-05-06)
==========================================
The existing scrapers/north_america/fda.py uses openFDA's
``api.fda.gov/food/enforcement.json`` endpoint, which has a known and
systematic publication delay: recalls only flow into it AFTER FDA has
formally classified them in the enforcement-reports cycle. Lag is
typically 5–30 days from press release to enforcement-endpoint visibility.

Concrete proof from production data (audit 2026-05-06):
  - 2026-05-04 + 2026-05-05 FDA recalls (incl. potato chips/Salmonella)
    were reported by foodsafetynews.com on 2026-05-05 22:27 UTC and
    landed in the FSIS NEWS sheet, but did NOT appear in our Recalls
    sheet — the openFDA scraper did not see them yet.
  - The same recalls had been live on
    fda.gov/safety/recalls-market-withdrawals-safety-alerts/ for hours.

This scraper closes that gap by reading FDA's official RSS feed of
recalls, market withdrawals & safety alerts, which updates within hours
of each press release. We KEEP scrapers/north_america/fda.py (openFDA)
running — it carries structured fields the press-release feed doesn't
expose (recall_number, classification, distribution_pattern). Both write
rows with Source="FDA"; merge_master dedupes by URL across them.

DESIGN DECISIONS
================
1. Pathogen + food filter — same shape as scrapers/north_america/cfia.py.
   Match a CORE pathogen keyword; require a food-context token; reject
   allergen-only via merge_master / claude-check downstream (we don't
   over-filter here — better to send to Pending and let the reviewers
   decide, since FDA RSS titles are often terse).
2. Outbreak detection — same EN tokens as CFIA but no FR (FDA is EN-only).
3. Date parsing — RSS pubDate is RFC-822; sometimes ``%a, %d %b %Y %H:%M:%S %z``
   sometimes ``GMT`` (handled both).
4. URL — taken from <link>. The recall page slug is stable; URL is the
   dedup key merge_master uses.
5. Class — RSS does not expose Class I/II/III. Leave as "Recall"; the
   openFDA scraper supplies that data when its slower path eventually
   ingests the same recall (merge_master keeps the first non-empty value
   per field on dedup).
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

log = logging.getLogger(__name__)


# Outbreak signal tokens (EN only — FDA RSS is English).
_OUTBREAK_TOKENS = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses",
)


def _detect_outbreak(merged_lower: str) -> int:
    return 1 if any(t in merged_lower for t in _OUTBREAK_TOKENS) else 0


def _matched_pathogen_keyword(text_lower: str,
                              keywords: Tuple[str, ...]) -> Optional[str]:
    for kw in keywords:
        if kw in text_lower:
            return kw
    return None


# Defensive URL filter — FDA RSS shouldn't link to anything generic but
# guard against feed-rendering glitches. Same patterns as
# merge_master._GENERIC_URL_PATTERNS for consistency.
_GENERIC_URL_SUBSTRINGS = (
    "/search/site",
    "/search?",
    "/page/",
    "page=",
)


def _is_generic_url(url: str) -> bool:
    if not url:
        return True
    u = url.lower()
    if any(p in u for p in _GENERIC_URL_SUBSTRINGS):
        return True
    # The bare landing page is also generic.
    bare = u.rstrip("/").split("?", 1)[0]
    if bare in (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
        "https://www.fda.gov/safety/recalls",
        "https://www.fda.gov/food/recalls-outbreaks-emergencies",
    ):
        return True
    return False


# Bilingual vocab is overkill for FDA — but using for_languages("en")
# means we share the same single source of truth as CFIA, USDA FSIS,
# FSANZ, FSA UK, and every other English-speaking regulator.
_PATHOGEN_KEYWORDS = for_languages("en")


# ─────────────────────────────────────────────────────────────────────────
# HTML fallback slug filtering (audit 2026-05-21)
# ─────────────────────────────────────────────────────────────────────────
# Background. When the FDA RSS feed is down, this scraper falls back to
# parsing the HTML recalls listing page. The 2026-05-21 audit found the
# previous regex href="(/safety/recalls-market-withdrawals-safety-alerts/[^"]+)"
# was matching SIX non-recall page-navigation links:
#
#   /safety/recalls-market-withdrawals-safety-alerts/recall-resources
#   /safety/recalls-market-withdrawals-safety-alerts/enforcement-reports
#   /safety/recalls-market-withdrawals-safety-alerts/industry-guidance-recalls
#   /safety/recalls-market-withdrawals-safety-alerts/major-product-recalls
#   /safety/recalls-market-withdrawals-safety-alerts/additional-information-about-recalls
#   /safety/recalls-market-withdrawals-safety-alerts/datatables-data?Randparam=…
#
# These leaked into Pending as fake recalls with Company="Recall Resources"
# etc. Plus three real-but-out-of-scope recalls (Doxorubicin = drug,
# Sensual Enhancement Capsules = supplement, Fly by Jing peanut cross-
# contact = allergen-only) sailed past because the HTML fallback didn't
# run the pathogen filter that _parse_item uses for RSS rows.
#
# Three filters together:
#   1. _FDA_NAV_SLUGS_BLOCKLIST — static list of known nav pages.
#   2. _RECALL_VERBS — slug must contain a recall verb (recalls, recalled,
#      voluntary-recall, issues-recall, initiates-voluntary, announces-recall).
#   3. _OUT_OF_SCOPE_SLUG_TOKENS — reject drug/allergen/quality slugs
#      (in AFTS scope: pathogens + biotoxins + mycotoxins + foreign material
#      + pest + chemical hazards; out of scope: drugs, allergen-only, quality).
# ─────────────────────────────────────────────────────────────────────────

_FDA_NAV_SLUGS_BLOCKLIST = frozenset({
    "recall-resources",
    "enforcement-reports",
    "industry-guidance-recalls",
    "major-product-recalls",
    "additional-information-about-recalls",
    "datatables-data",
    "voluntary-recall",            # share-link wrapper page itself
    "drug-shortage-product-search",
    "press-announcements",
    "outbreaks-and-advisories",
    "alerts-advisories-safety-information",
    "ireslibrary",
})

# Recall verbs that real FDA recall slugs always contain.
_RECALL_VERBS = (
    "recalls",
    "recalled",
    "recall-of",
    "voluntary-recall",
    "voluntarily-recalls",
    "voluntarily-issues",
    "issues-recall",
    "issues-voluntary",
    "initiates-voluntary",
    "initiates-recall",
    "announces-recall",
    "expands-recall",
    "expands-voluntary",
    "updates-recall",
    "amends-recall",
    "issues-public-health-alert",
)

# Tokens that mark out-of-scope items (drugs, allergen-only, quality).
# AFTS scope (per dashboard footer): pathogens + biotoxins + mycotoxins
# + foreign material + pest + chemical hazards. Allergen-only and
# quality/spoilage are excluded — even though the FDA publishes them
# under the same /safety/recalls-... path.
_OUT_OF_SCOPE_SLUG_TOKENS = (
    # Drugs / supplements / pharmaceuticals (not food)
    "capsule", "capsules",
    "tablet", "tablets",
    "doxorubicin", "sildenafil", "tadalafil",
    "-pharma-", "pharmaceutical",
    "injectable", "injection", "intravenous",
    "iv-fluid", "iv-bag",
    "drug-recall",
    "sensual-enhancement", "sexual-enhancement",
    "weight-loss-product", "erectile",
    # Allergen-only recalls (out of AFTS scope)
    "undeclared-peanut", "cross-contact-peanut",
    "undeclared-milk", "undeclared-dairy",
    "undeclared-egg", "undeclared-eggs",
    "undeclared-soy", "undeclared-wheat",
    "undeclared-tree-nut", "undeclared-treenut",
    "undeclared-sesame",
    "undeclared-shellfish", "undeclared-fish",
    "undeclared-sulfite", "undeclared-sulfites",
    "undeclared-allergen", "undeclared-allergens",
    "undeclared-color", "undeclared-coloring",
    "undeclared-fd-c",
    "mislabeling-undeclared",
    # Quality / spoilage / packaging (out of scope)
    "due-to-mould", "due-to-mold",
    "moldy", "mouldy",
    "spoilage", "premature-spoilage",
)


def _is_real_fda_recall_slug(slug: str) -> bool:
    """Return True only if this FDA listing slug is a genuine in-scope recall.

    Tightens the HTML fallback path to reject page-nav links and out-of-scope
    recall types (drugs / allergen-only / quality). The RSS path uses the
    structured pathogen + food-context filter in ``_parse_item``; this is the
    equivalent gate for the listing-HTML degraded mode.

    Accepts either a full path
    ("/safety/recalls-market-withdrawals-safety-alerts/<slug>?qs") or just
    the final tail segment ("<slug>"). The check operates on the last path
    segment after strip + lowercase.
    """
    if not slug:
        return False
    # Strip any query string / fragment — the listing href can carry filter
    # params (`?Randparam=...`) that we don't want as part of the slug.
    s = slug.split("?", 1)[0].split("#", 1)[0]
    # Extract the final path segment (the slug proper). If `slug` was just
    # the tail, this is a no-op.
    tail = s.rstrip("/").rsplit("/", 1)[-1].lower()
    if not tail:
        return False

    # 1. Static nav-page blocklist.
    if tail in _FDA_NAV_SLUGS_BLOCKLIST:
        return False

    # 2. Real recall slugs are long descriptive sentences. A slug with too
    #    few hyphens is either a nav stub or a category root.
    if tail.count("-") < 4:
        return False

    # 3. Must contain a recall verb. Pure category pages, indices, and
    #    dashboards don't carry one.
    if not any(v in tail for v in _RECALL_VERBS):
        return False

    # 4. Reject out-of-scope categories (drugs, allergen-only, quality).
    if any(tok in tail for tok in _OUT_OF_SCOPE_SLUG_TOKENS):
        return False

    return True

# At least one of these tokens must appear in the title+description, else
# we drop the row as non-food (FDA recalls cover drugs, devices, cosmetics,
# tobacco — the FOOD recall RSS feed at the URL we use should already be
# food-only, but the press-release feed multiplexes everything, so be
# defensive).
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
    # Generic verbs that almost always appear in food-recall titles
    "recalls", "recalled", "recall of", "recalls because",
    "voluntarily recalls", "voluntary recall",
    "issues recall", "issues alert",
)


class FDAPressReleaseScraper(BaseScraper):
    """Reads FDA's official RSS feed of recalls and safety alerts."""

    AGENCY = "FDA"
    COUNTRY = "USA"

    # Canonical FDA RSS feed. Audit 2026-05-07 — the previous feed at
    # /rss-feeds/recalls/rss.xml went dark (no response). Switched to the
    # food-specific feed which is verified live 2026-05-07 and has the
    # bonus of being pre-filtered to food (no need for the post-filter
    # _FOOD_CONTEXT_TOKENS pass to drop drug/device/cosmetic items).
    # If FDA migrates the feed, fall back to the listing HTML.
    FEED_URL = (
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/"
        "rss-feeds/food-safety-recalls/rss.xml"
    )
    FALLBACK_URL = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
    )

    # ── Akamai bot-detection bypass (audit 2026-05-09) ─────────────────
    # FDA.gov sits behind Akamai's CDN with bot detection enabled. The
    # base scraper's Chrome 127 User-Agent alone is no longer enough —
    # Akamai's fingerprinting checks for the modern client-hints headers
    # (sec-ch-ua-*) and resource-fetch metadata (sec-fetch-*) that real
    # Chrome sends. Without them, Akamai returns HTTP 404 (yes, 404, not
    # 403 — it deliberately confuses scrapers) with `server=AkamaiNetStorage`.
    #
    # Symptoms before this fix (May 9 run): all three FDA URLs returned
    # 404 from AkamaiNetStorage. The URLs themselves are live — verified
    # by browser fetch and external search returning fresh content. So
    # the gap is purely the request fingerprint. Adding these headers
    # makes the request indistinguishable from a real Chrome 127 navigation.
    #
    # Per-host override pattern: kept local to FDA so we don't disturb
    # other regulators (some block requests with sec-* headers because
    # their CDN is from a different vendor that interprets them differently).
    # If more US gov sites adopt Akamai bot detection, generalise this to
    # scrapers/_base.py's SPECIAL_HEADERS_BY_HOST hook (currently a TODO).
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
    }

    # RSS feeds expect XML in Accept; HTML pages expect text/html. The
    # base session sends the HTML Accept by default — for RSS endpoints
    # we override per-request. Akamai's bot rules cross-check that the
    # Accept header matches the requested resource type.
    _RSS_ACCEPT = (
        "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, "
        "*/*;q=0.5"
    )

    PATHOGEN_KEYWORDS = _PATHOGEN_KEYWORDS

    # FDA recall slugs typically live under one of these path prefixes.
    # Anything else is generic and rejected.
    _ACCEPTABLE_URL_PREFIXES = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/",
        "https://www.fda.gov/news-events/press-announcements/",
        "https://www.fda.gov/food/alerts-advisories-safety-information/",
    )

    @classmethod
    def _rss_headers(cls) -> dict:
        """Headers for RSS-XML endpoints — Akamai-bypass + XML Accept."""
        h = dict(cls._AKAMAI_BYPASS_HEADERS)
        h["Accept"] = cls._RSS_ACCEPT
        # RSS feeds are typically polled, not browsed-from-link
        h["sec-fetch-dest"] = "empty"
        h["sec-fetch-mode"] = "cors"
        return h

    @classmethod
    def _html_headers(cls) -> dict:
        """Headers for HTML listing fallback — Akamai-bypass + browse fingerprint."""
        return dict(cls._AKAMAI_BYPASS_HEADERS)

    def scrape(self, since_days: int = 30) -> List[Recall]:
        rows = self._scrape_rss(since_days)
        if rows:
            return rows
        log.warning("FDA RSS returned no rows; trying fallback HTML listing")
        return self._scrape_html_fallback(since_days)

    # ------------------------------------------------------------------
    def _scrape_rss(self, since_days: int) -> List[Recall]:
        r = fetch(self.session, self.FEED_URL, headers=self._rss_headers())
        if not r:
            log.warning("FDA RSS fetch failed: no response")
            return []
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            log.warning("FDA RSS parse failed: %s", e)
            return []

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen_urls: set = set()

        for item in root.iter("item"):
            try:
                rec = self._parse_item(item, cutoff, seen_urls)
                if rec is not None:
                    out.append(rec)
                    seen_urls.add(rec.URL)
            except Exception as e:
                log.warning("FDA RSS item parse failed: %s", e)

        log.info("FDA RSS: %d pathogen recalls (since_days=%d)",
                 len(out), since_days)
        return out

    # ------------------------------------------------------------------
    def _parse_item(self, item, cutoff: datetime,
                    seen_urls: set) -> Optional[Recall]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()

        if not link or _is_generic_url(link) or link in seen_urls:
            return None

        # URL must live on a real FDA recall path (defensive — RSS should
        # only emit valid links, but we have seen feeds glitch and emit
        # the bare landing page as a "self-link" item).
        if not any(link.startswith(p) for p in self._ACCEPTABLE_URL_PREFIXES):
            log.debug("FDA RSS: skipping non-recall URL %s", link[:80])
            return None

        # Strip HTML tags from description (FDA RSS descriptions are
        # sometimes raw HTML with <p>, <a>, etc.).
        desc_text = re.sub(r"<[^>]+>", " ", desc)
        desc_text = re.sub(r"\s+", " ", desc_text).strip()

        merged = (title + " " + desc_text).lower()

        # 1. Pathogen filter — must match at least one keyword
        matched_kw = _matched_pathogen_keyword(merged, self.PATHOGEN_KEYWORDS)
        if not matched_kw:
            return None

        # 2. Food-context filter — drop non-food (drugs, devices, cosmetics)
        if not any(tok in merged for tok in _FOOD_CONTEXT_TOKENS):
            return None

        # 3. Date parse — RSS pubDate is RFC-822
        d = self._parse_pubdate(pub)
        if d is None:
            log.debug("FDA RSS: unparseable pubDate %r", pub)
            return None
        if d < cutoff:
            return None

        # 4. Company / brand extraction. FDA titles are formatted as
        # "<Firm> Recalls <Product> Because of <Reason>" or variants.
        m = re.match(
            r"^(.+?)\s+(?:recalls?|issues?|voluntarily\s+recalls?|"
            r"announces?\s+recall\s+of)\s+",
            title, re.I,
        )
        company = (m.group(1).strip() if m
                   else title.split(" - ")[0]).strip()[:100]

        # 5. Outbreak detection
        outbreak = _detect_outbreak(merged)

        # 6. Build Recall
        return self._new_recall(
            Date=d.strftime("%Y-%m-%d"),
            Company=company,
            Brand="—",
            Product=title[:300],
            Pathogen=matched_kw,           # canonicalised by _new_recall
            Reason=desc_text[:400] or title[:400],
            Class="Recall",
            URL=link,
            Outbreak=outbreak,
            Notes="FDA RSS (press-release feed)",
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_pubdate(pub: str) -> Optional[datetime]:
        """Parse RFC-822 pubDate. RSS feeds emit several variants."""
        if not pub:
            return None
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%a, %d %b %Y %H:%M:%S",
            "%d %b %Y %H:%M:%S %z",
        ):
            try:
                d = datetime.strptime(pub, fmt)
                # Strip tz so we can compare against utcnow() cleanly
                return d.replace(tzinfo=None)
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    def _scrape_html_fallback(self, since_days: int) -> List[Recall]:
        """Parse the HTML listing page when RSS is unavailable.

        Degraded-mode path — used only when the RSS feed has moved, returns
        5xx, or has been temporarily emptied. The HTML listing has no
        structured pubDate so we extract only enough to write a row to
        Pending; the URL gate + claude-check will fetch each recall page
        for full details (Date, Pathogen, full Product).

        Filtering (audit 2026-05-21, see _is_real_fda_recall_slug docstring):
          - Strip query strings from each href before validation
          - Reject the 6 known FDA nav-page slugs (Recall Resources,
            Enforcement Reports, Industry Guidance Recalls, Major Product
            Recalls, Additional Information About Recalls, Datatables Data)
          - Require a recall-verb in the slug (recalls/recalled/voluntary/etc.)
          - Reject out-of-scope categories (drugs, allergen-only, quality)
          - Date left EMPTY (was: today's date) — claude-check fills it
            from the actual recall page during enrichment. Stamping today
            on a 3-month-old recall would mark it fresh and bypass the
            staleness gate in pipeline/gap_finder_tavily._item_to_recall.
        """
        r = fetch(self.session, self.FALLBACK_URL, headers=self._html_headers())
        if not r:
            log.warning("FDA HTML fallback also failed")
            return []

        # Recall slugs follow a stable pattern in the HTML:
        #   href="/safety/recalls-market-withdrawals-safety-alerts/<slug>[?qs]"
        slugs = re.findall(
            r'href="(/safety/recalls-market-withdrawals-safety-alerts/[^"]+)"',
            r.text,
        )
        if not slugs:
            log.warning("FDA HTML fallback: no recall slugs found")
            return []

        # Dedup + filter. The listing shows most-recent first; cap to 25
        # to bound the Pending sheet pressure if the listing ever balloons.
        seen: set = set()
        unique_slugs: List[str] = []
        dropped_nav = 0
        dropped_short = 0
        dropped_no_verb = 0
        dropped_oos = 0
        for s in slugs:
            # Strip query / fragment for dedup key (different ?Randparam=
            # query strings on the same nav page shouldn't count separately).
            key = s.split("?", 1)[0].split("#", 1)[0]
            if key in seen:
                continue
            # Inline classification so we can log per-category drop counts.
            tail = key.rstrip("/").rsplit("/", 1)[-1].lower()
            if tail in _FDA_NAV_SLUGS_BLOCKLIST:
                dropped_nav += 1
                seen.add(key)
                continue
            if tail.count("-") < 4:
                dropped_short += 1
                seen.add(key)
                continue
            if not any(v in tail for v in _RECALL_VERBS):
                dropped_no_verb += 1
                seen.add(key)
                continue
            if any(tok in tail for tok in _OUT_OF_SCOPE_SLUG_TOKENS):
                dropped_oos += 1
                seen.add(key)
                continue
            seen.add(key)
            unique_slugs.append(key)
            if len(unique_slugs) >= 25:
                break

        out: List[Recall] = []
        for slug in unique_slugs:
            url = f"https://www.fda.gov{slug}"
            # Title from the slug — claude-check will fetch the real one
            title = slug.rsplit("/", 1)[-1].replace("-", " ").title()[:300]
            out.append(self._new_recall(
                # Date left EMPTY — claude-check enriches from the page.
                # Stamping today was causing 3-month-old recalls to look fresh.
                Date="",
                Company=title.split(" Recalls ")[0][:100],
                Brand="—",
                Product=title,
                Pathogen="",   # Will be filled by claude-check page review
                Reason=title,
                Class="Recall",
                URL=url,
                Outbreak=0,
                Notes="FDA HTML fallback — claude-check needs to enrich Date+Pathogen",
            ))
        log.info(
            "FDA HTML fallback: %d candidate URLs kept (dropped: %d nav, "
            "%d short-slug, %d no-recall-verb, %d out-of-scope)",
            len(out), dropped_nav, dropped_short, dropped_no_verb, dropped_oos,
        )
        return out
