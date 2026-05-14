"""FSAI (IE) Ireland food alerts scraper — RSS-first, HTML listing fallback.

WHY THIS REPLACES THE PREVIOUS VERSION (audit 2026-05-06)
==========================================================
Production data: 2 rows captured in 4 months. FSAI publishes ~2-3 alerts
per week; expected capture ≈ 30-50 over the same period. The previous
scraper was 10 lines wrapping ``GenericGeminiScraper``:

    class FSAIScraper(GenericGeminiScraper):
        AGENCY = "FSAI (IE)"
        INDEX_URLS = ['https://www.fsai.ie/news-and-alerts/food-alerts']

It sent the listing HTML to Gemini and trusted whatever Gemini returned.
Failure modes — all silent:

  1. Gemini API rate-limited → returns empty
  2. Gemini decides "I see no recalls" → returns empty
  3. HTML structure changes → returns empty
  4. Network blip / timeout → returns empty

No deterministic backup, no logging when 0 rows came back, no retry.
Same fragility pattern that hit CFIA before we hardened it (see
scrapers/north_america/cfia.py audit 2026-05-06).

THIS VERSION
============
- RSS-first: tries the FSAI RSS feed (announced at /food-alerts/rss
  but multiple URL variants probed in case of CMS rewrite).
- HTTP status check + content-type check: reject non-200 responses
  and HTML bodies served instead of XML (WAF / 403 page).
- Format auto-detect: RSS 2.0 ``<item>`` first, then Atom 1.0
  ``<entry>`` with namespace-aware iteration.
- Multi-format date parser (7 formats; FSAI dates are usually
  RFC-822 from RSS but ISO 8601 if Atom).
- HTML listing fallback when every RSS endpoint returns 0 rows:
  scrapes ``/news-and-alerts/food-alerts`` and extracts every
  ``/news-and-alerts/food-alerts/<slug>`` href starting with
  ``recall-of-`` (FSAI's stable slug pattern, verified from production
  rows in xlsx). Detail page enrichment is left to claude-check.
- Loud logging at every step. Operators can grep workflow logs for
  ``FSAI:`` to see exactly which path executed and how many items
  came out.
- Returns the same Recall contract as before — Pathogen set to a CORE
  keyword (canonicalised by ``_new_recall``), bilingual outbreak
  detection (EN only since FSAI publishes EN), Company normalised.
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


_OUTBREAK_TOKENS = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses", "cluster of cases",
)


def _detect_outbreak(merged_lower: str) -> int:
    return 1 if any(t in merged_lower for t in _OUTBREAK_TOKENS) else 0


# Words that ``for_languages`` returns as "keywords" but which are NOT
# real pathogens — recall meta-vocabulary. Mirrors the CFIA scraper's
# _PATHOGEN_META_NOISE set for consistency.
_PATHOGEN_META_NOISE = frozenset({
    "recall", "recalled", "recalls", "recalling",
    "alert", "alerts", "warning", "warnings",
    "advisory", "advisories",
    "withdrawal", "withdrawals",
    "notice", "notices", "notification", "notifications",
})


def _matched_pathogen_keyword(text_lower: str,
                              keywords: Tuple[str, ...]) -> Optional[str]:
    """Find the first PATHOGEN keyword in text. Skips meta-vocabulary."""
    for kw in keywords:
        if kw in _PATHOGEN_META_NOISE:
            continue
        if kw in text_lower:
            return kw
    return None


def _parse_pubdate(s: str) -> Optional[datetime]:
    """Parse RSS pubDate or Atom published. Returns naive UTC.
    Handles 7 formats — same family as the CFIA scraper.
    """
    if not s:
        return None
    s = s.strip()
    formats = (
        "%a, %d %b %Y %H:%M:%S %z",     # RFC-822 with offset
        "%a, %d %b %Y %H:%M:%S GMT",    # RFC-822 GMT literal
        "%a, %d %b %Y %H:%M:%S",        # RFC-822 no zone
        "%Y-%m-%dT%H:%M:%S%z",          # ISO 8601 offset
        "%Y-%m-%dT%H:%M:%SZ",           # ISO 8601 Z
        "%Y-%m-%dT%H:%M:%S",            # ISO 8601 no zone
        "%Y-%m-%d",                     # date-only
    )
    for fmt in formats:
        try:
            d = datetime.strptime(s, fmt)
            if d.tzinfo is not None:
                d = d.replace(tzinfo=None)
            return d
        except ValueError:
            continue
    # Last resort — strip a timezone abbreviation if present
    m = re.match(r"^(.+?)\s+([A-Z]{2,4})$", s)
    if m:
        try:
            return datetime.strptime(m.group(1).strip(),
                                     "%a, %d %b %Y %H:%M:%S")
        except ValueError:
            pass
    return None


_PATHOGEN_KEYWORDS = for_languages("en")

# Food-context tokens — same shape as CFIA but EN only (FSAI publishes
# only in English; no Irish-language FSAI alerts exist).
_FOOD_CONTEXT_TOKENS = (
    "food", "recall", "alert",
    "salmon", "listeria", "e. coli", "stec", "botulin",
    "meat", "cheese", "fish", "milk", "dairy",
    "chicken", "beef", "pork", "turkey", "lamb", "duck",
    "vegetable", "vegetables", "fruit", "fruits",
    "spinach", "lettuce", "tomato", "onion", "carrot",
    "snack", "snacks", "chips", "crisps", "crackers",
    "bakery", "bread", "cake", "biscuit", "muffin",
    "candy", "chocolate", "confection",
    "spice", "spices", "herb", "herbs", "seasoning",
    "sauce", "soup", "stew", "dressing",
    "infant", "baby food", "infant formula",
    "rte", "ready to eat", "ready-to-eat",
    "frozen", "deli", "smoked",
    "supplement", "powder", "drink",
    "egg", "rice", "pasta", "noodle", "cereal", "granola",
    "pistachio", "almond", "peanut", "nut",
    "shellfish", "oyster", "mussel", "prawn",
    "yogurt", "yoghurt", "butter",
    # FSAI-specific category words
    "produce", "beverage", "bottled water", "tea", "coffee",
    "supplement", "moringa",
)


def _is_generic_url(url: str) -> bool:
    if not url:
        return True
    u = url.lower().rstrip("/").split("?", 1)[0].split("#", 1)[0]
    bare_landings = (
        "https://www.fsai.ie",
        "https://www.fsai.ie/news-and-alerts",
        "https://www.fsai.ie/news-and-alerts/food-alerts",
        "https://www.fsai.ie/news-and-alerts/latest-news",
    )
    if u in bare_landings:
        return True
    bad_substrings = (
        "/search?", "/search/", "/page/", "page=",
        "/categorie/", "/tag/",
    )
    return any(s in url.lower() for s in bad_substrings)


class FSAIScraper(BaseScraper):
    """FSAI (Ireland) food-alert scraper (RSS-first, HTML fallback)."""

    AGENCY = "FSAI (IE)"
    COUNTRY = "Ireland"

    # RSS feed candidates — FSAI's CMS has historically served the feed
    # at multiple paths. Try all in order; first non-empty wins.
    FEED_URLS = (
        "https://www.fsai.ie/news-and-alerts/food-alerts/rss",
        "https://www.fsai.ie/news-and-alerts/rss",
        "https://www.fsai.ie/rss/food-alerts.xml",
        "https://www.fsai.ie/feed/food-alerts",
        "https://www.fsai.ie/rss.xml",
    )

    # HTML listing — operators visit this page directly. Used when every
    # RSS feed returns empty / fails.
    LISTING_URL = "https://www.fsai.ie/news-and-alerts/food-alerts"

    PATHOGEN_KEYWORDS = _PATHOGEN_KEYWORDS

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # Try every RSS endpoint first
        for feed_url in self.FEED_URLS:
            log.info("FSAI: trying feed %s", feed_url)
            rows = self._scrape_feed(feed_url, since_days)
            if rows:
                log.info("FSAI: feed %s returned %d rows", feed_url, len(rows))
                return rows
            log.warning("FSAI: feed %s returned 0 rows", feed_url)

        log.warning("FSAI: all RSS feeds empty — trying HTML listing fallback")
        return self._scrape_html_listing(since_days)

    # ------------------------------------------------------------------
    def _scrape_feed(self, feed_url: str, since_days: int) -> List[Recall]:
        r = fetch(self.session, feed_url)
        if r is None:
            log.warning("FSAI: %s — fetch returned None", feed_url)
            return []
        if r.status_code != 200:
            log.warning("FSAI: %s — HTTP %d", feed_url, r.status_code)
            return []

        body = r.content
        head = body[:200].lstrip().lower()
        if not (head.startswith(b"<?xml") or head.startswith(b"<rss")
                or head.startswith(b"<feed") or head.startswith(b"<atom")):
            log.warning("FSAI: %s — body is not XML (first bytes: %r)",
                        feed_url, body[:60])
            return []

        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            log.warning("FSAI: %s — XML parse failed: %s", feed_url, e)
            return []

        # Auto-detect format
        items = list(root.iter("item"))
        atom_ns = "{http://www.w3.org/2005/Atom}"
        if not items:
            items = list(root.iter(f"{atom_ns}entry"))
            if items:
                log.info("FSAI: %s is Atom 1.0 (%d entries)",
                         feed_url, len(items))
                return self._parse_atom_entries(items, since_days, atom_ns)
        else:
            log.info("FSAI: %s is RSS 2.0 (%d items)", feed_url, len(items))
            return self._parse_rss_items(items, since_days)

        log.warning("FSAI: %s — unknown feed format (root=%s)",
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
                    log.warning("FSAI RSS: unparseable pubDate %r — keeping "
                                "row (claude-check will fix Date)", pub)
                    d = datetime.utcnow()
                if d < cutoff:
                    skipped_date += 1
                    continue

                rec = self._build_recall(title, link, desc, d, "FSAI RSS")
                if rec is None:
                    skipped_filter += 1
                    continue

                seen.add(link)
                out.append(rec)
            except Exception as e:
                log.warning("FSAI RSS item parse failed: %s", e)

        log.info("FSAI RSS: %d kept, %d skipped (filter=%d url=%d date=%d)",
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
                    log.warning("FSAI Atom: unparseable date %r — keeping row",
                                pub)
                    d = datetime.utcnow()
                if d < cutoff:
                    continue

                rec = self._build_recall(title, link, summary, d, "FSAI Atom")
                if rec is None:
                    continue
                seen.add(link)
                out.append(rec)
            except Exception as e:
                log.warning("FSAI Atom entry parse failed: %s", e)

        log.info("FSAI Atom: %d entries → %d recalls", len(entries), len(out))
        return out

    # ------------------------------------------------------------------
    def _build_recall(self, title: str, link: str, desc: str,
                      d: datetime, notes_label: str) -> Optional[Recall]:
        """Apply pathogen + food-context filters, build Recall."""
        desc_text = re.sub(r"<[^>]+>", " ", desc or "")
        desc_text = re.sub(r"\s+", " ", desc_text).strip()
        merged = (title + " " + desc_text).lower()

        # Pathogen filter
        matched_kw = _matched_pathogen_keyword(merged, self.PATHOGEN_KEYWORDS)
        if not matched_kw:
            return None

        # Food-context filter — drop non-food consumer alerts
        if not any(tok in merged for tok in _FOOD_CONTEXT_TOKENS):
            return None

        # Company / brand extraction. FSAI titles are formatted as:
        #   "Recall of [batches of] <Brand X> <product> due to <reason>"
        # The previous scraper sent these to Gemini; we extract
        # deterministically here. Pattern strips the "Recall of" /
        # "Recall of batches of" / "Recall of specific batches of"
        # prefix that merge_master.validate_pending_row would otherwise
        # reject as "garbage prefix".
        cleaned_title = re.sub(
            r"^(?:Recall\s+of\s+(?:specific\s+)?(?:a\s+)?(?:batches?\s+of\s+|batch\s+of\s+)?)",
            "", title, flags=re.I,
        ).strip()
        # Try "Brand X product due to ..." or "Brand X brand product ..."
        m = re.match(
            r"^(.+?)\s+(?:brand|due\s+to|because\s+of|over|after|"
            r"in\s+relation\s+to|caused\s+by|product|products|recalled|"
            r"recall|may\s+contain|contains)\s+",
            cleaned_title, re.I,
        )
        raw_company = (m.group(1).strip() if m
                       else cleaned_title.split(" - ")[0]).strip()
        # Strip stray punctuation
        raw_company = raw_company.strip(" .,;:'-—–")
        co, br = normalise_company_brand(raw_company[:100], "—")

        outbreak = _detect_outbreak(merged)

        return self._new_recall(
            Date=d.strftime("%Y-%m-%d"),
            Company=co,
            Brand=br,
            Product=cleaned_title[:300] or title[:300],
            Pathogen=matched_kw,
            Reason=desc_text[:400] or title[:400],
            Class="Alert",
            URL=link,
            Outbreak=outbreak,
            Notes=notes_label,
        )

    # ------------------------------------------------------------------
    def _scrape_html_listing(self, since_days: int) -> List[Recall]:
        """Last-resort: parse the HTML listing for recall slugs.

        FSAI listing renders cards with:
          <a href="/news-and-alerts/food-alerts/recall-of-batches-of-...">
            <h3>Recall of batches of ...</h3>
            <span>22 Apr 2026</span>
          </a>

        We extract every ``recall-of-...`` slug, pair it with the nearest
        date attribute, and return rows. Detail page enrichment is left
        to claude-check (which fetches each page on the next reviewer
        cycle and fills in Date/Pathogen/etc).
        """
        r = fetch(self.session, self.LISTING_URL)
        if r is None or r.status_code != 200:
            log.error("FSAI: HTML listing fetch failed (status=%s)",
                      r.status_code if r else None)
            return []

        html = r.text
        # Match every slug under /news-and-alerts/food-alerts/<slug>.
        # FSAI uses 'recall-of-' but also occasionally 'product-recall-'
        # and 'allergen-' (we filter allergen out via pathogen check).
        slugs = re.findall(
            r'href="(/news-and-alerts/food-alerts/[^"#?]+)"',
            html,
        )
        # Filter to only recall-style slugs
        slugs = [s for s in slugs
                 if any(kw in s.lower() for kw in
                        ("recall", "product-recall", "outbreak", "warning"))]
        if not slugs:
            log.error("FSAI: HTML listing — no /food-alerts/recall-* slugs "
                      "found (WAF block? page-structure change?)")
            return []

        # Dedup, cap at 50
        seen_slugs: set = set()
        unique = []
        for slug in slugs:
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            unique.append(slug)
            if len(unique) >= 50:
                break

        today = datetime.utcnow()
        cutoff = today - timedelta(days=since_days)
        out: List[Recall] = []

        for slug in unique:
            url = f"https://www.fsai.ie{slug}"

            # Title from a 1500-char window around the slug
            slug_pos = html.find(f'href="{slug}"')
            window_start = max(0, slug_pos - 100)
            window_end = min(len(html), slug_pos + 1500)
            window = html[window_start:window_end]

            # Title extraction.
            #
            # Audit 2026-05-14: the pre-fix regex captured the FIRST
            # non-tag text after the `<a href="...">` opening tag:
            #
            #   rf'href="{re.escape(slug)}"[^>]*>\s*(?:<[^>]+>\s*)*([^<]+?)\s*<'
            #
            # On FSAI's card-style listing markup, that's an image
            # caption / accessibility label rather than the recall
            # heading. The 2026-05-14 17:02 Athens orchestrator log
            # showed four FSAI rows reaching merge_master with Company
            # text like "Pictures of Lidl chicken products", "Picture of
            # Free Range 100% Irish Chicken Breast Fi", "Aldi Butchers
            # Selection & Tesco Roast in Bag Chick", and "A packet of
            # Good4U Super Sprouts Super Greens" — none of which are
            # real titles, all of which are figcaption / span-with-
            # caption text living between the <a href> and the <h2>
            # title in the card markup.
            #
            # Fix: search the surrounding window for a heading element
            # (h1-h4) explicitly. Headings carry the actual recall title;
            # captions/aria text do not. Fall back to slug-derived
            # Title-cased text if no heading is found, which is still
            # better than image caption noise.
            #
            # Why search the window (not just inside <a>...</a>): FSAI's
            # listing structure has shifted between heading-inside-link
            # and heading-as-sibling-of-link across redesigns; the
            # window approach tolerates both layouts. The 1500-char
            # window starts 100 chars before the slug and is the same
            # window used downstream for date parsing.
            heading_match = re.search(
                r'<h[1-4][^>]*>(.+?)</h[1-4]>',
                window,
                flags=re.S | re.I,
            )
            if heading_match:
                # Strip nested tags inside the heading (e.g.
                # <span class="badge">NEW</span> wrapper around title)
                title = re.sub(r'<[^>]+>', ' ', heading_match.group(1))
                title = re.sub(r'\s+', ' ', title).strip()
                # Sanity check — if for some reason the matched heading
                # is the page-level "Food Alerts" section heading rather
                # than the card title, the slug is more reliable.
                if title.lower() in ("food alerts", "news and alerts",
                                     "recalls and alerts", "alerts"):
                    title = slug.rsplit("/", 1)[-1].replace("-", " ").title()
            else:
                # Fall back to slug — derived from the URL, ASCII-safe,
                # always matches the actual recall identifier.
                title = slug.rsplit("/", 1)[-1].replace("-", " ").title()

            # Date — try <time datetime="..."> or visible "DD Mon YYYY"
            date_match = re.search(
                r'datetime="(\d{4}-\d{2}-\d{2})"', window
            )
            if not date_match:
                date_match = re.search(
                    r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
                    r'[a-z]*\s+(\d{4})', window, re.I,
                )
                if date_match:
                    months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                              "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
                    day = int(date_match.group(1))
                    mon = months[date_match.group(2)[:3].lower()]
                    yr = int(date_match.group(3))
                    d = datetime(yr, mon, day)
                else:
                    d = today
            else:
                d = _parse_pubdate(date_match.group(1)) or today

            if d < cutoff:
                continue

            # Apply pathogen + food filter on title only (we don't have
            # description in the listing). If neither matches, send to
            # Pending with empty Pathogen — claude-check fetches the
            # detail page and enriches.
            merged = title.lower()
            matched_kw = _matched_pathogen_keyword(
                merged, self.PATHOGEN_KEYWORDS,
            ) or ""

            cleaned_title = re.sub(
                r"^(?:Recall\s+of\s+(?:specific\s+)?(?:a\s+)?(?:batches?\s+of\s+|batch\s+of\s+)?)",
                "", title, flags=re.I,
            ).strip()
            m = re.match(
                r"^(.+?)\s+(?:brand|due\s+to|because\s+of|over|after|"
                r"product|products|recalled|recall)\s+",
                cleaned_title, re.I,
            )
            raw_company = (m.group(1).strip() if m
                           else cleaned_title.split(" - ")[0]).strip(" .,;:'-—–")
            co, br = normalise_company_brand(raw_company[:100], "—")

            out.append(self._new_recall(
                Date=d.strftime("%Y-%m-%d"),
                Company=co,
                Brand=br,
                Product=cleaned_title[:300] or title[:300],
                Pathogen=matched_kw,
                Reason=cleaned_title[:400] or title[:400],
                Class="Alert",
                URL=url,
                Outbreak=_detect_outbreak(merged),
                Notes="FSAI HTML listing fallback — claude-check please enrich "
                      "Pathogen+Date+Reason from detail page",
            ))

        log.warning("FSAI: HTML listing fallback produced %d candidate rows "
                    "(claude-check must enrich)", len(out))
        return out
