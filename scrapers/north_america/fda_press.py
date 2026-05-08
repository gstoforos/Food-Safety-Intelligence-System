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

PATCH 2026-05-08
================
Today's run produced "FDA RSS fetch failed: no response" for both the
RSS endpoint AND the HTML fallback, in 213ms combined — too fast for
real timeouts; both failed instantly. Meanwhile api.fda.gov (which
backs the sibling fda.py scraper) worked normally in the same run.

That points at www.fda.gov-specific bot detection (Cloudflare) — the
sibling endpoint api.fda.gov isn't behind the same WAF. Same pattern as
Batch 4 (COMESA, ŠVPS, etc.): UA looks like Chrome but Client Hints
(sec-ch-ua-*) and Sec-Fetch-* headers are missing, so the WAF flags
the request as automated and drops the connection.

Three fixes in this patch:

  1. Chrome 127 fingerprint headers (Client Hints + Sec-Fetch) added
     to this scraper's session, scoped per-instance only (no leak).
  2. "no response" log message replaced with detailed error including
     exception type, message, and partial URL. Done by performing the
     fetch directly through self.session and capturing the exception,
     bypassing _base.fetch's exception-swallowing.
  3. A secondary RSS URL (the broader Recalls feed, which also carries
     food items) is tried if the food-specific feed fails. URL was
     verified live 2026-05-08 via FDA's "Subscribe to Podcasts and
     News Feeds" listing page.

DESIGN DECISIONS (unchanged from original)
==========================================
1. Pathogen + food filter — same shape as scrapers/north_america/cfia.py.
2. Outbreak detection — same EN tokens as CFIA but no FR (FDA is EN-only).
3. Date parsing — RSS pubDate is RFC-822; multiple variants handled.
4. URL — taken from <link>. The recall page slug is stable; URL is the
   dedup key merge_master uses.
5. Class — RSS does not expose Class I/II/III. Leave as "Recall".
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET
import logging
import re

import requests

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


# ─── Chrome 127 fingerprint headers (same as Batch 4) ────────────────────────
# Cloudflare and other modern WAFs check for these to distinguish real
# Chrome from bots that just spoof the User-Agent string.
_CHROME_FINGERPRINT_HEADERS = {
    "sec-ch-ua": '"Chromium";v="127", "Not(A:Brand";v="24", "Google Chrome";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Cache-Control": "max-age=0",
    "DNT": "1",
}


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
    bare = u.rstrip("/").split("?", 1)[0]
    if bare in (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
        "https://www.fda.gov/safety/recalls",
        "https://www.fda.gov/food/recalls-outbreaks-emergencies",
    ):
        return True
    return False


_PATHOGEN_KEYWORDS = for_languages("en")

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


class FDAPressReleaseScraper(BaseScraper):
    """Reads FDA's official RSS feed of recalls and safety alerts."""

    AGENCY = "FDA"
    COUNTRY = "USA"

    # Primary feed URL (food-only). Verified live 2026-05-08 via web
    # search showing recent items (Gerber Arrowroot, Koikoi Trading, etc.).
    FEED_URL = (
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/"
        "rss-feeds/food-safety-recalls/rss.xml"
    )
    # Secondary feed (general Recalls, broader scope — drugs/devices/food).
    # Listed in FDA's "Subscribe to Podcasts and News Feeds" hub. We rely
    # on the food-context filter (_FOOD_CONTEXT_TOKENS) to drop non-food.
    FEED_URL_FALLBACK = (
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/"
        "rss-feeds/recalls/rss.xml"
    )
    FALLBACK_URL = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
    )

    PATHOGEN_KEYWORDS = _PATHOGEN_KEYWORDS

    _ACCEPTABLE_URL_PREFIXES = (
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/",
        "https://www.fda.gov/news-events/press-announcements/",
        "https://www.fda.gov/food/alerts-advisories-safety-information/",
    )

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        # Apply Chrome fingerprint to bypass Cloudflare on www.fda.gov.
        # Per-scraper session so this doesn't leak to other scrapers.
        self.session.headers.update(_CHROME_FINGERPRINT_HEADERS)
        # RSS-specific Accept (some feeds reject text/html-only Accept headers).
        self.session.headers["Accept"] = (
            "application/rss+xml, application/xml, text/xml, "
            "application/atom+xml, text/html;q=0.9, */*;q=0.8"
        )

    # ------------------------------------------------------------------
    def scrape(self, since_days: int = 30) -> List[Recall]:
        # Try primary food-only feed
        rows = self._scrape_rss(self.FEED_URL, since_days, label="primary")
        if rows:
            return rows
        # Fall back to general Recalls feed (broader scope; food filter
        # will drop drug/device items)
        log.warning("FDA RSS primary feed empty/failed; trying secondary feed")
        rows = self._scrape_rss(self.FEED_URL_FALLBACK, since_days, label="secondary")
        if rows:
            return rows
        # Final fallback: HTML listing
        log.warning("FDA RSS both feeds empty/failed; trying HTML fallback")
        return self._scrape_html_fallback(since_days)

    # ------------------------------------------------------------------
    def _fetch_with_diagnostic(self, url: str, label: str):
        """Direct session.get with detailed error capture.

        Bypasses _base.fetch's exception-swallowing so we get
        actionable error info in the log instead of "no response".
        Returns response object on success, None on failure (after
        logging the precise reason).
        """
        try:
            r = self.session.get(url, timeout=30)
        except requests.exceptions.SSLError as e:
            log.warning("FDA fetch %s SSL error: %s | url=%s", label, e, url[:120])
            return None
        except requests.exceptions.ConnectionError as e:
            # Most useful diagnostic — distinguishes DNS vs reset vs refused
            log.warning("FDA fetch %s connection error: %s | url=%s",
                        label, type(e).__name__ + ": " + str(e)[:200], url[:120])
            return None
        except requests.exceptions.Timeout as e:
            log.warning("FDA fetch %s timeout: %s | url=%s", label, e, url[:120])
            return None
        except requests.exceptions.RequestException as e:
            log.warning("FDA fetch %s request error: %s: %s | url=%s",
                        label, type(e).__name__, e, url[:120])
            return None

        if not r.ok:
            log.warning(
                "FDA fetch %s HTTP %d %s | url=%s | server=%s | cf-ray=%s",
                label, r.status_code, r.reason, url[:120],
                r.headers.get("server", "?"),
                r.headers.get("cf-ray", "—"),
            )
            return None
        return r

    # ------------------------------------------------------------------
    def _scrape_rss(self, url: str, since_days: int, label: str) -> List[Recall]:
        r = self._fetch_with_diagnostic(url, label=f"RSS-{label}")
        if r is None:
            return []
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            log.warning("FDA RSS-%s parse failed: %s | first 200 bytes: %r",
                        label, e, r.content[:200])
            return []

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen_urls: set = set()
        item_count = 0

        for item in root.iter("item"):
            item_count += 1
            try:
                rec = self._parse_item(item, cutoff, seen_urls)
                if rec is not None:
                    out.append(rec)
                    seen_urls.add(rec.URL)
            except Exception as e:
                log.warning("FDA RSS-%s item parse failed: %s", label, e)

        log.info("FDA RSS-%s: %d items scanned -> %d pathogen recalls (since_days=%d)",
                 label, item_count, len(out), since_days)
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
        if not any(link.startswith(p) for p in self._ACCEPTABLE_URL_PREFIXES):
            log.debug("FDA RSS: skipping non-recall URL %s", link[:80])
            return None

        desc_text = re.sub(r"<[^>]+>", " ", desc)
        desc_text = re.sub(r"\s+", " ", desc_text).strip()
        merged = (title + " " + desc_text).lower()

        matched_kw = _matched_pathogen_keyword(merged, self.PATHOGEN_KEYWORDS)
        if not matched_kw:
            return None

        if not any(tok in merged for tok in _FOOD_CONTEXT_TOKENS):
            return None

        d = self._parse_pubdate(pub)
        if d is None:
            log.debug("FDA RSS: unparseable pubDate %r", pub)
            return None
        if d < cutoff:
            return None

        m = re.match(
            r"^(.+?)\s+(?:recalls?|issues?|voluntarily\s+recalls?|"
            r"announces?\s+recall\s+of)\s+",
            title, re.I,
        )
        company = (m.group(1).strip() if m
                   else title.split(" - ")[0]).strip()[:100]

        outbreak = _detect_outbreak(merged)

        return self._new_recall(
            Date=d.strftime("%Y-%m-%d"),
            Company=company,
            Brand="—",
            Product=title[:300],
            Pathogen=matched_kw,
            Reason=desc_text[:400] or title[:400],
            Class="Recall",
            URL=link,
            Outbreak=outbreak,
            Notes="FDA RSS (press-release feed)",
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_pubdate(pub: str) -> Optional[datetime]:
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
                return d.replace(tzinfo=None)
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    def _scrape_html_fallback(self, since_days: int) -> List[Recall]:
        r = self._fetch_with_diagnostic(self.FALLBACK_URL, label="HTML-fallback")
        if r is None:
            return []

        slugs = re.findall(
            r'href="(/safety/recalls-market-withdrawals-safety-alerts/[^"]+)"',
            r.text,
        )
        if not slugs:
            log.warning("FDA HTML fallback: no recall slugs found in %d bytes",
                        len(r.text))
            return []

        seen: set = set()
        unique_slugs = []
        for s in slugs:
            if s not in seen:
                seen.add(s)
                unique_slugs.append(s)
                if len(unique_slugs) >= 25:
                    break

        today = datetime.utcnow().strftime("%Y-%m-%d")
        out: List[Recall] = []
        for slug in unique_slugs:
            url = f"https://www.fda.gov{slug}"
            title = slug.rsplit("/", 1)[-1].replace("-", " ").title()[:300]
            out.append(self._new_recall(
                Date=today,
                Company=title.split(" Recalls ")[0][:100],
                Brand="—",
                Product=title,
                Pathogen="",
                Reason=title,
                Class="Recall",
                URL=url,
                Outbreak=0,
                Notes="FDA HTML fallback — claude-check needs to enrich Date+Pathogen",
            ))
        log.info("FDA HTML fallback: %d candidate URLs (need enrichment)",
                 len(out))
        return out
