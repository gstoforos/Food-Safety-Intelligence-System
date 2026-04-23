"""
scrapers/_rss_base.py
======================
Shared base class for REGULATOR RSS scrapers — agency RSS feeds that publish
structured recall announcements. Different from scrapers/news_feeds/_news_base.py
which handles JOURNALIST news feeds writing to the NEWS sheet.

Regulator RSS feeds are simpler and more reliable than HTML scraping:
  - Schema is fixed (<item><title><link><pubDate>...)
  - Layout changes don't break the parser
  - Agency publishes the moment the recall is public
  - Zero AI calls needed — RSS gives us everything structured

Each concrete subclass sets:
    AGENCY     = "FSAI (IE)"
    COUNTRY    = "Ireland"
    FEED_URL   = "https://www.fsai.ie/rss.aspx"
    LANGUAGE   = "en"   (optional, for pathogen keyword dialect)

and inherits .scrape(since_days) which:
    1. fetches the feed
    2. parses RSS / Atom XML
    3. filters to pathogen-related items only
    4. builds Recall objects via BaseScraper._new_recall()
    5. returns the list
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen

log = logging.getLogger(__name__)


# Comprehensive pathogen keyword list — matches the FDA / CFIA scraper set.
# Used BEFORE calling normalize_pathogen() as a cheap pre-filter (many RSS
# items have nothing to do with pathogens and should be dropped fast).
PATHOGEN_KEYWORDS = (
    # --- Biological ---
    "listeria", "salmonella", "e. coli", "e.coli", "escherichia coli",
    "stec", "o157", "o26", "o103", "o111", "o121", "o145",
    "shiga", "botulin", "norovirus", "hepatitis", "campylobacter",
    "cyclospora", "vibrio", "cronobacter", "bacillus cereus", "cereulide",
    "shigella", "yersinia", "biotoxin", "histamine", "scombro", "brucell",
    # --- Mould / spoilage ---
    "mould", "mold",
    # --- Mycotoxins ---
    "aflatoxin", "ochratoxin", "patulin", "mycotoxin", "fumonisin",
    "zearalenone", "deoxynivalenol",
    # --- Physical / foreign-body hazards ---
    "glass fragment", "metal fragment", "plastic fragment",
    "foreign object", "foreign body", "foreign material",
    # --- Chemical ---
    "ethylene oxide", "dioxin", "mineral oil", "moah", "mosh",
    "heavy metal", "lead contamin", "cadmium", "mercury contamin",
    "arsenic", "rodenticide", "rat poison", "chlorate",
    "sudan", "melamine",
    # --- Pest ---
    "rodent", "insect", "pest contamination",
    # --- Native-language equivalents (commonly seen in EU agency feeds) ---
    "listeriose", "listeriosis", "salmonellose", "salmonelose",
    "botulismus", "botulisme", "botulismo",
    "mikrobiologisch", "microbiológica", "microbiologico", "microbiologique",
    "moisissure", "schimmel",
)


# Non-pathogen markers — reject items that are only about these
NON_PATHOGEN_REJECTS = (
    "undeclared milk", "undeclared egg", "undeclared peanut", "undeclared soy",
    "undeclared wheat", "undeclared gluten", "undeclared nut", "undeclared fish",
    "undeclared shellfish", "undeclared sesame", "undeclared sulphite",
    "allergen labelling", "allergen labeling",
    "glass fragment", "glass piece", "plastic fragment", "plastic piece",
    "metal fragment", "metal piece", "foreign body", "foreign object",
)


def _parse_rss_date(raw: str) -> Optional[datetime]:
    """Try RFC-822, ISO-8601, and a few agency-specific formats."""
    if not raw:
        return None
    raw = raw.strip()

    # RFC-822 (most RSS) — email.utils handles it cleanly
    try:
        d = parsedate_to_datetime(raw)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except (TypeError, ValueError):
        pass

    # ISO-8601 (Atom) — various flavors
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d.%m.%Y",
    ):
        try:
            d = datetime.strptime(raw, fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d
        except ValueError:
            continue

    return None


def _strip_html(s: str) -> str:
    """Remove HTML tags from an RSS description field."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _extract_company_from_title(title: str) -> str:
    """Heuristic: pull a company name out of a typical RSS title."""
    if not title:
        return ""
    # "Acme Foods recalls Widget due to Salmonella" -> "Acme Foods"
    m = re.match(r"^(.+?)\s+(?:recalls?|recalled|brand|may contain|withdraws?|withdraw(?:al|n)|voluntarily)\b",
                 title, re.I)
    if m:
        return m.group(1).strip()[:100]
    # Fallback: take first chunk before " - " or " – " or ":"
    for sep in (" - ", " – ", ": "):
        if sep in title:
            return title.split(sep, 1)[0].strip()[:100]
    return title[:100]


class BaseRegulatorRSS(BaseScraper):
    """
    Drop-in RSS regulator scraper. Concrete subclass sets FEED_URL and
    inherits .scrape() unchanged in most cases.

    Override points for agency quirks:
        EXTRA_PATHOGEN_KEYWORDS : extend the default whitelist (e.g. CFIA adds
                                  heavy-metal and rodenticide keywords)
        FEED_URL                : RSS URL
        FEED_URLS               : multiple RSS URLs (subclass sets this OR FEED_URL)
        REQUIRE_FOOD_CONTEXT    : True (default) drops items that don't mention
                                  food/aliment/Lebensmittel/etc. Set False for
                                  agencies whose feed is already food-only.
        FOOD_CONTEXT_TERMS      : words indicating a food item
    """

    # Can be a single URL or a list — subclass chooses
    FEED_URL: str = ""
    FEED_URLS: List[str] = []

    # Subclasses can extend but should rarely need to override
    EXTRA_PATHOGEN_KEYWORDS: tuple = ()
    REQUIRE_FOOD_CONTEXT: bool = True
    FOOD_CONTEXT_TERMS: tuple = (
        "food", "aliment", "lebensmittel", "alimento", "élelmiszer",
        "produtos alimentares", "voedsel", "livsmedel", "ruoka",
        "élelmiszer", "foodstuff", "dairy", "meat", "fish", "seafood",
        "cheese", "poultry", "produce", "beverage", "infant formula",
    )

    # Optional: localize class attr for easier debugging
    LANGUAGE: str = "en"

    # --- Public API ---
    def scrape(self, since_days: int = 30) -> List[Recall]:
        urls: List[str] = []
        if self.FEED_URLS:
            urls.extend(self.FEED_URLS)
        if self.FEED_URL:
            urls.append(self.FEED_URL)
        if not urls:
            self.logger.warning("%s has no FEED_URL(S) configured — skipping",
                                type(self).__name__)
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        all_rows: List[Recall] = []

        for url in urls:
            self.logger.info("fetching RSS: %s", url)
            r = fetch(self.session, url)
            if r is None or not getattr(r, "ok", False):
                self.logger.warning("RSS fetch failed: %s (status=%s)",
                                    url, getattr(r, "status_code", "?"))
                continue

            try:
                items = self._parse_feed(r.content)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("RSS parse failed for %s: %s", url, exc)
                continue

            self.logger.info("%d raw items in feed", len(items))
            for item in items:
                rec = self._item_to_recall(item, cutoff)
                if rec is not None:
                    all_rows.append(rec)

        self.logger.info("%s: %d pathogen recalls within %dd",
                         self.AGENCY, len(all_rows), since_days)
        return all_rows

    # --- Feed parsing ---
    def _parse_feed(self, body: bytes) -> List[dict]:
        """Parse RSS 2.0 or Atom, return list of normalized item dicts."""
        root = ET.fromstring(body)
        items: List[dict] = []

        # RSS 2.0: /rss/channel/item
        for it in root.iter("item"):
            items.append({
                "title": (it.findtext("title") or "").strip(),
                "link": (it.findtext("link") or "").strip(),
                "description": _strip_html(it.findtext("description") or ""),
                "pubDate": (it.findtext("pubDate") or "").strip(),
            })

        if items:
            return items

        # Atom: entries with namespaces
        # Use endswith match to be namespace-agnostic
        for entry in root.iter():
            if not entry.tag.endswith("entry"):
                continue
            title = ""
            link = ""
            desc = ""
            pub = ""
            for child in entry:
                tag = child.tag.split("}", 1)[-1]  # strip namespace
                if tag == "title":
                    title = (child.text or "").strip()
                elif tag == "link":
                    link = child.get("href") or (child.text or "").strip()
                elif tag in ("summary", "content"):
                    desc = _strip_html(child.text or "")
                elif tag in ("published", "updated"):
                    pub = (child.text or "").strip()
            items.append({
                "title": title, "link": link,
                "description": desc, "pubDate": pub,
            })
        return items

    # --- Per-item: RSS row -> Recall ---
    def _item_to_recall(self, item: dict, cutoff: datetime) -> Optional[Recall]:
        title = item.get("title", "")
        link = item.get("link", "")
        desc = item.get("description", "")
        pub_raw = item.get("pubDate", "")

        if not link:
            return None

        merged = (title + " " + desc).lower()

        # Reject allergen-only / foreign-object-only items
        if any(bad in merged for bad in NON_PATHOGEN_REJECTS):
            # Only accept if a pathogen is ALSO present
            if not self._mentions_pathogen(merged):
                return None

        # Require pathogen keyword
        if not self._mentions_pathogen(merged):
            return None

        # Require food context (unless disabled)
        if self.REQUIRE_FOOD_CONTEXT:
            if not any(term in merged for term in self.FOOD_CONTEXT_TERMS):
                # Allow obvious food-type words even without "food"
                if not re.search(r"\b(beef|chicken|pork|lamb|milk|yogurt|cheese|butter|pasta|bread|infant formula|baby food|sausage|salami|ham|spread)\b", merged, re.I):
                    return None

        # Parse date — drop items too old or unparseable
        pub = _parse_rss_date(pub_raw)
        if pub is None:
            # Keep undated items with current timestamp rather than drop them —
            # better to have the row for URL gate to validate than silently miss it.
            pub = datetime.now(timezone.utc)
        elif pub < cutoff:
            return None

        # Extract pathogen canonical form
        pathogen = normalize_pathogen(title + " " + desc)

        # Extract company
        company = _extract_company_from_title(title)

        # Outbreak flag
        outbreak = 1 if re.search(
            r"\b(outbreak|illness|sick|death|hospitali[sz]|cases? reported|linked to illness)\b",
            merged,
        ) else 0

        return self._new_recall(
            Date=pub.strftime("%Y-%m-%d"),
            Company=company,
            Brand="—",
            Product=title[:300],
            Pathogen=pathogen or (title[:100] if pathogen == "" else pathogen),
            Reason=desc[:300] or title[:300],
            Class="Recall",
            URL=link,
            Outbreak=outbreak,
            Notes=f"{self.AGENCY} RSS",
        )

    def _mentions_pathogen(self, text_lc: str) -> bool:
        """Cheap pre-filter — is any pathogen keyword present?"""
        keywords = PATHOGEN_KEYWORDS + tuple(k.lower() for k in self.EXTRA_PATHOGEN_KEYWORDS)
        return any(kw in text_lc for kw in keywords)


__all__ = ["BaseRegulatorRSS", "PATHOGEN_KEYWORDS"]
