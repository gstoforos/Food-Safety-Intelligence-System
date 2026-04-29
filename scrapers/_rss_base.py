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
    LANGUAGE   = "en"   (drives multilingual keyword expansion — see below)

and inherits .scrape(since_days) which:
    1. fetches the feed
    2. parses RSS / Atom XML
    3. filters to pathogen-related items only (multilingual)
    4. builds Recall objects via BaseScraper._new_recall()
    5. returns the list

MULTILINGUAL FILTERING (audit 2026-04-29)
-----------------------------------------
Pathogen / hazard vocabulary lives in scrapers/_pathogen_vocab.py — single
source of truth, ~158 universal terms + ~493 native-language terms across
34 locales. The base class automatically merges:

    CORE  ∪  for_languages(self.LANGUAGE)  ∪  self.EXTRA_PATHOGEN_KEYWORDS

every time _mentions_pathogen() is called. Concrete subclasses get
correct local-language coverage just by setting LANGUAGE; they only need
to add EXTRA_PATHOGEN_KEYWORDS for genuinely-unique vocabulary not yet
in _pathogen_vocab.BY_LANGUAGE.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional, Tuple

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen
from scrapers._pathogen_vocab import (
    CORE,
    NON_PATHOGEN_REJECTS as VOCAB_REJECTS,
    for_languages,
)

log = logging.getLogger(__name__)


# Backward-compat re-export — anything that previously did
#   `from scrapers._rss_base import PATHOGEN_KEYWORDS`
# still works. Now sourced from the central vocab.
PATHOGEN_KEYWORDS: Tuple[str, ...] = CORE


# Backward-compat: the original module had its own NON_PATHOGEN_REJECTS list.
# Mirrored to the centralised one but kept available under the old name.
NON_PATHOGEN_REJECTS: Tuple[str, ...] = VOCAB_REJECTS


# ─────────────────────────────────────────────────────────────────────────
# Per-LANGUAGE keyword cache — for_languages() does some work, no point
# re-doing it on every RSS item.
# ─────────────────────────────────────────────────────────────────────────
_LANG_CACHE: dict = {}


def _keywords_for_language(lang: str) -> Tuple[str, ...]:
    if lang not in _LANG_CACHE:
        _LANG_CACHE[lang] = for_languages(lang)
    return _LANG_CACHE[lang]


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
        LANGUAGE                : ISO-639 code. Drives multilingual keyword
                                  expansion via _pathogen_vocab.for_languages().
                                  Use the BY_LANGUAGE keys: "fr", "de", "es",
                                  "it", "pt", "nl", "sv", "da", "no", "fi",
                                  "pl", "hu", "ro", "bg", "cs", "sk", "sl",
                                  "hr", "et", "lv", "lt", "el", "tr", "ar",
                                  "he", "ja", "ko", "zh", "zh-Hant", "id",
                                  "ms", "th", "vi", "is", "en".
        EXTRA_PATHOGEN_KEYWORDS : agency-specific terms NOT yet in the central
                                  vocab. Use sparingly — better to add to
                                  _pathogen_vocab.BY_LANGUAGE so other agencies
                                  benefit too. Tuple of lowercase strings.
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
        # Multilingual additions (audit 2026-04-29) so REQUIRE_FOOD_CONTEXT
        # doesn't silently drop foreign-language fiches that lack the
        # English word "food".
        "food", "alimentos", "alimentaire", "alimentar",
        "ζώο", "τρόφιμο", "τρόφιμα",                  # Greek
        "élelmiszer",                                  # Hungarian
        "żywność",                                     # Polish
        "pārtika", "maistas", "toit", "toiduainete",   # LV/LT/ET
        "храна",                                       # Bulgarian
        "potravina", "potraviny",                      # CZ/SK
        "živilo", "živila", "hrana", "hrane",          # SI/HR
        "gıda",                                        # Turkish
        "غذاء",                                        # Arabic
        "מזון",                                        # Hebrew
        "食品", "食物",                                  # JA/ZH/KO
        "식품",                                         # Korean
        "makanan",                                     # Indonesian/Malay
        "อาหาร",                                       # Thai
        "thực phẩm",                                  # Vietnamese
        "matvæli",                                     # Icelandic
    )

    # Optional: localise class attr for easier debugging AND multilingual
    # keyword expansion. Default "en" gives you English + the universal
    # CORE list, which is what every previously-existing RSS scraper
    # already had — so this change is fully backward-compatible.
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

        # RSS 2.0
        items_xml = list(root.iter("item"))
        items: List[dict] = []
        if items_xml:
            for it in items_xml:
                items.append({
                    "title":       (it.findtext("title") or "").strip(),
                    "link":        (it.findtext("link") or "").strip(),
                    "description": _strip_html(it.findtext("description") or ""),
                    "pubDate":     (it.findtext("pubDate")
                                    or it.findtext("{http://purl.org/dc/elements/1.1/}date")
                                    or "").strip(),
                })
            return items

        # Atom
        atom_ns = "{http://www.w3.org/2005/Atom}"
        for it in root.iter(f"{atom_ns}entry"):
            title = (it.findtext(f"{atom_ns}title") or "").strip()
            link = ""
            for ln in it.iter(f"{atom_ns}link"):
                if ln.attrib.get("rel", "alternate") == "alternate":
                    link = ln.attrib.get("href", "").strip()
                    break
            desc = ""
            pub = ""
            for child in it:
                tag = child.tag.replace(atom_ns, "")
                if tag in ("summary", "content"):
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
            if not any(term.lower() in merged for term in self.FOOD_CONTEXT_TERMS):
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

        # Outbreak flag (multilingual: also catches German, French,
        # Spanish, Italian, Portuguese illness terms)
        outbreak = 1 if re.search(
            r"\b("
            r"outbreak|illness|sick|death|hospitali[sz]|cases? reported|linked to illness|"
            r"épidémie|maladie|hospitalisé|décès|"
            r"ausbruch|krankheit|krankenhaus|todesfall|"
            r"brote|enfermedad|hospitalizado|muerte|"
            r"epidemia|malattia|ricoverato|decesso|"
            r"surto|doença|hospitalizado|"
            r"επιδημία|ασθένεια|νοσηλεύεται"
            r")\b",
            merged, re.IGNORECASE | re.UNICODE,
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
        """Cheap pre-filter — is any pathogen keyword present?

        Combines (in order, deduped via _LANG_CACHE):
          - CORE (universal scientific names)
          - for_languages(self.LANGUAGE) (locale-specific terms)
          - self.EXTRA_PATHOGEN_KEYWORDS (agency-specific overrides)
        """
        kws = _keywords_for_language(self.LANGUAGE) + tuple(
            k.lower() for k in self.EXTRA_PATHOGEN_KEYWORDS
        )
        return any(kw in text_lc for kw in kws)


__all__ = [
    "BaseRegulatorRSS",
    "PATHOGEN_KEYWORDS",        # backward-compat re-export
    "NON_PATHOGEN_REJECTS",     # backward-compat re-export
]
