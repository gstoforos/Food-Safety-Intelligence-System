"""CFIA Canada food recall RSS feed.

Canada is officially BILINGUAL — CFIA recall notices appear in both English
and French on the same RSS feed (Anglophone provinces use the EN version,
Quebec uses the FR version, both reference the same underlying recall).
We pull from EN feed but match keywords in BOTH languages so a recall whose
RSS title happens to land in French (rare but does happen for Quebec-
distributed products) cannot be silently dropped.

Audit 2026-05-05 — fixes vs the previous active version:
  1. Outbreak detection. Was hardcoded ``Outbreak=0``. Now scans merged
     title+desc for English ("outbreak", "illnesses", "linked to") and
     French ("éclosion", "personnes malades", "lié à des malad")
     signals. Conservative — actual outbreak validation (case counts)
     is the reviewer's job per fsis_reviewer_prompts.md H-rules.
  2. Company normalisation. Was none. Now runs ``normalise_company_brand``
     so RSS-title cruft ("Brand X recalls", trailing punctuation, etc.)
     is stripped consistently with every other scraper.
  3. Pathogen field. Was ``desc[:200]`` — a raw RSS description that may
     not even contain the pathogen name in the first 200 chars. Now we
     track which PATHOGEN_KEYWORDS entry actually matched and pass that;
     Recall.normalize() canonicalises it via normalize_pathogen() so
     "salmonella" becomes "Salmonella", "listeria monocytogenes" stays
     as-is, etc. This also makes assign_tier() tier-classification
     deterministic (the keyword is always recognisable).
  4. Defensive URL filter. CFIA RSS shouldn't emit /search/site or
     /recherche/* URLs — those are search-result shells, not recalls —
     but if it ever does (e.g. an RSS rendering glitch), drop the row
     here instead of polluting Pending. Same patterns as
     merge_master._GENERIC_URL_PATTERNS for consistency.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages
from scrapers._company_normalise import normalise_company_brand

log = logging.getLogger(__name__)


# Outbreak signal phrases (EN + FR). Conservative — single keyword "illness"
# alone is too noisy ("food caused minor illness symptoms" appears on many
# routine recalls). Require either an explicit outbreak word OR an illness
# token + linkage word.
_OUTBREAK_TOKENS_EN = ("outbreak", "illnesses linked", "linked to illness",
                       "linked to investigation", "associated with illness")
_OUTBREAK_TOKENS_FR = ("éclosion", "eclosion", "personnes malades",
                       "lié à des malad", "liée à des malad", "cas de malad")

# Defensive URL filter — see module docstring item 4. These are substrings,
# checked case-insensitive against the URL. Mirrors a subset of
# merge_master._GENERIC_URL_PATTERNS that could plausibly slip into an RSS
# link field. Keep in sync with merge_master.
_GENERIC_URL_SUBSTRINGS = (
    "/search/site",      # CFIA advanced-search shell
    "/recherche?",       # FR search query
    "/recherche/",       # FR search path
    "/page/",            # paginated listings
    "page=",             # paginated query strings
)


def _detect_outbreak(merged_lower: str) -> int:
    if any(t in merged_lower for t in _OUTBREAK_TOKENS_EN):
        return 1
    if any(t in merged_lower for t in _OUTBREAK_TOKENS_FR):
        return 1
    return 0


def _matched_pathogen_keyword(merged_lower: str,
                              keywords: Tuple[str, ...]) -> Optional[str]:
    """Return the first PATHOGEN_KEYWORDS entry that appears in the text,
    or None. Used to drive the Pathogen field deterministically rather than
    feeding raw RSS desc to normalize_pathogen()."""
    for kw in keywords:
        if kw in merged_lower:
            return kw
    return None


def _is_generic_url(url: str) -> bool:
    if not url:
        return True
    u = url.lower()
    return any(p in u for p in _GENERIC_URL_SUBSTRINGS)


class CFIAScraper(BaseScraper):
    AGENCY = "CFIA"
    COUNTRY = "Canada"
    FEED_URL = "https://recalls-rappels.canada.ca/en/rss.xml"

    # Bilingual keyword set: English (universal CORE) + French (Quebec).
    # Centralised — the previous hardcoded 60-keyword list duplicated terms
    # already in scrapers/_pathogen_vocab.py and was missing several
    # mycotoxin variants. Now derived from the single source of truth.
    PATHOGEN_KEYWORDS = for_languages("en", "fr")

    # Bilingual food-context tokens (Anglophone + Quebec FR). At least one
    # must appear, otherwise the recall is non-food (consumer products,
    # cosmetics, etc.) and we drop it. CFIA RSS multiplexes everything.
    _FOOD_CONTEXT_TOKENS = (
        "food", "aliment",
        "recall - food", "rappel",
        "salmon", "listeria", "e. coli",
        "viande", "fromage", "poisson", "lait", "produit laitier",
        "meat", "cheese", "fish", "milk", "dairy",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        from xml.etree import ElementTree as ET
        r = fetch(self.session, self.FEED_URL)
        if not r:
            return []
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            log.warning("CFIA RSS parse failed: %s", e)
            return []

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        seen_urls = set()

        for item in root.iter("item"):
            try:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                desc = (item.findtext("description") or "").strip()
                pub = (item.findtext("pubDate") or "").strip()

                if not link or _is_generic_url(link):
                    continue
                if link in seen_urls:
                    continue

                merged = (title + " " + desc).lower()

                # 1. Pathogen / hazard filter — must match at least one keyword
                matched_kw = _matched_pathogen_keyword(merged, self.PATHOGEN_KEYWORDS)
                if not matched_kw:
                    continue

                # 2. Food-context filter — drop non-food consumer products
                if not any(tok in merged for tok in self._FOOD_CONTEXT_TOKENS):
                    continue

                # 3. Date parse
                try:
                    d = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
                except ValueError:
                    try:
                        d = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S GMT")
                    except ValueError:
                        log.debug("CFIA: skipping item with unparseable date %r", pub)
                        continue
                if d.replace(tzinfo=None) < cutoff:
                    continue

                # 4. Company / brand extraction (bilingual verb match)
                m = re.match(
                    r"^(.+?)\s+(?:recalls?|brand|recalled|may contain|"
                    r"rappelle|rappelés?|marque|peut contenir).*",
                    title, re.I,
                )
                raw_company = (m.group(1).strip() if m
                               else title.split(" - ")[0]).strip()
                co, br = normalise_company_brand(raw_company[:100], "—")

                # 5. Outbreak detection
                outbreak = _detect_outbreak(merged)

                # 6. Build Recall — _new_recall canonicalises Pathogen via
                # normalize_pathogen() and recomputes Tier via assign_tier()
                # using the (canonicalised) Pathogen + Outbreak.
                seen_urls.add(link)
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=co,
                    Brand=br,
                    Product=title[:300],
                    Pathogen=matched_kw,    # normalize_pathogen will canonicalise
                    Reason=desc[:400] or title[:400],
                    Class="Recall",
                    URL=link,
                    Outbreak=outbreak,
                    Notes="CFIA RSS",
                ))
            except Exception as e:
                log.warning("CFIA row parse failed: %s", e)

        log.info("CFIA: %d pathogen recalls (since_days=%d)",
                 len(out), since_days)
        return out
