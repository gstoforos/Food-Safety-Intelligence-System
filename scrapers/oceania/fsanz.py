"""FSANZ (Food Standards Australia New Zealand) food recall list — HTML scrape.

Rewrite notes (April 2026):
    The prior version set ``Pathogen = title[:200]`` — i.e. stored the entire
    recall headline as the canonical hazard. That worked when titles were
    short like "Listeria monocytogenes recall", but on the FSANZ listing
    page the same CSS selector (``article, .recall, li.views-row, ...``)
    occasionally matches the page's intro paragraph, whose text mentions
    "listeria" as an example hazard — which passed the keyword filter and
    wrote the whole paragraph to Pathogen.

    This version:
      - Extracts the canonical hazard via the shared ``normalize_pathogen``
        taxonomy from `_models.PATHOGEN_RULES` (same as every other scraper).
      - Rejects any "recall item" whose link is the listing page itself (a
        strong signal we matched a page-chrome block, not a real row).
      - Requires a published date *and* a per-recall URL (absence of either
        means we're looking at boilerplate, not a recall).
      - Uses ``datetime.now(timezone.utc)`` instead of deprecated ``utcnow()``.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen

log = logging.getLogger(__name__)


# Hazard keyword gate: text must contain at least one of these before we
# bother extracting. Mirrors the canonical taxonomy's key terms.
_HAZARD_GATE = (
    "listeria", "salmonella", "e. coli", "stec", "o157", "shiga",
    "botulin", "hepatitis", "hav", "norovirus", "campylobacter",
    "cyclospora", "vibrio", "cronobacter", "cereulide", "bacillus cereus",
    "biotoxin", "histamine", "shellfish", "aflatoxin", "ochratoxin",
    "patulin", "yersinia", "shigella", "mycotoxin",
    # Chemical tampering / heavy metals / foreign-body (April 2026+ scope)
    "rodenticide", "rat poison", "bromadiolon",
    "lead contamin", "cadmium", "arsenic", "mercury",
    "glass fragm", "metal fragm", "plastic fragm", "foreign matter",
    "foreign body",
)


def _extract_pathogen(text: str) -> str:
    """
    Try to produce a canonical Pathogen value from free recall-page text.
    Returns "" if nothing matched a canonical rule — caller should drop the
    row rather than store free-text or the raw title.
    """
    if not text:
        return ""
    canonical = normalize_pathogen(text)
    # normalize_pathogen falls through to raw text when nothing matched;
    # reject anything that didn't actually hit a rule. A rule-hit output
    # is short (< 80 chars) and different from the input.
    if canonical and canonical != text and len(canonical) < 80:
        return canonical
    return ""


class FSANZScraper(BaseScraper):
    AGENCY = "FSANZ (AU)"
    COUNTRY = "Australia"
    LIST_URL = "https://www.foodstandards.gov.au/food-recalls"

    # Boilerplate URLs that must NEVER appear as a recall's URL —
    # seeing any of these means the scraper matched a page-chrome block.
    _LISTING_URLS = {
        "https://www.foodstandards.gov.au/food-recalls",
        "https://www.foodstandards.gov.au/food-recalls/",
        "https://www.foodstandards.gov.au/food-recalls/recalls",
        "https://www.foodstandards.gov.au/food-recalls/recalls/",
    }

    def scrape(self, since_days: int = 30) -> List[Recall]:
        from bs4 import BeautifulSoup

        r = fetch(self.session, self.LIST_URL)
        if not r:
            log.warning("FSANZ: list-page fetch returned None")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

        out: List[Recall] = []
        dropped_no_date = dropped_listing_url = dropped_no_hazard = dropped_stale = 0

        for item in soup.select("article, .recall, li.views-row, .field--name-node-title"):
            try:
                text = item.get_text(" ", strip=True)
                low = text.lower()
                if not any(g in low for g in _HAZARD_GATE):
                    continue

                # Published date: "Published DD Month YYYY"
                m = re.search(r"published\s+(\d{1,2})\s+(\w+)\s+(\d{4})", text, re.I)
                if not m:
                    dropped_no_date += 1
                    continue
                try:
                    d = datetime.strptime(
                        f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    dropped_no_date += 1
                    continue
                if d < cutoff:
                    dropped_stale += 1
                    continue

                # Per-recall URL — MUST be a specific page, not the listing.
                link_el = item.find("a", href=True)
                href = (link_el["href"] if link_el else "").strip()
                if href.startswith("/"):
                    href = "https://www.foodstandards.gov.au" + href
                if not href or href in self._LISTING_URLS:
                    dropped_listing_url += 1
                    continue

                # Title is usually before "Published"
                title = re.split(r"published\s+\d", text,
                                 maxsplit=1, flags=re.I)[0].strip()

                # Canonical hazard from title+full-text (rule-hit only).
                pathogen = _extract_pathogen(title) or _extract_pathogen(text)
                if not pathogen:
                    dropped_no_hazard += 1
                    continue

                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=title.split(" - ")[0][:120],
                    Brand="—",
                    Product=title[:300],
                    Pathogen=pathogen,
                    Reason=text[:400],
                    Class="Recall",
                    URL=href,
                    Outbreak=1 if ("outbreak" in low or "illness" in low) else 0,
                    Notes="",
                ))
            except Exception as e:
                log.warning("FSANZ item parse failed: %s", e)

        log.info(
            "FSANZ: %d recalls captured (%d no-date, %d listing-URL, "
            "%d no-hazard, %d stale)",
            len(out), dropped_no_date, dropped_listing_url,
            dropped_no_hazard, dropped_stale,
        )
        return out
