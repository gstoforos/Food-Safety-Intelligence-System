"""CFIA Canada food recall RSS feed.

Canada is officially BILINGUAL — CFIA recall notices appear in both English
and French on the same RSS feed (Anglophone provinces use the EN version,
Quebec uses the FR version, both reference the same underlying recall).
We pull from EN feed but match keywords in BOTH languages so a recall whose
RSS title happens to land in French (rare but does happen for Quebec-
distributed products) cannot be silently dropped.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
import re
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


class CFIAScraper(BaseScraper):
    AGENCY = "CFIA"
    COUNTRY = "Canada"
    FEED_URL = "https://recalls-rappels.canada.ca/en/rss.xml"

    # Bilingual keyword set: English (universal CORE) + French (Quebec).
    # Centralised — the previous hardcoded 60-keyword list duplicated terms
    # already in scrapers/_pathogen_vocab.py and was missing several
    # mycotoxin variants. Now derived from the single source of truth.
    PATHOGEN_KEYWORDS = for_languages("en", "fr")

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
        for item in root.iter("item"):
            try:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                desc = (item.findtext("description") or "").strip()
                pub = (item.findtext("pubDate") or "").strip()
                # Filter to food + pathogen recalls
                merged = (title + " " + desc).lower()
                if not any(p in merged for p in self.PATHOGEN_KEYWORDS):
                    continue
                # Food-context check (CFIA RSS includes consumer products too)
                if "food" not in merged and "aliment" not in merged:
                    if not any(k in merged for k in (
                        "recall - food", "rappel", "salmon", "listeria", "e. coli",
                        # Bilingual food-product hints
                        "viande", "fromage", "poisson", "lait", "produit laitier",
                        "meat", "cheese", "fish", "milk", "dairy",
                    )):
                        continue
                # Parse date
                try:
                    d = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
                except ValueError:
                    try:
                        d = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S GMT")
                    except ValueError:
                        continue
                if d.replace(tzinfo=None) < cutoff:
                    continue
                # Extract company/product from title — bilingual verb match
                m = re.match(
                    r"^(.+?)\s+(?:recalls?|brand|recalled|may contain|"
                    r"rappelle|rappelés?|marque|peut contenir).*",
                    title, re.I,
                )
                company = m.group(1).strip() if m else title.split(" - ")[0]
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company[:100],
                    Brand="—",
                    Product=title[:300],
                    Pathogen=desc[:200],
                    Reason=desc[:300],
                    Class="Recall",
                    URL=link,
                    Outbreak=0,
                    Notes="CFIA RSS",
                ))
            except Exception as e:
                log.warning("CFIA row parse failed: %s", e)
        log.info("CFIA: %d pathogen recalls", len(out))
        return out
