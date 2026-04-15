"""CFIA Canada food recall RSS feed."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
import re
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class CFIAScraper(BaseScraper):
    AGENCY = "CFIA"
    COUNTRY = "Canada"
    FEED_URL = "https://recalls-rappels.canada.ca/en/rss.xml"

    PATHOGEN_KEYWORDS = (
        "listeria", "salmonella", "e. coli", "stec", "o157",
        "botulin", "norovirus", "hepatitis", "campylobacter",
        "cyclospora", "vibrio", "cronobacter", "bacillus", "histamine",
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
                if "food" not in merged and "aliment" not in merged:
                    # CFIA RSS includes consumer products too
                    if not any(k in merged for k in ("recall - food", "rappel", "salmon", "listeria", "e. coli")):
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
                # Extract company/product from title (format: "Company recalls Product due to ...")
                m = re.match(r"^(.+?)\s+(?:recalls?|brand|recalled|may contain).*", title, re.I)
                company = m.group(1).strip() if m else title.split(" - ")[0]
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company[:100],
                    Brand="—",
                    Product=title[:300],
                    Pathogen=title[:120],
                    Reason=desc[:300] or title[:300],
                    Class="Recall",
                    URL=link,
                    Outbreak=1 if "illness" in merged or "outbreak" in merged else 0,
                    Notes="CFIA RSS",
                ))
            except Exception as e:
                log.warning("CFIA item parse failed: %s", e)
        log.info("CFIA: %d pathogen recalls", len(out))
        return out
