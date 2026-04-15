"""FSANZ Australia food recall list — HTML scrape."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
import re
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FSANZScraper(BaseScraper):
    AGENCY = "FSANZ (AU)"
    COUNTRY = "Australia"
    LIST_URL = "https://www.foodstandards.gov.au/food-recalls"

    PATHOGEN_KEYWORDS = (
        "listeria", "salmonella", "e. coli", "stec", "o157",
        "botulin", "hepatitis", "norovirus", "campylobacter",
        "cyclospora", "vibrio", "cronobacter", "cereulide",
        "biotoxin", "histamine",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        from bs4 import BeautifulSoup
        r = fetch(self.session, self.LIST_URL)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []

        # FSANZ recall items are list items with title + reason + state
        for item in soup.select("article, .recall, li.views-row, .field--name-node-title"):
            try:
                text = item.get_text(" ", strip=True)
                low = text.lower()
                if not any(p in low for p in self.PATHOGEN_KEYWORDS):
                    continue
                # Extract date (Published DD Month YYYY)
                m = re.search(r"published\s+(\d{1,2})\s+(\w+)\s+(\d{4})", text, re.I)
                if not m:
                    continue
                try:
                    d = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
                except ValueError:
                    continue
                if d < cutoff:
                    continue
                # Find link
                link_el = item.find("a", href=True)
                href = link_el["href"] if link_el else ""
                if href.startswith("/"):
                    href = "https://www.foodstandards.gov.au" + href
                # Title is usually before "Published"
                title = re.split(r"published\s+\d", text, maxsplit=1, flags=re.I)[0].strip()
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=title.split(" - ")[0][:120],
                    Brand="—",
                    Product=title[:300],
                    Pathogen=title[:200],
                    Reason=text[:400],
                    Class="Recall",
                    URL=href,
                    Outbreak=1 if "outbreak" in low or "illness" in low else 0,
                    Notes="",
                ))
            except Exception as e:
                log.warning("FSANZ item parse failed: %s", e)
        log.info("FSANZ: %d pathogen recalls", len(out))
        return out
