"""UK FSA Food Alerts — official JSON feed."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FSAUKScraper(BaseScraper):
    AGENCY = "FSA (UK)"
    COUNTRY = "United Kingdom"
    FEED_URL = "https://data.food.gov.uk/food-alerts/id?_sort=-created&_view=published&_pageSize=100"

    PATHOGEN_KEYWORDS = (
        "listeria", "salmonella", "e. coli", "stec", "o157",
        "botulin", "norovirus", "hepatitis", "campylobacter",
        "cyclospora", "vibrio", "cronobacter", "bacillus",
        "histamine", "biotoxin", "aflatoxin", "ochratoxin",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        r = fetch(self.session, self.FEED_URL, headers={"Accept": "application/json"})
        if not r:
            return []
        try:
            data = r.json()
        except Exception:
            return []
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        for item in data.get("items", []):
            try:
                created = item.get("created", "")
                if not created:
                    continue
                d = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
                if d < cutoff:
                    continue
                summary = ((item.get("notation") or "") + " " +
                           (item.get("title") or "") + " " +
                           (item.get("description") or "")).lower()
                if not any(p in summary for p in self.PATHOGEN_KEYWORDS):
                    continue
                business = item.get("business", {})
                company = business.get("name", "") if isinstance(business, dict) else ""
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company or item.get("title", "")[:80],
                    Brand="—",
                    Product=item.get("title", "")[:300],
                    Pathogen=item.get("title", "")[:200],
                    Reason=item.get("description", "")[:300],
                    Class=item.get("alertType", {}).get("notation", "Alert") if isinstance(item.get("alertType"), dict) else "Alert",
                    URL=item.get("@id", "") or item.get("publication", {}).get("@id", ""),
                    Outbreak=1 if "illness" in summary or "outbreak" in summary else 0,
                    Notes=item.get("ftype", ""),
                ))
            except Exception as e:
                log.warning("FSA UK item parse failed: %s", e)
        log.info("FSA UK: %d pathogen alerts", len(out))
        return out
