"""USDA FSIS recalls — uses official FSIS recall API (JSON)."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


class USDAFSISScraper(BaseScraper):
    AGENCY = "USDA FSIS"
    COUNTRY = "USA"
    BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"

    # Audit 2026-04-29: previous list had only 9 keywords. FSIS recalls
    # routinely cite Bacillus cereus, Cronobacter, Cyclospora, Norovirus,
    # Hepatitis A and a handful of mycotoxins (e.g. peanut-paste aflatoxin
    # cross-contaminations) — none of which would match a 9-keyword list.
    # Now uses the central vocab CORE list (158 universal terms) which
    # includes every pathogen FSIS has ever cited as a recall trigger.
    PATHOGEN_KEYWORDS = for_languages("en")

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        r = fetch(self.session, self.BASE_URL)
        if not r:
            return []
        try:
            data = r.json()
        except Exception:
            return []
        out: List[Recall] = []
        for rec in data:
            try:
                date_str = rec.get("field_recall_date") or rec.get("field_last_modified_date") or ""
                if not date_str:
                    continue
                # FSIS dates may be MM/DD/YYYY or YYYY-MM-DD
                try:
                    d = datetime.fromisoformat(date_str[:10])
                except ValueError:
                    try:
                        d = datetime.strptime(date_str[:10], "%m/%d/%Y")
                    except ValueError:
                        continue
                if d < cutoff:
                    continue
                reason = (rec.get("field_summary", "") or "").lower()
                if not any(p in reason for p in self.PATHOGEN_KEYWORDS):
                    continue
                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=rec.get("field_establishment", "") or rec.get("field_recall_company", ""),
                    Brand="—",
                    Product=(rec.get("field_product_items", "") or rec.get("title", ""))[:300],
                    Pathogen=rec.get("field_summary", "")[:200],
                    Reason=rec.get("field_summary", "")[:300],
                    Class=rec.get("field_recall_classification", "Recall"),
                    URL=rec.get("field_recall_url", "") or rec.get("url", ""),
                    Outbreak=1 if "illness" in reason or "outbreak" in reason else 0,
                    Notes=f"FSIS recall #{rec.get('field_recall_number','')}; {rec.get('field_states','')}",
                ))
            except Exception as e:
                log.warning("FSIS row parse failed: %s", e)
        log.info("USDA FSIS: %d pathogen recalls", len(out))
        return out
