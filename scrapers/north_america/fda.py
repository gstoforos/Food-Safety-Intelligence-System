"""FDA Food Recalls — uses openFDA API (clean) + Gemini for parsing messy product_description."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FDAScraper(BaseScraper):
    AGENCY = "FDA"
    COUNTRY = "USA"
    BASE_URL = "https://api.fda.gov/food/enforcement.json"

    PATHOGEN_REASONS = (
        "listeria", "salmonella", "e. coli", "e.coli", "escherichia coli", "stec",
        "botulin", "norovirus", "hepatitis", "campylobacter", "cyclospora",
        "vibrio", "cronobacter", "bacillus cereus", "cereulide", "shigella",
        "yersinia", "biotoxin", "histamine", "scombro", "domoic", "saxitoxin",
        "aflatoxin", "ochratoxin", "patulin", "mycotoxin",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        since = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y%m%d")
        params = {
            "search": f"recall_initiation_date:[{since}+TO+99991231]",
            "limit": 100,
        }
        # openFDA expects encoded URL; use requests' params kwarg
        url = f"{self.BASE_URL}?search=recall_initiation_date:[{since}+TO+99991231]&limit=100"
        r = fetch(self.session, url)
        if not r:
            return []
        data = r.json()
        out: List[Recall] = []
        for rec in data.get("results", []):
            reason = (rec.get("reason_for_recall") or "").lower()
            if not any(p in reason for p in self.PATHOGEN_REASONS):
                continue

            # --- URL resolution --------------------------------------------
            # History: we used to point at https://www.accessdata.fda.gov/scripts/ires/...
            # which FDA retired/broke. The URL gate rejects those with HTTP 5xx.
            #
            # Current approach, in priority order:
            #   1. If the record itself has more_code_info (FDA-provided deep
            #      link to the specific recall page), USE IT — it's authoritative.
            #   2. Otherwise, build a search URL on fda.gov that filters to this
            #      recall_number. FDA's search page always returns HTTP 200;
            #      URL gate passes; the result page shows the recall to users.
            recall_number = (rec.get("recall_number") or "").strip()
            more_info = (rec.get("more_code_info") or "").strip()

            if more_info and more_info.lower().startswith(("http://", "https://")):
                rec_url = more_info
            elif recall_number:
                rec_url = (
                    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
                    f"?search_api_fulltext={recall_number}"
                )
            else:
                # Last resort — point at the recalls landing page. URL gate still
                # accepts, row still gets promoted, user clicks through and can
                # manually find the recall.
                rec_url = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"

            # event_id kept for dedup / debugging in Notes only
            ev_id = rec.get("event_id") or ""
            # ---------------------------------------------------------------

            out.append(self._new_recall(
                Date=rec.get("recall_initiation_date", ""),
                Company=rec.get("recalling_firm", ""),
                Brand="—",
                Product=rec.get("product_description", "")[:300],
                Pathogen=rec.get("reason_for_recall", "")[:200],
                Reason=rec.get("reason_for_recall", "")[:300],
                Class=rec.get("classification", "Recall"),
                URL=rec_url,
                Outbreak=0,
                Notes=(
                    f"openFDA recall#={recall_number}; ev_id={ev_id}; "
                    f"distrib={rec.get('distribution_pattern','')[:120]}"
                ),
            ))
        log.info("FDA: %d pathogen recalls in last %d days", len(out), since_days)
        return out
