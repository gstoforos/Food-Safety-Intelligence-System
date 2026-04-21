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

    # Keywords matched against openFDA's `reason_for_recall` field.
    # Extended April 2026 (HiPP baby-food tampering case) to include
    # rodenticides, heavy metals, and physical hazards. Biological keywords
    # are unchanged; new categories are grouped below.
    #
    # Note on ambiguous words:
    #   - "lead" alone would false-positive on "lead to illness", "lead times"
    #     etc. — so we require it paired with "contamin", "elevated lead",
    #     "levels of lead", or "lead in product".
    #   - "mercury" is rarely used outside the element in FDA recall reasons,
    #     so bare match is acceptable.
    PATHOGEN_REASONS = (
        # --- Biological (unchanged) ---
        "listeria", "salmonella", "e. coli", "e.coli", "escherichia coli", "stec",
        "botulin", "norovirus", "hepatitis", "campylobacter", "cyclospora",
        "vibrio", "cronobacter", "bacillus cereus", "cereulide", "shigella",
        "yersinia", "biotoxin", "histamine", "scombro", "domoic", "saxitoxin",
        "aflatoxin", "ochratoxin", "patulin", "mycotoxin",
        # --- Rodenticides / criminal tampering ---
        "rodenticide", "rat poison", "bromadiolon", "brodifacoum",
        "difethialon", "difenacoum", "chlorophacinon", "warfarin contamin",
        # --- Heavy metals (anchor "lead" to avoid false positives) ---
        "lead contamin", "elevated lead", "levels of lead", "lead in product",
        "excess lead", "cadmium", "arsenic", "mercury contamin",
        "mercury level", "heavy metal",
        # --- Physical / foreign-body hazards ---
        "glass fragment", "glass piece", "glass shard", "glass in",
        "metal fragment", "metal piece", "metal shard",
        "plastic fragment", "plastic piece",
        "foreign object", "foreign body", "foreign material",
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
            rec_url = (
                f"https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
                if not rec.get("more_code_info") else rec.get("more_code_info")
            )
            # FDA event ID -> can build a more specific URL via search
            ev_id = rec.get("event_id")
            if ev_id:
                rec_url = f"https://www.accessdata.fda.gov/scripts/ires/index.cfm?Product=&Event_ID={ev_id}"

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
                Notes=f"openFDA ev_id={ev_id}; distrib={rec.get('distribution_pattern','')[:120]}",
            ))
        log.info("FDA: %d hazard recalls (biological+chemical+physical) in last %d days",
                 len(out), since_days)
        return out
