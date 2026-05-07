"""FDA Food Recalls — uses openFDA API (clean) + Gemini for parsing messy product_description."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)

# openFDA enforcement-reports endpoint has a documented publication lag of
# 5–30 days from press release to API visibility (recalls flow in only after
# FDA formally classifies them in the weekly enforcement-reports cycle —
# see scrapers/north_america/fda_press.py docstring for the full audit trail).
#
# The orchestrator runs daily with SINCE_DAYS=2 (a tight rolling window
# appropriate for fast feeds like RappelConso / RASFF). Passing that 2-day
# window straight through to openFDA would guarantee 0 results on virtually
# every run because no recall has had time to flow into the API yet.
#
# Floor the lookback at MIN_SINCE_DAYS internally so we always sweep the
# full lag horizon. merge_master dedupes by URL across runs, so re-fetching
# is safe and free; the extra work also lets us absorb late Class I/II/III
# classifications, distribution_pattern fields etc. that openFDA back-fills
# onto previously-seen recalls.
MIN_SINCE_DAYS = 35


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
        # Floor the lookback to absorb openFDA's 5–30d publication lag
        # (see module docstring). The caller's smaller window is preserved
        # in `requested_window` for the Notes field.
        requested_window = since_days
        effective_window = max(since_days, MIN_SINCE_DAYS)
        if effective_window != requested_window:
            log.info(
                "FDA openFDA: orchestrator since_days=%d, but openFDA has "
                "5–30d publication lag — using effective window %d days",
                requested_window, effective_window,
            )
        since = (datetime.utcnow() - timedelta(days=effective_window)).strftime("%Y%m%d")
        # openFDA expects URL-encoded query; requests' default encoding mangles
        # the [TO] syntax so we pre-build the URL.
        url = f"{self.BASE_URL}?search=recall_initiation_date:[{since}+TO+99991231]&limit=100"
        r = fetch(self.session, url)
        if r is None or r.status_code != 200:
            log.warning("FDA openFDA: fetch failed (status=%s) for %s",
                        r.status_code if r is not None else None, url)
            return []
        data = r.json()
        total_results = len(data.get("results", []) or [])
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
        log.info(
            "FDA openFDA: %d pathogen recalls in last %d days "
            "(API returned %d total, %d non-pathogen filtered out)",
            len(out), effective_window, total_results, total_results - len(out),
        )
        return out
