"""UK FSA Food Alerts — official JSON feed at data.food.gov.uk.

AUDIT 2026-05-06
================
FSA UK has been capturing only 7 of the first 20 PRINs of 2026 (35%).
Missing PRINs: 1, 3, 4, 5, 6, 7, 8, 9, 12, 13, 16, 17, 18.

Without per-PRIN audit logging it's impossible to tell whether those
13 are legitimate filter skips (allergen-only / labeling / withdrawal)
or real misses (vague-pathogen alerts the keyword filter doesn't catch).

This version adds explicit per-row diagnostic logging:
  • Every alert returned by the API gets a one-line log entry
  • Each entry shows the PRIN ID, the disposition (KEPT / DROPPED-<reason>),
    and the alert title (truncated)
  • Drop reasons:
      DROP-DATE      — older than since_days cutoff
      DROP-NO-URL    — alert had no @id field
      DROP-NO-PATHOGEN — keyword filter didn't match
      DROP-PARSE     — exception during parsing (rare)

Operators can grep workflow logs for "FSA UK alert" to see exactly
what was filtered and why.

The keyword filter itself is unchanged in this commit — broadening it
is a separate decision (Fix 3 in the audit). This commit just gives
us the data to make that decision.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class FSAUKScraper(BaseScraper):
    AGENCY = "FSA (UK)"
    COUNTRY = "United Kingdom"
    FEED_URL = (
        "https://data.food.gov.uk/food-alerts/id?_sort=-created"
        "&_view=published&_pageSize=100"
    )

    PATHOGEN_KEYWORDS = (
        "listeria", "salmonella", "e. coli", "stec", "o157",
        "botulin", "norovirus", "hepatitis", "campylobacter",
        "cyclospora", "vibrio", "cronobacter", "bacillus",
        "histamine", "biotoxin",
        # Mycotoxins
        "aflatoxin", "ochratoxin", "patulin", "mycotoxin",
        "fumonisin", "zearalenone", "deoxynivalenol", "nivalenol",
        "alternaria", "alternariol", "tenuazonic",
        "t-2 toxin", "ht-2 toxin", "citrinin",
        "ergot", "claviceps", "fusarium",
        "ocratoxin", "ocratossin", "mykotoxin", "micotoxin",
        "micotossin", "mutterkorn",
        "mould", "mold",
        # Foreign objects
        "glass", "metal fragment", "plastic fragment",
        "foreign object", "foreign body", "foreign material",
        # Chemicals
        "ethylene oxide", "dioxin", "heavy metal",
        "lead contamin", "cadmium", "mercury", "arsenic",
        # Pests
        "rodenticide", "rat poison", "rodent", "insect",
        "chlorate", "sudan", "melamine", "mineral oil",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        # 2026-05-07 audit: previous code used `if not r:` which collapses
        # `r is None` (network/exception) and `r.status_code >= 400`
        # (HTTP error response) into the same branch with the misleading
        # message "no response". Distinguish both, log the actual status,
        # and include a body preview so future debugging starts with real
        # data — same pattern as FDA / CFIA / FSAI.
        r = fetch(self.session, self.FEED_URL,
                  headers={"Accept": "application/json"})
        if r is None:
            log.warning("FSA UK: fetch returned None (network/timeout/DNS) "
                        "for %s", self.FEED_URL)
            return []
        if r.status_code != 200:
            log.warning("FSA UK: HTTP %d for %s (body[:120]=%r)",
                        r.status_code, self.FEED_URL,
                        r.text[:120] if r.text else "")
            return []
        try:
            data = r.json()
        except Exception as e:
            log.warning("FSA UK: JSON parse failed: %s | body[:120]=%r",
                        e, r.text[:120] if r.text else "")
            return []

        items = data.get("items", [])
        log.info("FSA UK: API returned %d alerts (since_days=%d)",
                 len(items), since_days)

        cutoff = datetime.utcnow() - timedelta(days=since_days)
        out: List[Recall] = []
        n_kept = n_drop_date = n_drop_url = n_drop_pathogen = n_drop_parse = 0

        for item in items:
            # Every alert gets a diagnostic line. PRIN ID is extracted
            # from the @id URL (e.g. .../alert/fsa-prin-20-2026).
            pub_dict = item.get("publication") if isinstance(item.get("publication"), dict) else {}
            url = item.get("@id") or pub_dict.get("@id", "") or ""
            url = str(url)
            prin_match = re.search(r"fsa-prin-(\d+)-(\d{4})", url, re.I)
            prin_id = (f"PRIN-{int(prin_match.group(1)):02d}-{prin_match.group(2)}"
                       if prin_match else "PRIN-?")
            title = str(item.get("title", "") or "")[:80]

            try:
                created = item.get("created", "")
                if not created:
                    log.warning(
                        "FSA UK alert %s DROP-DATE [no 'created' field] | %s",
                        prin_id, title,
                    )
                    n_drop_date += 1
                    continue
                d = datetime.fromisoformat(
                    created.replace("Z", "+00:00")
                ).replace(tzinfo=None)
                if d < cutoff:
                    log.info(
                        "FSA UK alert %s DROP-DATE [created=%s < cutoff=%s] | %s",
                        prin_id, d.strftime("%Y-%m-%d"),
                        cutoff.strftime("%Y-%m-%d"), title,
                    )
                    n_drop_date += 1
                    continue

                if not url:
                    log.warning("FSA UK alert %s DROP-NO-URL | %s",
                                prin_id, title)
                    n_drop_url += 1
                    continue

                summary = ((item.get("notation") or "") + " " +
                           (item.get("title") or "") + " " +
                           (item.get("description") or "")).lower()

                matched_kw = next(
                    (p for p in self.PATHOGEN_KEYWORDS if p in summary),
                    None,
                )
                if not matched_kw:
                    # Log the actual content so operators can decide
                    # whether the filter should be broadened.
                    notation = str(item.get("notation", "") or "")[:40]
                    desc_preview = str(item.get("description", "") or "")[:120]
                    log.info(
                        "FSA UK alert %s DROP-NO-PATHOGEN | "
                        "notation=%r title=%r desc=%r",
                        prin_id, notation, title, desc_preview,
                    )
                    n_drop_pathogen += 1
                    continue

                business = item.get("business", {})
                company = (business.get("name", "")
                           if isinstance(business, dict) else "")

                alert_type = item.get("alertType", {})
                klass = (alert_type.get("notation", "Alert")
                         if isinstance(alert_type, dict) else "Alert")

                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company or item.get("title", "")[:80],
                    Brand="—",
                    Product=item.get("title", "")[:300],
                    Pathogen=matched_kw,
                    Reason=item.get("description", "")[:300],
                    Class=klass,
                    URL=url,
                    Outbreak=(1 if "illness" in summary or "outbreak" in summary
                              else 0),
                    Notes=item.get("ftype", "") or prin_id,
                ))
                log.info(
                    "FSA UK alert %s KEPT [pathogen=%r company=%r] | %s",
                    prin_id, matched_kw, company[:30], title,
                )
                n_kept += 1
            except Exception as e:
                log.warning(
                    "FSA UK alert %s DROP-PARSE [%s: %s] | %s",
                    prin_id, type(e).__name__, e, title,
                )
                n_drop_parse += 1

        log.info(
            "FSA UK: %d alerts processed → %d kept | %d dropped "
            "(date=%d, url=%d, pathogen=%d, parse=%d)",
            len(items), n_kept,
            n_drop_date + n_drop_url + n_drop_pathogen + n_drop_parse,
            n_drop_date, n_drop_url, n_drop_pathogen, n_drop_parse,
        )
        return out
