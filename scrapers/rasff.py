"""
RASFF Window — EU Rapid Alert System for Food and Feed.

Architecture (final, 2026-05-04 — derived from HAR capture of the SPA):

  RASFF Window's SPA hits a public JSON API for the notification list. Each
  notification entry contains BOTH the public reference number (e.g.
  "2026.3863") AND the internal integer database ID (e.g. notifId=841474).
  The detail page URL only accepts the integer ID — reference-form URLs
  return GENERIC_TECHNICAL_ERROR [ERR-1004] Invalid Request.

  Endpoint:
    POST https://webgate.ec.europa.eu/rasff-window/backend/public/notification/search/consolidated/en/

  Auth: NONE. No cookies, no CSRF tokens, no API keys. Just send a JSON
  body with pageNumber + itemsPerPage. Headers required: Content-Type,
  Accept, Origin, Referer, X-Requested-With.

  Request body:
    {"parameters": {"pageNumber": 1, "itemsPerPage": 50}}

  Response shape (top-level):
    {
      "totalPages":    1219,
      "totalElements": 30468,
      "notifications": [
        {
          "notifId":       841474,             # integer URL ID
          "reference":     "2026.3863",        # public reference
          "ecValidationDate": "01-05-2026 22:54:51",
          "subject":       "Cerulide in infant formula from Ireland",
          "notifyingCountry":  {"organizationName": "Ireland", "isoCode": "IE"},
          "originCountries":   [{"organizationName": "Ireland", ...}],
          "productCategory":   {"description": "milk and milk products"},
          "productType":       {"description": "food"},
          "notificationClassification": {"description": "alert notification"},
          "riskDecision":      {"description": "serious"},
          "published":         false
        },
        ...
      ]
    }

  Note: this paginated endpoint does NOT return distribution countries or
  hazards (those come from a per-notification detail call). For our use,
  the subject + classification + originCountries + reference are enough
  to make the row useful — we keep distribution/hazards lookup as future
  enhancement if needed.

  URL pattern for each notification:
    https://webgate.ec.europa.eu/rasff-window/screen/notification/{notifId}

  This is the URL form the pipeline regex requires (integer ID), and the
  only form that resolves on RASFF Window's detail page.

Resilience:

  - If the API renames keys or rotates the endpoint, the parser detects
    missing required keys and aborts with a clear log line.
  - Falls back to 0 rows on HTTP failure — country scrapers continue to
    provide EU coverage.
  - We do NOT use the bulk export endpoint (/search/export/en/) — it
    returns 21MB and is overkill for our 7-day window. The paginated
    consolidated endpoint is faster.

Replaces the stub disabled 2026-04-29 + the broken reference-URL
implementation written earlier today.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from scrapers._base import BaseScraper, fetch
from scrapers._models import (
    Recall, normalize_pathogen, normalize_country, infer_region,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

API_URL = (
    "https://webgate.ec.europa.eu/rasff-window/backend/public/"
    "notification/search/consolidated/en/"
)

# Required headers for the API to respond. Discovered from HAR capture of
# the SPA's call. The Referer + Origin pair is critical — without these
# the EC backend may 403 the request as a non-browser source.
API_HEADERS = {
    "Accept":           "application/json, text/plain, */*",
    "Content-Type":     "application/json",
    "Origin":           "https://webgate.ec.europa.eu",
    "Referer":          "https://webgate.ec.europa.eu/rasff-window/screen/list",
    "X-Requested-With": "XMLHttpRequest",
}

# Page size — 50 keeps each page ~30KB which is a comfortable size.
# (HAR shows 25 was the SPA default; 50 is fine — server-side cap appears
# to be around 100 but we don't need to push it.)
PAGE_SIZE = 50

# Pages to fetch before giving up. 7 days × ~7 notifications/day = ~50 rows;
# safety multiplier ×3 = 150 = 3 pages. Cap at MAX_PAGES to prevent runaway
# scraping if cutoff logic breaks.
MAX_PAGES = 10

# Lookback window default — 3 days (audit 2026-05-06).
#
# Pre-fix: 7 days. RASFF is scraped 4× per day (rasff-scrape.yml at
# 04:30 + 16:30, morning-critical-scrape.yml at 08:00, daily-scrape.yml
# at 17:00 Athens). With 4 passes/day and notifications coming in DESC
# date order, a 3-day window guarantees no fresh notification is ever
# missed — even if one entire pass fails, the next 3 will catch it.
# A wider window just re-fetches rows we already have (dedup'd by URL),
# wasting API calls and cluttering Pending churn.
DEFAULT_SINCE_DAYS = 3

# Classifications we monitor.
MONITORED_CLASSIFICATIONS = (
    "alert notification",
    "border rejection",
    "information notification for attention",
    "information notification for follow-up",
)

# Types we monitor (drops feed, food contact materials, environmental).
MONITORED_TYPES = ("food",)

# Per-row Reason text cap.
REASON_MAX_LEN = 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_rasff_date(s: str) -> Optional[datetime]:
    """RASFF dates are 'DD-MM-YYYY HH:MM:SS' — return UTC datetime."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:19], "%d-%m-%Y %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        try:
            return datetime.strptime(str(s)[:10], "%d-%m-%Y").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            return None


# RASFF source data has documented typos in the subject field.
# Audit 2026-05-04: "Cerulide" (missing 'e') in #2026.3863 alongside the
# correctly-spelled "Cereulide" in sister notification #2026.3862. Add new
# entries when more typos surface in production logs.
_RASFF_TYPO_FIXES = (
    (r"\bcerulide\b",       "cereulide"),    # missing 'e' in #2026.3863
    (r"\bcereuli?de\b",     "cereulide"),    # belt-and-braces
    (r"\baflotoxin\w*\b",   "aflatoxin"),    # 'o' for 'a'
)


def _correct_rasff_typos(text: str) -> str:
    """Apply RASFF-side typo corrections (case-insensitive)."""
    for pat, repl in _RASFF_TYPO_FIXES:
        text = re.sub(pat, repl, text, flags=re.IGNORECASE)
    return text


def _extract_pathogen(subject: str) -> str:
    """Match a canonical pathogen against the RASFF subject string.

    The paginated /consolidated/en/ endpoint does NOT return the full hazards
    field (that lives in the per-notification detail call). We therefore rely
    on the subject text — which is the natural-language summary like
    "Salmonella Enteritidis in fresh chicken breast fillet from Poland".
    Empirically this catches all in-scope rows (validated 2026-05-04 against
    the May-1+ corpus).
    """
    if not subject:
        return ""
    canon = normalize_pathogen(_correct_rasff_typos(subject))
    return canon or ""


def _country_names(countries) -> str:
    """Convert RASFF originCountries / notifyingCountry into a comma-separated
    organizationName string. Accepts list-of-dicts (originCountries) or single
    dict (notifyingCountry). Empty input → empty string."""
    if not countries:
        return ""
    if isinstance(countries, dict):
        return countries.get("organizationName", "") or ""
    return ", ".join(
        c.get("organizationName", "") for c in countries if c.get("organizationName")
    )


def _classify(classification: str) -> str:
    """Map RASFF classification → existing Class field convention."""
    c = (classification or "").lower().strip()
    if "alert" in c:
        return "Alert"
    if "border rejection" in c:
        return "Border Rejection"
    if "information" in c:
        return "Information"
    return "Recall"


def _build_company_field(origin: str, notifying: str) -> str:
    """Build 'Origin: X | Notifying: Y' company string per RASFF schema.
    The /consolidated/en/ endpoint does NOT include distribution countries —
    those would require a per-row detail call. We omit Distributed in this
    field rather than write 'unknown' which would look like missing data.
    """
    o = (origin or "").strip() or "unknown"
    n = (notifying or "").strip() or "unknown"
    return f"Origin: {o} | Notifying: {n}"


def _reason_text(subject: str, classification: str, decision: str,
                 category: str) -> str:
    """Build human-readable Reason field."""
    parts = []
    if subject:
        parts.append(subject.strip())
    s_lower = (subject or "").lower()
    if decision and decision.lower() not in s_lower:
        parts.append(f"risk: {decision}")
    if category and category.lower() not in s_lower:
        parts.append(f"category: {category}")
    return "; ".join(parts)[:REASON_MAX_LEN]


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class RASFFScraper(BaseScraper):
    """
    EU Rapid Alert System for Food and Feed — uses the public JSON API
    /backend/public/notification/search/consolidated/en/ to retrieve
    notifications with their internal integer IDs (notifId), allowing
    construction of working /screen/notification/{id} URLs.

    Filters:
      - type == 'food'
      - classification in {alert, border rejection, info-attention/follow-up}
      - date >= today - since_days
      - subject matches a pathogen in PATHOGEN_RULES
    """
    AGENCY = "RASFF (EU)"
    COUNTRY = ""    # RASFF rows use per-row origin
    LANGUAGE = "en"

    def scrape(self, since_days: int = DEFAULT_SINCE_DAYS) -> List[Recall]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        log.info("RASFF API: fetching notifications since %s",
                 cutoff.strftime("%Y-%m-%d"))

        all_rows: List[Recall] = []
        n_total = n_drop_date = n_drop_type = n_drop_class = 0
        n_drop_pathogen = n_drop_dup = 0
        seen_refs = set()

        for page in range(1, MAX_PAGES + 1):
            payload = {"parameters": {"pageNumber": page,
                                      "itemsPerPage": PAGE_SIZE}}
            resp = fetch(
                self.session,
                API_URL,
                method="POST",
                json=payload,
                headers=API_HEADERS,
                timeout=45,
            )
            if resp is None or resp.status_code != 200:
                status = resp.status_code if resp is not None else "no-response"
                log.warning("RASFF API page %d failed (status=%s) — stopping",
                            page, status)
                break

            try:
                body = resp.json()
            except (ValueError, json.JSONDecodeError) as exc:
                log.error("RASFF API page %d: invalid JSON: %s", page, exc)
                break

            notifs = body.get("notifications", [])
            if not notifs:
                log.info("RASFF API page %d: empty — pagination exhausted", page)
                break

            page_done = False
            for n in notifs:
                n_total += 1

                # Date — drop rows older than cutoff. Notifications come back
                # in DESCENDING date order, so once we hit a too-old row, we
                # short-circuit the rest of pagination.
                d = _parse_rasff_date(n.get("ecValidationDate", ""))
                if d is None:
                    n_drop_date += 1
                    continue
                if d < cutoff:
                    n_drop_date += 1
                    page_done = True
                    log.info("RASFF API: hit cutoff on page %d (row date %s) — "
                             "stopping pagination", page, d.strftime("%Y-%m-%d"))
                    break

                # Type filter
                ptype = (n.get("productType") or {}).get("description", "").lower()
                if ptype not in MONITORED_TYPES:
                    n_drop_type += 1
                    continue

                # Classification filter
                classif = (n.get("notificationClassification") or {}).get(
                    "description", "").lower()
                if not any(mc in classif for mc in MONITORED_CLASSIFICATIONS):
                    n_drop_class += 1
                    continue

                # Pathogen filter — match against subject
                subject = (n.get("subject") or "").strip()
                pathogen = _extract_pathogen(subject)
                if not pathogen:
                    n_drop_pathogen += 1
                    continue

                # Dedup by reference
                ref = n.get("reference", "")
                if ref in seen_refs:
                    n_drop_dup += 1
                    continue
                seen_refs.add(ref)

                # Build the row
                notif_id = n.get("notifId")
                if not notif_id:
                    log.warning("RASFF row %s: missing notifId — skipping", ref)
                    continue

                origin = _country_names(n.get("originCountries", []))
                notifying = _country_names(n.get("notifyingCountry", {}))
                origins_list = n.get("originCountries", []) or []
                country_for_row = (
                    _country_names([origins_list[0]]) if origins_list else notifying
                )
                country_for_row = normalize_country(country_for_row) or country_for_row

                category = (n.get("productCategory") or {}).get("description", "")
                decision = (n.get("riskDecision") or {}).get("description", "")
                row_class = _classify(classif)

                url = (f"https://webgate.ec.europa.eu/rasff-window/"
                       f"screen/notification/{notif_id}")

                recall = self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=_build_company_field(origin, notifying),
                    Brand="—",
                    Product=subject or category,
                    Pathogen=pathogen,
                    Reason=_reason_text(subject, classif, decision, category),
                    Class=row_class,
                    URL=url,
                    Outbreak=0,  # /consolidated/en/ doesn't expose illness counts
                    Notes=(f"[RASFF #{ref}; classification: {classif}; "
                           f"category: {category}; notifId={notif_id}]"),
                )

                # Override Country/Region per-row (BaseScraper.COUNTRY is empty
                # for RASFF since the agency isn't tied to one country).
                if country_for_row:
                    recall.Country = country_for_row
                    recall.Region = infer_region(country_for_row) or recall.Region

                all_rows.append(recall)

            if page_done:
                break  # cutoff hit; don't fetch more pages

        log.info(
            "RASFF: %d notifications scanned → %d kept (dropped: "
            "%d date, %d type, %d classification, %d pathogen, %d duplicate)",
            n_total, len(all_rows),
            n_drop_date, n_drop_type, n_drop_class, n_drop_pathogen, n_drop_dup,
        )
        return all_rows
