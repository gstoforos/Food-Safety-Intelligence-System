"""USDA FSIS recalls & Public Health Alerts — uses the official FSIS recall API (JSON).

API: https://www.fsis.usda.gov/fsis/api/recall/v/1

Hazard scope:
    Delegates to `_models.normalize_pathogen`, which covers the full hazard
    taxonomy (biological pathogens, rodenticides, heavy metals, physical
    foreign-body hazards, mycotoxins). The previous version used a 9-keyword
    local filter that silently dropped every foreign-material recall —
    ironically the single largest FSIS recall category in recent years.

Record shape used (FSIS field names):
    field_recall_number          e.g. "005-2025"
    field_title                  headline
    field_recall_reason          structured hazard category (e.g. "Listeria
                                 monocytogenes", "Foreign Matter Contamination",
                                 "E. coli O157:H7", "Misbranding, Unreported
                                 Allergens"). Primary hazard signal.
    field_recall_type            "Recall" | "Public Health Alert" | "Retraction"
    field_recall_classification  "Class I" | "Class II" | "Class III" (recalls
                                 only; PHAs have this blank)
    field_related_to_outbreak    "True" | "False"
    field_summary                HTML-laden prose description
    field_establishment          establishment name (primary)
    field_recall_company         distributing company (sometimes different)
    field_product_items          product list
    field_states                 distribution states
    field_recall_date            "YYYY-MM-DD" or "MM/DD/YYYY"
    field_last_modified_date     fallback if recall_date missing
    field_recall_url             canonical per-recall page URL
    field_archive_recall         "True" | "False"
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen

log = logging.getLogger(__name__)


# Reasons we explicitly drop. FSIS publishes a lot of label/inspection issues
# that aren't in-scope for a hazard-monitoring system. Matched case-insensitive
# on `field_recall_reason`.
_EXCLUDED_REASON_PATTERNS = [
    r"^misbranding(,\s*unreported\s*allergens?)?$",
    r"^unreported\s*allergens?$",
    r"^misbranding$",
    r"^produced\s*without\s*benefit\s*of\s*inspection$",
    r"^produced\s*without\s*inspection$",
    r"^import\s*violation$",
    r"^ineligible\s*(imported|for\s*import)",
    r"^no\s*inspection$",
]
_EXCLUDED_REASON_RE = re.compile("|".join(_EXCLUDED_REASON_PATTERNS), re.IGNORECASE)

# Retraction-type notices should not land in the dataset — they UNDO a prior
# alert. If the prior alert is already in the dataset, a retraction could be
# handled by flipping a status flag, but that's a future enhancement.
_RETRACTION_RE = re.compile(r"retract", re.IGNORECASE)

# Strip HTML tags from field_summary (which is server-rendered HTML).
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")

# Canonical hazard markers: after normalize_pathogen runs, a positive match
# will contain one of these substrings. Used to distinguish "real canonical
# hazard" from "normalize_pathogen fell through and returned raw input".
_CANONICAL_HAZARD_MARKERS = (
    "Listeria", "Salmonella", "E. coli", "STEC",
    "Clostridium", "Norovirus", "Hepatitis", "Cyclospora",
    "Vibrio", "Cronobacter", "Bacillus", "Campylobacter",
    "Shigella", "Yersinia", "Aflatoxin", "Mycotoxin",
    "Rodenticide", "Lead (Pb)", "Cadmium", "Arsenic",
    "Mercury", "Heavy metal",
    "Glass fragm", "Metal fragm", "Plastic fragm",
    "Physical/foreign",
)


def _strip_html(text: str) -> str:
    if not text:
        return ""
    t = _HTML_TAG_RE.sub(" ", text)
    return _WHITESPACE_RE.sub(" ", t).strip()


def _parse_fsis_date(text: str) -> Optional[datetime]:
    """FSIS serves ISO (2025-11-09) or US (11/09/2025). Return aware datetime or None."""
    if not text:
        return None
    s = str(text)[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _map_hazard(*sources: str) -> str:
    """Return canonical hazard name for whichever source has the most specific match.

    PATHOGEN_RULES in _models.py is ordered so that more-specific patterns come
    first (Glass fragments before Metal fragments before Physical/foreign-body,
    Salmonella Typhimurium before Salmonella spp., etc.). So the cleanest way
    to let specificity win is to concatenate all candidate text into one blob
    and let normalize_pathogen run once over the combined string.

    Example: field_recall_reason = "Foreign Matter Contamination" (generic) but
    field_title mentions "metal" — we want "Metal fragments", not the generic
    "Physical/foreign-body contamination".
    """
    combined = " || ".join(s for s in sources if s)
    if not combined:
        return ""
    cand = normalize_pathogen(combined)
    # normalize_pathogen falls through to raw text when nothing matched. Accept
    # only when a canonical rule actually fired.
    if cand and cand != combined and any(m in cand for m in _CANONICAL_HAZARD_MARKERS):
        return cand
    return ""


class USDAFSISScraper(BaseScraper):
    AGENCY = "USDA FSIS"
    COUNTRY = "USA"
    BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

        r = fetch(self.session, self.BASE_URL)
        if not r:
            log.warning("USDA FSIS: API fetch returned None")
            return []
        try:
            data = r.json()
        except ValueError as e:
            log.warning("USDA FSIS: JSON decode failed: %s", e)
            return []
        if not isinstance(data, list):
            log.warning("USDA FSIS: expected list, got %s", type(data).__name__)
            return []

        out: List[Recall] = []
        seen_numbers: set = set()
        skipped_stale = skipped_excluded = skipped_retraction = skipped_no_hazard = 0

        for rec in data:
            if not isinstance(rec, dict):
                continue
            try:
                # --- Date ---
                d = (_parse_fsis_date(rec.get("field_recall_date", ""))
                     or _parse_fsis_date(rec.get("field_last_modified_date", "")))
                if not d:
                    continue
                if d < cutoff:
                    skipped_stale += 1
                    continue

                # --- Dedup on recall number (API occasionally emits dupes) ---
                num = (rec.get("field_recall_number") or "").strip()
                if num and num in seen_numbers:
                    continue
                if num:
                    seen_numbers.add(num)

                # --- Filter by recall_type (drop retractions) ---
                rtype = (rec.get("field_recall_type") or "Recall").strip()
                if _RETRACTION_RE.search(rtype):
                    skipped_retraction += 1
                    continue

                # --- Filter by recall_reason (drop out-of-scope categories) ---
                reason_cat = _strip_html(rec.get("field_recall_reason") or "").strip()
                if reason_cat and _EXCLUDED_REASON_RE.match(reason_cat):
                    skipped_excluded += 1
                    continue

                # --- Determine hazard (canonical name via shared normalizer) ---
                # Priority: field_recall_reason > field_title > field_summary.
                title = _strip_html(rec.get("field_title") or "")
                summary = _strip_html(rec.get("field_summary") or "")
                pathogen = _map_hazard(reason_cat, title, summary)
                if not pathogen:
                    skipped_no_hazard += 1
                    continue

                # --- Outbreak flag ---
                outbreak_str = (rec.get("field_related_to_outbreak") or "").strip().lower()
                outbreak = 1 if outbreak_str == "true" else 0
                # Secondary signal: illness mention in summary
                if not outbreak and re.search(
                        r"\b(illness|outbreak|sickened|hospitalized|deaths?)\b",
                        summary, re.IGNORECASE):
                    outbreak = 1

                # --- Class / type ---
                cls_raw = (rec.get("field_recall_classification") or "").strip()
                if "public health alert" in rtype.lower() or num.upper().startswith("PHA-"):
                    klass = "Public Health Alert"
                elif cls_raw:
                    klass = cls_raw  # "Class I" / "Class II" / "Class III"
                else:
                    klass = "Recall"

                # --- Company / product / URL ---
                company = (rec.get("field_establishment")
                           or rec.get("field_recall_company") or "").strip()
                product = (rec.get("field_product_items") or title or "")[:300].strip()
                url = (rec.get("field_recall_url") or "").strip()
                if url and url.startswith("/"):
                    url = "https://www.fsis.usda.gov" + url

                states = (rec.get("field_states") or "").strip()
                notes_bits = []
                if num:
                    notes_bits.append(f"FSIS #{num}")
                if states:
                    notes_bits.append(f"Distribution: {states}")
                if reason_cat:
                    notes_bits.append(f"Reason: {reason_cat}")
                notes = "; ".join(notes_bits)

                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company or "—",
                    Brand="—",  # FSIS data doesn't have a distinct brand field
                    Product=product,
                    Pathogen=pathogen,
                    Reason=reason_cat or summary[:300],
                    Class=klass,
                    URL=url,
                    Outbreak=outbreak,
                    Notes=notes,
                ))
            except Exception as e:
                log.warning("USDA FSIS row parse failed: %s | rec=%s",
                            e, str(rec)[:200])

        log.info("USDA FSIS: %d in-scope recalls (%d stale, %d excluded-reason, "
                 "%d retraction, %d no-hazard-match)",
                 len(out), skipped_stale, skipped_excluded,
                 skipped_retraction, skipped_no_hazard)
        return out
