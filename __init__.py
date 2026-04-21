"""
Enrichment pipeline: takes raw scraped Recall objects and improves them
- Pathogen normalization (already done in Recall.normalize, but Gemini catches edge cases)
- Country normalization for non-English source text
- Tier reassignment after Gemini correction
- Outbreak flag detection from Notes/Reason
"""
from __future__ import annotations
import logging
from typing import List
from scrapers._models import Recall, normalize_pathogen, normalize_country, assign_tier, infer_region
from enrichment.gemini_client import enrich_row

log = logging.getLogger(__name__)

# Heuristic: rows that look "incomplete enough" to warrant a Gemini call
def _needs_enrichment(r: Recall) -> bool:
    if not r.Pathogen or r.Pathogen.strip() in ("", "—", "unknown", "—"):
        return True
    if not r.Country:
        return True
    if not r.Class:
        return True
    return False


def enrich_recalls(recalls: List[Recall], use_ai: bool = True, max_ai_calls: int = 100) -> List[Recall]:
    """
    Enrich a list of recall rows.
    - First pass: deterministic normalization
    - Second pass: Gemini fills gaps (capped to max_ai_calls to preserve free quota)
    """
    out: List[Recall] = []
    ai_used = 0
    for r in recalls:
        # Deterministic normalization first
        r.Pathogen = normalize_pathogen(r.Pathogen)
        if r.Country:
            r.Country = normalize_country(r.Country)
        if not r.Region and r.Country:
            r.Region = infer_region(r.Country)

        # AI enrichment if needed and within budget
        if use_ai and _needs_enrichment(r) and ai_used < max_ai_calls:
            try:
                fixed = enrich_row(r.to_dict())
                # Apply only if Gemini returned something usable
                if fixed.get("Pathogen") and not r.Pathogen:
                    r.Pathogen = normalize_pathogen(fixed["Pathogen"])
                if fixed.get("Country") and not r.Country:
                    r.Country = normalize_country(fixed["Country"])
                if fixed.get("Class") and not r.Class:
                    r.Class = fixed["Class"]
                if int(fixed.get("Outbreak", 0)):
                    r.Outbreak = 1
                ai_used += 1
            except Exception as e:
                log.warning("Gemini enrichment failed for row: %s", e)

        # Reassign tier after enrichment
        r.Tier = assign_tier(r.Pathogen, r.Outbreak)
        if not r.Region and r.Country:
            r.Region = infer_region(r.Country)
        out.append(r)
    log.info("Enrichment complete: %d rows, %d AI-enriched", len(out), ai_used)
    return out
