"""
Tier classification for official-feed records.

Reuses the EU gap_finder's classifier (pipeline.gap_finder.rules.classify)
on the clean hazard text the authority provides. Because the input is
structured (not noisy news headlines), classification is far more reliable
here than in the gap finder.

Locked rules (same as gap finder / FSIS reviewer prompts):
  - Salmonella, Listeria, STEC, cereulide, botulism → always Tier 1
  - Mycotoxins (aflatoxin, ochratoxin, etc.) → Tier 2
  - Hepatitis A/E, Campylobacter, generic E. coli → Tier 2
  - Allergens, foreign matter, synthetic chemicals → rejected (out of scope)
  - Pet/animal food → rejected (out of scope) — applied BEFORE the hazard
    classifier so pathogenic pet food doesn't slip in as Tier 1
"""

from __future__ import annotations

import sys
import os

# Import the gap_finder classifier. In production it lives at
# pipeline/gap_finder/rules.py; locally we add the path.
try:
    from pipeline.gap_finder.rules import classify as _gf_classify
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from gap_finder.rules import classify as _gf_classify


# Pet/animal food product-scope markers. Substring match (these are all 2+
# words / distinctive single words) on the lowercased text blob. If ANY
# matches, the record is rejected as out-of-scope without consulting the
# hazard classifier — preventing "dog food Listeria" from accepting as T1.
_PET_FOOD_MARKERS = (
    "pet food", "dog food", "cat food", "puppy food", "kitten food",
    "raw pet", "raw dog", "raw cat", "freeze-dried dog", "freeze dried dog",
    "kibble", "dog treat", "cat treat", "pet treat", "dog chew",
    "pet supplement", "dog supplement", "cat supplement",
    "animal feed", "livestock feed", "horse feed", "poultry feed",
    "for dogs", "for cats", "for pets",
    "veterinary diet", "vet diet",
)


def _is_pet_food(text: str) -> bool:
    t = (text or "").lower()
    return any(m in t for m in _PET_FOOD_MARKERS)


def classify_record(rec) -> dict:
    """
    Returns {'verdict': 'accept'|'reject', 'category': str, 'tier': int|None,
             'matched': str}.
    """
    blob = rec.hazard_blob()
    if _is_pet_food(blob):
        return {"verdict": "reject", "category": "pet_food",
                "tier": None, "matched": "pet_food"}
    r = _gf_classify(pathogen="", reason=blob, product=rec.product or "")
    return {
        "verdict": r.verdict,
        "category": r.category,
        "tier": r.tier,
        "matched": r.matched_term,
    }
