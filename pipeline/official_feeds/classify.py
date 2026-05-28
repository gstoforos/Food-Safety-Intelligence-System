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


def classify_record(rec) -> dict:
    """
    Returns {'verdict': 'accept'|'reject', 'category': str, 'tier': int|None,
             'matched': str}.
    """
    blob = rec.hazard_blob()
    r = _gf_classify(pathogen="", reason=blob, product=rec.product or "")
    return {
        "verdict": r.verdict,
        "category": r.category,
        "tier": r.tier,
        "matched": r.matched_term,
    }
