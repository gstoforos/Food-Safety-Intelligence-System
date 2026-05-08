"""FSIS Tier-1 pathogen scope — locked 2026-04-30.

Only recalls whose Pathogen matches one of these get into Recalls.
Anything else is silently dropped (or stays in Pending for re-extraction
if Company/Brand is missing).
"""
from __future__ import annotations

TIER1_KEYWORDS = (
    # Bacterial
    "listeria", "salmonella",
    "e. coli", "e.coli", "escherichia coli", "stec",
    "o157", "o104", "o121", "o26", "o45", "o103", "o111", "o145",
    "shiga toxin", "shigatoxin",
    "botulin", "botulisme", "clostridium botulin",
    "bacillus cereus", "cereulide",
    "cronobacter", "sakazakii",
    "staphylococcus", "staph", "enterotoxin", "entérotoxine",
    "campylobacter",
    # Viral
    "hepatitis a", "hépatite a", "norovirus",
    # Toxins (mycotoxins)
    "aflatoxin", "aflatoxine",
    "ochratoxin", "ochratoxine",
    "mycotoxin", "mycotoxine",
    "fumonisin", "zearalenone", "deoxynivalenol", "patulin",
)


# Sentinel values that mean "no pathogen identified yet" — distinct from
# "pathogen identified but not in our Tier-1 scope". Empty rows are
# candidates for AI enrichment (claude_check / gemini); out-of-scope rows
# are real but ignored by FSIS scope.
_EMPTY_SENTINELS = ("—", "-", "", "unknown", "none", "n/a", "na", "tbd")


def is_empty_pathogen(pathogen: str) -> bool:
    """True if Pathogen field is empty or a placeholder sentinel.

    Distinguishes "we don't know yet, need enrichment" from "we know it's
    out of scope". Used by merge_master.validate_pending_row to route
    empty-pathogen rows to a `pending_enrichment` status instead of
    rejecting them outright at the gate.
    """
    if not pathogen:
        return True
    s = str(pathogen).strip().lower()
    return s in _EMPTY_SENTINELS


def is_in_scope(pathogen: str) -> bool:
    """True if pathogen matches FSIS Tier-1 scope.

    Returns False for both empty and out-of-scope values. Callers that
    need to distinguish those two cases must call is_empty_pathogen()
    first.
    """
    if is_empty_pathogen(pathogen):
        return False
    s = str(pathogen).strip().lower()
    return any(t in s for t in TIER1_KEYWORDS)


def is_tier1(pathogen: str) -> bool:
    """Same as is_in_scope — kept for backward compat with Tier=1 enforcement."""
    return is_in_scope(pathogen)
