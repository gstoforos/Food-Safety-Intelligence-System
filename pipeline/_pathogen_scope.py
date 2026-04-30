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


def is_in_scope(pathogen: str) -> bool:
    """True if pathogen matches FSIS Tier-1 scope."""
    if not pathogen:
        return False
    s = str(pathogen).strip().lower()
    if s in ("—", "-", "", "unknown", "none"):
        return False
    return any(t in s for t in TIER1_KEYWORDS)


def is_tier1(pathogen: str) -> bool:
    """Same as is_in_scope — kept for backward compat with Tier=1 enforcement."""
    return is_in_scope(pathogen)
