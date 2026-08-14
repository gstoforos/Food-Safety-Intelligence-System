"""Vibrio scope and species tiering — added 2026-08-14.

OPERATOR INSTRUCTION
====================
"Add vibrio give All files correct real work no assumptions"

WHAT WAS WRONG
--------------
Vibrio was recognised by every part of the pipeline EXCEPT the one list
that decides whether a row is in scope at all:

    scrapers/_models.py            PATHOGEN_RULES, _TIERS, tier_2_pathogens
    pipeline/_publish_gate.py      HAZARD_CLASS_KEYWORDS["biological"]
    pipeline/verify_pathogen_in_source.py   PATHOGEN_ALIASES
    pipeline/regulator_apis.py     "Vibrio": "vibrio"
    every scraper PATHOGEN_KEYWORDS, every AI reviewer prompt
    pipeline/_pathogen_scope.py    TIER1_KEYWORDS      <-- ABSENT

So the scrapers found Vibrio recalls, the publish gate accepted them, the
reviewers were told they were in scope, and is_in_scope() threw them away
at the Pending gate as "pathogen_out_of_scope: 'Vibrio'". The 2026-08-13
daily run shows it happening to RASFF 865446. Result: zero Vibrio rows
across all 1415 rows of Recalls, and none in Pending, Weekly_Rejected or
NEWS. The register had never held one.

This is different in kind from "Histamine / scombrotoxin" and "Marine
biotoxin", which are excluded on purpose and have had tests defending
their exclusion since the scope was locked. Nothing defended Vibrio's
absence. That asymmetry is what identified it as an omission rather than
a policy, and this file removes it in the other direction.

SEVERITY IS SPLIT BY SPECIES, AND THE SPLIT IS SOURCED
------------------------------------------------------
Genus-level Vibrio, V. parahaemolyticus, V. alginolyticus and
non-O1/non-O139 V. cholerae keep Tier 2 — the tier the framework already
gave them ("Campylobacter, Yersinia, Vibrio etc. — always Tier 2 (FDA
Class II)"). Nothing new is assumed for them.

Two species are forced to Tier 1:

  Vibrio vulnificus
      CDC, "About Vibrio Infection": "Some Vibrio species, such as Vibrio
      vulnificus, can cause severe and life-threatening infections" and
      "About 1 in 5 people with this infection die, sometimes within a day
      or two of becoming ill."

  Vibrio cholerae O1 / O139
      FDA, Fish and Fishery Products Hazards and Controls Guidance,
      Chapter 4, separates "Vibrio cholerae O1 and O139" (fecal-origin,
      epidemic cholera) from "Vibrio cholerae non-O1 and non-O139"
      (naturally occurring).

Bare "Vibrio cholerae" with no serogroup is deliberately NOT forced: FDA's
own guidance splits on serogroup and this register does not invent one the
source did not state.

TWO BUGS THIS FILE EXISTS TO KEEP FIXED
---------------------------------------
Both were found by running the matrix below, not by reading the code.

1. is_always_tier1() matches by plain substring. "cholera" is a substring
   of "cholerae", so putting the epidemic serogroups in
   ALWAYS_TIER1_KEYWORDS forced EVERY V. cholerae row to Tier 1 — the
   exact opposite of the serogroup split. They live in
   _is_epidemic_cholera() instead.

2. "non-O1" and "non-O139" CONTAIN the tokens "O1" and "O139", so a naive
   serogroup search matched the strings that mean the opposite. The
   negative form is now tested first, in both _is_epidemic_cholera() and
   the PATHOGEN_RULES ordering.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._pathogen_scope import (          # noqa: E402
    enforce_tier1, is_always_tier1, is_in_scope,
)
from scrapers._models import assign_tier, normalize_pathogen   # noqa: E402


# (raw source string, expected canonical, expected tier)
MATRIX = [
    ("Vibrio",                             "Vibrio",                          2),
    ("Vibrio spp.",                        "Vibrio",                          2),
    ("vibrio",                             "Vibrio",                          2),
    ("Vibrio parahaemolyticus",            "Vibrio parahaemolyticus",         2),
    ("V. parahaemolyticus",                "Vibrio parahaemolyticus",         2),
    ("Vibrio alginolyticus",               "Vibrio alginolyticus",            2),
    ("Vibrio cholerae",                    "Vibrio cholerae",                 2),
    ("Vibrio cholerae non-O1 and non-O139","Vibrio cholerae non-O1/non-O139", 2),
    ("Vibrio vulnificus",                  "Vibrio vulnificus",               1),
    ("V. vulnificus",                      "Vibrio vulnificus",               1),
    ("Vibrio cholerae O1",                 "Vibrio cholerae O1/O139",         1),
    ("Vibrio cholerae O139",               "Vibrio cholerae O1/O139",         1),
    ("cholera",                            "Vibrio cholerae O1/O139",         1),
]


@pytest.mark.parametrize("raw,canonical,tier", MATRIX)
def test_every_vibrio_form_is_in_scope(raw, canonical, tier):
    assert is_in_scope(raw), f"{raw!r} must be in scope"
    assert is_in_scope(canonical), f"canonical {canonical!r} must be in scope"


@pytest.mark.parametrize("raw,canonical,tier", MATRIX)
def test_species_survive_normalisation(raw, canonical, tier):
    """The genus must not swallow the species.

    Before 2026-08-14 a single rule r"\\bvibrio\\b" collapsed every species
    to "Vibrio" BEFORE assign_tier() ran, so a V. vulnificus recall became
    indistinguishable from a V. parahaemolyticus one and came out Tier 2.
    Information destroyed at normalisation cannot be recovered downstream.
    """
    assert normalize_pathogen(raw) == canonical


@pytest.mark.parametrize("raw,canonical,tier", MATRIX)
def test_tier_matches_severity(raw, canonical, tier):
    assert assign_tier(canonical, 0, "", "raw oysters") == tier


@pytest.mark.parametrize("raw,canonical,tier", MATRIX)
def test_enforce_tier1_agrees_with_assign_tier(raw, canonical, tier):
    """enforce_tier1() must not disagree with assign_tier().

    They are separate code paths — one keyword-based in _pathogen_scope,
    one canonical-set-based in _models — and a row passes through both.
    """
    row = {
        "Pathogen": canonical,
        "Tier": 2,
        "Reason": f"contamination with {canonical}",
        "Product": "raw oysters",
        "Notes": "",
    }
    enforce_tier1(row)
    if tier == 1:
        assert row["Tier"] == 1, f"{canonical!r} must be forced to Tier 1"
    else:
        assert row["Tier"] == 2, f"{canonical!r} must NOT be escalated"


def test_bare_cholerae_is_not_escalated_by_substring():
    """Regression: 'cholera' is a substring of 'cholerae'.

    Listing the epidemic serogroups in ALWAYS_TIER1_KEYWORDS silently
    forced every V. cholerae row to Tier 1.
    """
    assert not is_always_tier1("Vibrio cholerae")
    assert not is_always_tier1("Vibrio parahaemolyticus")
    assert not is_always_tier1("Vibrio")
    assert is_always_tier1("Vibrio vulnificus")


@pytest.mark.parametrize("p", [
    "Vibrio cholerae non-O1",
    "Vibrio cholerae non O1",
    "Vibrio cholerae non-O139",
    "Vibrio cholerae non-O1 and non-O139",
])
def test_non_o1_is_not_read_as_o1(p):
    """Regression: 'non-O1' contains 'O1'.

    The naive serogroup search matched exactly the strings that mean the
    organism is NOT the epidemic serogroup.
    """
    row = {"Pathogen": p, "Tier": 2, "Reason": f"contamination with {p}",
           "Product": "raw oysters", "Notes": ""}
    enforce_tier1(row)
    assert row["Tier"] == 2, f"{p!r} is non-epidemic and must stay Tier 2"


def test_deliberate_exclusions_are_untouched():
    """Adding Vibrio must not widen scope for anything else."""
    assert not is_in_scope("Marine biotoxin")
    assert not is_in_scope("Histamine / scombrotoxin")
    assert not is_in_scope("")
    assert not is_in_scope("Undeclared allergen (milk)")


def test_vibrio_classifies_as_a_biological_hazard():
    """The publish gate must see Vibrio as biological.

    If it does not, a biological Reason reads as a class MISMATCH, which
    blocks the row AND suppresses its Tier-1 escalation via the
    contradiction guard in enforce_tier1().
    """
    from pipeline._publish_gate import pathogen_reason_class_mismatch
    for p in ("Vibrio", "Vibrio vulnificus", "Vibrio parahaemolyticus",
              "Vibrio cholerae O1/O139", "Cholera"):
        assert not pathogen_reason_class_mismatch(
            p, f"Product recalled due to contamination with {p}"
        ), f"{p!r} must not read as a hazard-class mismatch"


@pytest.mark.parametrize("canonical", [
    "Vibrio", "Vibrio vulnificus", "Vibrio parahaemolyticus",
    "Vibrio cholerae", "Vibrio cholerae O1/O139",
    "Vibrio cholerae non-O1/non-O139", "Vibrio alginolyticus",
])
def test_source_verification_has_usable_aliases(canonical):
    """Every canonical needs aliases a regulator would actually write.

    _aliases_for() falls back to [pathogen.lower()], so a canonical with
    no PATHOGEN_ALIASES entry is verified by searching the source page for
    the literal canonical string. "vibrio cholerae o1/o139" appears on no
    regulator page, so verification would fail and the gate would reject a
    correctly-extracted recall.
    """
    from pipeline.verify_pathogen_in_source import _aliases_for
    aliases = _aliases_for(canonical)
    assert any(a in ("vibrio", "cholera", "cholerae", "vulnificus",
                     "parahaemolyticus", "alginolyticus")
               or a.startswith("vibrio ") or a.startswith("v. ")
               for a in aliases), (
        f"{canonical!r} has no alias a source page would contain: {aliases}"
    )
