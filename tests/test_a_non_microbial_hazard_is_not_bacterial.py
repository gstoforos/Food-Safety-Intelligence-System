# -*- coding: utf-8 -*-
"""A hazard with no organism in it never lands in the bacterial bucket.

WHAT THIS CAUGHT (morning-fix 2026-10-07)
=========================================
``pipeline/enrich_schema._hazard_group`` ends in a catch-all:

    # Anything left that names an organism is bacterial.
    return "pathogen-bacterial"

Three comments in that file already record the catch-all lying — SUPPLX
yohimbine (2026-09-03), a DNP/fluoxetine supplement (2026-08-31),
uninspected product (2026-09-25) — and each was repaired by adding the one
label that had just been found. Measured on main at c540527 it was still
lying about **eleven published rows**, and the Analytical schema sweep
re-stamps all eleven every morning (EnrichedAt 2026-10-07 on each):

    Delta-9-THC (above acute reference dose)      2
    Mineral oil aromatic hydrocarbons (MOAH)      2
    Tropane alkaloids [/ above the legal limit]   2
    Pyrrolizidine alkaloids                       2
    Ergot alkaloids                               1

A chemical over a reference dose, a mineral-oil contaminant and three
plant/fungal toxin families, all filed as bacterial pathogens. Everything
else about these rows was right: each hazard is named in the AFTS scope
statement and each is reachable by a term in ``tools/alert_vocab.py``. Only
the group was wrong — and HazardGroup is what every stratification reads,
so the bacterial bucket (1449 rows) was quietly carrying them.

WHY THIS TEST SHAPE
-------------------
Not a case list of the eleven labels — that is what the three previous
repairs did, and the fourth recurrence arrived anyway. This asserts the
PROPERTY the catch-all's own comment claims: it is for labels that name an
organism. So a label built from non-microbial hazard vocabulary must not
reach it. The vocabulary is taken from the scope statement rather than from
the rows that happen to be in the register today, so a hazard family AFTS
accepts but has not yet seen fails here before its first row publishes.

MOULD IS DELIBERATELY NOT HERE. Eleven published rows carry Pathogen
"Mold" and also land on the bacterial catch-all. Mould is a fungus, it is
in scope by operator decision (2026-09-07), it is Tier 2 everywhere
(operator rule 2026-10-03) — and ``pipeline/agents/_vocabulary.HazardGroup``
has no fungal term to put it in. "mycotoxin" would be wrong: mould growth
is not a mycotoxin finding, and filing it there would corrupt a 279-row
stratum. Choosing between a new controlled value ("pathogen-fungal") and
one of the existing ten is a vocabulary decision that changes what the
dashboard groups by, so it is reported to the operator rather than taken
quietly in a morning pass. When it is decided, add mould here.
"""

from __future__ import annotations

import pytest

from pipeline.enrich_schema import _hazard_group

#: Hazard labels built from the scope statement's non-microbial families,
#: in the shape the register writes them, with the group each belongs to.
#: "not bacterial" is the claim; the exact group is asserted too so that a
#: future fix cannot satisfy this file by sending everything to "unknown".
NON_MICROBIAL = [
    # Chemical contaminants and residues
    ("Delta-9-THC (above acute reference dose)", "chemical"),
    ("THC above the acute reference dose", "chemical"),
    ("Cannabinoids above limit (THC)", "chemical"),
    ("Mineral oil aromatic hydrocarbons (MOAH)", "chemical"),
    ("Mineral oil saturated hydrocarbons (MOSH)", "chemical"),
    ("Bisphenol A (chemical contaminant)", "chemical"),
    ("Acetamiprid (pesticide residue)", "chemical"),
    ("Tetraconazole (pesticide residue)", "chemical"),
    ("Ethylene oxide", "chemical"),
    ("Undeclared drug (sildenafil)", "chemical"),
    # Plant toxins. The bare ("toxin", "biotoxin") rule would have caught
    # these if the word "alkaloid" contained "toxin"; it does not.
    ("Tropane alkaloids", "biotoxin"),
    ("Tropane alkaloids (above the legal limit)", "biotoxin"),
    ("Pyrrolizidine alkaloids", "biotoxin"),
    ("Histamine", "biotoxin"),
    # Fungal toxins. Ergot is Claviceps purpurea, so its alkaloids are a
    # mycotoxin and must beat the generic alkaloid rule.
    ("Ergot alkaloids", "mycotoxin"),
    ("Aflatoxin", "mycotoxin"),
    ("Ochratoxin A", "mycotoxin"),
    # Metals, physical hazards, and the unmeasured case
    ("Lead (heavy metal)", "heavy-metal"),
    ("Foreign material (glass fragment)", "foreign-material"),
    ("Uninspected product (hazard not assessed)", "hazard-not-assessed"),
    # ── morning-fix 2026-10-13. THE BARE NOUN. ──────────────────────────
    # The MAPAQ Québec row for IGA Grenville ground beef published with
    # Pathogen written as the bare material noun "Plastic" and landed in
    # the bacterial bucket — the only one of the register's 63
    # foreign-material rows to do so. Every QUALIFIED form was already in
    # _HAZARD_GROUP_RULES ("plastic fragment", "glass fragment"), so this
    # file's cases, all of them qualified too, could not see the hole.
    #
    # A review agent writing the hazard as free text has no reason to
    # prefer the qualified form, so the bare nouns are asserted here. Six
    # of them were measured landing in the bacterial bucket on main at
    # 98a7746b: Glass, Metal, Plastic pieces, Rubber, Wood, Stones.
    ("Plastic", "foreign-material"),
    ("Plastic pieces", "foreign-material"),
    ("Glass", "foreign-material"),
    ("Metal", "foreign-material"),
    ("Rubber", "foreign-material"),
    ("Wood", "foreign-material"),
    ("Stones", "foreign-material"),
    ("Bone fragments", "foreign-material"),
    # Same sweep: three published labels that name a metal, a drug and a
    # fungal toxin without using the word the table was waiting for.
    ("Mercury exceeding limit", "heavy-metal"),
    ("1, 3-Dimethylamylamine (DMAA)", "chemical"),
    ("Deoxynivalenol (DON)", "mycotoxin"),
    ("Yellow oleander (cardiac glycosides)", "biotoxin"),
]


@pytest.mark.parametrize("pathogen,want", NON_MICROBIAL,
                         ids=[p for p, _ in NON_MICROBIAL])
def test_a_non_microbial_hazard_is_not_filed_as_bacterial(pathogen, want):
    got = _hazard_group(pathogen)
    assert got != "pathogen-bacterial", (
        f"_hazard_group({pathogen!r}) fell through to the "
        f"'pathogen-bacterial' catch-all. That bucket is for labels that "
        f"name an organism — its own comment says so — and this one names "
        f"no organism at all. Add the vocabulary to "
        f"_HAZARD_GROUP_RULES; expected {want!r}.")
    assert got == want, (pathogen, got, want)


ORGANISMS = [
    ("Listeria monocytogenes", "pathogen-bacterial"),
    ("Salmonella", "pathogen-bacterial"),
    ("Salmonella Enteritidis", "pathogen-bacterial"),
    ("Shiga toxin-producing E. coli (STEC)", "pathogen-bacterial"),
    ("STEC", "pathogen-bacterial"),
    ("Clostridium botulinum", "pathogen-bacterial"),
    ("Norovirus", "pathogen-viral"),
    ("Hepatitis A", "pathogen-viral"),
    ("Cyclospora", "pathogen-parasitic"),
]


@pytest.mark.parametrize("pathogen,want", ORGANISMS,
                         ids=[p for p, _ in ORGANISMS])
def test_the_organisms_still_land_where_they_did(pathogen, want):
    """The other half of the claim. Widening the rule table is how an
    organism gets pulled out of its own group — "toxin-producing" already
    needed an override for exactly this — so the bacterial, viral and
    parasitic cases are asserted alongside."""
    assert _hazard_group(pathogen) == want, (
        pathogen, _hazard_group(pathogen), want)


def test_mould_is_the_open_decision_and_is_recorded_as_one():
    """Not an assertion that the current value is right.

    It pins what main does today so the operator's decision shows up as a
    change to this file rather than as a silent drift. If mould is given a
    group, this test is what says so.
    """
    assert _hazard_group("Mold") == "pathogen-bacterial", (
        "mould's hazard group has changed — if that was the operator "
        "decision this test records, move 'Mold'/'Mould' into "
        "NON_MICROBIAL with its new group and delete this test")


# ── review 2026-10-13: the bare material nouns are WHOLE WORDS, and never on
# a label that names an organism. As substrings they filed these as
# foreign material. None of them is in the register today; the next one
# would be.
NOT_FOREIGN_MATERIAL = [
    "Salmonella in sandwiches",
    "Listeria monocytogenes (smoked salmon on wood plank)",
    "Metalaxyl (pesticide residue)",
    "Plasticizers (phthalates)",
    "E. coli in stone fruit",
]


@pytest.mark.parametrize("pathogen", NOT_FOREIGN_MATERIAL, ids=NOT_FOREIGN_MATERIAL)
def test_a_material_word_inside_another_word_or_beside_an_organism_is_not_foreign_material(pathogen):
    assert _hazard_group(pathogen) != "foreign-material", pathogen
