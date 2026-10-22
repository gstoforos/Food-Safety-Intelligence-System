# -*- coding: utf-8 -*-
"""Bromide and bromate are chemical contaminants, and both gates must say so.

THE ROW THAT FOUND THIS. BLV (CH), 2026-10-20, sitting in Pending on the
2026-10-22 workbook with the regulator's own headline in its own Notes:

    Product  'Sator geschält'
    Company  'Asia Store'
    Notes    FSVO title: "Asia Store ruft das Produkt «Sator geschält»
             aufgrund eines zu hohen Bromidgehalts zurück"
    REJECTED by reviewer 1: "No specific hazard or pathogen found on the
             official recall page"

The hazard was never in doubt — it is the first clause of the regulator's
headline. Measured before the fix, every gate in the pipeline was blind to
every spelling of it:

    pipeline._publish_gate.classify_hazard("zu hoher bromidgehalt")  -> set()
    pipeline.gap_finder.rules.classify(reason="…bromidgehalt")
        -> reject / category='unknown' / "No matching hazard category"

"chlorate" and "perchlorate" have been in the gate's chemical class since it
was written. Bromide is the third anion of the same kind, capped by the same
sort of EU reference value, and arrives the same way — methyl-bromide
fumigation of dried Asian vegetables is a recurring EU/CH finding. Bromate
(potassium bromate, a flour treatment agent banned in the EU) is the same
family and was missing too.

THE FALSE FRIEND THE FIX CREATES. "hydrobromide" is the salt form of a long
list of drugs — dextromethorphan hydrobromide, scopolamine hydrobromide —
and it CONTAINS "bromide". Both matchers work on substrings, so without a
guard a pharmaceutical recall would acquire a chemical-contaminant hazard
class, reach the contradiction rule and the curator's scope test with it, and
be queued to Pending for _non_food_drug_blockers to throw out again. Both
files strip it first, the way _publish_gate already strips the shaped-mould
false friend ("moulded", "demoulded").

SCOPE, not a new policy: R6 already covers "heavy metals and chemical
contaminants (incl. pesticide residues, bisphenol A, …)". This is vocabulary.
"""
from __future__ import annotations

import pytest

import pipeline.gap_finder.rules as gap_rules
from pipeline._publish_gate import classify_hazard

# Every spelling the regulators in this register actually print.
BROMINE_HAZARD = [
    "bromide",
    "excessive bromide content",
    "bromide ion residues above the reference value",
    "zu hoher bromidgehalt",
    "Asia Store ruft das Produkt «Sator geschält» aufgrund eines zu hohen "
    "Bromidgehalts zurück",
    "bromuro di metile",
    "bromure de methyle",
    "methyl bromide fumigation residues",
    "potassium bromate",
    "bromate in flour",
]

NOT_A_BROMINE_HAZARD = [
    # The false friend, both ways round.
    "dextromethorphan hydrobromide",
    "Hydrobromide salt of the active ingredient",
    "hydro-bromide",
    # Words that merely look close.
    "bromelain",
    "brominated vegetable oil is not in this list",
]


@pytest.mark.parametrize("text", BROMINE_HAZARD)
def test_the_publish_gate_classifies_it_as_chemical(text):
    assert "chemical" in classify_hazard(text), (
        f"the gate's hazard classifier cannot see the hazard in {text!r}")


@pytest.mark.parametrize("text", BROMINE_HAZARD)
def test_the_gap_finder_accepts_it(text):
    got = gap_rules.classify(reason=text)
    assert got.verdict == "accept", (
        f"the gap finder refuses {text!r} as {got.category!r}: {got.rule}")
    assert got.category == "synthetic_chemical", got


@pytest.mark.parametrize("text", NOT_A_BROMINE_HAZARD)
def test_a_drug_salt_is_not_a_chemical_contaminant(text):
    assert "chemical" not in classify_hazard(text), (
        f"{text!r} acquired a chemical-contaminant hazard class — the "
        "hydrobromide guard in pipeline/_publish_gate.py regressed")
    assert gap_rules.classify(reason=text).category != "synthetic_chemical", (
        f"{text!r} classified as an in-scope synthetic chemical — the "
        "hydrobromide guard in pipeline/gap_finder/rules.py regressed")


def test_the_two_gates_agree_with_each_other():
    """The 2026-09-26 and 2026-10-03 audits both found the same shape: one
    gate knew a hazard and another did not. Pin the agreement for this one."""
    for text in BROMINE_HAZARD:
        gate = "chemical" in classify_hazard(text)
        gap = gap_rules.classify(reason=text).verdict == "accept"
        assert gate is gap is True, (
            f"gates disagree on {text!r}: publish_gate={gate}, "
            f"gap_finder={gap}")


def test_the_sibling_anions_are_still_there():
    """Guard against someone 'tidying' the list and dropping its neighbours."""
    for text in ("chlorate residues", "perchlorate in spinach"):
        assert "chemical" in classify_hazard(text), text
