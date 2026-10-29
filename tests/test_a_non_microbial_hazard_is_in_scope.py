# -*- coding: utf-8 -*-
""""Not a microbial pathogen" must not permanently reject an in-scope row.

FOUND 2026-10-29.

    Pending  2026-10-28  GIS (PL)
      product  "stwierdzenie substancji psychoaktywnej w suplemencie diety"
      reason   "unknown: URL agent: Hazard is not a microbial pathogen, but
                an undeclared psychoactive substance"

The notice is GIS's own public food warning for BRAIN WASH cannabis
gummies, DR JOINT sp. z o.o., lot 0708BW06O, published 28.10.2026, with
NIZP-PZH assessing a health risk at the detected THC content. THC over a
reference dose has been in AFTS scope since 2026-10-01 and the register
already publishes six Delta-9-THC rows out of RappelConso and three
"Undeclared pharmacological ingredient" rows out of the FDA.

"not a microbial pathogen" and "no microbial hazard" went into
_TERMINAL_REJECTION_MARKERS on 2026-10-02 for RappelConso fiche 23644 — a
shelf-life extension with no hazard at all, where the phrase is exactly
right. Reviewer 1 uses the same words for a REAL hazard that happens not
to be a bacterium, and the marker then makes that scope error permanent:
the repaired row passed the full publish gate and the re-promotion guard
still refused it.

More than half this register's hazard classes are non-microbial —
mycotoxins, heavy metals, pesticide residues, foreign material, mould,
pest, uninspected product — so the phrase could lock out any of them.

The repair is narrow and this test is the fence around it: the microbial
markers lose their force ONLY when the same text names an in-scope
non-microbial hazard AND carries no other terminal marker. Every other
marker keeps full force.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:                               # pragma: no cover
    sys.path.insert(0, str(ROOT))


def _final(desc: str) -> bool:
    from pipeline.merge_master import _is_terminal_rejection
    return _is_terminal_rejection(desc)


# ── the row that found this: retryable ────────────────────────────────────

def test_the_gis_thc_refusal_is_retryable():
    desc = ("unknown: URL agent: Hazard is not a microbial pathogen, but an "
            "undeclared psychoactive substance | REJECTED: URL agent: ")
    assert not _final(desc)


def test_every_in_scope_non_microbial_hazard_is_retryable():
    """One case per hazard family the register actually publishes."""
    from pipeline.merge_master import _NON_MICROBIAL_IN_SCOPE_HAZARDS

    for hazard in _NON_MICROBIAL_IN_SCOPE_HAZARDS:
        desc = ("unknown: URL agent: the hazard is not a microbial pathogen, "
                "it is %s" % hazard)
        assert not _final(desc), hazard


# ── the row the markers were added for: STILL terminal ────────────────────

def test_the_shelf_life_refusal_stays_terminal():
    """RappelConso 23644, the 2026-10-02 case. No hazard is named at all."""
    desc = ("unknown: URL agent: The hazard is not a microbial pathogen, it "
            "is a prolongation of shelf life || unknown: Arrived already "
            "marked rejected; confirmer did not re-review")
    assert _final(desc)


def test_a_bare_microbial_refusal_stays_terminal():
    assert _final("unknown: URL agent: Hazard is not a microbial pathogen")
    assert _final("unknown: URL agent: no microbial hazard in this notice")


# ── every other terminal marker keeps full force ──────────────────────────

def test_another_terminal_marker_alongside_still_blocks():
    """An operator refusing a chemical-hazard row on other grounds wins."""
    cases = (
        "operator review 2026-10-29: out_of_scope — not a microbial "
        "pathogen, it is a pesticide residue on a non-food article",
        "unknown: URL agent: not a microbial pathogen, it is a foreign "
        "material finding in pet food",
        "unknown: URL agent: not a microbial pathogen, undeclared allergen "
        "only",
        "unknown: URL agent: not a microbial pathogen — labelling, the "
        "aflatoxin figure is in the ingredient spec not the product",
        "unknown: URL agent: not a microbial pathogen, not a food product; "
        "the cadmium limit cited belongs to a cosmetic",
    )
    for desc in cases:
        assert _final(desc), desc


def test_a_microbial_refusal_naming_no_hazard_word_stays_terminal():
    """The excuse needs a hazard NAME, not merely the absence of one."""
    for desc in (
        "unknown: URL agent: not a microbial pathogen, it is a price error",
        "unknown: URL agent: no microbial hazard — this is a packaging "
        "integrity notice",
        "unknown: URL agent: not a microbial pathogen, the notice is a "
        "recall of a kitchen appliance",
    ):
        assert _final(desc), desc


def test_the_excuse_does_not_fire_without_a_microbial_marker():
    """A plain out-of-scope verdict naming a hazard is untouched."""
    assert _final("operator review 2026-10-29: out_of_scope — aflatoxin in "
                  "animal feed, not a food")
    assert _final("unknown: URL agent: pet food; cadmium over the limit")
