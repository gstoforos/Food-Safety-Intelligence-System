# -*- coding: utf-8 -*-
"""An undeclared DRUG is not an undeclared allergen.

WHAT THIS CAUGHT (morning-fix 2026-10-26)
=========================================
``pipeline/enrich_pending_offline.ALLERGEN_ONLY`` is the module's
out-of-scope test: a Pending row whose only stated hazard is an undeclared
allergen is reported for rejection rather than enriched into
publishability. Undeclared allergens are out of AFTS scope by the policy of
2026-07-29, so the test belongs there.

The pattern opened with the bare word ``undeclared``. So it fired on every
"undeclared <anything>", and two hazard classes that are squarely IN scope
were being reported as out of it:

  undeclared DRUGS       sildenafil, tadalafil, dapoxetine, DMAA, DNP
  unapproved ADDITIVES   "undeclared colour", Sudan dyes, an additive over
                         its limit

The register is the proof, and it is unambiguous. Run this module's own
``infer_pathogen()`` over Recalls as it stood at 3d82fcfb and THREE
PUBLISHED ROWS came back "hazard is an undeclared allergen — out of AFTS
scope":

    2026-09-15  FDA  X10 Natural Enhancement Supplement
    2026-09-15  FDA  biQ-FEL 500 mL liquid dietary supplement
    2026-09-03  FDA  Business Pill and Branch Manager for Men

all three carrying Pathogen "Undeclared pharmacological ingredient" —
plus SZPI (CZ) 2026-10-06 (ELIXIRMAN, undeclared sildenafil) published, and
FDA 2026-08-31 (Lipofit Extreme Fat Burner 2.0, undeclared DNP and
sibutramine) archived. ``tools/alert_vocab.py`` has offered subscribers the
term "Undeclared pharmacological ingredient" since 2026-10-10. The
register, the publish gate and the alert vocabulary all treated these as
publishable. Only this module did not.

Two more published rows were caught by the bare ``allerg`` token, where the
allergen is mentioned ALONGSIDE the real hazard — which the code's own
comment already said was fine:

    FSA (UK)   2026-07-22  Graham's Family Dairy semi-skimmed milk
               "a possible health risk for anyone with an allergy to
                antibiotics (penicillin)" — the hazard is the residue
    USDA FSIS  2026-03-25  raw beef and pork, false "EST. 1996" mark
               "may contain undeclared allergens, harmful bacteria or
                other contaminants" — FSIS boilerplate; the hazard is that
                nothing was inspected

WHY THIS TEST SHAPE
-------------------
The regression assertion is the one that cannot be argued with: no row the
register has PUBLISHED may be called out of scope by the module that
decides scope. That is a contradiction in the system about itself, it needs
no judgement to evaluate, and it holds whatever the pattern is rewritten
to next.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.enrich_pending_offline import ALLERGEN_ONLY, infer_pathogen

ROOT = Path(__file__).resolve().parents[1]
RECALLS_JSON = ROOT / "docs" / "data" / "recalls.json"

OUT_OF_SCOPE = "out of AFTS scope"


def test_no_published_row_is_called_out_of_scope():
    """The contradiction test. The register has already decided these."""
    offenders = []
    for row in json.loads(RECALLS_JSON.read_text("utf-8")):
        _pathogen, note = infer_pathogen(row)
        if OUT_OF_SCOPE in note:
            offenders.append((str(row.get("Date"))[:10],
                              str(row.get("Source")),
                              str(row.get("Pathogen"))[:48],
                              str(row.get("Product"))[:48]))
    assert not offenders, (
        "%d PUBLISHED row(s) are reported out of AFTS scope by the module "
        "that decides scope for Pending rows. One of the two is wrong, and "
        "the published row is the register's own decision:\n    %s" % (
            len(offenders),
            "\n    ".join("%s  %s  %r / %r" % o for o in offenders)))


@pytest.mark.parametrize("text,expected", [
    ("Presence of undeclared sildenafil",
     "Undeclared pharmacological ingredient"),
    ("Undeclared tadalafil and dapoxetine detected",
     "Undeclared pharmacological ingredient"),
    ("undeclared drug ingredients (DNP and sibutramine)",
     "Undeclared pharmacological ingredient"),
    ("Contains the unapproved drug phenolphthalein",
     "Undeclared pharmacological ingredient"),
    ("Pre-workout powder containing DMAA",
     "Undeclared pharmacological ingredient"),
    # The two that the bare "allerg" token was swallowing.
    ("Presence of veterinary medicines including antibiotics. The residues "
     "may make the product unsafe to consume and are a possible health risk "
     "for anyone with an allergy to antibiotics (penicillin).",
     "Veterinary drug residue"),
    ("Produced without the benefit of federal inspection; FSIS: food "
     "produced without inspection may contain undeclared allergens, harmful "
     "bacteria or other contaminants.",
     "Uninspected product (hazard not assessed)"),
])
def test_an_in_scope_hazard_is_inferred(text, expected):
    got, note = infer_pathogen({"Reason": text, "Notes": "", "Product": ""})
    assert got == expected, f"{text[:60]!r} -> {got!r} ({note})"


@pytest.mark.parametrize("text", [
    "Recall due to undeclared peanut",
    "Undeclared milk",
    "undeclared sesame seeds",
    "allergene non declare : lait",
    "Présence de gluten non déclaré",
    "undeclared egg in the product",
    "Nicht deklariert: Haselnuss",
])
def test_an_allergen_only_hazard_is_still_refused(text):
    got, note = infer_pathogen({"Reason": text, "Notes": "", "Product": ""})
    assert got is None and OUT_OF_SCOPE in note, (
        f"{text!r} is an allergen-only hazard and must stay out of scope; "
        f"got {got!r} ({note})")
    assert ALLERGEN_ONLY.search(text)


@pytest.mark.parametrize("text", [
    # The hazard is real; an allergen named beside it must not win.
    "Presence of Listeria monocytogenes and undeclared milk",
    "Salmonella spp. detected; label also omits the soy allergen",
])
def test_a_real_hazard_beside_an_allergen_keeps_its_hazard(text):
    got, _note = infer_pathogen({"Reason": text, "Notes": "", "Product": ""})
    assert got is not None, (
        f"{text!r} names a real hazard and an allergen; the real hazard must "
        f"be the one inferred")


def test_the_bare_word_undeclared_no_longer_matches_on_its_own():
    """The defect itself, asserted rather than assumed.

    A truncated FDA headline — "…Due Presence Undeclared", which is how
    the Tonight We Ride / Ultimate Forza row reached Pending on
    2026-10-25 — must report that the row states no hazard, not that it is
    out of scope. The row needs its detail page read; it is not a
    dismissal.
    """
    assert not ALLERGEN_ONLY.search("Due Presence Undeclared")
    got, note = infer_pathogen({
        "Reason": "Tonight We Ride Llc Issues Voluntary Recall Ultimate "
                  "Forza Male Supplement Due Presence Undeclared",
        "Notes": "FDA HTML fallback — claude-check needs to enrich "
                 "Date+Pathogen",
        "Product": "Tonight We Ride Llc Issues Voluntary Recall Ultimate "
                   "Forza Male Supplement Due Presence Undeclared"})
    assert got is None
    assert OUT_OF_SCOPE not in note
    assert "no hazard term" in note
