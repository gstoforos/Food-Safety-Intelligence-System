"""One scope at every gate (operator decision 2026-09-30: "printed scope
everywhere").

The AFTS scope printed on every brief since 2026-07-29: pathogens +
biotoxins + mycotoxins + foreign material + pest + chemical hazards;
visible mould since 2026-09-07; uninspected product since 2026-09-25.
Allergen-only, labelling and quality/spoilage are out.

Before this, the Pending gate refused a foreign-body or chemical row that
arrived with its hazard named, while the publish gate published the same
kind of row when it arrived empty and was enriched later — 38 September rows
came in by that side door. This sweeps every gate with the same cases.
"""
from __future__ import annotations

import pytest

from pipeline._pathogen_scope import is_in_afts_scope, is_in_scope
from pipeline.gap_finder.rules import classify

IN = [
    "Listeria monocytogenes", "Salmonella", "Aflatoxin", "Mold",
    "Foreign material (metal)", "Foreign material (shell fragments)",
    "Cadmium (heavy metal)", "Lead (heavy metal)", "Histamine / scombrotoxin",
    "Undeclared pharmacological ingredient (sildenafil, tadalafil)",
    "Yellow oleander (cardiac glycosides)", "Tropane alkaloids",
    "Uninspected product (hazard not assessed)",
]
OUT = ["Peanut", "Undeclared milk", "Spoilage", "", "—"]


@pytest.mark.parametrize("p", IN)
def test_in_scope(p):
    assert is_in_afts_scope(p), p


@pytest.mark.parametrize("p", OUT)
def test_out_of_scope(p):
    assert not is_in_afts_scope(p), p


def test_tier1_scope_is_unchanged():
    """is_in_scope is also the Tier-1 test; it must NOT widen."""
    assert not is_in_scope("Foreign material (metal)")
    assert is_in_scope("Listeria monocytogenes")


@pytest.mark.parametrize("p", [
    "Foreign material (metal)", "Cadmium (heavy metal)",
    "Yellow oleander (cardiac glycosides)",
])
def test_the_pending_gate_admits_a_named_non_pathogen_hazard(p):
    from pipeline.merge_master import validate_pending_row
    row = {"Date": "2026-09-28", "Source": "BVL (DE)", "Company": "Acme GmbH",
           "Brand": "Acme", "Product": "Mini salami 100 g", "Pathogen": p,
           "Reason": "Recall", "Country": "Germany",
           "URL": "https://www.lebensmittelwarnung.de/x/260928_11_BY_x.html"}
    ok, why = validate_pending_row(row, set())
    assert "pathogen_out_of_scope" not in str(why), (p, why)


def test_the_pending_gate_still_refuses_allergen_only():
    from pipeline.merge_master import validate_pending_row
    row = {"Date": "2026-09-28", "Source": "BVL (DE)", "Company": "Acme GmbH",
           "Brand": "Acme", "Product": "Biscuits 200 g", "Pathogen": "Peanut",
           "Reason": "Undeclared peanut", "Country": "Germany",
           "URL": "https://www.lebensmittelwarnung.de/x/260928_12_BY_y.html"}
    ok, why = validate_pending_row(row, set())
    assert not ok


@pytest.mark.parametrize("reason,cat", [
    ("glass fragments", "foreign_matter"), ("cadmium above limit", "heavy_metal"),
    ("pesticide residue chlorpyrifos", "synthetic_chemical"),
])
def test_the_gap_finder_accepts_the_printed_scope(reason, cat):
    c = classify(reason=reason)
    assert (c.verdict, c.category) == ("accept", cat)


@pytest.mark.parametrize("mod", ["pipeline.url_gate_gemini"])
def test_the_checkers_use_the_afts_scope(mod):
    import importlib.util
    from pathlib import Path
    src = Path(importlib.util.find_spec(mod).origin).read_text(encoding="utf-8")
    assert "is_in_afts_scope as is_tier1_pathogen" in src
