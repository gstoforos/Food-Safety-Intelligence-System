"""RASFF already classified a third of the register. Don't re-infer it.

These tests pin the parser against the exact string shapes found in the
corpus on 2026-08-28, and against the two ways this kind of parser fails:
capturing past the end of a value, and silently absorbing a vocabulary
term it does not actually know.
"""
from __future__ import annotations

import pytest

from pipeline.regulator_fields import (
    CATEGORY_MAP, CLASSIFICATION_MAP, RISK_VALUES, parse_rasff,
)

REASON = ("Listeria monocytogenes in halloumi from Cyprus; risk: serious; "
          "category: milk and milk products")
NOTES = ("[RASFF #2026.7533; classification: alert notification; "
         "category: milk and milk products; notifId=868574] "
         "[gemini-enrich 2026-08-27: Brand '—'→'DEU'] "
         "[promoted 2026-08-28: official RASFF scrape]")


def test_parses_the_real_row():
    f = parse_rasff(REASON, NOTES)
    assert f.rasff_id == "2026.7533"
    assert f.notif_id == "868574"
    assert f.category_raw == "milk and milk products"
    assert f.food_category == "dairy-other"
    assert f.risk == "serious"
    assert f.classification_raw == "alert notification"
    assert f.notice_type == "consumer-recall"


def test_value_stops_at_the_next_stamp():
    """The bug that split one category into five buckets.

    A value must end at ';' or at the '[' opening the next Notes stamp.
    Anything looser swallows the stamp and every distinct suffix becomes
    its own category.
    """
    n = "[RASFF #2026.1; category: fruits and vegetables; notifId=1] [promoted 2026-08-28]"
    f = parse_rasff("", n)
    assert f.category_raw == "fruits and vegetables"
    assert "[" not in f.category_raw and "promoted" not in f.category_raw


def test_reason_and_notes_are_not_concatenated():
    """Joining them lets a value run off the end of one field into the next."""
    f = parse_rasff("Salmonella in turkey; risk: serious", "[RASFF #2026.9]")
    assert f.risk == "serious"
    assert f.rasff_id == "2026.9"


def test_an_unknown_category_is_a_gap_not_a_guess():
    f = parse_rasff("", "[category: warp core coolant]")
    assert f.category_raw == "warp core coolant"
    assert f.food_category is None, (
        "an unmapped RASFF term must surface as a gap, not be absorbed into "
        "'other' where nobody will notice the vocabulary moved")


def test_an_unknown_risk_grade_is_rejected():
    f = parse_rasff("risk: catastrophic", "")
    assert f.risk is None, "risk must come from RASFF's own scale, not any word"


def test_trailing_prose_after_a_category_is_tolerated():
    f = parse_rasff("", "[category: poultry meat. RASFF notification 852690.]")
    assert f.food_category == "meat-poultry"


def test_empty_input_is_empty_output():
    f = parse_rasff("", "")
    assert f.filled() == 0
    assert f.food_category is None and f.risk is None


def test_every_mapped_category_targets_the_documented_vocabulary():
    allowed = {"meat-poultry", "meat-other", "fish-seafood", "dairy-soft-cheese",
               "dairy-other", "eggs-egg-products", "bakery-cereal",
               "nuts-seeds", "dried-fruit", "fresh-produce", "frozen-produce",
               "herbs-spices", "confectionery-snacks", "prepared-meals",
               "sauces-condiments", "beverages", "infant-food", "supplements",
               "other"}
    bad = {k: v for k, v in CATEGORY_MAP.items() if v not in allowed}
    assert not bad, f"category map targets values outside the vocabulary: {bad}"


def test_classification_map_targets_the_notice_type_vocabulary():
    allowed = {"consumer-recall", "withdrawal", "border-rejection",
               "public-warning", "information"}
    bad = {k: v for k, v in CLASSIFICATION_MAP.items() if v not in allowed}
    assert not bad, bad


def test_risk_scale_is_rasffs_own():
    assert set(RISK_VALUES) == {"serious", "potentially serious",
                                "not serious", "potential risk"}


@pytest.mark.parametrize("cls,expect", [
    ("alert notification", "consumer-recall"),
    ("border rejection notification", "border-rejection"),
    ("information notification for attention", "information"),
    ("information notification for follow-up", "information"),
])
def test_every_observed_classification_maps(cls, expect):
    assert parse_rasff("", f"[classification: {cls}]").notice_type == expect


def test_corpus_coverage_has_not_regressed():
    """Guard the headline claim: ~96% of RASFF rows carry the taxonomy."""
    import pandas as pd
    from pathlib import Path
    xlsx = Path(__file__).resolve().parent.parent / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():
        pytest.skip("recalls.xlsx not present")
    df = pd.read_excel(xlsx, "Recalls")
    rows = df[df["Source"].astype(str).str.strip() == "RASFF (EU)"]
    if len(rows) < 50:
        pytest.skip("too few RASFF rows to measure")
    parsed = [parse_rasff(r.get("Reason"), r.get("Notes")) for _, r in rows.iterrows()]
    got = sum(1 for p in parsed if p.food_category) / len(parsed)
    assert got > 0.90, f"food_category coverage fell to {got:.1%}"
    unmapped = [p.category_raw for p in parsed if p.category_raw and not p.food_category]
    assert not unmapped, f"unmapped RASFF categories: {sorted(set(unmapped))}"
