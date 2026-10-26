# -*- coding: utf-8 -*-
"""Operator rule 2026-10-26: foreign material (glass, metal, plastic,
stones, ...) is Tier 3 for every source — not Tier 2, not Tier 1.
Exception kept from the scraper framework (H13): infant / baby food is Tier 1.
"""
import json
from pathlib import Path

import pytest

from pipeline._pathogen_scope import (PHYSICAL_TIER, _is_physical_only,
                                      enforce_tier1)

ROOT = Path(__file__).resolve().parents[1]


def test_the_tier_is_3():
    assert PHYSICAL_TIER == 3


@pytest.mark.parametrize("p", [
    "Glass fragments", "Foreign material (glass)", "Foreign material (metal)",
    "Foreign body", "Possible foreign body (Steel sphere)",
    "Foreign material (stones)", "Foreign material (plastic pieces)",
    "Physical/foreign-body contamination", "Foreign object",
    "Foreign material (mold-like matter)",
])
def test_physical_hazards_are_recognised(p):
    assert _is_physical_only(p)


@pytest.mark.parametrize("p", [
    "Listeria monocytogenes", "Mold", "Aflatoxin", "Cadmium (heavy metal)",
    "Rodent contamination (physical/microbial hazard)",
    "Listeria monocytogenes and foreign body", "",
])
def test_other_hazards_are_not_physical_only(p):
    assert not _is_physical_only(p)


@pytest.mark.parametrize("start", [1, 2, 3, "", None])
def test_foreign_material_is_set_to_tier_3_from_any_tier(start):
    row = {"Pathogen": "Possible foreign body (Steel sphere)",
           "Product": "Tiramisù", "Tier": start, "Notes": ""}
    enforce_tier1(row)
    assert int(row["Tier"]) == 3
    once = dict(row)
    enforce_tier1(row)
    assert row == once, "the guard must be idempotent"


def test_infant_food_keeps_h13_tier_1():
    row = {"Pathogen": "Foreign material (glass)",
           "Product": "Baby food jar, carrot purée", "Tier": 3, "Notes": ""}
    enforce_tier1(row)
    assert int(row["Tier"]) == 1


def test_a_pathogen_beside_foreign_matter_keeps_tier_1():
    row = {"Pathogen": "Listeria monocytogenes and foreign body",
           "Product": "Cheese", "Tier": 1, "Notes": ""}
    enforce_tier1(row)
    assert int(row["Tier"]) == 1


def test_every_published_foreign_material_row_is_tier_3():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    rows = rows if isinstance(rows, list) else rows.get("recalls", [])
    bad = []
    for r in rows:
        if not _is_physical_only(r.get("Pathogen")):
            continue
        want = int(enforce_tier1(dict(r)).get("Tier") or 0)
        if str(r.get("Tier")) != str(want):
            bad.append((r.get("Source"), r.get("Date"),
                        str(r.get("Product"))[:40], r.get("Tier")))
    assert not bad, bad
