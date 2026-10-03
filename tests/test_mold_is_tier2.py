"""Mold is Tier 2 for every source (operator rule 2026-10-03:
"bring mold in tier two for everything").

The register carried mold at Tier 2 from CAA, RASFF, FSANZ and EFET and at
Tier 3 from RappelConso (Cokoc gummies, fiche 23674): each scraper set its
own tier and nothing enforced _TIERS["Mold"] = 2. The tier guard
(pipeline/_pathogen_scope.enforce_tier1) now SETS it, up from 3 and down
from 1, and the writer runs that guard on every Recalls save.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._pathogen_scope import MOLD_TIER, _is_mold, enforce_tier1  # noqa: E402


def test_the_rule_is_tier_2():
    assert MOLD_TIER == 2


@pytest.mark.parametrize("p", ["Mold", "Mould", "mold", "schimmel", "plesn",
                               "Moisissures", "Mold (Aspergillus)"])
def test_every_spelling_is_mold(p):
    assert _is_mold(p)


@pytest.mark.parametrize("p", ["Aflatoxin", "Ochratoxin", "Mycotoxin",
                               "Foreign material (mold-like matter)",
                               "Listeria monocytogenes", ""])
def test_other_hazards_are_not_mold(p):
    assert not _is_mold(p)


@pytest.mark.parametrize("start", [1, 3, "", None])
def test_mold_is_set_to_tier_2_from_any_tier(start):
    row = {"Pathogen": "Mold", "Tier": start, "Notes": ""}
    enforce_tier1(row)
    assert int(row["Tier"]) == 2
    once = dict(row)
    enforce_tier1(row)
    assert row == once, "the guard must be idempotent"


def test_a_mycotoxin_keeps_its_own_tier():
    row = {"Pathogen": "Aflatoxin", "Tier": 3, "Notes": ""}
    enforce_tier1(row)
    assert row["Tier"] == 3


def test_every_published_mold_row_is_tier_2():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json").read_text(encoding="utf-8"))
    rows = rows if isinstance(rows, list) else rows.get("recalls", [])
    bad = [(r.get("Source"), r.get("Company"), r.get("Tier")) for r in rows
           if _is_mold(r.get("Pathogen")) and str(r.get("Tier")) != "2"]
    assert not bad, f"mold rows not at Tier 2: {bad}"
