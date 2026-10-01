"""One spelling per firm and brand (operator 2026-10-01).

"LIDL" (RappelConso) next to "Lidl" (NVWA); "CARREFOUR LE MARCHE", "Carrefour
le Marche" and "Carrefour le Marché" — 51 names written two or three ways on
2026-10-01. See pipeline/_firm_names.py for the rule.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._firm_names import (  # noqa: E402
    UNBRANDED, canonical_map, choose, fold, unify_firm_names,
)


def _rows():
    return json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))


def test_the_register_has_one_spelling_per_name():
    m = canonical_map(_rows())
    assert not m, f"{len(m)} name(s) still spelled more than one way: " \
                  f"{list(m.items())[:6]}"


def test_no_brand_has_one_spelling():
    bad = Counter(r["Brand"] for r in _rows()
                  if r.get("Source") != "RASFF (EU)"
                  and fold(r.get("Brand")) in {"sans marque", "neutre",
                                               "fabrique sur place",
                                               "non communique"})
    assert not bad, bad


@pytest.mark.parametrize("forms,expected", [
    ({"LIDL": 3, "Lidl": 4}, "Lidl"),
    ({"Carrefour le Marché": 10, "Carrefour le Marche": 28,
      "CARREFOUR LE MARCHE": 11}, "Carrefour le Marché"),
    ({"E. Leclerc": 1, "E.Leclerc": 2, "E.LECLERC": 3}, "E.Leclerc"),
    ({"GAEC de la Cascade": 1, "GAEC DE LA CASCADE": 1}, "GAEC de la Cascade"),
    ({"Akar GmbH": 1, "AKAR GmbH": 1}, "Akar GmbH"),
    ({"MONOPRIX": 5, "Monoprix": 2}, "Monoprix"),
])
def test_the_chosen_form(forms, expected):
    assert choose(Counter(forms)) == expected


def test_different_words_are_never_merged():
    rows = [{"Source": "RappelConso (FR)", "Company": "Carrefour", "Brand": ""},
            {"Source": "RappelConso (FR)", "Company": "Carrefour France", "Brand": ""},
            {"Source": "RappelConso (FR)", "Company": "SASU Paturages Comtois", "Brand": ""},
            {"Source": "RappelConso (FR)", "Company": "Paturages Comtois", "Brand": ""}]
    assert unify_firm_names(rows) == 0


def test_across_sources_and_fields():
    rows = [{"Source": "RappelConso (FR)", "Company": "LIDL", "Brand": "Sans marque"},
            {"Source": "NVWA (NL)", "Company": "Lidl", "Brand": "/"},
            {"Source": "AGES (AT)", "Company": "x", "Brand": "lidl"}]
    unify_firm_names(rows)
    assert [r["Company"] for r in rows[:2]] == ["Lidl", "Lidl"]
    assert rows[2]["Brand"] == "Lidl"
    assert rows[0]["Brand"] == rows[1]["Brand"] == UNBRANDED


def test_rasff_is_untouched():
    rows = [{"Source": "RASFF (EU)", "Company": "Origin: Poland | Notifying: Slovakia",
             "Brand": "SK"},
            {"Source": "RASFF (EU)", "Company": "ORIGIN: POLAND | NOTIFYING: SLOVAKIA",
             "Brand": "/"}]
    before = [dict(r) for r in rows]
    unify_firm_names(rows)
    assert rows == before


def test_the_writer_applies_it():
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "from pipeline._firm_names import unify_firm_names" in src
