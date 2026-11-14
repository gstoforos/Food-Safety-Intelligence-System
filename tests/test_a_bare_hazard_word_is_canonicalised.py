# -*- coding: utf-8 -*-
"""A filled Pathogen can still be the wrong word for the hazard.

pipeline/enrich_pending_offline.py only ever looked at rows whose Pathogen
was EMPTY. A row arriving with a hazard WORD rather than this register's
label for that hazard was never touched, and nothing downstream
canonicalised it.

THE ROW THAT FOUND IT. CFIA, 2026-11-13, "Producers brand Brown Rice
recalled due to presence of insects", Tree of Life Canada. The scraper
wrote Pathogen "Insect". Six published rows call that hazard "Foreign
material (pest)"; pipeline/_publish_gate.py and
tests/test_pest_and_foreign_material_are_one_hazard.py are both written
around that label; the curator's scope map does not carry "Insect" at all.
Publishing the row as it stood turned
tests/test_curator_scope_2026_09_09.py RED — the curator refusing the
hazard of a row this register had just published.

The repair is deliberately narrow, and these tests are mostly about what
it must NOT touch.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.enrich_pending_offline import (  # noqa: E402
    BARE_HAZARD_WORDS, canonical_hazard_word,
)

RECALLS = ROOT / "docs" / "data" / "recalls.json"


@pytest.mark.parametrize("word,canon", [
    ("Insect", "Foreign material (pest)"),
    ("insects", "Foreign material (pest)"),
    ("  Pest  ", "Foreign material (pest)"),
    ("Glass.", "Foreign material (glass)"),
    ("metal", "Foreign material (metal)"),
])
def test_a_bare_hazard_word_gets_the_registers_label(word, canon):
    assert canonical_hazard_word(word) == canon


@pytest.mark.parametrize("value", [
    "", "   ", None,
    # already canonical — nothing to do, and no churn stamp either
    "Foreign material (pest)", "Foreign material (glass)",
    # a decision some stage already made, in more than one word: left alone
    "Metal fragments found in the product",
    "Insect fragments above the defect action level",
    "Glass fragments in the yoghurt",
    "Listeria monocytogenes", "Salmonella spp.",
    "Unspecified microbiological contamination",
    "Foreign material (pest) and mould",
])
def test_everything_else_is_left_exactly_alone(value):
    assert canonical_hazard_word(value) is None


def test_the_table_only_maps_onto_labels_the_register_publishes():
    """A canonical label nobody publishes is a new vocabulary, not a fix."""
    published = {str(r.get("Pathogen") or "")
                 for r in json.loads(RECALLS.read_text("utf-8"))}
    missing = sorted(set(BARE_HAZARD_WORDS.values()) - published)
    assert not missing, (
        f"these canonical labels appear nowhere in the published register: "
        f"{missing}. Add the label to the register's vocabulary first, or "
        f"map onto one it already uses.")


def test_no_published_row_carries_a_bare_hazard_word():
    offenders = sorted({str(r.get("Pathogen") or "")
                        for r in json.loads(RECALLS.read_text("utf-8"))
                        if canonical_hazard_word(r.get("Pathogen"))})
    assert not offenders, (
        f"published rows carry a bare hazard word rather than this "
        f"register's label for it: {offenders}")
