# -*- coding: utf-8 -*-
"""Rule 7 must not read the regulator's own wording as a contradiction.

WHAT THIS CAUGHT (morning-fix 2026-10-06)
=========================================
Japan's Consumer Affairs Agency published a recall for Kinokuniya miso
peanuts on 2026/10/05 (rcl 35916). Its 回収理由 is, in full,

    虫（ノシメマダラメイガ）の混入

— contamination with an insect, the Indian meal moth. Nothing else. Queued
with the register's existing published label for that hazard,

    Pathogen  "Foreign material (pest)"
    Reason    "Insect contamination: the Indian meal moth was found in the
               product."

``_publish_gate`` rule 7 refused it:

    Pathogen 'Foreign material (pest)' contradicts Reason
    (['physical'] vs ['pest']) — one of the two fields is not what the
    source page says

Both fields say exactly what the source page says. "foreign material"
classifies 'physical', "insect" classifies 'pest', the two sets share
nothing, and ``pathogen_reason_class_mismatch`` fired on the difference.

They are not two hazards. Rule 8's own scope line, twenty lines further
down the same file, prints the AFTS scope as "Pathogens + biotoxins +
mycotoxins + foreign material + pest + chemical hazards only" — foreign
material and pest, listed together as one scope. And "Foreign material
(pest)" is not an invented label: two FDA rows are PUBLISHED under it.

Those two passed only by luck of phrasing. Their Reasons read "foreign
objects such as Pest contaminant in jar" and "a single lot … was recalled
… due to potential pest inclusion" — both contain a word that classifies
'physical', so both sides agreed. A notice that says only "insect" could
never pass. The rule was therefore refusing rows for how the regulator
chose to word its notice, which is the one thing a deterministic gate must
not do.

WHY THIS SHAPE
--------------
The fix could have been to reword the Reason until the classifier agreed —
"Foreign material: insect (Indian meal moth) contamination" passes. That
is a row-shaped workaround for a gate-shaped defect, and it would have put
the next pest recall in front of the same wall.

So the fix pairs the two classes in ``_ONE_FAMILY`` and this test holds
both halves: that a pest/physical pair passes whichever side the word
falls on, and — the part that matters more — that the mismatches rule 7
exists for still fail. The 2026-08-02 incident in the module docstring, an
invented "Listeria monocytogenes" on a row whose Reason said "undeclared
allergen (peanuts)", is pinned by name below.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import (                        # noqa: E402
    classify_hazard,
    pathogen_reason_class_mismatch,
)

#: pest/physical pairs that must all pass, in both field orders.
SAME_FAMILY = [
    # The 2026-10-06 row, verbatim.
    ("Foreign material (pest)",
     "Insect contamination: the Indian meal moth was found in the product."),
    # The two already-published FDA rows, which passed by phrasing luck.
    ("Foreign material (pest)",
     "Product may contain foreign objects such as Pest contaminant in jar"),
    ("Foreign material (pest)",
     "Recalled because a single lot of the Golden Greek Peperoncini used as "
     "an ingredient was recalled by G. L. Mezzetta Inc. due to potential "
     "pest inclusion"),
    # The reverse: a pest-classed Pathogen against a physical-classed Reason.
    ("Insect infestation",
     "Foreign material found in the product"),
]

#: The mismatches rule 7 was written for. Every one must still fire.
TRUE_MISMATCHES = [
    # 2026-08-02, pinned by name: an invented pathogen on an allergen recall.
    ("Listeria monocytogenes", "undeclared allergen (peanuts)"),
    ("Salmonella", "Undeclared milk allergen"),
    ("Foreign material (metal fragments)",
     "undeclared allergen (sesame) not listed on the label"),
    ("Listeria monocytogenes", "Metal fragments found in the product"),
]


@pytest.mark.parametrize("pathogen,reason", SAME_FAMILY)
def test_a_pest_hazard_and_a_foreign_material_label_do_not_contradict(
        pathogen, reason):
    assert not pathogen_reason_class_mismatch(pathogen, reason), (
        f"rule 7 refused {pathogen!r} against its own source wording "
        f"{reason[:60]!r} "
        f"({sorted(classify_hazard(pathogen))} vs "
        f"{sorted(classify_hazard(reason))}). Foreign material and pest are "
        f"ONE family in the AFTS scope — rule 8 prints them on the same "
        f"line — and 'Foreign material (pest)' is the register's published "
        f"label for this hazard")


@pytest.mark.parametrize("pathogen,reason", TRUE_MISMATCHES)
def test_a_real_contradiction_still_fails(pathogen, reason):
    assert pathogen_reason_class_mismatch(pathogen, reason), (
        f"rule 7 stopped catching a real contradiction: {pathogen!r} vs "
        f"{reason[:60]!r}. Widening _ONE_FAMILY must never reach these")


def test_the_family_does_not_quietly_swallow_everything():
    """A ratchet on the exemption itself.

    _ONE_FAMILY is a hole in rule 7. One group of two classes is a
    judgement about the AFTS scope; a growing list of them is the rule
    being switched off one pair at a time.
    """
    from pipeline._publish_gate import _ONE_FAMILY

    assert len(_ONE_FAMILY) == 1, (
        f"_ONE_FAMILY has {len(_ONE_FAMILY)} groups. Each one is a pair of "
        f"hazard classes rule 7 can no longer tell apart — a decision about "
        f"the scope, not a test fix")
    assert _ONE_FAMILY[0] == frozenset({"physical", "pest"})


def test_the_published_pest_rows_still_pass_their_own_gate():
    """The two FDA rows this label already lives on, read out of the
    register rather than copied into this file."""
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    rows = list(wb["Recalls"].values)
    hdr = [str(h) for h in rows[0]]
    ip, ir = hdr.index("Pathogen"), hdr.index("Reason")
    checked = 0
    for r in rows[1:]:
        if not r or not r[ip]:
            continue
        if "pest" not in str(r[ip]).lower():
            continue
        checked += 1
        assert not pathogen_reason_class_mismatch(str(r[ip]), str(r[ir] or "")), (
            f"published row refuses its own gate: {str(r[ip])!r} vs "
            f"{str(r[ir])[:60]!r}")
    assert checked >= 2, (
        f"only {checked} published pest row(s) found; this test was written "
        f"against the two carrying 'Foreign material (pest)'")
