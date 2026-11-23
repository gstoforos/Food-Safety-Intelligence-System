# -*- coding: utf-8 -*-
"""A named pesticide active substance must reach a subscriber alert.

MORNING FIX 2026-11-23
----------------------
R6 names pesticide residues as in scope. Every pesticide hazard this
register has ever PUBLISHED carries the words "pesticide residue" as a
parenthetical the enrichment adds:

    Procymidone (pesticide residue)
    Acetamiprid (pesticide residue)
    Tolfenpyrad (pesticide residue)
    Chlorpyrifos (pesticide residue) and unauthorized dye E 122
    Pesticide residue exceeding limit (klorpirifos)

So the whole class reached an alert only through the words "pesticide" and
"residue", and tools/alert_vocab.py's own note already said that was "a
thinner thread than it looks".

It broke this morning. Mattilsynet's wording for the Hewad Mohmand raisin
recall of 18.11.2026 is "Chlorpyrifos exceeding limit" — no parenthetical —
and the collector wrote it through verbatim, which is correct. The publish
gate's unreachable-hazard rule held the row in Pending, which is the gate
doing its job; the defect was that an in-scope hazard had no word to reach
it, so the row was unpublishable until a human renamed the hazard.

This test holds the general statement: a hazard written as the bare active
substance must be reachable without the parenthetical.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import alert_vocab as V          # noqa: E402

#: The regulator's own bare wording, with no "(pesticide residue)" added.
BARE_ACTIVE_HAZARDS = [
    "Chlorpyrifos exceeding limit",
    "Chlorpyrifos",
    "Pesticide residue exceeding limit (klorpirifos)",
    "Klorpirifos over grenseverdi",
    "Acetamiprid",
    "Procymidone",
    "Tolfenpyrad",
]


@pytest.mark.parametrize("pathogen", BARE_ACTIVE_HAZARDS)
def test_a_bare_active_substance_is_a_chemical_hazard(pathogen):
    """The publish gate's class map must see it too.

    The alert vocabulary and the hazard class map are separate lists with
    the same blind spot, and the class map's is the costlier one: a row
    that classifies as nothing is refused by
    pipeline.agents.curator.check_scope AND by
    merge_master.validate_pending_row ("pathogen_out_of_scope"), so the
    recall never reaches Pending at all.
    """
    from pipeline._publish_gate import classify_hazard
    assert "chemical" in classify_hazard(pathogen), (
        "classify_hazard(%r) = %r — a pesticide residue is a chemical "
        "hazard and R6 names it in scope. Add the active to "
        "HAZARD_CLASS_KEYWORDS['chemical'] in pipeline/_publish_gate.py."
        % (pathogen, classify_hazard(pathogen)))


def test_ordinary_prose_is_not_turned_into_a_pesticide_hazard():
    """The actives must be specific enough to cost nothing."""
    from pipeline._publish_gate import classify_hazard
    innocent = [
        "Listeria monocytogenes",
        "Chocolate flavour pudding, 200 g",
        "Undeclared peanut",
        "Foreign material (metal pieces)",
        "Salmonella spp (agent responsable de la salmonellose)",
    ]
    wrong = [t for t in innocent if "chemical" in classify_hazard(t)]
    assert not wrong, "these are not chemical hazards: %s" % wrong


@pytest.mark.parametrize("pathogen", BARE_ACTIVE_HAZARDS)
def test_a_bare_active_substance_reaches_an_alert(pathogen):
    row = {"Pathogen": pathogen}
    assert any(V.matches(row, "pathogen", t) for t in V.PATHOGEN), (
        "%r reaches no subscriber alert word. A published hazard no alert "
        "can reach is invisible to every subscriber. Add the active to "
        "tools/alert_vocab.py and re-run "
        "`python3 tools/gen_alert_vocab.py --write`." % pathogen)


def test_the_named_actives_are_the_ones_the_corpus_holds():
    """Every active offered must be producible from a real row.

    tests/test_alert_vocab::test_every_pathogen_term_is_producible asserts
    this for the whole vocabulary; named here as well so that an active
    added on a hunch rather than from a notice shows up in THIS file, next
    to the reason the actives exist at all.
    """
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                # pragma: no cover
        pytest.skip("workbook not present in this checkout")
    actives = {"chlorpyrifos", "klorpirifos", "acetamiprid",
               "procymidone", "tolfenpyrad"}
    # PATHOGEN maps an offered category label -> its match tokens.
    tokens = {t.lower() for toks in V.PATHOGEN.values() for t in toks}
    assert actives <= tokens, (
        "actives missing from the vocabulary: %s" % sorted(actives - tokens))

    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    try:
        blob = []
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            rows = ws.iter_rows(values_only=True)
            head = [str(c) for c in next(rows)]
            idx = [head.index(c) for c in ("Pathogen", "Reason") if c in head]
            for row in rows:
                for i in idx:
                    if i < len(row) and row[i]:
                        blob.append(str(row[i]).lower())
    finally:
        wb.close()
    hay = " | ".join(blob)
    ungrounded = sorted(a for a in actives if a not in hay)
    assert not ungrounded, (
        "these actives appear in no row of the register — a vocabulary word "
        "with no notice behind it is a guess: %s" % ungrounded)
