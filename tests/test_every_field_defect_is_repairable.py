# -*- coding: utf-8 -*-
"""The re-promotion guard's repairable-defect list vs the publish gate's own
field-defect messages. Morning fix pass 2026-11-18.

THE INCIDENT
------------
Fødevarestyrelsen (DK) 13.11.2026 — Lidl Danmark K/S, Eridanous Chicken
gyros 330 g, Salmonella, TIER 1 — on the regulator's own per-recall URL.
The scraper had written the page <title> into Product ("Fødevarestyrelsen")
and the Danish headline into Company AND Brand. The publish gate archived it
on 2026-11-14 for the Product defect:

    [publish-gate 2026-08-01: Product 'Fødevarestyrelsen' carries the
     authority's own name ('fødevarestyrelsen') — the page <title> or its
     site-name suffix was parsed as the product]

The morning fix pass read the Fødevarestyrelsen page, repaired all three
fields, and the row then passed the FULL publish gate. It was refused
anyway:

    re-promotion BLOCKED … — already in Rejected (confirm-agent: …)

because merge_master's REPAIRABLE_DEFECTS — the tuple that decides whether
an archived reason was a judgement on the RECALL or merely on the SHAPE OF A
FIELD — did not contain that phrase. It is a hand-kept subset of the gate's
messages and the gate had grown past it. Third instance of the 2026-10-05
Mattilsynet shape: a verified recall blocked for ever by a reader that does
not know what the writers write.

WHAT THIS FILE PINS
-------------------
1. Every field-shape verdict the publish gate ACTUALLY emits — read off the
   gate itself, not retyped — is matched by REPAIRABLE_DEFECTS.
2. Every such verdict found in this workbook's archives is matched too.
3. A real SCOPE verdict is NOT matched, so widening the list can never
   resurrect an out-of-scope row. Condition 2 of the exception (the full
   publish gate must pass on the repaired row) is the other half of that
   guarantee and is unchanged.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
MERGE = ROOT / "pipeline" / "merge_master.py"
PROMOTE = ROOT / "pipeline" / "promote_gate_passing.py"
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: THE TWO LISTS THAT MUST MOVE TOGETHER.
#:
#: merge_master.REPAIRABLE_DEFECTS decides whether an archived row may be
#: RE-PROMOTED once its field is repaired. promote_gate_passing.SUPERSEDE_IF
#: decides whether that row's ARCHIVE COPY is then marked SUPERSEDED. The
#: 2026-09-30 note in promote_gate_passing states the rule — "the two lists
#: must move together or a re-promoted row leaves its archive copy
#: contradicting the register" — and on 2026-11-18 that is exactly what
#: happened: the first list gained the gate's remaining field messages, the
#: second did not, the Fødevarestyrelsen Chicken gyros row re-promoted,
#: Rejected was marked SUPERSEDED and the Weekly_Rejected copy stayed
#: silent, turning test_a_recall_is_not_both_published_and_rejected RED on a
#: contradiction the promoter had just created. So both are checked here.
LISTS = (("REPAIRABLE_DEFECTS", "pipeline/merge_master.py"),
         ("SUPERSEDE_IF", "pipeline/promote_gate_passing.py"))


def _literal_strings(path: Path, name: str) -> tuple[str, ...]:
    """String constants of the `name = (...)` literal in `path`.

    Both lists are locals inside functions, so neither can be imported.
    Reading the source is deliberate: a copy in this file would be the very
    drift the test exists to catch.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == name
                   for t in node.targets):
            continue
        if isinstance(node.value, (ast.Tuple, ast.List)):
            for el in node.value.elts:
                if isinstance(el, ast.Constant) and isinstance(el.value, str):
                    found.append(el.value)
    assert found, (f"{name} not found as a literal tuple in {path.name} — "
                   f"if it moved, this test must follow it rather than be "
                   f"deleted.")
    return tuple(found)


def _repairable_defects() -> tuple[str, ...]:
    return _literal_strings(MERGE, "REPAIRABLE_DEFECTS")


def _supersede_if() -> tuple[str, ...]:
    return _literal_strings(PROMOTE, "SUPERSEDE_IF")


def _matches(verdict: str, which: str = "REPAIRABLE_DEFECTS") -> bool:
    low = verdict.lower()
    phrases = (_repairable_defects() if which == "REPAIRABLE_DEFECTS"
               else _supersede_if())
    return any(d in low for d in phrases)


def test_the_two_lists_agree_on_every_field_defect():
    """A phrase in one list and not the other is the 2026-11-18 defect."""
    rep = set(_repairable_defects())
    sup = set(_supersede_if())
    # SUPERSEDE_IF legitimately carries verdicts that are not field defects
    # ("fabricated_pathogen", "confirmer did not re-review",
    # "out_of_scope_import_reinspection", "no official regulator url") and
    # merge_master covers the "<field> is empty" family through its own
    # entries while promote_gate_passing matches them with a regex. So the
    # rule checked here is one-directional and exact: every FIELD-SHAPE
    # phrase that lets a row BACK IN must also retire its archive copy.
    empty_family = {p for p in rep if p.endswith(" is empty")}
    missing = sorted(p for p in rep - sup - empty_family)
    assert not missing, (
        "these phrases let an archived row be RE-PROMOTED "
        "(merge_master.REPAIRABLE_DEFECTS) but do not mark its archive copy "
        "SUPERSEDED (promote_gate_passing.SUPERSEDE_IF):\n  "
        + "\n  ".join(missing) +
        "\n\nA row promoted on one of these leaves a silent contradiction "
        "behind it. Add them to SUPERSEDE_IF — the two lists move together.")


# ── 1. THE GATE'S OWN MESSAGES ────────────────────────────────────────────
#
# Each row below is built to trip ONE field-shape rule, and the message is
# taken from what publish_blockers() returns — never retyped here. If the
# gate rewords a message, this test fails and the tuple gets the new wording.
_BASE = {
    "Date": "2026-11-13", "Source": "Fødevarestyrelsen (DK)",
    "Company": "Lidl Danmark K/S", "Brand": "Eridanous",
    "Product": "Chicken gyros, 330 g", "Pathogen": "Salmonella",
    "Reason": "Salmonella found in the product at routine control",
    "Class": "Recall", "Country": "Denmark", "Tier": 1,
    "URL": ("https://www.foedevarestyrelsen.dk/nyheder/tilbagekaldte-"
            "produkter/2026/nov/tilbagekald-salmonella-i-chicken-gyros"),
}

#: field -> the bad value that trips a field-shape rule. The expected
#: substring is only used to pick the right blocker out of the list.
_FIELD_DEFECT_ROWS = [
    ("Product", "Fødevarestyrelsen", "authority's own name"),
    ("Company", "/", "contains no letter"),
    ("Brand", "/", "contains no letter"),
]


@pytest.mark.parametrize("field,bad,needle", _FIELD_DEFECT_ROWS)
def test_a_field_shape_verdict_the_gate_emits_is_repairable(field, bad, needle):
    from pipeline._publish_gate import publish_blockers
    row = dict(_BASE)
    row[field] = bad
    blockers = publish_blockers(row)
    hit = [b for b in blockers if needle in b]
    assert hit, (
        f"the publish gate no longer emits a {needle!r} blocker for "
        f"{field}={bad!r}; it returned {blockers!r}. Re-point this case at "
        f"the gate's current wording — do not delete it.")
    for b in hit:
        assert _matches(b), (
            f"the publish gate archives a row with this verdict:\n\n    {b}\n\n"
            f"and merge_master's REPAIRABLE_DEFECTS does not match it, so a "
            f"row archived on it can never be re-promoted even after the "
            f"field is repaired — the Fødevarestyrelsen Chicken gyros "
            f"incident of 2026-11-18. This is a judgement on the SHAPE OF A "
            f"FIELD, not on the recall. Add its phrase to the tuple.")


# ── 2. WHAT IS ACTUALLY IN THE ARCHIVES ───────────────────────────────────

_GATE_NOTE = re.compile(r"\[publish-gate [0-9-]+: ([^\]]{0,600})\]")

#: Gate verdicts that are NOT field-shape defects and are not expected to be
#: repairable. Keep this list honest and short: a verdict belongs here only
#: when repairing a field could not make the row publishable.
_NOT_A_FIELD_DEFECT = (
    "is empty",                 # already covered where it is repairable
    "is not yyyy-mm-dd",
    "is not absolute",
    "is a date, not a food",
    "disease-surveillance bulletin",
    "non-food consumer product",
    "website section",
)


def test_every_archived_field_defect_in_this_workbook_is_repairable():
    if not XLSX.exists():                                 # pragma: no cover
        pytest.skip("no workbook")
    x = pd.ExcelFile(XLSX)
    unmatched: list[str] = []
    seen = 0
    for sheet in ("Rejected", "Weekly_Rejected", "Pending"):
        if sheet not in x.sheet_names:
            continue
        df = x.parse(sheet, dtype=str).fillna("")
        if "Notes" not in df.columns:
            continue
        for note in df["Notes"]:
            for block in _GATE_NOTE.findall(note):
                for part in block.split("; "):
                    part = part.strip()
                    if not part:
                        continue
                    low = part.lower()
                    if any(k in low for k in _NOT_A_FIELD_DEFECT):
                        continue
                    seen += 1
                    if not _matches(part):
                        unmatched.append(part[:160])
    assert seen, ("no publish-gate field verdicts found in the archives — "
                  "if the stamp format changed, re-point _GATE_NOTE.")
    assert not unmatched, (
        f"{len(set(unmatched))} distinct publish-gate FIELD verdict(s) in "
        f"this workbook cannot be re-promoted after repair:\n  " +
        "\n  ".join(sorted(set(unmatched))[:8]) +
        "\n\nEach is a judgement on a field's shape, not on the recall. Add "
        "its phrase to REPAIRABLE_DEFECTS in pipeline/merge_master.py.")


# ── 3. THE LIST MUST NOT SWALLOW A SCOPE VERDICT ──────────────────────────

@pytest.mark.parametrize("verdict", [
    "out of scope: allergen-only recall",
    "Review agent: REJECTED — not a food product",
    "out of scope (R1 — food only). NAFDAC public alert 030/2026 withdraws "
    "LEVAMISOLE-CONTAINING MEDICINES",
    "URL agent: The hazard is not a microbial pathogen, but possible "
    "contamination by cleaning products.",
    "no-authority-url: no official Salute press-release URL in the source "
    "article (news-only discovery)",
    "duplicate: this URL is already published on 2026-10-07",
    "fabricated_pathogen_and_out_of_scope",
])
def test_a_scope_verdict_is_not_a_repairable_field_defect(verdict):
    assert not _matches(verdict), (
        f"REPAIRABLE_DEFECTS matches a verdict about the RECALL, not about a "
        f"field:\n\n    {verdict}\n\nWidening the tuple must never let an "
        f"out-of-scope, duplicate or unverifiable row walk back into the "
        f"register. Narrow the phrase that matched it.")
