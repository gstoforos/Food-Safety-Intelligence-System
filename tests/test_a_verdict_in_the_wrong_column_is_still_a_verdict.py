"""The re-promotion guard must see the verdict wherever a writer put it.

2026-10-05. A verified Tier-2 recall was barred from the register for ever
because its archive row's verdict was written in the wrong column.

    Rejected idx904 — Mattilsynet (NO), Herbapol Pokrazywa nettle tea,
    pyrrolizidine alkaloids above the limit

        Reason        "No matching hazard category — defer to manual
                       review."
        RejectReason  (empty)
        RejectedBy    gap_finder/no/rules.py
        Notes         "Discovered via news: mattilsynet.no"

"No matching hazard category" is the FIRST entry in
``merge_master.REPAIRABLE_DEFECTS`` — it means the classifier could not
NAME a hazard, which is a missing field, not a judgement on the recall. A
row archived that way is supposed to come back the moment it passes the
full publish gate.

It could not. ``load_rejected_urls`` builds the description the guard
tests from ``RejectReason``/``RejectionReason``/``RejectedReason`` plus
``Notes``. The gap-finder fleet writes its refusal into ``Reason`` — the
HAZARD field — so the guard saw

    "gap_finder/no/rules.py:  | Discovered via news: mattilsynet.no"

found no repairable defect named in it, and blocked the row. Demonstrated
live: on 2026-10-05 the row passed every publish-gate rule and
``promote_gate_passing --apply`` still printed "re-promotion BLOCKED".

MEASURED BREADTH on the 2026-10-05 workbook: 849 of 1,088 ``Rejected``
rows carry an empty ``RejectReason``, and 685 of those have no verdict in
``Notes`` either. Many of those hold their verdict in ``Reason``, written
there by gap_finder/{no,za,ng,es,pt,…}/rules.py.

THIS IS THE THIRD PLACE TODAY WITH ONE SHAPE — a guard reads a fixed set
of columns and the writers put the verdict somewhere else.
``load_rejected_urls`` learned it about ``Notes`` on 2026-09-01;
``supersede_archived_copies`` had the same blindness, found this morning;
``Reason`` is the third column.

The fix folds ``Reason`` in LAST, so a real ``RejectReason`` still reads
first and nothing about precedence changes. On a live row ``Reason`` holds
the hazard, which names no defect and no scope verdict, so folding it in
adds noise and matches nothing — every search the callers run is for a
defect or verdict phrase, never for a hazard.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

openpyxl = pytest.importorskip("openpyxl")

from pipeline.merge_master import load_rejected_urls  # noqa: E402

URL = ("https://www.mattilsynet.no/tilbakekallinger/herbapol-pokrazywa-te-i-"
       "pose-med-smak-av-brennesle-tilbakekalles-pa-grunn-av-innhold-av-"
       "giftige-plantestoffer")


def _archive(tmp_path, *, reason_col="", reject_col="", notes="", name="a.xlsx"):
    wb = openpyxl.Workbook()
    r = wb.active
    r.title = "Recalls"
    r.append(["Date", "Product", "URL"])
    a = wb.create_sheet("Rejected")
    a.append(["Date", "Product", "Reason", "URL", "Notes", "Status",
              "RejectedBy", "RejectReason"])
    a.append(["2026-09-21", "Pokrazywa", reason_col, URL, notes, "rejected",
              "gap_finder/no/rules.py", reject_col])
    p = tmp_path / name
    wb.save(p)
    return p


def test_a_verdict_written_into_the_reason_column_is_visible(tmp_path):
    p = _archive(tmp_path,
                 reason_col="No matching hazard category — defer to manual "
                            "review.",
                 notes="Discovered via news: mattilsynet.no")
    desc = load_rejected_urls(p)
    assert desc, "the archive row was not read at all"
    body = " ".join(desc.values()).lower()
    assert "no matching hazard category" in body, (
        "the gap-finder fleet writes its refusal into the Reason column. "
        "A guard that cannot see it bars a repaired row for ever — which "
        "is what happened to the Mattilsynet pyrrolizidine-alkaloid recall "
        "on 2026-10-05.")


def test_the_defect_the_guard_tests_for_is_reachable(tmp_path):
    """The description is only useful if REPAIRABLE_DEFECTS can match it."""
    from pipeline.merge_master import load_rejected_urls as L
    p = _archive(tmp_path,
                 reason_col="No matching hazard category — defer to manual "
                            "review.",
                 notes="Discovered via news: mattilsynet.no")
    desc = " ".join(L(p).values()).lower()
    # The same literal the guard searches for in merge_master.
    assert "no matching hazard category" in desc


def test_a_real_reject_column_still_reads_first(tmp_path):
    """Folding Reason in must not change precedence."""
    p = _archive(tmp_path,
                 reason_col="Listeria monocytogenes",
                 reject_col="pet_food_out_of_scope — HUMAN-food register.",
                 notes="")
    desc = " ".join(load_rejected_urls(p).values())
    assert desc.index("pet_food_out_of_scope") < desc.index(
        "Listeria monocytogenes"), (
        "RejectReason is the verdict column and must appear before the "
        "hazard text folded in behind it")


def test_a_hazard_in_reason_names_no_defect_and_no_verdict(tmp_path):
    """The noise this adds on a normal row must match nothing.

    If a plain hazard string could match a repairable-defect phrase or a
    scope verdict, folding Reason in would start re-promoting or barring
    rows on their hazard. It cannot: the phrases are about FIELDS and
    SCOPE, never about organisms.
    """
    import re
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    block = src[src.index("REPAIRABLE_DEFECTS = ("):]
    block = block[:block.index("\n            )")]
    _rep = re.findall(r'"([^"]+)"', "\n".join(
        l for l in block.splitlines() if not l.strip().startswith("#")))
    assert _rep, "could not read REPAIRABLE_DEFECTS"
    for hazard in ("Listeria monocytogenes", "Salmonella", "Aflatoxin",
                   "Pyrrolizidine alkaloids", "Foreign material (metal)",
                   "Shiga toxin-producing E. coli (STEC)",
                   "Mineral oil aromatic hydrocarbons (MOAH)",
                   "Uninspected product (hazard not assessed)"):
        hits = [d for d in _rep if d in hazard.lower()]
        assert not hits, (
            f"the hazard {hazard!r} matches the repairable-defect phrase(s) "
            f"{hits} — folding Reason into the guard's description would "
            f"let a row be re-promoted on its hazard rather than on a "
            f"repair")


def test_load_rejected_urls_reads_the_reason_column():
    """Pinned in source too, so a refactor cannot quietly drop it."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    i = src.index("def load_rejected_urls")
    body = src[i:src.index("\ndef ", i + 100)]
    assert '"Reason"' in body, (
        "load_rejected_urls no longer reads the archive row's Reason "
        "column. 849 of 1,088 Rejected rows carry an empty RejectReason "
        "and the gap-finder fleet puts the verdict in Reason.")
