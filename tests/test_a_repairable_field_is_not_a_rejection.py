"""
AUDIT 2026-10-23 — REVIEWER 3 THREW AWAY A RECALL OVER A FIELD ITS OWN
REASON FILLED
======================================================================

Found by the morning fix pass in Weekly_Rejected on main at 44c81b0:

    Date    : 2026-10-22
    Source  : FDA
    Company : Vitamin Cottage Natural Food Markets, Inc.
    Brand   : Natural Grocers
    Product : "2 pound Mung Beans"
    Reason  : "Possible foreign material contamination with glass"
    URL     : fda.gov/safety/recalls-market-withdrawals-safety-alerts/
              natural-grocers-recalls-natural-grocers-brand-organic-
              2-pound-mung-beans-due-potential-presence
    RejectionReason: "Confirmer: reviewer 2 approved but Pathogen is empty"

Everything about that row was right. Foreign material is in scope. The URL
is FDA's own per-recall notice. Reviewer 1 confirmed the URL, reviewer 2
approved the row, the review agent verified it — all three stamps are in
its Notes. The date is inside the window. The register already holds EIGHT
rows whose Pathogen is "Foreign material (glass)", and the one dated
2026-08-18 carries the identical Reason string.

The single empty cell was the hazard name, and the hazard is written out in
the row's own Reason.

pipeline/enrich_pending_offline.py already derives it, deterministically,
offline, with no model:

    >>> infer_pathogen({"Reason": "Possible foreign material contamination
    ...                  with glass"})
    ('Foreign material (glass)', "matched 'glass'")

That module only sees rows still in Pending. Reviewer 3 archives first, so
the enricher never got a look — a recall was lost in the gap between two
pieces of the pipeline that each knew better.

The repair runs the same deterministic table at the last gate, before the
verdict. It is the same character as the englishify_reason call that
already sits in this agent: a table of derivations that never invents,
applied instead of letting the row go.

What these tests hold:
  * a row whose hazard is in its own text is repaired and publishes;
  * a row with NO hazard term anywhere in its own text is NOT repaired and
    is still blocked — the fix must not become a hazard generator;
  * the repair touches nothing but Pathogen, and records what it did;
  * both workflow heredocs carry it (operator rule: byte-identical).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.recall_confirm_agent import (                  # noqa: E402
    _repair_from_own_text, confirm)
from pipeline.enrich_pending_offline import infer_pathogen    # noqa: E402


def _mung_bean_row(pathogen: str = "") -> dict:
    """The row as it stood when reviewer 3 archived it."""
    return {
        "Date": "2026-10-22",
        "Source": "FDA",
        "Company": "Vitamin Cottage Natural Food Markets, Inc.",
        "Brand": "Natural Grocers",
        "Product": "2 pound Mung Beans",
        "Pathogen": pathogen,
        "Reason": "Possible foreign material contamination with glass",
        "Class": "Recall",
        "Country": "United States",
        "Region": "North America",
        "Tier": 3,
        "Outbreak": 0,
        "URL": "https://www.fda.gov/safety/recalls-market-withdrawals-"
               "safety-alerts/natural-grocers-recalls-natural-grocers-"
               "brand-organic-2-pound-mung-beans-due-potential-presence",
        "Notes": "FDA listing HTML (Layer 1)",
        "Status": "pending_gap_v3",
    }


# ──────────────────────────────────────────────────────────────────────
# The row that was lost
# ──────────────────────────────────────────────────────────────────────

def test_the_enricher_can_read_the_hazard_off_this_row():
    """The derivation this agent was not using."""
    value, why = infer_pathogen(_mung_bean_row())
    assert value == "Foreign material (glass)"
    assert "glass" in why


def test_the_row_is_repaired_instead_of_rejected():
    rows = [_mung_bean_row()]
    out = _repair_from_own_text(rows)
    assert rows[0]["Pathogen"] == "Foreign material (glass)"
    assert rows[0]["URL"] in out


def test_the_repaired_row_is_no_longer_blocked_on_its_pathogen():
    rows = [_mung_bean_row()]
    before = confirm(_mung_bean_row(), [])
    assert any("Pathogen is empty" in p for p in before), (
        "the un-repaired row must still be blocked — otherwise this test "
        "is not measuring the fix")
    _repair_from_own_text(rows)
    after = confirm(rows[0], [])
    assert not any("Pathogen is empty" in p for p in after), (
        f"still blocked on Pathogen after repair: {after}")


# ──────────────────────────────────────────────────────────────────────
# It must not become a hazard generator
# ──────────────────────────────────────────────────────────────────────

def test_a_row_with_no_hazard_in_its_own_text_is_left_alone():
    """Rule 5: never invent a field value.

    These are real rows from Pending on main at 44c81b0 that
    enrich_pending_offline reports as "no hazard term in the row's own
    text". They must stay empty and stay blocked, not acquire a guess.
    """
    rows = [
        {"Source": "RappelConso (FR)", "Pathogen": "",
         "Product": "pawell friandise boeuf chat 1.5mg de cbd",
         "Reason": "", "Notes": "", "URL": "https://rappel.conso.gouv.fr/"
                                           "fiche-rappel/23800/interne"},
        {"Source": "RappelConso (FR)", "Pathogen": "",
         "Product": "lait de croissance nature- 6x1l",
         "Reason": "", "Notes": "", "URL": "https://rappel.conso.gouv.fr/"
                                           "fiche-rappel/23774/interne"},
        {"Source": "FDA", "Pathogen": "",
         "Product": "Par Health Issues Voluntary Nationwide Recall",
         "Reason": "", "Notes": "", "URL": "https://www.fda.gov/safety/"
                                           "recalls-market-withdrawals-"
                                           "safety-alerts/par-health"},
    ]
    out = _repair_from_own_text(rows)
    assert out == {}, f"invented a hazard for a row that names none: {out}"
    for r in rows:
        assert not str(r.get("Pathogen") or "").strip()
        assert "self-repair" not in str(r.get("Notes") or "")


def test_a_row_that_already_names_its_hazard_is_untouched():
    row = _mung_bean_row(pathogen="Listeria monocytogenes")
    _repair_from_own_text([row])
    assert row["Pathogen"] == "Listeria monocytogenes", (
        "the repair fills an EMPTY cell; it must never overwrite a hazard "
        "a reviewer or a scraper already read off the notice")
    assert "self-repair" not in row["Notes"]


def test_only_the_pathogen_cell_changes():
    before = _mung_bean_row()
    after = _mung_bean_row()
    _repair_from_own_text([after])
    changed = {k for k in before if before[k] != after[k]}
    assert changed == {"Pathogen", "Notes"}, (
        f"the repair changed {changed - {'Pathogen', 'Notes'}} as well — it "
        f"must touch the hazard cell and the audit note, nothing else")


def test_the_repair_records_what_it_did_and_why():
    row = _mung_bean_row()
    _repair_from_own_text([row])
    notes = row["Notes"]
    assert "FDA listing HTML (Layer 1)" in notes, "prior Notes must survive"
    assert "self-repair" in notes
    assert "Foreign material (glass)" in notes
    assert "glass" in notes, "the matched term belongs in the audit trail"


def test_the_repair_is_idempotent():
    """It runs twice per invocation — on the review lane and on the list
    that is saved. The second pass must be a no-op."""
    row = _mung_bean_row()
    _repair_from_own_text([row])
    first = dict(row)
    _repair_from_own_text([row])
    assert row == first, "a second pass changed the row"
    assert row["Notes"].count("self-repair") == 1


def test_a_sentinel_pathogen_counts_as_empty():
    for sentinel in ("—", "-", "nan", "None", "?", "   "):
        row = _mung_bean_row(pathogen=sentinel)
        _repair_from_own_text([row])
        assert row["Pathogen"] == "Foreign material (glass)", (
            f"{sentinel!r} is a placeholder, not a hazard — the publish "
            f"gate treats it as empty and so must the repair")


# ──────────────────────────────────────────────────────────────────────
# It has to actually run in production
# ──────────────────────────────────────────────────────────────────────

def test_the_agent_repairs_before_it_reviews():
    src = (ROOT / "pipeline" / "recall_confirm_agent.py").read_text("utf-8")
    assert src.index("_repair_from_own_text(pending)") < src.index(
        "confirmed, blocked, waiting = [], [], []"), (
        "the repair must run before the decision loop, or the row is "
        "archived before anything can fix it")
    assert "_repair_from_own_text(full_pending)" in src, (
        "full_pending is a second read of the sheet and is the list that "
        "gets saved — the fill must be in it too or it is not persisted")


def test_both_workflow_heredocs_carry_the_repair():
    """Reviewer 3 runs from a heredoc copy inside each of its workflows.

    A fix that lands only in the module is a fix two nightly runs do not
    have. Full byte-identity is held by
    tests/test_no_country_is_invisible_to_the_reviewers.py; this names the
    specific fix so a drift report says what went missing.
    """
    missing = []
    for wf in ("recall-confirm-agent.yml", "recallconfirmagent.yml"):
        text = (ROOT / ".github" / "workflows" / wf).read_text("utf-8")
        if "_repair_from_own_text" not in text:
            missing.append(wf)
    assert not missing, (
        "rebuild the heredoc pair after editing "
        "pipeline/recall_confirm_agent.py — missing in: "
        + ", ".join(missing))
