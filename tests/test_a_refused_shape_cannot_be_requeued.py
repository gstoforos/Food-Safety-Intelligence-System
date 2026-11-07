# -*- coding: utf-8 -*-
"""A shape the publish gate refuses never reaches Pending either.

THE LOOP (2026-11-07)
=====================
On 2026-11-06 a cordless electric shaver — Camry, made by Adler Sp. z o.o.,
recalled by Hungary's NKFH because the batteries inside it hold too much
mercury to throw away — was found PUBLISHED in Recalls, promoted by
``pipeline.promote_gate_passing`` with the report line "passes every
publish-gate rule". It is not a food, and the harm the notice names is to
soil and groundwater on disposal. That morning's pass removed it, archived
it to Rejected with a terminal verdict, and added
``_publish_gate._non_food_appliance_blockers`` so the publish doorway would
refuse the shape.

On the morning of 2026-11-07 Recalls was clean and the row was BACK IN
PENDING, Status 'pending', scraped that night, with no re-promotion stamp on
it — and mirrored into Weekly_Review, the slice the operator's Sunday review
email reads. Two tests red again, on two sheets.

This is the same lesson ``refuse_stale_alert_numbers`` records one rule
earlier: a gate in the publish doorway cannot stop a collector that does not
use that doorway. There are two intake doorways and both now carry the rule:

  * ``merge_master.validate_pending_row`` — every scraper and manual inject;
  * ``gap_finder.main.refuse_non_food_appliances`` — every gap finder, which
    reaches Pending through ``write_pending_rows`` and consults nothing in
    merge_master.

MEASURED on the 2026-11-07 register before wiring it in: 0 of 2306 published
Recalls rows, 1 of 22 Pending rows (the shaver), 1 of 31 Weekly_Review rows
(the same one). It costs no row that can be published, since the publish gate
refuses the shape one stage later anyway.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "pipeline"))

from pipeline import merge_master as MM                 # noqa: E402
from pipeline._publish_gate import (                    # noqa: E402
    _non_food_appliance_blockers,
)
from pipeline.gap_finder.main import (                  # noqa: E402
    refuse_non_food_appliances, refuse_stale_alert_numbers,
)

SHAVER = {
    "Date": "2026-11-03",
    "Source": "NKFH (HU)",
    "Company": "Adler Sp. z o. o. (Lengyelország)",
    "Brand": "Camry",
    "Product": "Electric shaver",
    "Pathogen": "Mercury (heavy metal)",
    "Reason": "Serious environmental hazard! The internal power sources of "
              "the electric shavers contain an excessive amount of mercury. "
              "Mercury is difficult to dissolve and can permanently damage "
              "the soil and groundwater when the discarded products reach "
              "the ground.",
    "Class": "Recall",
    "Country": "Hungary",
    "Region": "Europe",
    "Tier": "2",
    "URL": "https://nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-"
           "elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-"
           "kockazat-miatt-1",
    "Notes": "Discovered via news: blikk.hu [locality: Hungary]",
}

#: Mercury IN a food is in scope, and must stay in scope. The 2026-11-06
#: note on _non_food_appliance_blockers makes the point: the hazard word was
#: right, which is why the empty-Pathogen test was vacuous here.
MERCURY_IN_FISH = {
    "Date": "2026-11-03",
    "Source": "AESAN (ES)",
    "Company": "Pescados Example S.L.",
    "Brand": "Example",
    "Product": "Frozen swordfish loins, 400 g",
    "Pathogen": "Mercury (heavy metal)",
    "Reason": "Mercury above the legal limit in swordfish",
    "Class": "Recall",
    "Country": "Spain",
    "Region": "Europe",
    "Tier": "2",
    "URL": "https://www.aesan.gob.es/en/alertas/2026_99",
    "Notes": "",
}


# ---------------------------------------------------------------------------
# doorway 1: every scraper and manual inject
# ---------------------------------------------------------------------------

def test_merge_master_refuses_the_appliance_at_intake():
    ok, why = MM.validate_pending_row(SHAVER, set())
    assert ok is False, "the shaver can still be queued into Pending"
    assert "non_food_appliance" in why, why


def test_merge_master_imports_the_rule_rather_than_copying_it():
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "from pipeline._publish_gate import _non_food_appliance_blockers" \
        in src, ("a second copy of the marker list is how two gates drift "
                 "apart — import it, as the two rules above it are")


def test_merge_master_still_admits_mercury_in_a_food():
    ok, why = MM.validate_pending_row(MERCURY_IN_FISH, set())
    assert ok is True, f"a real mercury-in-food recall was refused: {why}"


# ---------------------------------------------------------------------------
# doorway 2: every gap finder
# ---------------------------------------------------------------------------

def test_the_gap_finder_refuses_the_appliance():
    keep, refused = refuse_non_food_appliances([SHAVER])
    assert keep == []
    assert len(refused) == 1
    assert "non_food_appliance" in refused[0]["RejectionReason"]


def test_the_gap_finder_still_passes_mercury_in_a_food():
    keep, refused = refuse_non_food_appliances([MERCURY_IN_FISH])
    assert refused == []
    assert len(keep) == 1


def test_the_refusal_is_archived_not_dropped():
    """A dropped row is re-discovered tomorrow; an archived one is seen."""
    src = (ROOT / "pipeline" / "gap_finder" / "main.py").read_text(
        encoding="utf-8")
    body = src[src.index("def write_pending_rows("):]
    body = body[:body.index("\ndef ", 1)]
    assert "refuse_non_food_appliances(" in body, (
        "write_pending_rows is the only way a gap finder reaches Pending — "
        "the rule must fire there or it fires nowhere")
    assert body.count("write_rejected_rows(") >= 2, (
        "the appliance refusal must archive its rows, exactly as the "
        "stale-alert-number refusal above it does")


def test_the_two_gap_finder_refusals_do_not_interfere():
    """Both run on the same batch, in order, and keep the clean row."""
    clean = dict(MERCURY_IN_FISH)
    rows, stale = refuse_stale_alert_numbers([clean, SHAVER])
    rows, appliances = refuse_non_food_appliances(rows)
    assert [r["URL"] for r in rows] == [MERCURY_IN_FISH["URL"]]
    assert len(appliances) == 1
    assert stale == []


# ---------------------------------------------------------------------------
# and the publish doorway is still the backstop
# ---------------------------------------------------------------------------

def test_the_publish_gate_still_refuses_it_too():
    assert _non_food_appliance_blockers(SHAVER), (
        "the intake rules are an addition to the publish gate, never a "
        "replacement for it")


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
