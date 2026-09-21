# -*- coding: utf-8 -*-
"""The Monday statistics reviewer must catch the bug it was built for.

A reviewer that agrees with everything is not a reviewer. The tests that
matter here are the INJECTION tests: a board is deliberately corrupted in
the exact way the 2026-09-21 audit found, and the reviewer must say so.

WHAT IT REVIEWS (and why each check exists)
===========================================
Each of these is something a hand audit found on 2026-09-14/20 that the
pipeline had no way to notice, because none of them is wrong code:

  fdr_recompute     BH was corrected over len(candidates) = 3 instead of
                    strata_tested = 15. The recompute is written out a
                    SECOND time inside the reviewer on purpose — an audit
                    that imports the function it audits cannot find a bug
                    in that function.
  overdispersion    STEC · France: baseline [0,0,9,1,1,1,0], variance ~7x
                    what Binomial(57, 0.0269) assumes. Regulators publish
                    in batches; the exact p is optimistic, in the
                    direction that makes alarms.
  fragile_baseline  75% of that baseline is one week.
  trend             The same series reads 0 → 2 → 3 → 6. The strongest
                    evidence on the board was never tested, because the
                    ramp weeks sit inside the guard band.
  re_alarm          Salmonella · France alarmed at 15 on 2026-08-31 and
                    again at 10 on 2026-09-14. The 15 is inside the guard
                    band of the second test, so the smaller week clears a
                    lower bar BECAUSE the bigger one came first.
  publisher         Cuts both ways, and that is the point. Aflatoxin is
                    100% RASFF and RASFF's share rose — confounded. Both
                    French signals are RappelConso-dominated and
                    RappelConso's share FELL — which makes them stronger,
                    and nothing on the board was saying so.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import signal_stats_reviewer as R  # noqa: E402

BOARD_PATH = ROOT / "docs" / "data" / "signals-board.json"


@pytest.fixture(scope="module")
def board():
    if not BOARD_PATH.exists():
        pytest.skip("no signals-board.json")
    return R._load(BOARD_PATH)


@pytest.fixture(scope="module")
def result(board):
    return R.review(board)


# --------------------------------------------------------------------------
# it runs at all
# --------------------------------------------------------------------------

def test_it_reviews_the_live_board(result):
    assert result["meta"]["week"]
    assert result["signals"], "no signals reviewed"
    assert result["verdict"]


def test_the_digest_renders(result):
    md = R.to_markdown(result)
    assert md.startswith("# Signal statistics review")
    assert "Benjamini-Hochberg, recomputed independently" in md
    assert "advisory" in md.lower()


def test_it_is_json_serialisable(result):
    json.dumps(result, default=str)


def test_it_never_claims_to_suppress_anything(result):
    """Advisory means advisory. No key may look like a gate."""
    flat = json.dumps(result).lower()
    for word in ('"suppress"', '"retract"', '"unpublish"'):
        assert word not in flat


# --------------------------------------------------------------------------
# INJECTION — the reviewer must fail a board it should fail
# --------------------------------------------------------------------------

def test_it_catches_the_2026_09_21_denominator_defect(board):
    """The whole reason this agent exists.

    Rebuild the defective board: m = len(candidates), so everything passes.
    """
    b = copy.deepcopy(board)
    b["meta"]["strata_tested"] = 15
    b["meta"]["fdr_m"] = 3                      # what the detector used
    for s in b["signals"]:
        s["fdr_pass"] = True                    # what it published
    r = R.review(b)

    assert any(d["severity"] == "defect" for d in r["defects"]), (
        "a board whose FDR denominator is the survivor count must be "
        "flagged — this is the exact defect of 2026-09-21")
    assert r["verdict"] == "DEFECTS FOUND"
    joined = " ".join(d["detail"] for d in r["defects"])
    assert "3" in joined and "15" in joined, (
        "the finding must print both numbers so a reader can check it")


def test_it_catches_a_published_verdict_that_does_not_recompute(board):
    b = copy.deepcopy(board)
    b["meta"]["strata_tested"] = 500            # enormous family
    b["meta"]["fdr_m"] = 500
    for s in b["signals"]:
        s["fdr_pass"] = True                    # nothing could pass at m=500
    r = R.review(b)
    assert r["fdr"]["disagreements"], (
        "every share-channel row was published as FDR-pass against a family "
        "of 500; the recompute must disagree")
    assert r["verdict"] == "DEFECTS FOUND"


def test_a_board_that_agrees_is_not_flagged(board):
    """The other half. A reviewer that always alarms is noise."""
    b = copy.deepcopy(board)
    m = int(b["meta"]["strata_tested"])
    b["meta"]["fdr_m"] = m
    share = [s for s in b["signals"] if s.get("channel") == "proportion"]
    keep = R._bh_reject([s["p_value"] for s in share], b["meta"]["fdr_q"], m)
    for s, k in zip(share, keep):
        s["fdr_pass"] = k
    r = R.review(b)
    assert not r["fdr"]["disagreements"]
    assert not [d for d in r["defects"] if d["severity"] == "defect"]
    assert r["verdict"] != "DEFECTS FOUND"


def test_a_missing_denominator_is_a_note_not_a_defect(board):
    """Older boards predate `fdr_m`. Say so; do not cry defect."""
    b = copy.deepcopy(board)
    b["meta"].pop("fdr_m", None)
    m = int(b["meta"]["strata_tested"])
    share = [s for s in b["signals"] if s.get("channel") == "proportion"]
    keep = R._bh_reject([s["p_value"] for s in share], b["meta"]["fdr_q"], m)
    for s, k in zip(share, keep):
        s["fdr_pass"] = k
    r = R.review(b)
    sev = {d["severity"] for d in r["defects"]}
    assert "note" in sev and "defect" not in sev


# --------------------------------------------------------------------------
# the independent implementation must really be independent
# --------------------------------------------------------------------------

def test_the_reviewer_does_not_import_the_function_it_audits():
    src = (ROOT / "pipeline" / "signal_stats_reviewer.py").read_text(
        encoding="utf-8")
    assert "from pipeline.signal_detector import" not in src, (
        "importing benjamini_hochberg would make the audit incapable of "
        "finding a bug in benjamini_hochberg")
    assert "def _bh_reject(" in src


def test_the_two_implementations_agree_on_a_correct_input():
    """Independent, but not wrong. They must match when nothing is broken."""
    from pipeline.signal_detector import benjamini_hochberg as bh
    for m in (3, 5, 15, 40):
        for p in ([0.001, 0.02, 0.3],
                  [0.5, 0.6],
                  [0.0001],
                  [0.01, 0.01, 0.01, 0.01]):
            assert R._bh_reject(p, 0.1, m) == bh(p, 0.1, m=m), (p, m)


# --------------------------------------------------------------------------
# the individual checks, on constructed rows
# --------------------------------------------------------------------------

def test_overdispersion_fires_on_the_stec_baseline():
    row = {"baseline_values": [0, 0, 9, 1, 1, 1, 0], "share_baseline": 0.0269}
    c = R.check_overdispersion(row, 57)
    assert c and c["ratio"] >= R.OVERDISPERSION_WARN
    assert "anti-conservative" in c["reading"]


def test_overdispersion_is_quiet_on_a_well_behaved_baseline():
    row = {"baseline_values": [2, 3, 2, 2, 3, 2, 2], "share_baseline": 0.04}
    assert R.check_overdispersion(row, 57) is None


def test_fragile_baseline_fires_when_one_week_dominates():
    c = R.check_fragile_baseline({"baseline_values": [0, 0, 9, 1, 1, 1, 0]})
    assert c and c["share_of_baseline_mass"] == 0.75


def test_fragile_baseline_is_quiet_when_the_mass_is_spread():
    assert R.check_fragile_baseline(
        {"baseline_values": [2, 8, 4, 7, 4, 3, 4]}) is None


def test_trend_finds_the_ramp_the_guard_band_hides():
    c = R.check_trend({"series": [0, 0, 0, 0, 9, 1, 1, 1, 0, 2, 3, 6]}, 2)
    assert c and c["run_length_weeks"] == 4
    assert c["tail"] == [0, 2, 3, 6]
    assert c["weeks_inside_guard_band"] == [2, 3]


def test_trend_is_quiet_on_a_flat_series():
    assert R.check_trend({"series": [4, 3, 5, 4, 3, 5, 4, 3, 5, 4, 3, 5]}, 2) is None


def test_publisher_credits_a_signal_that_rose_while_its_publisher_shrank():
    row = {"dominant_source": "RappelConso (FR)", "dominant_share": 1.0}
    pubs = [{"source": "RappelConso (FR)", "now": 23, "now_share": 0.4035,
             "base_mean": 30.71, "base_share": 0.4821}]
    c = R.check_publisher(row, pubs)
    assert c["verdict"] == "not-a-publisher-artefact"
    assert c["severity"] == "supporting"


def test_publisher_flags_a_signal_that_rose_with_its_publisher():
    row = {"dominant_source": "RASFF (EU)", "dominant_share": 1.0}
    pubs = [{"source": "RASFF (EU)", "now": 28, "now_share": 0.4912,
             "base_mean": 22.0, "base_share": 0.3565}]
    c = R.check_publisher(row, pubs)
    assert c["verdict"] == "confounded"
    assert c["severity"] == "caution"


def test_re_alarm_notices_a_larger_week_inside_the_guard_band():
    ledger = [
        {"week": "w1", "alarms": []},
        {"week": "w2", "alarms": [{"label": "Salmonella · France",
                                   "observed": 15}]},
        {"week": "w3", "alarms": []},
        {"week": "now", "alarms": []},
    ]
    c = R.check_re_alarm({"label": "Salmonella · France", "observed": 10},
                         ledger, "now", guard_weeks=2)
    assert c and c["severity"] == "caution"
    assert "lower because of the earlier spike" in c["reading"]


def test_re_alarm_is_quiet_for_a_first_time_stratum():
    ledger = [{"week": "w1", "alarms": [{"label": "Other", "observed": 9}]}]
    assert R.check_re_alarm({"label": "New", "observed": 4},
                            ledger, "now", 2) is None


# --------------------------------------------------------------------------
# every finding must carry its arithmetic
# --------------------------------------------------------------------------

def test_every_check_shows_its_working(result):
    """A reader must be able to redo any finding without running this."""
    for s in result["signals"]:
        for c in s["checks"]:
            assert c.get("reading"), f"{s['label']}/{c['check']} has no reading"
            numeric = [k for k, v in c.items()
                       if isinstance(v, (int, float, list)) and k != "severity"]
            assert numeric, (
                f"{s['label']}/{c['check']} states a conclusion with no "
                f"numbers behind it")


def test_the_fdr_steps_print_the_formula(result):
    for s in result["fdr"]["steps"]:
        assert "*" in s["formula"] and "/" in s["formula"]
        assert s["threshold"] >= 0
