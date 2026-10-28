"""A hand-added row that does not land must say so. 2026-10-28.

WHAT HAPPENED
-------------
``tools/add_manual_row.py`` reported one number — the net growth of the
Pending sheet — and returned 0 whenever that number was positive. A net
count cannot name a row, so a batch in which one row was dropped looked
exactly like a batch in which all of them landed.

On 2026-10-27 the morning-fix pass queued three operator-verified rows in
one call. Two landed. The third — AESAN alert 2026_75, Salmonella spp. in
Arkan Pure Sesame Tahini, Tier 1 — was blocked by the terminal-rejection
guard, because the same script had just archived six language copies of
that alert stamped "operator review 2026-10-27", and
``_is_human_rejection`` reads that stamp as a permanent verdict. The guard
logs at INFO; the tool does not configure logging. The run printed
"Queued 2 row(s)", exited 0, and the recall was in no sheet the next day.

WHAT THESE TESTS PIN
--------------------
1. ``append_to_pending`` can be asked for a verdict per input row, and
   gives one for EVERY row, including the ones it drops.
2. ``override_terminal`` is the only way past an archived terminal
   verdict, and it is off by default.
3. The tool exits non-zero when any requested row did not land, and names
   it on stdout.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import merge_master as mm                      # noqa: E402
from scrapers._models import Recall                          # noqa: E402

TOOL = ROOT / "tools" / "add_manual_row.py"


def _recall(url, product="Smoked sprats, 200 g tin, lot 9921",
            company="Test Foods Ltd", pathogen="Listeria monocytogenes"):
    return Recall(
        Date="2026-10-27", Source="AESAN (ES)", Company=company,
        Brand="TestBrand", Product=product, Pathogen=pathogen,
        Reason="Presence of %s" % pathogen, Class="Recall",
        Country="Spain", Region="Europe", Tier=1, Outbreak=0,
        URL=url, Notes="unit test row",
    )


# ---------------------------------------------------------------------------
# 1. every input row gets a verdict
# ---------------------------------------------------------------------------

def test_outcomes_has_one_record_per_input_row():
    rows = [_recall("https://www.aesan.gob.es/alertas/2026_900",
                    product="Product A", company="Alpha SL"),
            _recall("https://www.aesan.gob.es/alertas/2026_901",
                    product="Product B", company="Beta SL")]
    outcomes: list = []
    out = mm.append_to_pending([], [], rows, "2026-10-28 06:00:00",
                               outcomes=outcomes)
    assert len(outcomes) == len(rows), outcomes
    assert all(o["outcome"] for o in outcomes)
    assert all(o["url"] for o in outcomes)
    # whatever the verdicts, the sheet and the report must agree
    landed = sum(1 for o in outcomes
                 if o["outcome"] in ("queued", "queued-enrichment", "requeued"))
    assert landed == len(out)


def test_a_row_already_in_recalls_is_reported_not_silently_skipped():
    """Whichever gate catches it, the drop is named — that is the contract."""
    row = _recall("https://www.aesan.gob.es/alertas/2026_902",
                  company="Gamma SL")
    approved = [row.to_dict()]
    outcomes: list = []
    out = mm.append_to_pending([], approved, [row], "2026-10-28 06:00:00",
                               outcomes=outcomes)
    assert out == []
    assert len(outcomes) == 1
    assert outcomes[0]["outcome"] in ("already-approved", "gate-rejected"), \
        outcomes
    assert outcomes[0]["detail"], "a drop with no reason is still silent"


# ---------------------------------------------------------------------------
# 2. the terminal-rejection guard: reported, and overridable on purpose
# ---------------------------------------------------------------------------

def test_a_terminal_rejection_drop_is_reported(monkeypatch):
    url = "https://www.aesan.gob.es/alertas/2026_903"
    key = mm._normalize_url_for_dedup(url)
    monkeypatch.setattr(
        mm, "load_rejected_urls",
        lambda *a, **k: {key: "operator review 2026-10-27: REJECTED — "
                              "duplicate page-copy junk"})
    outcomes: list = []
    out = mm.append_to_pending([], [], [_recall(url, company="Delta SL")],
                               "2026-10-28 06:00:00", outcomes=outcomes)
    assert out == []
    assert [o["outcome"] for o in outcomes] == ["terminal-rejection"]
    # the verdict it was blocked on has to be in the report, or the operator
    # cannot tell a defensible block from this bug.
    assert "operator review 2026-10-27" in outcomes[0]["detail"]


def test_override_terminal_lets_a_verified_row_through(monkeypatch):
    url = "https://www.aesan.gob.es/alertas/2026_904"
    key = mm._normalize_url_for_dedup(url)
    monkeypatch.setattr(
        mm, "load_rejected_urls",
        lambda *a, **k: {key: "operator review 2026-10-27: REJECTED — "
                              "duplicate page-copy junk"})
    outcomes: list = []
    out = mm.append_to_pending([], [], [_recall(url, company="Epsilon SL")],
                               "2026-10-28 06:00:00", outcomes=outcomes,
                               override_terminal={key})
    assert len(out) == 1, outcomes
    assert outcomes[0]["outcome"] in ("queued", "queued-enrichment")


def test_override_is_off_by_default(monkeypatch):
    """Passing nothing must behave exactly as before this change."""
    url = "https://www.aesan.gob.es/alertas/2026_905"
    key = mm._normalize_url_for_dedup(url)
    monkeypatch.setattr(
        mm, "load_rejected_urls",
        lambda *a, **k: {key: "operator review 2026-10-27: REJECTED — "
                              "duplicate page-copy junk"})
    assert mm.append_to_pending([], [], [_recall(url, company="Zeta SL")],
                                "2026-10-28 06:00:00") == []


# ---------------------------------------------------------------------------
# 3. the tool's exit status
# ---------------------------------------------------------------------------

#: A row whose URL carries a terminal rejection in the archives — the exact
#: shape that lost AESAN 2026_75. This is a LOST row, not a benign no-op.
_LOST_ROW = {
    "Date": "2026-10-27",
    "Source": "AESAN (ES)",
    "Company": "Omega SL",
    "Brand": "OmegaBrand",
    "Product": "Cooked mussels, vacuum tray 200 g, lot 77123",
    "Pathogen": "Listeria monocytogenes",
    "Reason": "Presence of Listeria monocytogenes",
    "Class": "Recall",
    "Country": "Spain",
    "URL": "https://www.aesan.gob.es/alertas/2026_906",
    "Notes": "unit test row",
}


def _workbook_with_a_near_duplicate(tmp_path, row):
    """A workbook holding the same (source, company, pathogen) at ANOTHER URL.

    The near-duplicate index is built from the approved+pending lists the
    caller passes, so this reproduces a real LOST row entirely inside
    tmp_path. (The terminal-rejection guard cannot be driven this way:
    load_rejected_urls() is called with no path and always reads the repo's
    own workbook, whatever --xlsx says. That is covered above, at the
    function level, with a monkeypatch.)
    """
    xlsx = tmp_path / "recalls.xlsx"
    twin = dict(row, URL=row["URL"] + "-other-address",
                Region="Europe", Tier=1, Outbreak=0)
    mm.save_xlsx_with_pending([twin], [], xlsx)
    return xlsx


@pytest.mark.parametrize("flag", [[], ["--dry-run"]])
def test_tool_exits_nonzero_when_a_requested_row_is_lost(tmp_path, flag):
    """The whole point: a row something refused cannot leave the exit at 0."""
    xlsx = _workbook_with_a_near_duplicate(tmp_path, _LOST_ROW)
    spec = tmp_path / "rows.json"
    spec.write_text(json.dumps([_LOST_ROW]), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, str(TOOL), str(spec), "--xlsx", str(xlsx)] + flag,
        capture_output=True, text=True, cwd=str(ROOT))

    assert proc.returncode != 0, (
        "a requested row did not land and the tool still exited 0:\n"
        + proc.stdout + proc.stderr)
    assert "DID NOT LAND" in proc.stdout, proc.stdout
    assert "LOST" in proc.stdout, proc.stdout
    assert "2026_906" in proc.stdout, proc.stdout


def test_a_row_the_register_already_holds_stays_a_success(tmp_path):
    """Re-running on an already-published row is a no-op, not a failure.

    tests/test_add_manual_row.py::test_running_twice_does_not_duplicate has
    required this since the tool was written, and the 2026-10-28 change must
    not turn an idempotent re-run into a red exit.
    """
    xlsx = tmp_path / "recalls.xlsx"
    approved = [dict(_LOST_ROW, Region="Europe", Tier=1, Outbreak=0)]
    mm.save_xlsx_with_pending(approved, [], xlsx)
    spec = tmp_path / "rows.json"
    spec.write_text(json.dumps([_LOST_ROW]), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(TOOL), str(spec), "--xlsx", str(xlsx)],
        capture_output=True, text=True, cwd=str(ROOT))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "SKIP" in proc.stdout, proc.stdout
    assert "DID NOT LAND" not in proc.stdout, proc.stdout


def test_tool_exits_zero_when_every_row_lands(tmp_path):
    xlsx = tmp_path / "recalls.xlsx"
    mm.save_xlsx_with_pending([], [], xlsx)
    rows = [{
        "Date": "2026-10-27",
        "Source": "AESAN (ES)",
        "Company": "Theta SL",
        "Brand": "ThetaBrand",
        "Product": "Cooked mussels, vacuum tray 200 g, lot 88456",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Presence of Listeria monocytogenes",
        "Class": "Recall",
        "Country": "Spain",
        "URL": "https://www.aesan.gob.es/alertas/2026_907",
        "Notes": "unit test row",
    }]
    spec = tmp_path / "rows.json"
    spec.write_text(json.dumps(rows), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(TOOL), str(spec), "--xlsx", str(xlsx)],
        capture_output=True, text=True, cwd=str(ROOT))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Queued 1 of 1" in proc.stdout, proc.stdout
