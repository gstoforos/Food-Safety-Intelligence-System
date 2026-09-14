# -*- coding: utf-8 -*-
"""tools/add_manual_row.py — the operator's hand-add path.

A hand-added row is the one row nobody can audit later by re-running a
scraper, so the tool has to be strict about two things: it must go into
Pending through the pipeline's own ``append_to_pending`` (same dedup,
same review), and it must never touch Recalls.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.add_manual_row import build_recall  # noqa: E402

TOOL = ROOT / "tools" / "add_manual_row.py"
ROW_JSON = ROOT / "rows" / "gis-pesto-2026-08-28.json"


# --------------------------------------------------------------------------
# the shipped row
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pesto():
    return json.loads(ROW_JSON.read_text(encoding="utf-8"))


def test_the_pesto_row_file_is_present_and_valid_json(pesto):
    assert pesto["Source"] == "GIS (PL)"
    assert pesto["Date"] == "2026-08-28"


def test_pesto_row_is_tier_1_botulism_and_outbreak_flagged(pesto):
    r = build_recall(pesto)
    assert r.Pathogen == "Clostridium botulinum"
    assert int(r.Tier) == 1
    assert int(r.Outbreak) == 1
    assert r.Country == "Poland"
    assert r.Region == "Europe"


def test_pesto_row_points_at_the_regulator_not_the_news_story(pesto):
    """The news article is corroboration. The source of record is GIS."""
    assert pesto["URL"].startswith("https://www.gov.pl/web/gis/")
    assert "foodsafetynews" not in pesto["URL"]


def test_pesto_row_carries_the_batch_identifiers(pesto):
    """A botulism warning without the lot is not actionable in a store room."""
    prod = pesto["Product"]
    assert "L 605192FE I" in prod
    assert "180 g" in prod
    assert "08.2027" in prod


def test_pesto_notes_record_who_verified_what_and_what_is_unverified(pesto):
    notes = pesto["Notes"]
    assert "gov.pl" in notes or "GIS warning page" in notes
    # The Netherlands distribution comes from Food Safety News quoting
    # RASFF; the GIS page does not mention export and the RASFF
    # notification was not located. It must be marked, not asserted.
    assert "UNVERIFIED" in notes
    assert "Netherlands" in notes


def test_pesto_row_clears_the_publish_gate(pesto):
    from pipeline import _publish_gate as pg
    r = build_recall(pesto)
    row = {f: getattr(r, f) for f in
           ("Date", "Source", "Company", "Brand", "Product", "Pathogen",
            "Reason", "Class", "Country", "Region", "Tier", "Outbreak",
            "URL", "Notes")}
    assert pg.publish_blockers(row) == []


def test_pesto_pathogen_is_in_tier_1_scope(pesto):
    from pipeline import _pathogen_scope as ps
    r = build_recall(pesto)
    assert ps.is_in_scope(r.Pathogen)
    assert ps.is_always_tier1(r.Pathogen)


# --------------------------------------------------------------------------
# the tool's contract
# --------------------------------------------------------------------------

@pytest.mark.parametrize("field", ["Date", "Source", "Product", "URL"])
def test_missing_required_field_is_refused(pesto, field):
    spec = dict(pesto)
    spec.pop(field)
    with pytest.raises(SystemExit):
        build_recall(spec)


def test_a_bad_date_is_refused_rather_than_coerced(pesto):
    spec = dict(pesto, Date="28.08.2026")
    with pytest.raises(SystemExit):
        build_recall(spec)


def test_tier_is_computed_not_taken_from_the_json(pesto):
    """A hand-set Tier is exactly the kind of quiet error this avoids."""
    spec = dict(pesto, Tier=3)
    assert int(build_recall(spec).Tier) == 1


def test_pathogen_is_canonicalised(pesto):
    spec = dict(pesto, Pathogen="C. botulinum")
    assert build_recall(spec).Pathogen == "Clostridium botulinum"


def test_the_tool_never_writes_to_recalls():
    """Read the source: Recalls is loaded and passed straight back."""
    src = TOOL.read_text(encoding="utf-8")
    assert "append_to_pending" in src
    assert "promote_approved" not in src
    assert "save_xlsx(" not in src          # the Recalls-only writer


# --------------------------------------------------------------------------
# end to end, against a copy of the real workbook
# --------------------------------------------------------------------------

@pytest.fixture
def workbook_copy(tmp_path, pesto):
    """A copy of the real workbook, with the shipped row's own URL removed
    from Recalls if present.

    The fixture below exercises "does the tool queue THIS row into
    Pending" against the real workbook. Once the shipped row is itself
    live in production Recalls (which is the point of shipping it), that
    URL is permanently "already approved" and the tool correctly skips
    it — the mechanics test would fail forever, not because the tool is
    broken but because its precondition (row not yet present) no longer
    holds. Strip it back out of this throwaway copy so the test keeps
    testing what it was written to test.
    """
    src = ROOT / "docs" / "data" / "recalls.xlsx"
    if not src.exists():
        pytest.skip("recalls.xlsx not present")
    dst = tmp_path / "recalls.xlsx"
    dst.write_bytes(src.read_bytes())

    url = str(pesto["URL"]).strip().lower().rstrip("/")
    wb = load_workbook(dst)
    ws = wb["Recalls"]
    hdr = [str(c.value or "") for c in ws[1]]
    if "URL" in hdr:
        col = hdr.index("URL") + 1
        for row in range(ws.max_row, 1, -1):
            v = str(ws.cell(row=row, column=col).value or "").strip().lower().rstrip("/")
            if v == url:
                ws.delete_rows(row)
    wb.save(dst)
    return dst


def _counts(path):
    wb = load_workbook(path, read_only=True, data_only=True)
    n = {s: max(0, len(list(wb[s].iter_rows(values_only=True))) - 1)
         for s in ("Recalls", "Pending")}
    wb.close()
    return n


def _run(*argv):
    return subprocess.run([sys.executable, str(TOOL), *argv],
                          cwd=ROOT, capture_output=True, text=True)


def test_dry_run_writes_nothing(workbook_copy):
    before = workbook_copy.read_bytes()
    r = _run(str(ROW_JSON), "--xlsx", str(workbook_copy), "--dry-run")
    assert r.returncode == 0, r.stderr
    assert "DRY RUN" in r.stdout
    assert workbook_copy.read_bytes() == before


def test_queues_one_row_into_pending_and_leaves_recalls_alone(workbook_copy):
    before = _counts(workbook_copy)
    r = _run(str(ROW_JSON), "--xlsx", str(workbook_copy))
    assert r.returncode == 0, r.stderr
    after = _counts(workbook_copy)
    assert after["Pending"] == before["Pending"] + 1
    assert after["Recalls"] == before["Recalls"]


def test_the_queued_row_is_status_pending(workbook_copy):
    _run(str(ROW_JSON), "--xlsx", str(workbook_copy))
    wb = load_workbook(workbook_copy, read_only=True, data_only=True)
    rows = list(wb["Pending"].iter_rows(values_only=True))
    hdr = [str(c or "") for c in rows[0]]
    hits = [dict(zip(hdr, x)) for x in rows[1:]
            if "pesto" in str(dict(zip(hdr, x)).get("Product", "")).lower()]
    wb.close()
    assert len(hits) == 1
    assert str(hits[0]["Status"]).lower() == "pending"
    assert int(hits[0]["Tier"]) == 1


def test_running_twice_does_not_duplicate(workbook_copy):
    """append_to_pending's own rule: already pending -> skip."""
    _run(str(ROW_JSON), "--xlsx", str(workbook_copy))
    mid = _counts(workbook_copy)
    r = _run(str(ROW_JSON), "--xlsx", str(workbook_copy))
    assert r.returncode == 0, r.stderr
    assert _counts(workbook_copy)["Pending"] == mid["Pending"]
    assert "Nothing queued" in r.stdout
