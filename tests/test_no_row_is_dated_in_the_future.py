"""A publication date cannot be in the future.

INCIDENT 2026-10-24, found by the morning fix pass of 2026-10-25.

Recalls held this row, promoted the day before:

    Date         2026-11-01
    report_week  W44
    Source       BLV (CH)
    Company      Danforel A/S
    Product      Forellenfilets geräuchert - Nature
    Pathogen     Listeria monocytogenes
    Tier         1
    URL          recallswiss.admin.ch/customer-access/#Recalls/1168
    Notes        FSVO title: "DANFOREL A/S ruft das Produkt «Forellen-
                 filets geräuchert - Nature» mit dem Mindesthaltbarkeits-
                 datum 01.11.2026» wegen Listerien zurück"
    Notes        [promote 2026-10-24: ... passes every publish-gate rule]

01.11.2026 is the product's best-before date, printed inside the FSVO
listing's own link text. FSVO published the recall on 23.10.2026 — it is
filed under Rückrufe with that date on blv.admin.ch's own public listing of
warnings and recalls.

WHY IT MATTERS MORE THAN A WRONG FIELD. Date is the row's position in time
for everything downstream: compute_report_week, the daily brief, the weekly
page, the monthly roll-up. A future Date does not read as a defect; it reads
as a recall that has not happened yet, filed into a report nobody has built.
So a Tier-1 Listeria recall was absent from the week it belongs to and
waiting in a November week.

`validate_pending_row` has had a FLOOR since 2026-04-30 (nothing before
2026-01-01) and had no ceiling. A shelf-life date is always in the future,
so a future Date is the signature of exactly this mis-parse — whichever
regulator it arrives from. The scraper is fixed at source in
scrapers/europe_non_eu/blv_ch.py; this is the net under all the others.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import merge_master as mm          # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"


def _row(**kw):
    row = {
        "Date": "2026-10-23",
        "Source": "BLV (CH)",
        "Company": "Danforel A/S",
        "Brand": "—",
        "Product": "Smoked trout fillets, plain",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Listeria monocytogenes — FSVO recall",
        "Class": "Recall",
        "Country": "Switzerland",
        "Tier": 1,
        "Outbreak": 0,
        "URL": "https://www.recallswiss.admin.ch/customer-access/#Recalls/1168",
        "Notes": "listing (deterministic parse)",
    }
    row.update(kw)
    return row


# ── the gate ──────────────────────────────────────────────────────────────

def test_the_live_shape_is_refused():
    """The Danforel row, exactly as it was promoted."""
    ok, why = mm.validate_pending_row(_row(Date="2026-11-01"), set())
    assert not ok
    assert "future" in why.lower(), why


def test_a_date_years_out_is_refused():
    far = (date.today() + timedelta(days=900)).isoformat()
    ok, why = mm.validate_pending_row(_row(Date=far), set())
    assert not ok
    assert "future" in why.lower(), why


def test_today_is_fine():
    ok, _ = mm.validate_pending_row(
        _row(Date=date.today().isoformat()), set())
    assert ok


def test_yesterday_is_fine():
    ok, _ = mm.validate_pending_row(
        _row(Date=(date.today() - timedelta(days=1)).isoformat()), set())
    assert ok


def test_tomorrow_is_fine_because_of_the_timezone_the_gate_runs_in():
    """Regulators at UTC+9..+13 publish on a date that is still tomorrow
    here. One day of slack admits CAA JP, MFDS KR, MPI NZ and FSANZ, and
    still refuses the seven-day miss this file is named for."""
    ok, _ = mm.validate_pending_row(
        _row(Date=(date.today() + timedelta(days=1)).isoformat()), set())
    assert ok


def test_the_day_after_tomorrow_is_not():
    ok, why = mm.validate_pending_row(
        _row(Date=(date.today() + timedelta(days=2)).isoformat()), set())
    assert not ok
    assert "future" in why.lower(), why


def test_the_floor_still_holds():
    """Widening one end must not open the other."""
    ok, why = mm.validate_pending_row(_row(Date="2025-12-31"), set())
    assert not ok
    assert "2026-01-01" in why, why


def test_the_ceiling_moves_with_the_calendar():
    assert mm._max_valid_date(date(2026, 10, 25)) == "2026-10-26"
    assert mm._max_valid_date(date(2026, 12, 31)) == "2027-01-01"


# ── the register ──────────────────────────────────────────────────────────

def _published_rows():
    if not XLSX.exists():
        pytest.skip("no recalls.xlsx")
    wb = load_workbook(XLSX, read_only=True, data_only=True)
    try:
        ws = wb["Recalls"]
        it = ws.iter_rows(values_only=True)
        head = list(next(it))
        return [dict(zip(head, r)) for r in it
                if any(v is not None and str(v).strip() for v in r)]
    finally:
        wb.close()


def _future(rows):
    ceiling = date.today() + timedelta(days=1)
    out = []
    for r in rows:
        raw = str(r.get("Date") or "")[:10]
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            continue
        if d > ceiling:
            out.append((raw, r.get("Source"), r.get("Company"),
                        r.get("Pathogen"), r.get("report_week")))
    return out


def test_no_published_row_is_dated_in_the_future():
    bad = _future(_published_rows())
    assert not bad, (
        "%d published row(s) carry a Date later than tomorrow — a "
        "publication date cannot be ahead of today, and a shelf-life date "
        "read off a notice always is:\n    %s"
        % (len(bad), "\n    ".join(map(str, bad))))


def test_the_json_mirror_agrees():
    if not JSON.exists():
        pytest.skip("no recalls.json")
    rows = json.loads(JSON.read_text(encoding="utf-8"))
    bad = _future(rows)
    assert not bad, "future-dated row(s) on the dashboard: %s" % (bad,)


def test_no_report_week_is_ahead_of_the_calendar():
    """The downstream consequence, asserted on its own: a future Date lands
    a row in a week nobody has built. Checked through report_week so the
    defect is visible even if Date is later repaired without it."""
    today = date.today()
    this_week = today.isocalendar()[1]
    ahead = []
    for r in _published_rows():
        tag = str(r.get("report_week") or "").strip()
        if not (len(tag) >= 2 and tag[0] in "Ww" and tag[1:].isdigit()):
            continue
        if int(tag[1:]) > this_week:
            ahead.append((r.get("Date"), r.get("Source"), tag,
                          r.get("Company")))
    assert not ahead, (
        "row(s) stamped with a report week that has not started (today is "
        "W%02d):\n    %s" % (this_week, "\n    ".join(map(str, ahead))))
