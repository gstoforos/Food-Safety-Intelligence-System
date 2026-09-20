# -*- coding: utf-8 -*-
"""The register must never lose published rows without an explicit decision.

THE INCIDENT (2026-09-20)
-------------------------
Commit ``02d97dde`` — message "Add files via upload" — took Recalls from
**1751 to 1722**. Twenty-nine published rows disappeared, **nineteen of
them Tier 1**, covering every promotion made on 2026-09-18 and 09-19.

Nothing failed. No workflow went red, no email was sent, no test
complained. The register simply got smaller, the dashboard showed the
newest recall as 17 September, and the week looked stalled at 25 rows.
The pipeline upstream had been working perfectly the whole time.

The cause was a stale data file: ``docs/data/recalls.xlsx`` had been
shipped inside a dated zip, and the zip was uploaded two days later,
overwriting the live register with a snapshot of itself. Confirmed by a
byte-for-byte comparison.

WHY A TEST AND NOT A CONVENTION
-------------------------------
"Never ship the register in a zip" is a rule someone has to remember
every single time. This is the check that does not.

Deletions are legitimate — a duplicate batch, a mis-scraped row, a
rejected set. What is never legitimate is a deletion nobody declared. So
the count may fall, but only when the commit that lowers it says so.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: A commit may remove rows when its message says so in as many words.
DELETION_MARKERS = (
    "remove-rows", "delete-rows", "reject-batch",
    "deliberate row removal", "approved deletion",
)


def _rows(path):
    from openpyxl import load_workbook
    wb = load_workbook(path, read_only=True, data_only=True)
    raw = list(wb["Recalls"].iter_rows(values_only=True))
    hdr = [str(c or "") for c in raw[0]]
    out = [dict(zip(hdr, r)) for r in raw[1:]]
    wb.close()
    return out


def _git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)


def _previous_workbook():
    if _git("rev-parse", "HEAD~1").returncode:
        return None
    show = subprocess.run(["git", "show", "HEAD~1:docs/data/recalls.xlsx"],
                          cwd=ROOT, capture_output=True)
    if show.returncode or not show.stdout:
        return None
    fh = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    fh.write(show.stdout)
    fh.close()
    return Path(fh.name)


def test_the_workbook_is_readable():
    assert XLSX.exists(), "docs/data/recalls.xlsx is missing"
    assert len(_rows(XLSX)) > 0


def test_recalls_did_not_shrink_without_saying_so():
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    prev = _previous_workbook()
    if prev is None:
        pytest.skip("no previous revision of the workbook")

    before, after = _rows(prev), _rows(XLSX)
    Path(prev).unlink(missing_ok=True)

    keys = {str(r.get("URL") or "").strip().lower() for r in after}
    gone = [r for r in before
            if str(r.get("URL") or "").strip().lower() not in keys]
    if not gone:
        return

    msg = _git("log", "-1", "--format=%B").stdout.lower()
    if any(m in msg for m in DELETION_MARKERS):
        return

    tier1 = sum(1 for r in gone if str(r.get("Tier")).strip() == "1")
    sample = "\n  ".join(
        "%s | %s | %s" % (str(r.get("Date"))[:10], str(r.get("Source"))[:16],
                          str(r.get("Product"))[:54])
        for r in gone[:8])
    raise AssertionError(
        "%d published row(s) vanished from Recalls (%d Tier 1) and this "
        "commit's message does not declare a deletion.\n\nThis is the "
        "2026-09-20 shape: an 'Add files via upload' commit overwrote the "
        "live register with an older snapshot and removed 29 rows, 19 Tier 1, "
        "in silence.\n\nIf intended, say so in the commit message using one "
        "of: %s\n\nRemoved (first 8 of %d):\n  %s"
        % (len(gone), tier1, ", ".join(DELETION_MARKERS), len(gone), sample))


def test_the_json_mirror_matches_the_workbook():
    import json
    js = ROOT / "docs" / "data" / "recalls.json"
    if not js.exists():
        pytest.skip("recalls.json not present")
    n_json = len(json.loads(js.read_text(encoding="utf-8-sig")))
    n_xlsx = len(_rows(XLSX))
    assert n_json == n_xlsx, (
        "recalls.json has %d rows, recalls.xlsx has %d — the site is serving "
        "a different register from the workbook" % (n_json, n_xlsx))

# ---------------------------------------------------------------------------
# EVERY SHEET, NOT JUST RECALLS (audit 2026-09-20, same day, second incident)
# ---------------------------------------------------------------------------
# The guard above was written hours earlier and watched ``Recalls`` alone.
# The very next upload proved why that is not enough.
#
# Restoring the 29 deleted Recalls rows carried the rest of the workbook
# with it, and the NEWS sheet went back to its 2026-09-18 state — roughly
# 36 hours of news collection, reverted. Recalls was correct, so the guard
# stayed green and said nothing, and the hourly news job carried on
# committing on top of a sheet that had been rolled back underneath it.
#
# One workbook, six sheets, and a rule that covered one of them.
#
# NEWS carries its own freshness stamp, so it gets a second check the
# others cannot have: a sheet whose newest row goes BACKWARDS has been
# overwritten with an older copy, whatever its row count says. Here the
# reverted sheet had MORE rows (39) than the current one (28), because the
# pipeline purges on a rolling window — so counting alone would have
# called the regression an improvement.

SHEETS_THAT_MUST_NOT_SHRINK = ("Recalls", "Rejected", "Weekly_Rejected")

#: Sheets whose row count legitimately falls: Pending drains as rows are
#: promoted, NEWS is purged on a rolling window, Weekly_Review is rebuilt
#: per week. They are covered by the freshness check below instead.
SHEETS_THAT_MAY_SHRINK = ("Pending", "NEWS", "Weekly_Review")


def _sheet_rows(path, sheet):
    from openpyxl import load_workbook
    wb = load_workbook(path, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        return None
    raw = list(wb[sheet].iter_rows(values_only=True))
    hdr = [str(c or "") for c in raw[0]] if raw else []
    out = [dict(zip(hdr, r)) for r in raw[1:]]
    wb.close()
    return out


@pytest.mark.parametrize("sheet", SHEETS_THAT_MUST_NOT_SHRINK)
def test_no_append_only_sheet_loses_rows(sheet):
    """Recalls, Rejected and Weekly_Rejected only ever grow."""
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    prev = _previous_workbook()
    if prev is None:
        pytest.skip("no previous revision of the workbook")
    before = _sheet_rows(prev, sheet)
    after = _sheet_rows(XLSX, sheet)
    Path(prev).unlink(missing_ok=True)
    if before is None or after is None:
        pytest.skip("sheet %r not present in both revisions" % sheet)

    msg = _git("log", "-1", "--format=%B").stdout.lower()
    if any(m in msg for m in DELETION_MARKERS):
        return
    assert len(after) >= len(before), (
        "sheet %r went from %d rows to %d and the commit message does not "
        "declare a deletion. An upload that overwrites the workbook takes "
        "EVERY sheet with it, not just the one you were looking at."
        % (sheet, len(before), len(after)))


def test_the_news_sheet_did_not_go_backwards_in_time():
    """A freshness check, because NEWS is purged and row counts lie.

    On 2026-09-20 the reverted NEWS sheet had 39 rows against the live
    sheet's 28 — MORE rows, 36 hours older. Only the timestamp shows it.
    """
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    prev = _previous_workbook()
    if prev is None:
        pytest.skip("no previous revision of the workbook")
    before = _sheet_rows(prev, "NEWS")
    after = _sheet_rows(XLSX, "NEWS")
    Path(prev).unlink(missing_ok=True)
    if not before or not after:
        pytest.skip("NEWS not present in both revisions")

    def newest(rows):
        stamps = []
        for r in rows:
            for k, v in r.items():
                if "retriev" in k.lower() or "date" in k.lower():
                    s = str(v or "").strip()
                    if s:
                        # normalise "2026-09-20 06:07 UTC" and
                        # "2026-09-18T01:37:24Z" to a sortable prefix
                        stamps.append(s.replace("T", " ")[:16])
        return max(stamps) if stamps else ""

    b, a = newest(before), newest(after)
    if not b or not a:
        pytest.skip("no usable timestamp column in NEWS")
    assert a >= b, (
        "the NEWS sheet's newest entry moved BACKWARDS, from %r to %r. The "
        "workbook has been overwritten with an older copy. Row counts will "
        "not show this — a purged sheet can be smaller AND newer." % (b, a))
