"""tools/rebase_and_verify.py — the parts that decide what is copied and what
is rebuilt (operator 2026-10-04: "do we have reduced mistakes").

The run itself needs git and a live origin; these hold the rules it applies.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.rebase_and_verify import (  # noqa: E402
    Refused, affected_dates, base_sha_from_name, classify, diff_rows,
    iso_week_file, needs_removal_word, strip_wrapper,
)


def test_base_sha_is_read_from_the_zip_name():
    assert base_sha_from_name("2-CODE-2026-10-04-base-d16ae98d.zip") == "d16ae98d"
    assert base_sha_from_name("/x/1-DATA-docs-folder-base-4009b9d-UPLOAD-NOW.zip") == "4009b9d"
    with pytest.raises(Refused):
        base_sha_from_name("2-CODE.zip")


@pytest.mark.parametrize("path,kind", [
    ("docs/data/recalls.xlsx", "derived"),          # never copied: rebuilt by the repair
    ("docs/data/recalls.json", "derived"),
    ("docs/data/afts-recalls-public.xlsx", "derived"),
    ("docs/daily/2026-10-01.html", "report"),       # rebuilt, never copied
    ("docs/2026-W39.html", "report"),
    ("docs/daily-index.json", "report"),
    ("docs/data/weekly-summary-latest.json", "report"),
    ("docs/data/weekly-review-latest.json", "report"),
    ("docs/2026-M09.html", "monthly"),              # the day-10 check owns these
    ("docs/data/monthly-index.json", "monthly"),
    ("docs/index.html", "docs"),
    ("pipeline/merge_master.py", "code"),
    ("tools/repair_2026_10_04.py", "code"),
    ("pipeline/__pycache__/x.cpython-311.pyc", "skip"),
    ("READ-ME-FIRST.md", "skip"),
])
def test_every_file_has_one_destination(path, kind):
    assert classify(path) == kind


def test_a_single_wrapper_folder_is_stripped():
    m = strip_wrapper(["fix/pipeline/a.py", "fix/tests/t.py"])
    assert set(m.values()) == {"pipeline/a.py", "tests/t.py"}
    m = strip_wrapper(["pipeline/a.py", "docs/index.html"])
    assert set(m.values()) == {"pipeline/a.py", "docs/index.html"}


def test_the_register_diff_and_the_dates_it_touches():
    b = {"u1": {"URL": "u1", "Date": "2026-10-01", "Tier": "3"},
         "u2": {"URL": "u2", "Date": "2026-09-01", "Tier": "1"},
         "u3": {"URL": "u3", "Date": "2026-09-20", "Tier": "1"}}
    a = {"u2": {"URL": "u2", "Date": "2026-09-29", "Tier": "1"},
         "u3": {"URL": "u3", "Date": "2026-09-20", "Tier": "1"},
         "u4": {"URL": "u4", "Date": "2026-10-02", "Tier": "1"}}
    added, removed, changed = diff_rows(b, a)
    assert [r["URL"] for r in added] == ["u4"]
    assert [r["URL"] for r in removed] == ["u1"]
    assert [(x[0]["URL"], x[2]) for x in changed] == [("u2", ["Date"])]
    # a re-dated row touches BOTH its old and its new day
    assert affected_dates(added, removed, changed) == {
        "2026-10-01", "2026-10-02", "2026-09-01", "2026-09-29"}


def test_week_file_names():
    assert iso_week_file("2026-10-04") == "2026-W40.html"   # Sunday closes W40
    assert iso_week_file("2026-09-28") == "2026-W40.html"
    assert iso_week_file("2026-09-27") == "2026-W39.html"


def test_a_removal_needs_the_word_the_shrink_guard_reads():
    assert needs_removal_word(2, "morning fix")
    assert not needs_removal_word(2, "remove-rows: morning fix")
    assert not needs_removal_word(0, "morning fix")


def test_changed_files_keep_their_first_character():
    """2026-10-05: the first modified file lost its first character (a
    trimmed leading space) and was left out of the test run and the zip."""
    from tools.rebase_and_verify import parse_porcelain_z
    out = (" M pipeline/_pathogen_scope.py\0M  docs/data/recalls.xlsx\0"
           "?? tests/test_x.py\0R  tools/new.py\0tools/old.py\0"
           "?? pipeline/__pycache__/a.pyc\0")
    assert parse_porcelain_z(out) == ["docs/data/recalls.xlsx", "pipeline/_pathogen_scope.py",
                                      "tests/test_x.py", "tools/new.py"]


def test_failed_ids_are_read_from_the_pytest_summary():
    from tools.rebase_and_verify import failed_test_ids
    out = ("x\nFAILED tests/a.py::test_one[Weekly_Rejected-RejectionReason] - AssertionError: 1 row\n"
           "FAILED tests/b.py::test_two\nERROR tests/c.py - ImportError\n2 failed, 3 passed\n")
    assert failed_test_ids(out) == ["tests/a.py::test_one[Weekly_Rejected-RejectionReason]",
                                    "tests/b.py::test_two", "tests/c.py"]


def test_a_deleted_file_is_not_mistaken_for_a_dropped_one():
    from tools.rebase_and_verify import deleted_in_porcelain_z
    out = " D docs/old.html\0 M docs/new.html\0D  tools/x.py\0"
    assert deleted_in_porcelain_z(out) == {"docs/old.html", "tools/x.py"}
