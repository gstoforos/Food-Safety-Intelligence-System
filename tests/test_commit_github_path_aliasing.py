# -*- coding: utf-8 -*-
"""An absolute path must not alias past the cumulative-file guards.

WHAT THIS HOLDS
---------------
`pipeline.commit_github` protects docs/data/recalls.xlsx by never copying
one side over the other: every restore loop skips the cumulative files and
defers to the row-union merge. Each of those skips is written as a
comparison against the RELATIVE constant:

    if rel in (XLSX_REL, JSON_REL): continue

Nine callers hand the module an ABSOLUTE path instead —

    git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)

— and `str(ROOT / "docs/data/recalls.xlsx")` is the same file under a
different dict key. It walked past every guard.

WHAT IT COST (measured, 2026-10-13)
-----------------------------------
The operator uploaded the morning fix pass between 19:12:01 and 19:14:14
UTC: Recalls 2022 -> 2024 rows, plus field repairs (the GIS (PL) August
notice dated back off 2026-10-12, the FSANZ company/product split, the
MAPAQ "Plastic" hazard, four French RappelConso rows translated).

The next commit to touch the workbook, "Tavily gap-finder (standalone):
+4 rows to Pending (2026-10-13T19:12:30Z)", landed at 19:15:05 UTC. Its
parent is the last upload commit — so it HAD the repaired workbook — and
it committed Recalls back at 2022 rows with every repair gone. Layer 3's
post-push verification compared stale against stale and saw nothing.

The whole pass was lost three minutes after it was uploaded, and the same
five tests were red again the next morning.

These tests are about the SHAPE, not that one caller: `_rel` normalises at
the entry point so all nine are fixed at once, and `_is_cumulative`
accepts any spelling `_rel` can produce.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.commit_github import (  # noqa: E402
    JSON_REL,
    XLSX_REL,
    _is_cumulative,
    _rel,
    _restore_non_cumulative,
    _save_snapshots,
)

CUMULATIVE = (XLSX_REL, JSON_REL)


def test_an_absolute_path_normalises_to_the_relative_constant():
    """The exact spelling the nine callers use."""
    assert _rel("/srv/repo", "/srv/repo/" + XLSX_REL) == XLSX_REL
    assert _rel("/srv/repo", XLSX_REL) == XLSX_REL
    assert _rel("/srv/repo", "/srv/repo/" + JSON_REL) == JSON_REL


def test_a_path_outside_the_repo_is_left_alone():
    """Normalisation must not silently rewrite an unrelated path."""
    assert _rel("/srv/repo", "/etc/passwd") == "/etc/passwd"


@pytest.mark.parametrize("spelling", [
    XLSX_REL,
    "./" + XLSX_REL,
    JSON_REL,
    "./" + JSON_REL,
])
def test_every_spelling_of_a_cumulative_file_is_recognised(spelling):
    assert _is_cumulative(spelling), spelling


def test_a_non_cumulative_file_is_not_recognised():
    for other in ("docs/alerts.html", "docs/2026-W41.html",
                  "docs/data/weekly-index.json", "docs/data/sources.json"):
        assert not _is_cumulative(other), other


def test_restore_never_writes_the_workbook_however_it_was_keyed(tmp_path):
    """THE REGRESSION ITSELF.

    Build a repo whose working tree holds the just-pulled REMOTE workbook
    and whose snapshot dict holds our STALE one keyed by the absolute path
    — the state `_preemptive_sync` is in after `git stash pop` conflicts.
    `_restore_non_cumulative` must leave the workbook alone so the
    row-union merge, not a file copy, decides what survives.
    """
    repo = tmp_path / "repo"
    (repo / "docs" / "data").mkdir(parents=True)
    xlsx = repo / XLSX_REL
    jsonf = repo / JSON_REL
    xlsx.write_bytes(b"REMOTE-WORKBOOK-2024-ROWS")
    jsonf.write_text("REMOTE-MIRROR", encoding="utf-8")

    other = repo / "docs" / "alerts.html"
    other.write_text("REMOTE-PAGE", encoding="utf-8")

    snaps = tmp_path / "snaps"
    snaps.mkdir()
    stale_xlsx = snaps / "ours.xlsx"
    stale_xlsx.write_bytes(b"OURS-STALE-2022-ROWS")
    stale_json = snaps / "ours.json"
    stale_json.write_text("OURS-STALE-MIRROR", encoding="utf-8")
    stale_page = snaps / "ours.html"
    stale_page.write_text("OURS-PAGE", encoding="utf-8")

    saved = {
        # the alias the gap finders produce
        str(repo / XLSX_REL): stale_xlsx,
        str(repo / JSON_REL): stale_json,
        # and the relative spellings _save_snapshots always adds
        XLSX_REL: stale_xlsx,
        JSON_REL: stale_json,
        # a genuinely non-cumulative file, which SHOULD be restored
        "docs/alerts.html": stale_page,
    }

    _restore_non_cumulative(str(repo), saved)

    assert xlsx.read_bytes() == b"REMOTE-WORKBOOK-2024-ROWS", (
        "the stale workbook was copied over the just-pulled remote — this "
        "is the 2026-10-13 silent overwrite")
    assert jsonf.read_text(encoding="utf-8") == "REMOTE-MIRROR"
    # Non-cumulative files are still restored; that behaviour is unchanged.
    assert other.read_text(encoding="utf-8") == "OURS-PAGE"


def test_snapshots_of_an_absolute_caller_path_still_carry_the_relative_key(tmp_path):
    """`_save_snapshots` must produce a key the guards can match, so the
    row-union merge runs for absolute-path callers too."""
    repo = tmp_path / "repo"
    (repo / "docs" / "data").mkdir(parents=True)
    (repo / XLSX_REL).write_bytes(b"x")
    (repo / JSON_REL).write_text("y", encoding="utf-8")
    tmp = tmp_path / "tmp"
    tmp.mkdir()

    # What git_commit_and_push now hands on, after normalising.
    files = [_rel(str(repo), str(repo / XLSX_REL))]
    saved = _save_snapshots(str(repo), files, tmp)

    assert XLSX_REL in saved
    assert JSON_REL in saved
    assert not any(Path(k).is_absolute() for k in saved), sorted(saved)
