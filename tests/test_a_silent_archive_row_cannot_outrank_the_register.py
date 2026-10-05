"""supersede_archived_copies must see the verdict wherever it is written.

2026-10-05. ``tests/test_a_recall_is_not_both_published_and_rejected`` was
RED again: 36 archive rows shared a URL with a published recall and said
nothing about which copy wins — 1 in Weekly_Rejected, 35 in Rejected —
eight days after ``promote_gate_passing.supersede_archived_copies`` was
written to prevent exactly that.

It was not that the function was wrong about what it did. It was that it
could not SEE most of the rows it exists to act on. Three measured causes:

  1. IT READ THE REASON COLUMN ONLY. Of the 36, exactly ONE matched on its
     reason column. The other 35 have ``RejectReason`` blank and keep the
     verdict in ``Notes``. ``merge_master.load_rejected_urls`` learned this
     on 2026-09-01 — "the reason COLUMN is frequently uninformative … the
     actual verdict lives in Notes" — and folds Notes in. This function was
     written after that and did not. Folding Notes in resolves 27 of 36.

  2. THE FIELD LIST WAS A HAND-PICKED SUBSET. Publish-gate rule 4 emits
     "<Field> is empty" for Company, Product, Class, Date, Source and URL.
     SUPERSEDE_IF listed company, pathogen and reason. An FSAI Wrights of
     Marino row archived for "Date is empty" — the same repairable field
     defect, one field over — matched nothing.

  3. A ROW WITH NO VERDICT AT ALL WAS SKIPPED. Eight gap-finder rows carry
     no reason and no verdict in Notes, only "Discovered via news: <site>",
     which says where the row came from and not why it was refused. Silence
     is not a verdict and cannot outrank a published recall.

And one structural cause behind all three: the sweep only ever ran over the
URLs ONE PROMOTER RUN had just promoted. The hourly merge, the three
reviewers, the confirm agent and the gap finders all publish without
calling it. ``supersede_every_published_url`` reads the published set out
of Recalls instead, so the sweep no longer depends on who did the
publishing.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

openpyxl = pytest.importorskip("openpyxl")

from pipeline.promote_gate_passing import (                  # noqa: E402
    supersede_archived_copies,
    supersede_every_published_url,
)

URL = "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch"
RESOLVED = ("superseded", "duplicate", "unresolved contradiction")


def _workbook(tmp_path, *, reason, notes):
    """A register with one published recall and one archive copy of it."""
    wb = openpyxl.Workbook()
    r = wb.active
    r.title = "Recalls"
    r.append(["Date", "Company", "Product", "Pathogen", "URL", "Notes"])
    r.append(["2026-09-18", "Dunnes Stores", "Potato Waffles", "MOAH", URL, ""])
    a = wb.create_sheet("Rejected")
    a.append(["Date", "Company", "Product", "URL", "Notes", "Status",
              "RejectedBy", "RejectReason"])
    a.append(["2026-09-20", "Dunnes Stores", "Potato Waffles", URL, notes,
              "rejected", "unknown", reason])
    p = tmp_path / "recalls.xlsx"
    wb.save(p)
    return p


def _reason(path):
    wb = openpyxl.load_workbook(path)
    return str(wb["Rejected"].cell(2, 8).value or "")


@pytest.mark.parametrize("reason,notes,why", [
    # CAUSE 1 — the verdict is in Notes, the reason column is blank.
    ("", "REJECTED: Confirmer: row was at pending_gap_v2 (not reviewed by "
         "reviewer 2) and Pathogen is empty",
     "the verdict lives in Notes on 35 of the 36 rows found 2026-10-05"),
    # CAUSE 2 — a field the hand-picked subset did not list.
    ("", "REJECTED: Confirmer: row was at pending (not reviewed by "
         "reviewer 2) and Date is empty",
     "'Date is empty' is publish-gate rule 4, same as 'Pathogen is empty'"),
    ("", "REJECTED: Confirmer: Source is empty", "rule 4, another field"),
    # CAUSE 3 — no verdict recorded anywhere.
    ("", "Discovered via news: natemat.pl",
     "silence is not a verdict and cannot outrank a published recall"),
    # The case that already worked, pinned so it keeps working.
    ("unknown: No matching hazard category — defer to manual review.", "",
     "the one spelling the old code did match"),
])
def test_a_published_url_retires_its_archive_copy(tmp_path, reason, notes, why):
    p = _workbook(tmp_path, reason=reason, notes=notes)
    assert supersede_every_published_url(p) == 1, (
        f"the archive copy was left silently contradicting the register — "
        f"{why}")
    out = _reason(p).lower()
    assert any(m in out for m in RESOLVED), (
        f"the archive row must SAY which copy wins; it reads {out[:80]!r}")
    assert dt.date.today().isoformat() in _reason(p)


def test_settled_duplicate_text_is_left_alone(tmp_path):
    """Rewriting settled audit text is how audit text stops being trusted."""
    settled = ("duplicate_already_approved — the same RASFF notification as "
               "the row kept in Recalls.")
    p = _workbook(tmp_path, reason=settled, notes="")
    assert supersede_every_published_url(p) == 0
    assert _reason(p) == settled


def test_a_real_scope_verdict_is_not_quietly_overturned(tmp_path):
    """An out-of-scope refusal is a judgement, not a repairable defect.

    If such a row shares a URL with a published recall, that is a genuine
    unresolved contradiction for a person to settle — this sweep must not
    silently decide it in the register's favour.
    """
    verdict = ("pet_food_out_of_scope — AFTS-FSIS is a HUMAN-food register.")
    p = _workbook(tmp_path, reason=verdict, notes="")
    assert supersede_every_published_url(p) == 0, (
        "a scope verdict must not be auto-superseded")
    assert _reason(p) == verdict


def test_the_sweep_is_idempotent(tmp_path):
    p = _workbook(tmp_path, reason="",
                  notes="REJECTED: Confirmer: Pathogen is empty")
    assert supersede_every_published_url(p) == 1
    assert supersede_every_published_url(p) == 0, (
        "a second run must not stack a second banner on the same row")


def test_an_unpublished_url_is_untouched(tmp_path):
    p = _workbook(tmp_path, reason="",
                  notes="REJECTED: Confirmer: Pathogen is empty")
    wb = openpyxl.load_workbook(p)
    wb["Rejected"].cell(2, 4).value = "https://example.gov/other-recall"
    wb.save(p)
    assert supersede_every_published_url(p) == 0, (
        "only an archive row whose URL is PUBLISHED is a contradiction")


def test_the_sweep_is_not_tied_to_one_promoter_run():
    """The structural cause: it only saw URLs that promoter had promoted."""
    src = (ROOT / "pipeline" / "promote_gate_passing.py").read_text(
        encoding="utf-8")
    assert "def supersede_every_published_url" in src
    body = src[src.find("def main"):]
    assert "supersede_every_published_url" in body, (
        "main() must run the sweep over the whole register. Passing only "
        "this run's `promoted` set is what let 36 contradictions published "
        "by the hourly merge, the reviewers and the confirm agent sit "
        "unnoticed for eight days.")
    assert body.count("supersede_every_published_url") >= 2, (
        "the sweep must also run on a day with nothing to promote — the "
        "rows it fixes were published by other paths, so an early "
        "'nothing to promote' return must not skip it.")


def test_supersede_archived_copies_still_takes_an_explicit_url_set(tmp_path):
    """The narrow entry point stays, so a caller can scope the sweep."""
    p = _workbook(tmp_path, reason="",
                  notes="REJECTED: Confirmer: Pathogen is empty")
    assert supersede_archived_copies(p, []) == 0
    assert supersede_archived_copies(p, [URL]) == 1
