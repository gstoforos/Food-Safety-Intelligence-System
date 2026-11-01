"""Pending must not hold two rows for one recall URL.

WHY (morning fix 2026-11-01)
============================
On 2026-11-01 Pending held two rows for

    fsai.ie/news-and-alerts/food-alerts/recall-of-several-supervalu-and-
    inspired-by-centra

with the same ScrapedAt (2026-10-31T03:12:21Z) and the same source_id. One
had been enriched by reviewer 2 to pending_gap_v3 (Company "Supervalu",
Brand "Supervalu, Inspired By Centra", Product "Smoked Salmon, Mackerel,
Prawn Cocktail"); the other still carried the FSAI page headline as its
Product, had no Company, and was parked under a reviewer-3 hold reading
"no recalling firm on the row (Brand ''); not published and not rejected —
reviewer 2 to read the firm from the notice".

Reviewer 2 could not satisfy that hold however often it ran, because
pipeline/recall_review_agent.py indexes Pending like this::

    url_to_idx: Dict[str, int] = {}
    for i, pr in enumerate(full_pending):
        u = str(pr.get("URL", "")).strip()
        if u:
            url_to_idx[u] = i

One slot per URL, last write wins. With two rows at one URL the EARLIER one
is invisible to reviewer 2 for the rest of its life: reviewer 3 keeps
holding it, the offline promoter keeps listing it under "Left in Pending
(never archived here)", and nothing can ever move it. A duplicate pair is
therefore not a cosmetic problem — it is a row that can never be published
and can never be refused.

The collector-side cause is still open. This test is the ratchet that stops
a second pair forming unnoticed while it is: the writer may queue a notice
once, and a row that is already queued must be UPDATED, not appended beside
itself. It reads the live register deliberately, in the style of
tests/test_no_country_goes_dark.py — the defect is a state of the data, and
a test over fixtures alone would have stayed green through all of it.
"""

from __future__ import annotations

import collections
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def _rows(sheet: str):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    try:
        if sheet not in wb.sheetnames:
            return []
        ws = wb[sheet]
        head = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        return [dict(zip(head, r)) for r in ws.iter_rows(min_row=2,
                                                         values_only=True)
                if any(v is not None for v in r)]
    finally:
        wb.close()


def _dupes(sheet: str):
    """URL dedup key -> count, for keys appearing more than once.

    Keyed with the writer's own normaliser, not a hand-rolled strip: a
    trailing slash hid one of two rows from the repair pass of 2026-10-26.
    """
    from pipeline.merge_master import _normalize_url_for_dedup as key
    counts = collections.Counter()
    for row in _rows(sheet):
        u = str(row.get("URL") or "").strip()
        if u:
            counts[key(u)] += 1
    return {k: n for k, n in counts.items() if n > 1}


@pytest.mark.skipif(not XLSX.exists(), reason="no register in this checkout")
def test_pending_has_at_most_one_row_per_url():
    dupes = _dupes("Pending")
    assert not dupes, (
        "Pending holds more than one row for the same recall URL:\n  "
        + "\n  ".join(f"{n}x {k}" for k, n in sorted(dupes.items()))
        + "\n\nReviewer 2 indexes Pending as url_to_idx[url] = i (one slot "
          "per URL, last write wins), so every copy but the last is "
          "invisible to it: it can never be corrected, never promoted and "
          "never refused, while reviewer 3 holds it and the offline "
          "promoter reports it every morning. Update the queued row; do "
          "not append beside it.")


@pytest.mark.skipif(not XLSX.exists(), reason="no register in this checkout")
def test_recalls_has_at_most_one_row_per_url():
    """The published sheet, for the same reason one level downstream: two
    published rows at one URL are two counts of one recall in every monthly
    and weekly total."""
    dupes = _dupes("Recalls")
    assert not dupes, (
        "Recalls holds more than one PUBLISHED row for the same URL — every "
        "report counts that recall twice:\n  "
        + "\n  ".join(f"{n}x {k}" for k, n in sorted(dupes.items())))


@pytest.mark.skipif(not XLSX.exists(), reason="no register in this checkout")
def test_reviewer_two_still_indexes_pending_by_url():
    """Guards the REASON this test exists.

    If reviewer 2 is ever changed to correct every row at a URL rather than
    the last one, the failure mode above changes shape and this file's
    message would be describing code that no longer exists. Fail loudly then
    rather than quietly misleading the next pass.
    """
    src = (ROOT / "pipeline" / "recall_review_agent.py").read_text(
        encoding="utf-8")
    assert "url_to_idx" in src, (
        "pipeline/recall_review_agent.py no longer builds url_to_idx. "
        "Re-read it and update this test's rationale — the duplicate-row "
        "failure mode it describes may have changed.")
