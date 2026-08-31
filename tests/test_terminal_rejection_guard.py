"""Regression: a permanently-rejected item must not re-enter Pending.

2026-08-31 — measured on the live workbook: 5 of the 9 rows sitting in
Pending were re-ingestions of items that already carried a permanent
verdict in the Rejected archive (Baxter Anticoagulant Sodium Citrate —
not_food, rejected 2026-08-27; Kosilum lighting — not_food; Capri-Sun —
labelling; Yopokki — spoilage; Racines — spoilage).

load_rejected_urls() existed and was correct, but only promote_approved
consulted it, and promote_approved only sees rows a reviewer reached. A
scraped row lands at 'pending_enrichment', reviewers skip that status,
so the permanent verdict was never applied and the row re-appeared on
every scrape — indefinitely.

The guard must stay NARROW: a transient rejection (http_error, dead link,
missing field) has to remain retryable, because giving a source the chance
to fix a broken link is the documented purpose of the retry path.
"""
from __future__ import annotations

import pytest

from pipeline.merge_master import _is_terminal_rejection


TERMINAL = [
    "operator review: not_food — Kosilum lighting product",
    "a reviewer: not a food. Compounding pharmacy recalling drug lots.",
    "out_of_scope_labelling — Capri-Sun mislabelled as Orange Zero",
    "out_of_scope_quality_spoilage — possible spoilage, no pathogen",
    "duplicate_of_published — already live as FSA-PRIN-42-2026",
    "operator_decision_one_outbreak_one_source",
    "REJECTED: NOT_FOOD — uppercase must match too",
]

TRANSIENT = [
    "REJECTED: http_error",
    "URL agent: link did not resolve",
    "fetch timed out after 25s",
    "missing Pathogen field",
    "a reviewer: needs enrichment from the detail page",
    "",
]


@pytest.mark.parametrize("desc", TERMINAL)
def test_terminal_reasons_block_reingestion(desc):
    assert _is_terminal_rejection(desc) is True, f"should block: {desc!r}"


@pytest.mark.parametrize("desc", TRANSIENT)
def test_transient_reasons_stay_retryable(desc):
    assert _is_terminal_rejection(desc) is False, f"must stay retryable: {desc!r}"


def test_none_is_not_terminal():
    assert _is_terminal_rejection(None) is False


def test_the_exact_baxter_verdict_blocks():
    """The row that proved the bug: re-ingested after a not_food verdict."""
    desc = ("a reviewer: not_food — Baxter voluntary nationwide recall of one "
            "lot of Anticoagulant Sodium Citrate solution — a medical/"
            "pharmaceutical product, not a food.")
    assert _is_terminal_rejection(desc) is True


def test_guard_is_wired_into_append_to_pending():
    """The vocabulary is useless if nothing calls it at ingest time."""
    import inspect
    from pipeline import merge_master
    src = inspect.getsource(merge_master.append_to_pending)
    assert "_is_terminal_rejection" in src or "_terminal" in src, (
        "append_to_pending must consult the terminal-rejection map; "
        "without it the guard only protects promotion, which is the "
        "exact gap this test exists for")
