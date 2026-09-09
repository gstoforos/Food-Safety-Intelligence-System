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


# ── 2026-09-01 follow-up ─────────────────────────────────────────────────
# The first version of the vocabulary held only machine codes
# ("not_food", "out_of_scope_labelling", ...) and blocked NOTHING, because
# weekly_rejected_capture stores a SHORTENED reason: the Capri-Sun verdict
# "out_of_scope_labelling — ..." lands in RejectionReason as
# "labelling — Capri-Sun Orange multipacks...". Worse, the permanent
# Rejected sheet often stores only a bare stamp ("operator review
# 2026-08-14") or the literal word "unknown" (73 rows), with the real
# verdict in Notes. Four permanently-rejected items were re-ingested on
# 2026-09-01, one day after the guard shipped.

# REVERSED POLICY IS NOT A PERMANENT PROPERTY (2026-09-07). The stored
# reason "spoilage — RappelConso motif moisissures" used to belong in the
# list below. Visible mould moved INTO scope on 2026-09-07, so a rejection
# whose reason names mould must now be retryable — otherwise the guard
# would keep enforcing a policy the operator has withdrawn. Its case now
# lives in STORED_REVERSED.
STORED_REVERSED = [
    "spoilage — RappelConso motif moisissures",
    "out_of_scope_quality_spoilage — visible mould in the bottle neck",
    "quality/spoilage — microbial (Mould) contamination",
]

STORED_TERMINAL = [
    "labelling — Capri-Sun Orange multipacks mislabelled as Orange Zero",
    "quality/spoilage — possible spoilage, no pathogen named",
    "pet food — AFTS-FSIS is a human-food register",
    "duplicate of the Summit Foods FSA-PRIN-40-2026 notice",
    "Outside AFTS scope. The register monitors pathogens",
    "not a food product recall",
    "allergen — undeclared milk, labelling only",
    "not a recall notice. product/company hold raised",
]

STORED_TRANSIENT = [
    "unknown",
    "REJECTED: http_error",
    "verification. not rejected - unverified, not refuted",
    "hazard not established — the detail URL does not resolve",
    "broken provenance. the stored url is truncated",
]


@pytest.mark.parametrize("desc", STORED_TERMINAL)
def test_reason_strings_actually_stored_block(desc):
    assert _is_terminal_rejection(desc) is True, f"should block: {desc!r}"


@pytest.mark.parametrize("desc", STORED_TRANSIENT)
def test_reason_strings_actually_stored_stay_retryable(desc):
    assert _is_terminal_rejection(desc) is False, f"must retry: {desc!r}"


def test_transient_wins_over_terminal_when_both_present():
    """A row rejected for a dead link must retry even if the note also
    mentions a scope word — the link may start working."""
    assert _is_terminal_rejection(
        "http_error | possible labelling issue, unverified") is False


def test_guard_reads_notes_not_only_the_reason_column():
    """The verdict is frequently in Notes, not RejectionReason."""
    import inspect
    from pipeline import merge_master
    src = inspect.getsource(merge_master.load_rejected_urls)
    assert "Notes" in src, (
        "load_rejected_urls must fold Notes into the description; the "
        "reason column alone is 'unknown' on 73 archived rows")


# ── 2026-09-01, second follow-up ─────────────────────────────────────────
# Descriptions are a concatenation of the reason column and the WHOLE Notes
# history. Kofinas' note carried an old "REJECTED: http_error" alongside the
# operator's scope verdict; the transient veto matched the historical
# http_error and cancelled the current verdict, so the row read as retryable
# and re-entered Pending after being explicitly rejected — twice.
from pipeline.merge_master import _latest_operator_verdict


def test_operator_verdict_outranks_stale_transient_marker():
    desc = ("unknown: http_error | FDA HTML fallback — claude-check needs to "
            "enrich Date+Pathogen [operator review 2026-09-01: REJECTED — "
            "out_of_scope_no_hazard_named — VERIFIED at fda.gov]")
    assert _is_terminal_rejection(desc) is True


def test_latest_verdict_wins_over_an_earlier_approval():
    """Lipofit was APPROVED on 08-31 then reversed the same day."""
    desc = ("[operator review 2026-08-31: APPROVED — verified at fda.gov] "
            "[operator review 2026-08-31: UNPUBLISHED — not_food — FDA "
            "classifies it an unapproved drug]")
    assert _latest_operator_verdict(desc) == ("UNPUBLISHED", "not_food")
    assert _is_terminal_rejection(desc) is True


def test_a_standing_approval_is_not_terminal():
    desc = "[operator review 2026-08-31: APPROVED — in scope, verified]"
    assert _is_terminal_rejection(desc) is False


def test_no_operator_stamp_falls_back_to_markers():
    assert _is_terminal_rejection("REJECTED: http_error") is False
    assert _is_terminal_rejection("labelling — mislabelled pack") is True


def test_terminal_beats_transient_across_sheets(tmp_path):
    """Precedence, exercised end to end on a real workbook.

    The permanent Rejected sheet is read FIRST. If it holds a stale transient
    verdict for a URL and Weekly_Rejected holds the operator's newer terminal
    verdict for the same URL, the terminal one must win — otherwise the row is
    read as retryable and re-enters Pending after being explicitly rejected,
    which is exactly what happened to Lipofit and Kofinas on 2026-09-01.
    """
    import openpyxl
    from pipeline.merge_master import load_rejected_urls

    url = "https://example.org/safety/recall-widget"
    hdr = ["Date", "Source", "Company", "URL", "Notes", "RejectionReason"]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Rejected"
    ws.append(hdr)
    # older, permanent, TRANSIENT
    ws.append(["2026-08-01", "FDA", "Widget Co", url,
               "REJECTED: http_error", "unknown"])
    wr = wb.create_sheet("Weekly_Rejected")
    wr.append(hdr)
    # newer, rolling, TERMINAL operator verdict
    wr.append(["2026-09-01", "FDA", "Widget Co", url,
               "[operator review 2026-09-01: REJECTED — not_food — a drug]",
               "not a food"])
    x = tmp_path / "recalls.xlsx"
    wb.save(x)

    m = load_rejected_urls(x)
    key = next(k for k in m if "recall-widget" in k)
    assert _is_terminal_rejection(m[key]) is True, (
        f"stale transient verdict won over the newer terminal one: {m[key]!r}")


@pytest.mark.parametrize("desc", STORED_REVERSED,
                         ids=[d[:44] for d in STORED_REVERSED])
def test_reversed_policy_rejections_are_retryable(desc):
    """A rejection made under a policy since withdrawn must not block."""
    assert _is_terminal_rejection(desc) is False, f"should not block: {desc!r}"


# ──────────────────────────────────────────────────────────────────────
# AUDIT 2026-09-08 — the four-night resurrection
# ──────────────────────────────────────────────────────────────────────
# The merlan (TVB-N, RappelConso 23399) and Jelly's straws (choking hazard,
# 23409) rows were archived by the operator on 2026-09-05 and re-entered
# Pending on the 6th, 7th and 8th. The vocabulary was widened twice and did
# not help, because the operator-verdict branch NEVER REACHED IT: the verdict
# regex captured a single token, so
#
#   "[operator review 2026-09-05: REJECTED — out of scope: choking hazard …]"
#
# parsed to the code "out" — no marker matches "out", and "out" does not
# start with "out_of_scope" — and the branch returned False for a deliberate
# human rejection. Two fixes, both pinned here: the code is parsed as a
# phrase, and an inconclusive code falls through to the terminal vocabulary
# read against the whole note instead of returning "retryable".

_MERLAN = ("e.leclerc [operator review 2026-09-05: REJECTED — out of scope: "
           "exceedance of the TVB-N (ABVT, total volatile basic nitrogen) "
           "limit in whiting is a spoilage/freshness indicator, the same "
           "class as histamine, which the register excludes.]")
_JELLYS = ("au comptoir des sorciers 2 rue maréchal joffre 35000 rennes "
           "[operator review 2026-09-05: REJECTED — out of scope: choking "
           "hazard from the product's own firm texture, a design property "
           "and not a contamination.]")


@pytest.mark.parametrize("desc", [_MERLAN, _JELLYS], ids=["merlan", "jellys"])
def test_the_four_night_resurrection_is_blocked(desc):
    assert _is_terminal_rejection(desc) is True, (
        "an explicit operator rejection written in words must block a "
        "re-ingestion")


def test_a_spaced_verdict_code_is_parsed_as_a_phrase():
    v = _latest_operator_verdict(
        "[operator review 2026-09-05: REJECTED — out of scope: choking]")
    assert v == ("REJECTED", "out of scope"), v


def test_machine_codes_still_parse_exactly_as_before():
    assert _latest_operator_verdict(
        "[operator review 2026-08-31: UNPUBLISHED — not_food — lamps]"
    ) == ("UNPUBLISHED", "not_food")
    assert _latest_operator_verdict(
        "[operator review 2026-09-06: REJECTED — duplicate_of_published: x]"
    ) == ("REJECTED", "duplicate_of_published")


def test_an_inconclusive_verdict_code_still_reads_the_whole_note():
    """The code decides nothing; the note says 'labelling'. It must block."""
    desc = ("[operator review 2026-09-05: REJECTED — see below] the defect is "
            "a labelling error and nothing else")
    assert _is_terminal_rejection(desc) is True


def test_an_operator_rejection_is_not_cancelled_by_a_stale_transient_marker():
    """The reason this branch skips the transient veto in the first place."""
    desc = ("unknown: http_error [operator review 2026-09-05: REJECTED — out "
            "of scope: choking hazard, a design property]")
    assert _is_terminal_rejection(desc) is True


# The mould reversal excuses decisions taken under the OLD rule, not the
# subject matter for ever (audit 2026-09-08).
def test_a_mould_rejection_made_before_the_reversal_is_retryable():
    assert _is_terminal_rejection(
        "[operator review 2026-08-14: REJECTED — out of scope: visible mould "
        "is a quality defect]") is False


def test_a_mould_rejection_made_after_the_reversal_sticks():
    assert _is_terminal_rejection(
        "[operator review 2026-09-10: REJECTED — duplicate_of_published: the "
        "same mould recall is already published from the regulator's page]"
    ) is True


def test_an_undated_mould_rejection_stays_retryable():
    """Undated notes predate the operator-review stamp, so they are old."""
    assert _is_terminal_rejection(
        "quality/spoilage — mould on the crust") is False
