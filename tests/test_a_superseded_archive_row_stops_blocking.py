"""A wrong verdict must not be able to make itself unappealable.

WHAT WAS WRONG (found 2026-10-11)
=================================
BAMA's kale recall — Mattilsynet, published 05.10.2026, "funn av
plantevernmiddel over grenseverdi" (pesticide residues above the limit,
squarely in AFTS scope) — was archived on 2026-10-06 by the Norwegian gap
finder as:

    allergen: Undeclared allergen — out of scope (Rule B reject).

The classifier had no Norwegian word for a plant-protection agent at all, so
it reached the allergen branch and refused the row. "allergen" is in
_TERMINAL_REJECTION_MARKERS, which is correct for a real allergen recall and
fatal for a misclassified one: load_rejected_urls turned the mistake into a
PERMANENT verdict. From then on

  * tools/add_manual_row.py could not re-queue the recall. With the
    authority page read and quoted in Notes, it printed only "Nothing
    queued — append_to_pending skipped every row (already approved, or
    already waiting in Pending)" — indistinguishable from a duplicate.
  * nine further rows for the same recall arrived from the news collectors
    between 2026-10-02 and 2026-10-10 and each was archived as
    "no-authority-url: no official Mattilsynet press-release URL in the
    source article" — while the authority URL sat in Weekly_Rejected.

The marker to say "this verdict was wrong" already existed and already had
a meaning everywhere else: "[SUPERSEDED <date> — …]" is what four morning
passes wrote (2026-10-05, -06, -08, -10) and what
test_a_recall_is_not_both_published_and_rejected accepts as declaring which
copy of a shared URL wins. It simply had no effect on re-entry, in EITHER of
the two places that bar it — which is the shape this repo keeps meeting: a
rule that lives in two files gets half-fixed.

WHAT THIS TEST PINS
-------------------
1. _is_terminal_rejection reads SUPERSEDED, so the row can reach Pending.
2. promote_approved's re-promotion guard reads it too, so the row can leave
   Pending. One without the other just moves where it dies.
3. UNRESOLVED does NOT unbar anything — it marks a contradiction nobody has
   decided yet.
4. A plain terminal verdict still bars, so this is not a hole in the guard.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import _is_terminal_rejection      # noqa: E402

ALLERGEN = ("gap_finder/no/rules.py: allergen: Undeclared allergen — out of "
            "scope (Rule B reject). | Discovered via news: mattilsynet.no")


def test_a_plain_allergen_verdict_is_still_terminal():
    assert _is_terminal_rejection(ALLERGEN), (
        "A real allergen rejection must stay permanent — that is what the "
        "marker list is for. If this fails the guard has a hole.")


def test_superseded_defuses_a_terminal_verdict():
    superseded = ("[SUPERSEDED 2026-10-11 - the reject reason was WRONG and "
                  "the recall is now published.] " + ALLERGEN)
    assert not _is_terminal_rejection(superseded), (
        "An archive row a human has explicitly marked SUPERSEDED still "
        "blocks re-ingestion. A misclassification then cannot be appealed "
        "by any means, and add_manual_row reports the refusal as a "
        "duplicate skip.")


def test_unresolved_does_not_defuse_anything():
    unresolved = ("[UNRESOLVED 2026-10-11 - in-scope recall archived by a "
                  "classifier gap. OPERATOR DECISION.] " + ALLERGEN)
    assert _is_terminal_rejection(unresolved), (
        "UNRESOLVED marks a contradiction nobody has decided yet. Only "
        "SUPERSEDED is a decision; treating the two alike would let every "
        "flagged-for-review row back in unreviewed.")


def test_the_promotion_guard_reads_superseded_too():
    """The second bar, in promote_approved. Source-level, because the guard
    is inline in a long function and reaching it needs a whole workbook."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    i = src.find("re-promotion BLOCKED")
    assert i > 0, "the re-promotion guard moved; find it and re-point this test"
    window = src[max(0, i - 9000):i]
    assert '"superseded" in (_prior_reason or "").lower()' in window, (
        "promote_approved's re-promotion guard does not read SUPERSEDED. "
        "_is_terminal_rejection reading it only gets the row into Pending; "
        "without this the row is refused at promotion instead, which is the "
        "same recall lost one step later.")
    assert '_repromote_kind = "superseded"' in window, (
        "the superseded exception must name itself, like the refusal and "
        "reversal exceptions, so the Notes stamp records WHY the row came "
        "back.")
