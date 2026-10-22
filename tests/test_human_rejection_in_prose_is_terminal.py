"""Regression: an operator's rejection written in PROSE is terminal.

2026-10-22 — measured on the live workbook (main at 800d0398): of the 123
archive rows whose RejectedBy names an operator review or a morning-fix
pass, 70 were classified RETRYABLE by _is_terminal_rejection.

Cause. The function recognised a human decision through exactly one route,
_latest_operator_verdict(), whose regex demands a machine-shaped stamp:

    operator review <date>: REJECTED — <code> — <prose>

An operator writing an ordinary sentence never produces that shape, so the
verdict fell through to a keyword vocabulary written for classifier output
("not_food", "out_of_scope", "duplicate", "allergen") and matched nothing.

The two rows that found it, both stamped `operator review 2026-10-21`:

    Rejected idx2204  "url cites a different recall: this row is about
      CARREFOUR COD FISH FILLET ... and its own URL is the Ministero della
      Salute recall form for TAHINA ARKAN — tahini."
    Rejected idx2205  "url cites a different recall: ... PEDIAVIT
      COMPLESSO B SCIROPPO, lot 26040043 — a vitamin-B syrup."

Why it matters. load_rejected_urls() keeps ONE description per URL under the
rule "a TERMINAL verdict outranks a transient one". idx2204's human verdict
was filed as transient, so it LOST precedence to Weekly_Rejected idx304's
classifier line. That line is terminal too, so the row held — by luck. Had
the competing line named a repairable defect ("Pathogen is empty", "No
matching hazard category"), the first re-promotion exception in
promote_approved() would have tested THAT string, found a repairable defect,
confirmed the row passes the full publish gate — promote_gate_passing reports
it gate-passing on this very workbook — and republished a recall a human had
read the authority page for and refused the day before.

The guard must stay NARROW, and every escape hatch that existed before this
change still wins, because all of them are tested earlier in the function:
SUPERSEDED, a reversed policy, an operator APPROVED stamp, and a transient
marker left in the same note.
"""
from __future__ import annotations

import re

import pytest

from pipeline.merge_master import _is_human_rejection, _is_terminal_rejection


# The two live rows, verbatim enough to carry the shape.
LIVE_2026_10_21 = [
    'operator review 2026-10-21: url cites a different recall: this row is '
    'about CARREFOUR COD FISH FILLET (Company and Brand "Carrefour", Reason '
    '"Non conformity microbiological", Date 2026-10-19), and its own URL is '
    "the Ministero della Salute recall form for TAHINA ARKAN — tahini.",
    'operator review 2026-10-21: url cites a different recall: this row is '
    'about TUNA FILLETS and histamine (Company "IN\'s Mercato"), and its own '
    "URL is the Ministero della Salute recall form for PEDIAVIT COMPLESSO B "
    "SCIROPPO, lot 26040043 — a vitamin-B syrup.",
]

# Human rejections in prose that name no classifier keyword at all.
HUMAN_PROSE_TERMINAL = LIVE_2026_10_21 + [
    "operator review 2026-10-19: not a recall: the URL is Livsmedelsverket's "
    "shellfish production-AREA classification page for Edsviken.",
    "morning fix 2026-10-12: wrong_url: the row is an October 2026 news "
    "headline and the URL is an FDA outbreak investigation page.",
    # Every spelling of the label the live archives use.
    "operator-review-2026-08-28: Cited fiche is a plush toy, not a food.",
    "operator-review: the ministry page names a different firm.",
    "morning fix pass 2026-10-17: the slug is a headline, not a page.",
    "OPERATOR REVIEW 2026-09-04: the notice covers a different lot.",
]

# Everything that must STILL be retryable. Each is defended by a check that
# runs before the new one.
STILL_RETRYABLE = [
    # SUPERSEDED — a later pass retired the verdict.
    "operator review 2026-08-20: [SUPERSEDED 2026-09-28 — the defect named "
    "below was repaired and this recall is PUBLISHED in Recalls.] "
    "not_a_notice — FSIS page",
    # A transient marker in the same note.
    "operator review 2026-08-14: REJECTED: http_error",
    "operator-review-2026-08-28: Broken provenance. Cited fiche 22230 was "
    "opened on 2026-08-28 and is a SHEIN donkey plush toy.",
    "operator review 2026-09-02: the link did not resolve, retry tomorrow",
    "operator review 2026-09-02: host unreachable from this runner",
    # An operator APPROVED stamp outranks any rejection label beside it.
    "a reviewer:  | FDA HTML fallback [operator review 2026-08-31: APPROVED "
    "— verified_on_authority_page — read the notice]",
    # No human stamp at all: a repairable defect stays repairable.
    "unknown: Confirmer: row was at pending and Pathogen is empty",
    "a reviewer: URL agent: No official recall page found",
    "gap_finder/no/rules.py: No matching hazard category — defer to manual "
    "review.",
]


@pytest.mark.parametrize("desc", HUMAN_PROSE_TERMINAL)
def test_human_rejection_in_prose_is_terminal(desc):
    assert _is_human_rejection(desc), (
        "the label was not recognised as a human rejection: " + desc[:90])
    assert _is_terminal_rejection(desc) is True, (
        "a person read the notice and refused this row; the guard calls it "
        "retryable: " + desc[:120])


@pytest.mark.parametrize("desc", STILL_RETRYABLE)
def test_the_escape_hatches_still_win(desc):
    assert _is_terminal_rejection(desc) is False, (
        "this must stay retryable — the new human-stamp rule is too wide: "
        + desc[:120])


def test_the_label_is_not_matched_in_ordinary_hazard_text():
    """The label must not fire on words a recall notice legitimately uses.

    _is_terminal_rejection is handed bare Notes in places, so the pattern is
    searched anywhere in the string rather than anchored. It therefore has to
    be specific enough not to match a Product, Reason or Company value.
    """
    for innocent in (
            "Operator error in the packing line was ruled out",
            "reviewed by the operator of the establishment",
            "morning goods: croissants and pains au chocolat",
            "Morning Fresh brand orange juice",
            "operators of food business reviewed their HACCP plan",
    ):
        assert not _is_human_rejection(innocent), (
            "the human-rejection label matched ordinary text: " + innocent)


def test_the_live_archive_has_no_human_rejection_filed_as_transient_vocabulary():
    """No human prose rejection may rely on the keyword vocabulary.

    Guards the regression directly: strip every escape hatch and the verdict
    must still come out terminal.
    """
    hatch = re.compile(
        r"superseded|http_error|did not resolve|unreachable|timeout|"
        r"broken provenance|not rejected|approved", re.IGNORECASE)
    for desc in LIVE_2026_10_21:
        assert not hatch.search(desc), "test fixture drifted"
        assert _is_terminal_rejection(desc) is True
