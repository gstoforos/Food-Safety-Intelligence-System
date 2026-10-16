# -*- coding: utf-8 -*-
"""The gap finder must not reject what the publish gate publishes.

WHY THIS FILE EXISTS
--------------------
Found by the morning fix pass of 2026-10-16. Four FSIS IMPORT-ELIGIBILITY
notices were sitting in the archives rejected as

    "allergen: Undeclared allergen — out of scope (Rule B reject)"

by pipeline/gap_finder/rules.py, while the scraper path had PUBLISHED every
one of them:

    asian-america-trading   RTE Siluriformes fish products, China not
                            eligible to export them
    el-eden-import          chips with chicharrón, Colombia not eligible
    sempio-food             RTE chicken stew, no import reinspection
    shanghai-ravioli        produced without inspection, false EST. mark

Uninspected product went into scope on 2026-09-25 and import violations on
2026-09-30. The decision reached pipeline/_publish_gate.py,
pipeline/merge_master.py, pipeline/enrich_schema.py, tools/alert_vocab.py,
tools/apps_script/AftsAlerts.gs and pipeline/gap_finder/countries/us.py —
and stopped one gate short of classify(). FSIS's own hazard sentence reads
"may contain undeclared allergens, harmful bacteria, or other
contaminants", and "Siluriformes FISH products" puts an allergen lexicon
word in the title, so the allergen reject was the only branch left.

That is the THIRD time this exact shape has bitten: mould (in scope
2026-09-07, this classifier told 2026-09-30) and foreign matter (in scope
on every printed brief since 2026-07-29, this classifier told 2026-09-30).
A news-discovered recall could not reach the register while a scraped one
could.

So this file does not test four URLs. It tests the INVARIANT that makes the
shape impossible: for every hazard class the publish gate recognises as
in-scope, a Reason built from that class's own vocabulary must not be
rejected by classify() as out of scope. Widen the gate and this test
widens with it.

Sweeping that invariant on 2026-10-16 found FIVE more live gaps beyond the
four FSIS rows — histamine, pest, MOAH, and worst, undeclared MEDICINES,
where "undeclared" sent every sildenafil supplement recall to the allergen
reject while the register had been publishing them (X10 Natural
Enhancement and biQ-FEL, both 2026-09-15).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import HAZARD_CLASS_KEYWORDS      # noqa: E402
from pipeline.gap_finder.rules import (                       # noqa: E402
    UNINSPECTED,
    classify,
)

#: Classes the printed AFTS scope excludes. Allergen-only is out by Rule B;
#: unintended fermentation / spoilage is a quality defect.
OUT_OF_SCOPE_GATE_CLASSES = {"allergen", "fermentation"}

IN_SCOPE_GATE_CLASSES = sorted(
    set(HAZARD_CLASS_KEYWORDS) - OUT_OF_SCOPE_GATE_CLASSES)

#: Classes this pass closed COMPLETELY — every keyword the gate carries
#: reaches an accept. These are asserted at zero and must stay there.
CLOSED_GATE_CLASSES = ("uninspected", "pest", "biotoxin", "chemical",
                       "mould")

#: Classes with keywords this classifier still cannot see. A RATCHET, not
#: an acceptance: the number may only go DOWN. Measured 2026-10-16 after
#: the fix, having started at 72 keywords across three classes.
#:
#: Checked one by one on 2026-10-16: all 28 are BARE STEMS OR SHORT FORMS
#: that the probe sentence below exercises in isolation and that no real
#: regulator notice uses alone. Each is reached here through a fuller form
#: the classifier does carry — "escherichia" through "escherichia coli",
#: "staphylococcus" through "staphylococcus aureus", "perfringens" through
#: "clostridium perfringens", "shiga toxin" through "stec", "norwalk"
#: through "norwalk virus", "ht-2" through "t-2 toxin", " don " through
#: "deoxynivalenol". Two are deliberate refusals the gate shares: " hav "
#: (a bare three-letter needle matched the word "have" in every FSIS
#: notice — audit 2026-08-31) and the "suspected/unspecified
#: microbiological" family (a bare microbial claim naming no organism stays
#: unknown here exactly as it does at the gate).
#:
#: So the remaining 28 are believed to be probe artefacts, not live gaps —
#: but "believed" is why the number is locked instead of the list being
#: deleted. If a later pass proves one live, fix it and lower the number.
MAX_KEYWORDS_STILL_UNSEEN = 28


def test_the_gate_still_has_the_classes_this_test_reasons_about():
    """A guard on the guard: if someone renames a gate class, this file
    must fail loudly rather than silently test nothing."""
    assert "uninspected" in HAZARD_CLASS_KEYWORDS
    assert OUT_OF_SCOPE_GATE_CLASSES <= set(HAZARD_CLASS_KEYWORDS)
    assert len(IN_SCOPE_GATE_CLASSES) >= 8


def test_the_gap_finder_reads_uninspected_from_the_gate_not_a_copy():
    """The two gates must not be able to drift. A hand-copied lexicon is
    how 'uninspected' came to exist in five places and not in the sixth."""
    assert UNINSPECTED == set(HAZARD_CLASS_KEYWORDS["uninspected"])


def _unseen(gate_class):
    """Keywords of `gate_class` that classify() does NOT accept.

    The keyword goes in the Reason, which is where a regulator states the
    hazard and the only field (with Pathogen) that classify() matches its
    lexicons against.
    """
    out = []
    for kw in sorted(HAZARD_CLASS_KEYWORDS[gate_class]):
        reason = "Recalled due to %s found in the product." % kw.strip()
        c = classify(pathogen="", reason=reason, product="")
        if c.verdict != "accept":
            out.append((kw, c.category, c.rule))
    return out


@pytest.mark.parametrize("gate_class", CLOSED_GATE_CLASSES)
def test_closed_gate_classes_are_fully_visible_to_the_gap_finder(gate_class):
    """Every keyword the gate would publish on must reach an ACCEPT here."""
    rejected = _unseen(gate_class)
    assert not rejected, (
        "%d keyword(s) of the publish gate's in-scope class %r are "
        "REJECTED by pipeline/gap_finder/rules.py classify(), so a "
        "news-discovered recall naming them can never reach the register "
        "while a scraped one can:\n    %s"
        % (len(rejected), gate_class,
           "\n    ".join("%r -> %s (%s)" % r for r in rejected[:12]))
    )


def test_the_remaining_gate_drift_does_not_grow():
    """A ratchet. The gap finder is still blind to some of the gate's
    keywords; the count may only go DOWN. Adding a keyword to the gate
    without teaching the classifier fails here, which is the whole point —
    this drift is how mould (2026-09-07), foreign matter (2026-07-29) and
    uninspected product (2026-09-25) each spent weeks publishable by the
    scrapers and unreachable by the fleet."""
    unseen = {c: _unseen(c) for c in IN_SCOPE_GATE_CLASSES}
    total = sum(len(v) for v in unseen.values())
    assert total <= MAX_KEYWORDS_STILL_UNSEEN, (
        "gate/gap-finder drift GREW from %d to %d keywords. Either teach "
        "pipeline/gap_finder/rules.py the new hazard wording, or — if the "
        "refusal is deliberate, as it is for a bare ' hav ' — say so in "
        "the note above MAX_KEYWORDS_STILL_UNSEEN and raise the number "
        "with the reason.\n%s"
        % (MAX_KEYWORDS_STILL_UNSEEN, total,
           "\n".join("    %-12s %d  %s" % (k, len(v), [x[0] for x in v])
                     for k, v in sorted(unseen.items()) if v))
    )
    # NO failure when the count comes in LOWER. A ratchet that fails on an
    # improvement is a ratchet that blocks the upload gate for doing the
    # right thing — and this one would have: run alone the count is 28,
    # run inside the full suite it is 27, because another test in the
    # suite mutates one of these lexicons in process. So the bound is an
    # UPPER bound only. Lowering it is welcome but never required.


# ── the four rows that started it, as regression cases ──────────────────
FSIS_IMPORT_ELIGIBILITY = [
    pytest.param(
        "Uninspected product (hazard not assessed)",
        "Produced without the benefit of inspection and bearing a false "
        "establishment number, “EST. 18004”.",
        "Frozen, not-ready-to-eat buffalo chicken products",
        id="shanghai-ravioli",
    ),
    pytest.param(
        "",
        "Ready-to-eat chicken stew products imported from the Republic of "
        "Korea without the benefit of import reinspection.",
        "Korean Ginseng ready-to-eat chicken stew",
        id="sempio-food",
    ),
    pytest.param(
        "",
        "Products were produced in the Republic of Colombia, a country "
        "that is not eligible to export meat products to the United "
        "States.",
        "De Todito NATURAL and BBQ mixed chips with chicharrón",
        id="el-eden-import",
    ),
    pytest.param(
        "",
        "Produced in The People's Republic of China, a country that is not "
        "eligible to export ready-to-eat Siluriformes fish products to the "
        "United States.",
        "Ready-to-eat fish skin crackers, 50 g (Pangasius)",
        id="asian-america-trading",
    ),
]


@pytest.mark.parametrize("pathogen,reason,product", FSIS_IMPORT_ELIGIBILITY)
def test_fsis_import_eligibility_notices_are_accepted(pathogen, reason,
                                                      product):
    c = classify(pathogen=pathogen, reason=reason, product=product)
    assert c.verdict == "accept", (
        "an FSIS import-eligibility notice classified %s/%s (%s) — these "
        "are in scope since 2026-09-25 and all four are PUBLISHED in the "
        "register" % (c.verdict, c.category, c.matched_term))
    assert c.category == "uninspected", (
        "classified as %r, not 'uninspected' — a notice whose only hazard "
        "statement is 'not inspected' must not borrow another category"
        % c.category)


def test_an_undeclared_medicine_is_not_read_as_an_allergen_recall():
    """'undeclared' is an allergen lexicon word. The register has been
    publishing undeclared-sildenafil supplement recalls since at least
    2026-09-15 (X10 Natural Enhancement, biQ-FEL); the gap finder was
    rejecting every one it discovered."""
    c = classify(
        pathogen="Sildenafil",
        reason="Contains the undeclared prescription drug sildenafil.",
        product="Mixed Herbal Blend Paste, 240 g food supplement")
    assert c.verdict == "accept" and c.category != "allergen", (
        "undeclared-medicine recall classified %s/%s" % (c.verdict,
                                                         c.category))


# ── what must still be REJECTED (the branches are not a free-for-all) ───
STILL_OUT = [
    pytest.param("", "Presence of undeclared allergen: wheat flour "
                 "(gluten).", "cinnamon biscuits", id="allergen-only"),
    pytest.param("", "Missing best-before date on the package.",
                 "dried bonito pack", id="missing-date-label"),
    pytest.param("", "Food labelling sticker not affixed.",
                 "salmon sashimi", id="label-not-affixed"),
    pytest.param("", "The product may not be properly sealed.",
                 "baumkuchen", id="seal-defect"),
    pytest.param("", "A refrigerated product requiring 10°C or below "
                 "was sold at ambient temperature.", "egg tart",
                 id="temperature-abuse"),
]


@pytest.mark.parametrize("pathogen,reason,product", STILL_OUT)
def test_out_of_scope_recalls_are_still_rejected(pathogen, reason, product):
    c = classify(pathogen=pathogen, reason=reason, product=product)
    assert c.verdict == "reject", (
        "an out-of-scope recall was ACCEPTED as %s (%s) — the 2026-10-16 "
        "branches widened the classifier too far"
        % (c.category, c.matched_term))


def test_a_named_pathogen_still_decides_the_category():
    """The new branches run after the pathogen accepts, so an import
    re-inspection sentence on a Listeria row stays a Listeria row — the
    Prime Line case that pipeline/_publish_gate.py's note warns about."""
    c = classify(
        pathogen="Listeria monocytogenes",
        reason="Confirmed by FSIS routine import re-inspection sampling.",
        product="ready-to-eat deli meat")
    assert c.verdict == "accept"
    assert c.category == "pathogen", (
        "a Listeria row classified as %r — a named pathogen must outrank "
        "every hazard-class branch" % c.category)
