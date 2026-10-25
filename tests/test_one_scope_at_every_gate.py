"""One scope at every gate (operator decision 2026-09-30: "printed scope
everywhere").

The AFTS scope printed on every brief since 2026-07-29: pathogens +
biotoxins + mycotoxins + foreign material + pest + chemical hazards;
visible mould since 2026-09-07; uninspected product since 2026-09-25.
Allergen-only, labelling and quality/spoilage are out.

Before this, the Pending gate refused a foreign-body or chemical row that
arrived with its hazard named, while the publish gate published the same
kind of row when it arrived empty and was enriched later — 38 September rows
came in by that side door. This sweeps every gate with the same cases.
"""
from __future__ import annotations

import pytest

from pipeline._pathogen_scope import is_in_afts_scope, is_in_scope
from pipeline.gap_finder.rules import classify

IN = [
    "Listeria monocytogenes", "Salmonella", "Aflatoxin", "Mold",
    "Foreign material (metal)", "Foreign material (shell fragments)",
    "Cadmium (heavy metal)", "Lead (heavy metal)", "Histamine / scombrotoxin",
    "Undeclared pharmacological ingredient (sildenafil, tadalafil)",
    "Yellow oleander (cardiac glycosides)", "Tropane alkaloids",
    "Uninspected product (hazard not assessed)",
]
OUT = ["Peanut", "Undeclared milk", "Spoilage", "", "—"]


@pytest.mark.parametrize("p", IN)
def test_in_scope(p):
    assert is_in_afts_scope(p), p


@pytest.mark.parametrize("p", OUT)
def test_out_of_scope(p):
    assert not is_in_afts_scope(p), p


def test_tier1_scope_is_unchanged():
    """is_in_scope is also the Tier-1 test; it must NOT widen."""
    assert not is_in_scope("Foreign material (metal)")
    assert is_in_scope("Listeria monocytogenes")


@pytest.mark.parametrize("p", [
    "Foreign material (metal)", "Cadmium (heavy metal)",
    "Yellow oleander (cardiac glycosides)",
])
def test_the_pending_gate_admits_a_named_non_pathogen_hazard(p):
    from pipeline.merge_master import validate_pending_row
    row = {"Date": "2026-09-28", "Source": "BVL (DE)", "Company": "Acme GmbH",
           "Brand": "Acme", "Product": "Mini salami 100 g", "Pathogen": p,
           "Reason": "Recall", "Country": "Germany",
           "URL": "https://www.lebensmittelwarnung.de/x/260928_11_BY_x.html"}
    ok, why = validate_pending_row(row, set())
    assert "pathogen_out_of_scope" not in str(why), (p, why)


def test_the_pending_gate_still_refuses_allergen_only():
    from pipeline.merge_master import validate_pending_row
    row = {"Date": "2026-09-28", "Source": "BVL (DE)", "Company": "Acme GmbH",
           "Brand": "Acme", "Product": "Biscuits 200 g", "Pathogen": "Peanut",
           "Reason": "Undeclared peanut", "Country": "Germany",
           "URL": "https://www.lebensmittelwarnung.de/x/260928_12_BY_y.html"}
    ok, why = validate_pending_row(row, set())
    assert not ok


@pytest.mark.parametrize("reason,cat", [
    ("glass fragments", "foreign_matter"), ("cadmium above limit", "heavy_metal"),
    ("pesticide residue chlorpyrifos", "synthetic_chemical"),
])
def test_the_gap_finder_accepts_the_printed_scope(reason, cat):
    c = classify(reason=reason)
    assert (c.verdict, c.category) == ("accept", cat)


# The checker leg (url_gate_gemini) was removed 2026-09-30 with every other
# Gemini path — operator ruling "no Gemini anywhere". The gates left are the
# Pending gate and the gap-finder classifier, both swept above.


# ── ONE TIER-1 VOCABULARY AT BOTH GATES (2026-10-24) ─────────────────────
#
# The sweeps above check the printed scope CATEGORIES. They did not check
# that the two gates know the same Tier-1 ORGANISMS, and the lexicons had
# drifted: pipeline/_pathogen_scope.ALWAYS_TIER1_KEYWORDS uses the bare
# needle "botulin" and pipeline/gap_finder/rules.PATHOGENS_TIER_1 listed
# only "clostridium botulinum", "botulinum toxin", "botulism",
# "botulismus", "botulisme" and "botulinumtoxin" — none of which is a
# substring of "botulino" or "tossina botulinica", the two forms Italy
# uses.
#
# WHAT IT COST. On 2026-10-22 the Italian gap finder read a Ministero della
# Salute roundup of four recalls, took Pathogen "uova" (eggs) off the
# headline, and archived the suspected-botulinum salsa tartufata recall as
#     "allergen: Undeclared allergen — out of scope (Rule B reject)"
# — the verdict belonging to the sulphite recall in the same article. The
# correct row then sat in Pending, blocked by merge_master's re-promotion
# guard for being "already in Weekly_Rejected".
#
# Three more terms were rejected outright by the same sweep: "shiga toxin"
# (only the hyphenated and "-producing" forms were listed), "vulnificus"
# and "sakazakii". All four added 2026-10-24.
_BARE_SEROGROUPS = frozenset({
    # Deliberately NOT required of the gap-finder lexicon. These never
    # appear alone on a notice — the real shape is "E. coli O157", which IS
    # listed, and a bare "escherichia coli" is accepted at Tier 2 and
    # escalated by _pathogen_scope.enforce_tier1 — while a three-character
    # needle like "o26" matches inside lot and reference codes.
    "o157", "o104", "o121", "o26", "o45", "o103", "o111", "o145",
})


def _always_tier1_terms():
    from pipeline._pathogen_scope import ALWAYS_TIER1_KEYWORDS
    return [k for k in ALWAYS_TIER1_KEYWORDS if k not in _BARE_SEROGROUPS]


@pytest.mark.parametrize("term", _always_tier1_terms())
def test_the_gap_finder_never_rejects_an_always_tier1_hazard(term):
    """A hazard the publish gate forces to Tier 1 may not be REJECTED here.

    Accepted at Tier 2 is fine — enforce_tier1 escalates it downstream.
    Rejected, or classified "unknown", means a news-discovered notice
    naming that hazard can never reach the register while a scraped one
    always can: the asymmetry every note in gap_finder/rules.py is about.
    """
    c = classify(pathogen=term, reason="recall", product="food")
    assert c.verdict == "accept", (
        f"{term!r} is in ALWAYS_TIER1_KEYWORDS but the gap finder returns "
        f"{c.verdict}/{c.category} — add the needle to "
        f"pipeline/gap_finder/rules.PATHOGENS_TIER_1, or list it in "
        f"_BARE_SEROGROUPS here with the reason")


@pytest.mark.parametrize("text", [
    # The exact row text that was archived as an allergen.
    "Sospetto botulino: richiamata salsa tartufata",
    "botulino",
    "tossina botulinica",
    "sospetta presenza di tossina botulinica",
    # Iberian and Romanian spellings of the same adjective.
    "toxina botulínica",
    "toxină botulinică",
    # The three swept with it.
    "Shiga toxin detected in the batch",
    "Vibrio vulnificus in raw oysters",
    "Enterobacter sakazakii in infant formula",
])
def test_a_tier1_hazard_in_the_rows_own_text_beats_an_allergen_word(text):
    """Pathogen says "uova"; the row's own text says botulinum.

    The allergen reject is checked LAST among rejects precisely so a named
    pathogen wins — it only ever won when the lexicon could see the
    pathogen. With Pathogen holding another recall's allergen word, the
    hazard named in the row's own Product/Reason text must still decide.
    """
    c = classify(pathogen="uova", reason="", product=text)
    assert c.verdict == "accept", (
        f"{text!r} classified {c.verdict}/{c.category} on matched_term "
        f"{c.matched_term!r} — a Tier-1 hazard in the row's own text was "
        f"refused as an allergen")
    assert c.tier == 1, (text, c.tier, c.matched_term)


def test_an_allergen_only_recall_is_still_rejected():
    """The widening must not swallow the allergen reject itself."""
    c = classify(pathogen="uova", reason="allergene non dichiarato: uova",
                 product="Pasta all'uovo 500 g")
    assert (c.verdict, c.category) == ("reject", "allergen"), (
        c.verdict, c.category, c.matched_term)
