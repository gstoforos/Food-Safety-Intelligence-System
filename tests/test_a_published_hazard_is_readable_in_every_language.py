# -*- coding: utf-8 -*-
"""A hazard the register publishes must be readable by the gap finder —
in the languages the regulators actually write it in.

WHY THIS FILE EXISTS
--------------------
Found by the morning fix pass of 2026-11-11.
tests/test_a_recall_is_not_both_published_and_rejected failed on main with
two rows, and one of them was the Ministero della Salute tuna recall of
2026-11-03: PUBLISHED as "Histamine / scombrotoxin", Tier 1, and
simultaneously archived to Weekly_Rejected as

    "allergen: Undeclared allergen — out of scope (Rule B reject)"

with Pathogen "mostarda". The Italian gap finder had rewritten that
rejection on every run since, because nothing in
pipeline/gap_finder/rules.py could see the word "istamina". Only the long
phrase "intossicazione da istamina" was listed, and a headline never says
"intossicazione". With the real hazard invisible, the only lexicon left
that matched anything in the article was the ALLERGEN list — matching a
SECOND recall mentioned in the same article ("mostarda") — so an in-scope
Tier-1 recall was thrown away as an allergen.

That is the shape tests/test_import_eligibility_is_in_gap_finder_scope.py
documents (mould 2026-09-07, foreign matter 2026-09-30, import
eligibility 2026-10-16) with one extra turn of the screw: there the
English wording was missing too, so the gap was visible in English. Here
English was fine and only the other languages were blind, which is worse,
because the English self-tests in rules.py all passed.

The same sweep, run over every distinct Pathogen the Recalls sheet
publishes, found three more instances of it:

  · "Staphylococcus enterotoxin" — 7 published rows. classify() could
    only see the 3 rows that spell out "staphylococcus aureus"; the
    toxin itself was in no lexicon, in any language.
  · "Pseudomonas aeruginosa" — 1 published row. The publish gate and
    tools/alert_vocab.py both took it on 2026-10-29; this gate was not
    told. "Stopped one gate short", again.
  · Hydrocyanic acid — "hydrocyanic" lived in the publish gate's
    'chemical' class and NOT in this lexicon, so French "Teneur en acide
    cyanhydrique dépassée" matched nothing. That would have cost two rows
    the same morning: RappelConso fiches 24053 and 24054 (Biocoop brown
    flaxseed, published 10/11/2026) carry exactly that motif.

So this file asks two questions the English self-tests cannot:

  1. For each hazard family repaired here, does a REAL regulator sentence
     in each of the languages that regulator writes in classify as
     accept? (Table below — the sentences are the wordings the sources
     actually use, not invented strings.)
  2. Over the whole published register: does any hazard the register
     publishes classify as the ALLERGEN reject? That verdict means a
     published recall can be re-archived as out of scope, which is the
     contradiction the companion test catches one day later.

Question 2 is the invariant. It widens by itself as the register grows.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.gap_finder.rules import classify

try:
    import openpyxl
except ImportError:                                      # pragma: no cover
    openpyxl = None

XLSX = Path(__file__).resolve().parents[1] / "docs" / "data" / "recalls.xlsx"


# ── 1. the repaired families, in the languages the sources write them in ──
#
# Each sentence is the shape a regulator or its press really uses. The
# comment names where that wording comes from.
HAZARD_SENTENCES = [
    # Histamine / scombrotoxin — 24 published rows.
    ("Tonno richiamato per istamina: nuovo lotto segnalato",      # IT press
     "natural_toxin"),
    ("Histamin in Thunfisch nachgewiesen",                        # DE/AT/CH
     "natural_toxin"),
    ("Retirada por histamina en atun en conserva",                # ES AESAN
     "natural_toxin"),
    ("Rappel pour histamine dans le thon",                        # FR
     "natural_toxin"),
    ("Recall due to histamine in tuna fillets",                   # EN control
     "natural_toxin"),
    # Hydrocyanic acid — the hazard on fiches 24053 / 24054.
    ("Teneur en acide cyanhydrique depassee",                     # FR fiche
     "natural_toxin"),
    ("Superamento del limite di acido cianidrico",                # IT Salute
     "natural_toxin"),
    ("Blausaure in Leinsamen ueber dem Grenzwert",                # DE BVL
     "natural_toxin"),
    ("Hydrocyanic acid above the legal limit in linseed",         # EN control
     "natural_toxin"),
    # Staphylococcal enterotoxin — 10 published rows.
    ("Staphylococcal enterotoxin detected in cheese",             # EN
     "microbial_toxin"),
    ("Enterotossina stafilococcica nel formaggio",                # IT
     "microbial_toxin"),
    ("Enterotoxina estafilococica detectada en queso",            # ES
     "microbial_toxin"),
    # Pseudomonas aeruginosa — in scope at the publish gate since
    # 2026-10-29.
    ("Potential presence of Pseudomonas aeruginosa",              # EN
     "pathogen"),
    ("Presenza di Pseudomonas aeruginosa in acqua minerale",      # IT
     "pathogen"),
]


@pytest.mark.parametrize("sentence,category", HAZARD_SENTENCES,
                         ids=[s[:34] for s, _ in HAZARD_SENTENCES])
def test_a_regulator_sentence_classifies_as_accept(sentence, category):
    c = classify(sentence, sentence)
    assert c.verdict == "accept", (
        f"classify() rejected a hazard the register publishes:\n"
        f"    {sentence!r}\n"
        f"  verdict={c.verdict} category={c.category} rule={c.rule}\n\n"
        "  A hazard that is in scope in English must be in scope in the "
        "language its regulator writes in. Add the stem to the lexicon in "
        "pipeline/gap_finder/rules.py — do not widen the allergen reject."
    )
    assert c.category == category, (
        f"{sentence!r} classified as {c.category!r}, expected {category!r} "
        f"(matched {c.matched_term!r})"
    )


# ── 2. these must still be rejected: the fix must not swallow allergens ──
ALLERGEN_SENTENCES = [
    "Recalled for undeclared mustard",
    "Undeclared milk allergen in the product",
    "Presence de l'allergene LAIT non etiquete",
    "Nicht gekennzeichnetes Allergen Haselnuss",
]


@pytest.mark.parametrize("sentence", ALLERGEN_SENTENCES,
                         ids=[s[:34] for s in ALLERGEN_SENTENCES])
def test_an_allergen_only_recall_is_still_out_of_scope(sentence):
    c = classify(sentence, sentence)
    assert c.verdict == "reject" and c.category == "allergen", (
        f"{sentence!r} should still be an allergen reject, got "
        f"{c.verdict}/{c.category} (matched {c.matched_term!r}). "
        "Allergen-only is out of scope by Rule B; a lexicon addition that "
        "accepts one has over-reached."
    )


# ── 3. the invariant: no PUBLISHED hazard may be an allergen reject ──────
#
# A published hazard that classify() calls an allergen is a recall the
# gap finder will archive as out of scope while the register shows it —
# the published-and-rejected contradiction, one day before
# test_a_recall_is_not_both_published_and_rejected can see it.
#
# This reads the Pathogen column only. That is on purpose: the Pathogen
# value is the register's own name for the hazard, so if the gate cannot
# read THAT, it cannot read the notice either.
def _published_hazards() -> list[str]:
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    try:
        ws = wb["Recalls"]
        rows = ws.iter_rows(values_only=True)
        head = [str(h or "") for h in next(rows)]
        i = head.index("Pathogen")
        seen, out = set(), []
        for r in rows:
            v = str(r[i] or "").strip()
            if v and v.lower() not in seen:
                seen.add(v.lower())
                out.append(v)
        return out
    finally:
        wb.close()


@pytest.mark.skipif(openpyxl is None or not XLSX.exists(),
                    reason="workbook not available")
def test_no_published_hazard_classifies_as_an_allergen_reject():
    offenders = []
    for hazard in _published_hazards():
        c = classify(hazard, hazard)
        if c.category == "allergen":
            offenders.append((hazard, c.matched_term))
    assert not offenders, (
        "Hazard(s) the register PUBLISHES classify as an allergen reject in "
        "pipeline/gap_finder/rules.py:\n"
        + "\n".join(f"    {h!r} matched allergen term {t!r}" for h, t in offenders)
        + "\n\n  The gap finder will re-archive these recalls as out of "
          "scope while the register shows them. Add the hazard's own stem to "
          "the right lexicon so it is matched BEFORE the allergen reject — "
          "which is last on purpose — rather than narrowing the allergen "
          "list."
    )
