"""A hazard label in the wrong language is invisible to every subscriber.
Pinned 2026-11-10.

THE FAULT
---------
Pending held a Ministero della Salute (IT) recall of frozen tuna fillets
(IN's Mercato / Selezione Più, lot 76504_01_001) whose Pathogen read

    "Istamina exceeding limit"

and the publish gate held it on rule 10c, in its own words: "Pathogen
'Istamina exceeding limit' matches no word any subscriber alert offers".

The gate was right and the vocabulary was never the gap. "Histamine /
scombrotoxin" has been an offered alert term with the tokens
["histamine", "scombrotox"] all along, and the register has 23 PUBLISHED rows
under that exact label, several of them Italian tuna. "Istamina" is simply
Italian for histamine, and ``scrapers/_models.PATHOGEN_RULES`` matched only
the English spelling — so the Italian label normalised to nothing, survived
to Pending, and stopped at a gate whose purpose is to catch an invented or
unreachable hazard.

A SECOND COPY of the same defect was already archived in Rejected
(2026-10-20, "Tuna fillets") on the same Italian label, so this is a shape
the extractor repeats, not one bad row.

THE RULE
--------
The histamine pattern accepts the spellings the European registers actually
print — istamina (it), histamina (es/pt), histamin (de/sv/no), histamine
(en/fr) — and still refuses every near-miss that merely ends in "-amine".
That last half is the point of this test: "melamine" is a different hazard
this register publishes, and matching it to histamine would be worse than
the fault being fixed.
"""
from __future__ import annotations

import pytest

from scrapers._models import normalize_pathogen

CANON = "Histamine / scombrotoxin"


@pytest.mark.parametrize("raw", [
    "Istamina exceeding limit",                       # the row that was stuck
    "Possible presence of istamina above legal limits",   # its own Reason
    "istamina",
    "Istamine",
    "histamina",                                      # es / pt
    "Histamin",                                       # de / sv / no
    "histamine",                                      # en / fr
    "Histamine",
    "scombrotoxin",
    "scombroid poisoning",
])
def test_every_spelling_the_registers_print_normalises_to_the_canon(raw):
    assert normalize_pathogen(raw) == CANON, (
        f"{raw!r} did not normalise to {CANON!r}. A label the normaliser "
        f"cannot read reaches the publish gate as free text and is held by "
        f"rule 10c as a hazard no subscriber alert can reach.")


@pytest.mark.parametrize("raw", [
    "amine",
    "melamine",                 # a DIFFERENT hazard this register publishes
    "Melamine in milk powder",
    "dimethylamylamine",        # an offered alert term in its own right
    "DMAA",
    "histaminergic",
    "biogenic",
])
def test_a_near_miss_is_never_called_histamine(raw):
    got = normalize_pathogen(raw)
    assert got != CANON, (
        f"{raw!r} normalised to {CANON!r}. Matching anything that ends in "
        f"'-amine' to histamine would mislabel a different hazard, which is "
        f"worse than the fault this pattern was widened to fix.")


def test_the_canonical_label_is_reachable_by_a_subscriber_alert():
    """Rule 10c's half: the value the normaliser emits must be alertable."""
    from tools import alert_vocab as av
    hits = [term for term in av.PATHOGEN
            if av.matches({"Pathogen": CANON}, "pathogen", term)]
    assert hits, (
        f"{CANON!r} is reachable by no offered alert term. Every published "
        f"hazard must be reachable by an alert, or the row is invisible to "
        f"every subscriber.")
    assert "Histamine / scombrotoxin" in hits, hits


def test_melamine_stays_reachable_by_its_own_alert_term():
    """The widened pattern must not have cost melamine its own alert."""
    from tools import alert_vocab as av
    hits = [term for term in av.PATHOGEN
            if av.matches({"Pathogen": "Melamine"}, "pathogen", term)]
    assert hits, "melamine is no longer reachable by any alert term"
