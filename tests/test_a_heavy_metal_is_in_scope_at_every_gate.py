"""A heavy metal is in scope at EVERY gate, not at some of them. 2026-11-08.

THE ROW THAT FOUND IT
---------------------
FDA, 2026-11-03: "Snack Right Super Foods, Inc., doing business as Brad's Raw
Chips Recalls Brad's Crunchy Kale & Veggie Chips Due to Potential Thallium
Contamination", reason field "Product may contain thallium". Read on the FDA's
own page by the morning fix pass of 2026-11-08, which filled the Date and the
Pathogen the row had arrived without.

Heavy metals are in the AFTS scope printed on every daily brief and weekly
report. Thallium was in scope at NONE of the places that decide:

  * ``_publish_gate.classify_hazard("Thallium")`` returned the empty set, so
    ``_pathogen_scope.is_in_afts_scope`` said False and
    ``tests/test_curator_scope_2026_09_09.py`` went red the moment the row was
    promoted — the curator refuses the hazard of a published row, and a row
    whose hazard the curator refuses is a row it cannot correct a typo on.
  * ``tools/alert_vocab.py`` "Heavy metals" held cadmium, kadmium, lead,
    mercury, arsenic and molybdenum, so no subscriber alert could have
    reached a thallium recall. That half is held by
    ``tests/test_a_published_hazard_is_reachable_by_an_alert.py`` and
    ``tests/test_alert_vocab.py``; this file holds the scope half and the
    agreement between the two lists.

THE SHAPE IS WIDER THAN ONE METAL
---------------------------------
Measured the same morning by pushing metal names through
``is_in_afts_scope`` with an empty Reason:

    mercury  cadmium  arsenic           in scope
    lead                                in scope only as "Lead (heavy metal)"
                                        or in a qualified Reason form, on
                                        purpose — see the 2026-08-02 note in
                                        _publish_gate
    thallium nickel tin antimony
    chromium copper aluminium
    barium   cobalt                     OUT OF SCOPE

``merge_master``'s own in-scope token list has carried "nickel" in its
"metals and contaminants" block since it was written, so the two gates had
disagreed about nickel for as long as both existed. Thallium and nickel are
now in both. The rest are deliberately NOT added — no row has named one, and
this repo adds vocabulary on evidence and records the evidence.

WHAT MUST NOT HAPPEN
--------------------
"tin" must never become a bare token in the chemical class. That class is
matched against Reason as well as Pathogen, and the register is full of
"180 g tin", "sold in tins" and "swelling of some tins". The same reasoning
already keeps a bare " lead " out (audit 2026-08-02, where
"…can lead to poor preservation" classified as a chemical hazard and
manufactured a contradiction against a correct Listeria row).
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

from pipeline._pathogen_scope import is_in_afts_scope  # noqa: E402
from pipeline._publish_gate import classify_hazard     # noqa: E402

#: Named on a regulator's page and published by this register.
IN_SCOPE_BARE = ("Mercury", "Cadmium", "Arsenic", "Thallium", "Nickel")

#: The register's own spelling for a metal hazard.
IN_SCOPE_LABELLED = ("Lead (heavy metal)", "Cadmium (heavy metal)",
                     "Thallium (heavy metal)")

#: Words that must not classify as a chemical hazard on their own, because
#: they are ordinary packaging and prose and this class is matched against
#: Reason too.
NEVER_A_HAZARD_ON_ITS_OWN = (
    "Sardinella pâté, Pinton, 180 g tin",
    "Organic extra-fine green beans, 660 g jar",
    "Swelling of some tins caused by a seaming fault",
    "Poor refrigeration can lead to microbial growth",
    "sold in tins of 200 g",
)


@pytest.mark.parametrize("pathogen", IN_SCOPE_BARE + IN_SCOPE_LABELLED)
def test_a_metal_is_in_scope(pathogen):
    assert is_in_afts_scope(pathogen, ""), (
        f"{pathogen!r} is a heavy metal and heavy metals are in the printed "
        f"AFTS scope, but is_in_afts_scope refuses it. A hazard the scope "
        f"gate refuses is a hazard the curator cannot correct a typo on, and "
        f"tests/test_curator_scope_2026_09_09.py goes red the moment such a "
        f"row is published. Add the token to the 'chemical' class in "
        f"pipeline/_publish_gate.py.")


@pytest.mark.parametrize("pathogen", IN_SCOPE_BARE + IN_SCOPE_LABELLED)
def test_a_metal_classifies_as_chemical(pathogen):
    assert "chemical" in classify_hazard(pathogen), (
        f"{pathogen!r} must classify as a CHEMICAL hazard, not merely scrape "
        f"past is_in_afts_scope by some other route — the class is what the "
        f"publish gate, the curator and the HazardGroup enrichment all read.")


@pytest.mark.parametrize("text", NEVER_A_HAZARD_ON_ITS_OWN)
def test_packaging_and_prose_are_not_chemical_hazards(text):
    assert "chemical" not in classify_hazard(text), (
        f"{text!r} classified as a chemical hazard. The chemical class is "
        f"matched against Reason as well as Pathogen, so a bare metal or "
        f"material token here invents hazards out of packaging and prose — "
        f"which is why a bare ' lead ' was removed on 2026-08-02 and why "
        f"'tin' must never be added.")


def test_the_two_gates_agree_about_metals():
    """merge_master's in-scope tokens and the publish gate's chemical class.

    The two lists are written and maintained separately, which is how nickel
    sat in one and not the other for months. Every metal either names must be
    in scope by the shared gate.
    """
    src = (ROOT / "pipeline" / "merge_master.py").read_text("utf-8")
    block = re.search(r"# metals and contaminants\n(.*?)\n\s*# physical",
                      src, re.S)
    assert block, ("the 'metals and contaminants' block has moved in "
                   "merge_master.py — re-point this test at it rather than "
                   "deleting it")
    tokens = set(re.findall(r'"([^"]+)"', block.group(1)))
    metals = {t for t in tokens
              if t in {"cadmium", "lead", "mercury", "arsenic", "nickel",
                       "thallium", "tin", "antimony", "chromium"}}
    assert metals, "no metal tokens found — has the block been reworded?"

    refused = sorted(m for m in metals
                     if m != "lead" and not is_in_afts_scope(m.title(), ""))
    assert not refused, (
        f"merge_master's in-scope token list names {refused} as metals worth "
        f"ingesting, but the publish gate's chemical class refuses them, so a "
        f"row carrying one as its Pathogen is ingested and then held — or "
        f"published and then refused by the curator. The two lists must agree. "
        f"('lead' is excluded here on purpose: the bare token was removed from "
        f"the chemical class on 2026-08-02 and the register spells it "
        f"'Lead (heavy metal)'.)")
