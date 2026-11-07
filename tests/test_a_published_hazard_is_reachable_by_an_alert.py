# -*- coding: utf-8 -*-
"""A hazard no subscriber alert can match never reaches Recalls.

THE INCIDENT (2026-11-07)
=========================
At 03:45 UTC the offline promoter published, with the report line "passes
every publish-gate rule":

    Date      2026-11-06
    Source    BVL (DE)
    Company   Norderstedter Fleisch- und Wurstwaren Peter Faden GmbH & Co KG
    Product   Rinder-Peperonisalami, Rindfleischsalami, Rindfleischsalami
              rein Rind, Pizza Rindfleischsalami
    Pathogen  Bacillus
    Reason    Bakterienfund
    Tier      1

"Bacillus" is a bare genus with no species. It is not on the regulator's
page, not in the firm's press release, and not anywhere in the row's own
text: the German news headline the gap finder had read said
"Bakterienfund" — "bacteria found" — and the extractor turned that into a
genus name. The authority's own notice says *Shigatoxin-bildende
Escherichia coli (STEC)*, and the gap finder had already fetched that
sentence into its own verified.jsonl.

Two separate failures, one row:

  * a fabricated field value, which is the one unforgivable error in this
    register; and
  * the only hazard among 2306 published rows that no subscriber alert
    could reach, so nobody who subscribed to anything would have been told
    about a Tier-1 STEC recall across fifteen German federal states.

``tests/test_alert_vocab::test_every_row_matches_at_least_one_pathogen_term``
has asserted the second half for months. A test fires the morning after the
row goes live; this one asserts that the GATE refuses it, so the row never
goes live at all.

WHAT THE RULE COSTS
===================
Measured on the 2026-11-07 register before it was wired in: 1 of 2306
published rows (this one), 1 of 31 Weekly_Review rows (the same one), and in
Pending the empty value plus 'Infection' and 'Substandard and Falsified' —
the two NAFDAC medicine rows that rule 10 refuses already. No publishable
row pays for it.

The rule is deliberately two-sided, and its message says so: an unreachable
hazard is either a wrong value (this case) or a missing vocabulary word, and
the second fix is one line in tools/alert_vocab.py plus
`python3 tools/gen_alert_vocab.py --write`.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import (  # noqa: E402
    _load_alert_vocab, _unreachable_hazard_blockers, publish_blockers,
)

#: The row as it was published, Pathogen included.
THE_ROW = {
    "Date": "2026-11-06",
    "Source": "BVL (DE)",
    "Company": "Norderstedter Fleisch- und Wurstwaren Peter Faden GmbH & Co KG",
    "Brand": "",
    "Product": "Beef pepperoni salami, beef salami, pure-beef salami, "
               "pizza beef salami",
    "Pathogen": "Bacillus",
    "Reason": "Bacteria found",
    "Class": "Recall",
    "Country": "Germany",
    "Region": "Europe",
    "Tier": "1",
    "URL": ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
            "Meldungen/2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/"
            "261106_05_SH_Rindfleischsalamiprodukte_Presse.pdf"
            "?__blob=publicationFile&v=2"),
    "Notes": "Discovered via news: tagesschau.de [locality: Norderstedt]",
}

#: The register's own canonical spelling for this organism — 120 published
#: rows carry it, more than every other E. coli spelling put together.
CANONICAL_STEC = "Shiga toxin-producing E. coli (STEC)"


def test_the_vocabulary_is_actually_loaded():
    """A silent import failure would make this whole rule vacuous."""
    assert _load_alert_vocab() is not None, (
        "tools/alert_vocab.py did not load — the gate would then pass every "
        "hazard, including an invented one")


def test_the_bare_genus_is_refused():
    why = _unreachable_hazard_blockers(THE_ROW)
    assert why, "'Bacillus' passed the hazard-reachability rule"
    assert "no word any subscriber alert offers" in why[0]


def test_the_refusal_names_both_repairs():
    why = _unreachable_hazard_blockers(THE_ROW)[0]
    assert "alert_vocab" in why, (
        "the message must say how to fix the OTHER case — a real hazard the "
        "vocabulary is missing — or an operator will edit the data instead")
    assert "gen_alert_vocab.py --write" in why


def test_the_whole_gate_refuses_the_row_as_published():
    assert any("no word any subscriber alert offers" in p
               for p in publish_blockers(THE_ROW)), (
        "publish_blockers still admits the row — rule 10c is not wired in")


def test_the_corrected_row_is_admitted():
    """The fix is the right value, and the right value must pass."""
    fixed = dict(THE_ROW, Pathogen=CANONICAL_STEC,
                 Reason="Shiga toxin-producing Escherichia coli (STEC) "
                        "detected in a single sample")
    assert _unreachable_hazard_blockers(fixed) == [], (
        f"{CANONICAL_STEC!r} is the register's own canonical spelling and "
        f"must be reachable by an alert")


@pytest.mark.parametrize("pathogen", [
    "Listeria monocytogenes",
    "Salmonella",
    "Clostridium botulinum",
    "Shiga toxin-producing E. coli (STEC)",
    "Foreign material (glass)",
    "Foreign material (aluminium)",
    "Bisphenol A (chemical contaminant)",
    "Mercury (heavy metal)",
    "Bacillus cereus",
])
def test_real_hazards_are_not_caught(pathogen):
    assert _unreachable_hazard_blockers({"Pathogen": pathogen}) == [], (
        f"{pathogen!r} is a hazard this register publishes — the rule is "
        f"too wide")


def test_an_empty_pathogen_is_rule_ones_business_not_this_rules():
    """Two blockers for one defect reads as two defects."""
    assert _unreachable_hazard_blockers({"Pathogen": ""}) == []
    assert _unreachable_hazard_blockers({"Pathogen": "   "}) == []
    # rule 1 still refuses it
    assert any("pathogen" in p.lower()
               for p in publish_blockers(dict(THE_ROW, Pathogen="")))


# ---------------------------------------------------------------------------
# the live register
# ---------------------------------------------------------------------------

def test_no_published_row_carries_an_unreachable_hazard():
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                               # pragma: no cover
        pytest.skip("no workbook")
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    bad = {}
    for name in ("Recalls", "Weekly_Review"):
        if name not in wb.sheetnames:
            continue
        rows = list(wb[name].values)
        head = list(rows[0])
        for r in rows[1:]:
            if not any(r):
                continue
            row = dict(zip(head, r))
            if _unreachable_hazard_blockers(row):
                bad.setdefault(name, set()).add(str(row.get("Pathogen")))
    assert not bad, (
        "hazards on live sheets that no subscriber alert can reach: %s\n"
        "Check the regulator's own notice first — a bare genus is usually a "
        "misread headline, not a missing vocabulary word." % (
            {k: sorted(v) for k, v in bad.items()},))


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
