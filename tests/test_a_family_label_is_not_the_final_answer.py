# -*- coding: utf-8 -*-
"""A row must not name a vaguer organism than its own Reason does.

THE ROW (FDA, 2026-09-29)
=========================
    Company   Sierra Nevada Cheese Company
    Brand     Sierra Nevada Graziers
    Product   Monterey Jack, Jalapeno Jack, Medium Cheddar, and Sharp Cheddar
              raw milk cheeses
    Pathogen  "Escherichia coli (generic)"
    Reason    "Potential to be contaminated with Shiga toxin-producing
               Escherichia coli (STEC)"
    Outbreak  0

The row contradicted itself in two adjacent fields. FDA's notice names
*Escherichia coli O26:H11*, explicitly as a Shiga toxin-producing E. coli, in
an FDA/CDC outbreak investigation with 13 illnesses epidemiologically
associated with the Graziers Medium Cheddar. The same workbook's NEWS sheet
carried "E. coli / STEC · Outbreak · Sickens 13" that morning. The register
held the right answer twice and published neither.

NOTHING WAS BROKEN IN THE NORMALISER
------------------------------------
    normalize_pathogen("...Shiga toxin-producing Escherichia coli (STEC)")
        -> 'Shiga toxin-producing E. coli (STEC)'

The row was built by the FDA HTML listing fallback (Layer 1), where the only
token available is a bare "E. coli" — and a bare "E. coli" canonicalises to
the generic label CORRECTLY, because that is all the listing says. Then the
detail page filled Reason, and nothing re-derived Pathogen.

That is the same failure as Country, Source and Class: a field normalised
where it is CREATED, then the row updated in place with no earlier gate
running again. Those three are re-derived at the single writer choke point.
Pathogen was not. It is now, and this pins it.

WHY THE FIX IS DELIBERATELY NARROW
----------------------------------
It only ever replaces a FAMILY label with one of that family's own members,
and only when the row's own text names the member. It cannot move Salmonella
to Listeria, cannot generalise a specific organism back to its family, and
cannot invent anything the row does not say. Two families qualify today and
both are listed by hand, so widening it is a decision somebody makes on
purpose.

WHAT THIS DOES NOT FIX
----------------------
``Outbreak`` stayed 0 and that was CORRECT given the row's own text: the
outbreak evidence is on FDA's page and in the NEWS sheet, and neither reached
the Reason field. The outbreak flag is evidence-gated on purpose — see
TestOutbreakEvidence in test_publish_gate.py, written after five rows were
published as outbreaks on the strength of a RASFF "risk: serious" string. So
the flag is not force-set here; the evidence is put into the row (by
tools/repair_2026_09_29.py) and the flag follows from it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

SIERRA_URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
              "alerts/sierra-nevada-cheese-company-recalls-graziers-raw-milk-"
              "cheese-because-possible-health-risk")

SCHEMA = ["Date", "Source", "Company", "Product", "Pathogen", "Reason",
          "Tier", "Outbreak", "URL", "Notes"]


def _through_the_writer(tmp_path, rows, sheet="Recalls"):
    from openpyxl import Workbook
    import pandas as pd
    from pipeline.merge_master import _write_sheet
    wb = Workbook()
    wb.remove(wb.active)
    _write_sheet(wb, sheet, SCHEMA, [dict(r) for r in rows])
    f = tmp_path / "w.xlsx"
    wb.save(f)
    return pd.read_excel(f, sheet)


def _row(**kw):
    base = dict(Date="2026-09-29", Source="FDA", Company="C", Product="p",
                Pathogen="", Reason="", Tier=2, Outbreak=0,
                URL="https://www.fda.gov/x", Notes="")
    base.update(kw)
    return base


# --------------------------------------------------------------------------
# the normaliser was never the problem
# --------------------------------------------------------------------------

def test_the_normaliser_reads_the_reason_correctly():
    from scrapers._models import normalize_pathogen
    assert normalize_pathogen(
        "Potential to be contaminated with Shiga toxin-producing "
        "Escherichia coli (STEC)") == "Shiga toxin-producing E. coli (STEC)"


def test_a_bare_e_coli_still_yields_the_generic_label():
    """Layer 1 was right for its input — this is not the thing to 'fix'."""
    from scrapers._models import normalize_pathogen
    assert normalize_pathogen("E. coli") == "Escherichia coli (generic)"


# --------------------------------------------------------------------------
# the writer specialises, and only in the one direction
# --------------------------------------------------------------------------

def test_the_sierra_nevada_row_no_longer_says_generic(tmp_path):
    out = _through_the_writer(tmp_path, [_row(
        Company="Sierra Nevada Cheese Company",
        Product="Monterey Jack, Jalapeno Jack, Medium Cheddar, and Sharp "
                "Cheddar raw milk cheeses",
        Pathogen="Escherichia coli (generic)",
        Reason="Potential to be contaminated with Shiga toxin-producing "
               "Escherichia coli (STEC)",
        URL=SIERRA_URL)])
    assert out["Pathogen"].iloc[0] == "Shiga toxin-producing E. coli (STEC)"
    assert "pathogen-specialised" in str(out["Notes"].iloc[0])


def test_the_tier_follows_the_organism(tmp_path):
    """Vibrio is Tier 2; Vibrio vulnificus is Tier 1. The family label costs
    a tier as well as a name, which is why this runs BEFORE the tier guard."""
    out = _through_the_writer(tmp_path, [_row(
        Company="V", Product="oysters", Pathogen="Vibrio",
        Reason="Vibrio vulnificus detected in oysters", Tier=2)])
    assert out["Pathogen"].iloc[0] == "Vibrio vulnificus"
    assert int(out["Tier"].iloc[0]) == 1


def test_a_family_label_stays_when_the_row_says_nothing_more(tmp_path):
    """No evidence, no change. The generic label is an honest answer."""
    out = _through_the_writer(tmp_path, [_row(
        Pathogen="Escherichia coli (generic)",
        Reason="E. coli contamination")])
    assert out["Pathogen"].iloc[0] == "Escherichia coli (generic)"
    assert "pathogen-specialised" not in str(out["Notes"].iloc[0])


def test_a_specific_organism_is_never_generalised(tmp_path):
    out = _through_the_writer(tmp_path, [_row(
        Pathogen="Shiga toxin-producing E. coli (STEC)",
        Reason="E. coli found in beef", Tier=1)])
    assert out["Pathogen"].iloc[0] == "Shiga toxin-producing E. coli (STEC)"


def test_an_unrelated_organism_is_never_replaced(tmp_path):
    """The rule may specialise within a family. It may not change organism."""
    out = _through_the_writer(tmp_path, [_row(
        Pathogen="Listeria monocytogenes",
        Reason="Shiga toxin-producing Escherichia coli (STEC) is mentioned "
               "in passing in this notice", Tier=1)])
    assert out["Pathogen"].iloc[0] == "Listeria monocytogenes"


def test_nothing_is_invented_from_an_empty_reason(tmp_path):
    out = _through_the_writer(tmp_path, [_row(
        Pathogen="Escherichia coli (generic)", Reason="")])
    assert out["Pathogen"].iloc[0] == "Escherichia coli (generic)"


def test_it_applies_to_pending_too(tmp_path):
    """A row should not have to be published before it stops contradicting
    itself — Pending feeds the daily brief and the reviewers read it."""
    out = _through_the_writer(tmp_path, [_row(
        Pathogen="Escherichia coli (generic)",
        Reason="Shiga toxin-producing Escherichia coli (STEC)")],
        sheet="Pending")
    assert out["Pathogen"].iloc[0] == "Shiga toxin-producing E. coli (STEC)"


# --------------------------------------------------------------------------
# the table itself
# --------------------------------------------------------------------------

def _families():
    """The table's live text, with `#` comment lines removed.

    The comments necessarily quote strings like "E. coli" while explaining
    why the generic label is correct for a bare token. A parser that read
    them as table entries would fail on the explanation rather than the
    rule — which has happened in this repo five times now.
    """
    import re
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    m = re.search(r"_PATHOGEN_FAMILIES = \{(.*?)\n    \}", src, re.S)
    assert m, "the family table is gone from merge_master"
    return "\n".join(l for l in m.group(1).splitlines()
                     if not l.strip().startswith("#"))


def test_every_member_is_a_value_the_normaliser_can_actually_produce():
    """A member nothing can emit is a rule that never fires."""
    import re
    from scrapers._models import normalize_pathogen
    body = _families()
    members = re.findall(r'"([^"]+)"', body)
    # family keys are lower-cased; members are canonical labels
    members = [m for m in members if m != m.lower()]
    assert members, "no members in the family table"
    for m in members:
        assert normalize_pathogen(m) == m, (
            f"{m!r} is not a canonical the normaliser produces — the rule "
            f"can never fire on it")


def test_no_family_lists_itself_as_a_member():
    """Then it would be a no-op wearing the shape of a rule."""
    import re
    body = _families()
    for fam, block in re.findall(r'"([a-z0-9 ().\-/]+)":\s*\((.*?)\),\n',
                                 body, re.S):
        for member in re.findall(r'"([^"]+)"', block):
            assert member.lower() != fam, f"{fam!r} lists itself"


def test_the_published_register_holds_no_row_vaguer_than_its_own_reason():
    """The consequence, measured on the live workbook rather than assumed."""
    pd = pytest.importorskip("pandas")
    from scrapers._models import normalize_pathogen
    import re
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                    # pragma: no cover
        pytest.skip("no workbook")
    body = _families()
    fams = {}
    for fam, block in re.findall(r'"([a-z0-9 ().\-/]+)":\s*\((.*?)\),\n',
                                 body, re.S):
        fams[fam] = set(re.findall(r'"([^"]+)"', block))
    bad = []
    for sheet in ("Recalls", "Pending"):
        try:
            d = pd.read_excel(xlsx, sheet)
        except ValueError:                                   # pragma: no cover
            continue
        for _, r in d.iterrows():
            cur = str(r.get("Pathogen") or "").strip()
            members = fams.get(cur.lower())
            if not members:
                continue
            better = normalize_pathogen(str(r.get("Reason") or "") + " " +
                                        str(r.get("Product") or ""))
            if better and better in members:
                bad.append((sheet, str(r.get("Date"))[:10],
                            str(r.get("Source"))[:14], cur, better,
                            str(r.get("Product"))[:40]))
    assert not bad, (
        f"{len(bad)} row(s) name a vaguer organism than their own Reason "
        f"does:\n  " + "\n  ".join(map(str, bad[:8])) +
        "\n\nThis is the Sierra Nevada shape: the answer is inside the row.")
