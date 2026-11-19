# -*- coding: utf-8 -*-
"""Region is one of eight buckets, and the row's own Country decides which.

THE INCIDENT — 2026-11-19
=========================
pipeline/_publish_gate.VALID_REGIONS is a controlled vocabulary of eight
buckets, and its own comment states the rule: "A value outside these is an
extraction artifact, not a regional judgement call." Several collectors write
the notice's DISTRIBUTION AREA into the field instead, and the gate then
refuses the row on a string the row's own Country already answers.

Measured on the workbook of 2026-11-19, every sheet swept:

    Pending          'National'                        CFIA      1 row
    Weekly_Rejected  'Quebec'                          CFIA      1 row
    Rejected         'Poland'                          GIS       1 row
    Rejected         'Not specified in the article'    NCC (ZA)  1 row
    Rejected         'Nationwide and online'           FDA       1 row

THE ROW THAT FOUND IT. CFIA 2026-11-12, Archibald Inc., "Nuit Blanche White
IPA and Désirée Session IPA recalled due to pieces of metal", on the CFIA's
own per-recall URL. Archived by the confirm agent with "NO_REASON_RECORDED —
… treat as UNAUDITED, not as a confirmed rejection" and carrying the publish
gate's "Region 'Quebec' is not one of ['Africa', 'Asia', …]". A real
foreign-material recall held out of the register by the word "Quebec" in a
field that wanted "North America".

AND IT WAS A DEAD END, NOT A DELAY. That gate phrase was the only publish-gate
FIELD verdict in the workbook that merge_master.REPAIRABLE_DEFECTS did not
match — so tests/test_every_field_defect_is_repairable.py was RED on main this
morning, and the row could never have been re-promoted however well it was
repaired. Fourth instance of the 2026-10-05 Mattilsynet shape: a verified
recall blocked for ever by a reader that does not know what the writers write.

A second copy of the same CFIA beer recall family sat in Pending on Region
'National' — its ONLY publish-gate blocker, for a Class 3 "Food - Extraneous
Material" recall covering 31 products on the regulator's own page.

WHAT THIS FILE PINS
-------------------
1. apply_label_aliases — the normaliser both _write_sheet and the writers
   that bypass it call — replaces a Region outside the vocabulary with the
   bucket the row's own Country implies.
2. It never touches a Region that is already valid, 'Unknown' included.
3. It invents nothing: with no mappable Country the value becomes 'Unknown',
   which is itself a bucket, and the change is recorded in Notes.
4. No sheet in the workbook carries a Region outside the vocabulary.
5. The gate's enum verdict is repairable, in BOTH lists that must move
   together.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline._publish_gate import VALID_REGIONS, publish_blockers
from pipeline.merge_master import apply_label_aliases

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def _row(**kw):
    base = {"Country": "Canada", "Source": "CFIA", "Notes": ""}
    base.update(kw)
    return base


# ── 1. THE FIVE VALUES ACTUALLY FOUND IN THE WORKBOOK ─────────────────────

@pytest.mark.parametrize("country,bad,expected", [
    ("Canada", "National", "North America"),
    ("Canada", "Quebec", "North America"),
    ("Poland", "Poland", "Europe"),
    ("South Africa", "Not specified in the article", "Africa"),
    ("United States", "Nationwide and online", "North America"),
])
def test_a_distribution_area_becomes_the_country_s_bucket(country, bad,
                                                          expected):
    rows = [_row(Country=country, Region=bad)]
    apply_label_aliases(rows, where="test")
    assert rows[0]["Region"] == expected
    assert "region-normalise" in rows[0]["Notes"], (
        "the change must be recorded on the row — a field rewritten in "
        "silence is indistinguishable from one the collector got right")
    assert bad in rows[0]["Notes"], "the original value must be recoverable"


def test_the_archibald_row_then_passes_the_gate_on_region():
    """The whole point: with the Region repaired, the gate stops objecting to
    it. Other fields may still block a row; this asserts only that the enum
    verdict is gone."""
    rows = [_row(Country="Canada", Region="Quebec")]
    before = publish_blockers(dict(
        rows[0], Date="2026-11-12", Company="Archibald Inc.",
        Brand="Archibald", Product="Nuit Blanche White IPA and Désirée "
                                   "Session IPA",
        Pathogen="Pieces Of Metal", Tier=3, Class="Recall",
        Reason="Archibald brand Nuit Blanche White IPA and Désirée Session "
               "IPA recalled due to pieces of metal",
        URL="https://recalls-rappels.canada.ca/en/alert-recall/archibald-"
            "brand-nuit-blanche-white-ipa-and-desiree-session-ipa-recalled-"
            "due-pieces"))
    assert any("is not one of" in b for b in before), (
        "the gate no longer emits the enum verdict for Region 'Quebec' — "
        "re-point this test at its current wording rather than deleting it")
    apply_label_aliases(rows, where="test")
    after = publish_blockers(dict(
        rows[0], Date="2026-11-12", Company="Archibald Inc.",
        Brand="Archibald", Product="Nuit Blanche White IPA and Désirée "
                                   "Session IPA",
        Pathogen="Pieces Of Metal", Tier=3, Class="Recall",
        Reason="Archibald brand Nuit Blanche White IPA and Désirée Session "
               "IPA recalled due to pieces of metal",
        URL="https://recalls-rappels.canada.ca/en/alert-recall/archibald-"
            "brand-nuit-blanche-white-ipa-and-desiree-session-ipa-recalled-"
            "due-pieces"))
    assert not any("is not one of" in b for b in after), after


# ── 2. A VALID VALUE IS NEVER TOUCHED ─────────────────────────────────────

@pytest.mark.parametrize("region", sorted(VALID_REGIONS))
def test_every_valid_bucket_is_left_exactly_as_it_is(region):
    """Including 'Unknown', which IS a bucket. Normalising it would overwrite
    a deliberate 'we do not know' with a guess from the Country."""
    rows = [_row(Country="Canada", Region=region)]
    apply_label_aliases(rows, where="test")
    assert rows[0]["Region"] == region
    assert "region-normalise" not in rows[0]["Notes"]


def test_an_empty_region_is_not_filled_in():
    """Only a WRONG value is replaced. Filling an empty field is a different
    decision and the publish gate has its own say on emptiness."""
    rows = [_row(Country="Canada", Region="")]
    apply_label_aliases(rows, where="test")
    assert rows[0]["Region"] == ""
    assert "region-normalise" not in rows[0]["Notes"]


# ── 3. NOTHING IS INVENTED ────────────────────────────────────────────────

def test_an_unmappable_country_yields_unknown_not_a_guess():
    rows = [_row(Country="Atlantis", Region="the eastern provinces")]
    apply_label_aliases(rows, where="test")
    assert rows[0]["Region"] == "Unknown"


def test_the_result_is_always_in_the_gate_s_vocabulary():
    """Whatever goes in, what comes out must be a value the gate accepts —
    otherwise the normaliser just swaps one refusal for another."""
    for country in ("Canada", "Poland", "South Africa", "United States",
                    "Japan", "Australia", "Brazil", "Saudi Arabia",
                    "Atlantis", ""):
        rows = [_row(Country=country, Region="somewhere")]
        apply_label_aliases(rows, where="test")
        assert rows[0]["Region"] in VALID_REGIONS, (country, rows[0]["Region"])


def test_the_normaliser_reads_the_gate_s_own_vocabulary():
    """A copied list is the drift this test exists to prevent."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "from pipeline._publish_gate import VALID_REGIONS" in src, (
        "merge_master must import the gate's vocabulary rather than keep its "
        "own copy, or the normaliser and the gate that refuses the row can "
        "disagree.")


# ── 4. THE WORKBOOK ITSELF ────────────────────────────────────────────────

def test_no_sheet_carries_a_region_outside_the_vocabulary():
    if not XLSX.exists():                                     # pragma: no cover
        pytest.skip("no workbook")
    x = pd.ExcelFile(XLSX)
    offenders: list[str] = []
    for sheet in ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected",
                  "Rejected"):
        if sheet not in x.sheet_names:
            continue
        df = x.parse(sheet, dtype=str).fillna("")
        if "Region" not in df.columns:
            continue
        for i, v in enumerate(df["Region"], start=2):
            v = str(v).strip()
            if v and v not in VALID_REGIONS:
                offenders.append(f"{sheet} row {i}: {v!r}")
    assert not offenders, (
        "Region holds a distribution area, not a bucket, in:\n  "
        + "\n  ".join(offenders[:12]) +
        "\n\nmerge_master.apply_label_aliases normalises this at the writer "
        "from the row's own Country. A row already in the workbook needs a "
        "repair script — see tools/repair_2026_11_19.py.")


# ── 5. AND THE VERDICT IT ARCHIVED IS REPAIRABLE ──────────────────────────

@pytest.mark.parametrize("name,rel", [
    ("REPAIRABLE_DEFECTS", "pipeline/merge_master.py"),
    ("SUPERSEDE_IF", "pipeline/promote_gate_passing.py"),
])
def test_the_enum_verdict_is_in_both_lists(name, rel):
    """Both lists move together — the 2026-09-30 rule, re-broken on
    2026-11-18 and pinned by tests/test_every_field_defect_is_repairable.py.
    Named here too so the Region case is traceable from its own incident."""
    src = (ROOT / rel).read_text(encoding="utf-8")
    block = src[src.index(f"{name} = ("):]
    block = block[:block.index(")\n") + 1]
    assert '"is not one of"' in block, (
        f"{name} in {rel} does not match the publish gate's enum verdict "
        f"(\"Region 'Quebec' is not one of [...]\"), so a row archived on it "
        f"cannot be re-promoted after the field is repaired.")
