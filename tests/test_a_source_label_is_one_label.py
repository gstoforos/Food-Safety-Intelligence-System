"""One regulator, one published label — and the two lists must agree.

WHY THIS EXISTS (2026-10-04)
============================
Three hand-written lists name the same regulators and nothing made them
agree:

    tools/monitored_sources.SOURCES   the labels the dashboard's "Sources
                                      monitored" tile counts
    merge_master.SOURCE_ALIASES       what the writer canonicalises to
    _source_canon.CANONICAL           the identity map the aggregator and
                                      independence checks read

On 2026-10-04 the first Italian recalls from the gap-finder collector were
published. It writes a bare "Salute". SOURCE_ALIASES had never heard of it,
so the label reached Recalls verbatim; the registry calls that regulator
"Ministero della Salute (IT)", so
test_monitored_sources::test_every_published_source_is_counted broke; and
_source_canon — whose own docstring counts "ONE ministry, three spellings"
— returned kind='unknown' for both the bare "Salute" AND the registry's own
label, so the ministry really had five spellings and two of them were
nobody.

A second defect showed up in the same place and is NOT fixed here, by
choice: pipeline/promote_gate_passing.py publishes with R.append() rather
than through merge_master._write_sheet, so none of the writer's guards have
ever run on the rows the daily offline promoter publishes. Only the label
canonicalisation is wired into that path; see the note on
merge_master.apply_label_aliases for why the rest is left visible.
"""
from __future__ import annotations

import pytest

from pipeline._source_canon import canonical_source
from pipeline.merge_master import SOURCE_ALIASES, apply_label_aliases
from tools.monitored_sources import SOURCES

REGISTRY_LABELS = {label for label, _, _ in SOURCES}


def test_every_alias_target_is_a_label_the_dashboard_counts():
    """The writer must never canonicalise to a label the registry lacks."""
    unknown = sorted({v for v in SOURCE_ALIASES.values()
                      if v not in REGISTRY_LABELS})
    assert not unknown, (
        "merge_master.SOURCE_ALIASES canonicalises to labels that "
        f"tools/monitored_sources.SOURCES does not list: {unknown}")


@pytest.mark.parametrize("label", sorted({v for v in SOURCE_ALIASES.values()}))
def test_every_label_the_writer_produces_is_a_known_regulator(label):
    """Scoped to what the writer PRODUCES, not to its lookup keys.

    NOT asserted over the whole registry on purpose: 44 of the 76 labels in
    tools/monitored_sources.SOURCES resolve to kind='unknown' in
    _source_canon today — AFSCA (BE), CAA (JP), FSS (Scotland) and 41 more.
    That is a real gap and it is reported rather than hidden, but closing it
    means writing 44 identities by hand and this test would then be a
    different test asserting a different thing. What this one holds is the
    boundary that broke on 2026-10-04: a label the writer itself stamps onto
    a published row must be a regulator the rest of the code can identify.
    """
    key, kind, _ = canonical_source(label)
    assert kind != "unknown", (
        f"the writer canonicalises Source to {label!r}, which _source_canon "
        f"does not recognise as a regulator")


def test_the_italian_ministry_has_one_identity():
    keys = {canonical_source(s)[0] for s in (
        "Salute", "Salute (IT)", "Min. Salute (IT)",
        "Ministero della Salute", "Ministero della Salute (IT)")}
    assert keys == {"IT-SALUTE"}, keys


def test_the_promoter_canonicalises_what_it_publishes():
    """promote_gate_passing appends rows itself and bypasses the writer."""
    rows = [{"Source": "Salute", "Country": "Italy"},
            {"Source": "fsis", "Country": "usa"}]
    apply_label_aliases(rows)
    assert rows[0]["Source"] == "Ministero della Salute (IT)"
    assert rows[1]["Source"] == "USDA FSIS"
    assert rows[1]["Country"] == "United States"


def test_the_promoter_actually_calls_it():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "pipeline"
           / "promote_gate_passing.py").read_text(encoding="utf-8")
    assert "apply_label_aliases" in src, (
        "the offline promoter publishes most of the register and does not "
        "go through merge_master._write_sheet; it must canonicalise labels "
        "itself")
