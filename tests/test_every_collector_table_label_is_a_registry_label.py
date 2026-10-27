"""Every Source label a collector TABLE can emit resolves to a registry label.

WHAT WAS WRONG (audit 2026-10-27)
=================================
Two rows reached Recalls on 2026-10-24 with Source "MPI NZ":

    Hutchinsons Ltd      Arla Protein Pudding Vanilla Cookie (200g)
    Hutchinsons Limited  Arla Protein Pudding Vanilla Cookie (200g)

"MPI NZ" is not a label any registry uses. The registry
(tools/monitored_sources.SOURCES) calls that regulator "MPI (NZ)"; so does
scrapers/oceania/mpi_nz.AGENCY, so does verify_urls.HOST_FOR_SOURCE, and
pipeline/_source_canon maps it to "NZ-MPI". The label came from three
host->label tables that nothing checked:

    pipeline/regulatory_domains.REGULATOR_DOMAINS
    pipeline/gap_finder_tavily.HOST_TO_SOURCE
    review/gap_finder_tavily.HOST_TO_SOURCE

and the consequences were not cosmetic:

  * verify_urls could not host-check either row. The canonical-source
    indirection added on 2026-10-26 exists precisely so a renamed label
    cannot switch the check off — but it resolves through _source_canon,
    and _source_canon had never heard of "MPI NZ" either, so
    canonical_source() invented "MPI-NZ" and
    HOST_FOR_CANONICAL_SOURCE["NZ-MPI"] never ran.
  * _source_canon returned kind="unknown", so one regulator counted as two
    independent observers against "MPI (NZ)".
  * tools/monitored_sources did not count it: the dashboard's "Sources
    monitored" tile was short by one.

WHY A TEST OVER THE TABLES AND NOT OVER THE REGISTER
====================================================
This is the FOURTH recurrence of one defect, and every previous repair was
triggered by a row that had ALREADY PUBLISHED:

    2026-10-03  Czechia   "SZPI"        found in Recalls
    2026-10-04  Italy     "Salute"      found in Recalls
    2026-10-07  Hungary   "NÉBIH"       found in Recalls
    2026-10-27  NZ        "MPI NZ"      found in Recalls

tests/test_monitored_sources and tests/test_a_source_label_is_one_label
both assert over published rows, so they can only ever go red AFTER a
mislabelled row is in the register — and the right repair is never to add
the stray label to the registry (the Hungary note in merge_master records
why: NÉBIH was merged into NKFH, the registry was right, the writer had no
alias). tests/test_every_collector_label_is_a_registry_label.py closed the
same hole for the ~46 gap_finder/countries configs. The three tables above
are the collectors it does not cover.

So this test reads the tables and asks of each label what the writer will
ask of it, BEFORE any row exists. Table entry N+1 cannot be added without
either using the registry's own label or giving the writer an alias.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._source_canon import canonical_source            # noqa: E402
from pipeline.merge_master import SOURCE_ALIASES               # noqa: E402
from tools.monitored_sources import SOURCES                    # noqa: E402

REGISTRY_LABELS = {label for label, _country, _domains in SOURCES}

# Entries in these tables that name media or an aggregator rather than a
# regulator. They are leads, never a Source on a Recalls row, and
# tools/monitored_sources says so in its own docstring.
NOT_A_REGULATOR = {
    "Food Safety News", "Food Safety Magazine", "Outbreak News Today",
    "National Authority",        # gap_finder_tavily's "host known, agency not"
    "DG SANTE",                  # Commission directorate, not a recall issuer
    "Dubai Municipality", "MoEC", "ADAFSA", "ALVA", "BAG", "BfR", "ANSA",
    "MINSAL", "ACHIPIA",         # monitored by name elsewhere; no Recalls rows
}


def _tables():
    """(name, {host: (label, country)}) for each LIVE collector table.

    review/gap_finder_tavily.py carries a third copy of this table with the
    pre-repair labels and is deliberately NOT asserted on: nothing imports
    it (checked below), and tools/rebase_and_verify.CODE_ROOTS does not
    carry review/ into an upload, so a repair made there today cannot reach
    main and would only make this suite disagree with the tree the gate
    builds. CODE_ROOTS gained "review/" on 2026-10-27 so the next pass can
    repair it; until a commit carries that change, the copy stays stale and
    inert and test_the_review_copy_is_not_live is what keeps it inert.
    """
    import pipeline.regulatory_domains as rd
    import pipeline.gap_finder_tavily as pgt
    out = [("pipeline/regulatory_domains.REGULATOR_DOMAINS",
            dict(rd.REGULATOR_DOMAINS))]
    table = getattr(pgt, "HOST_TO_SOURCE", None)
    if table:
        out.append(("pipeline/gap_finder_tavily.HOST_TO_SOURCE", dict(table)))
    return out


def test_the_review_copy_is_not_live():
    """The stale third copy must stay unimported by running code.

    If it is ever wired back in, its labels have to be repaired first —
    and this test is what says so, since _tables() cannot cover it.
    """
    hits = []
    for path in sorted(ROOT.glob("**/*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(("review/", "tests/", ".git")):
            continue
        src = path.read_text(encoding="utf-8", errors="ignore")
        if "review.gap_finder_tavily" in src or "review/gap_finder_tavily" in src:
            hits.append(rel)
    assert not hits, (
        "review/gap_finder_tavily.py is imported by live code, so its "
        f"host->label table has to meet the same rule: {hits}")


def _resolves_to(label: str) -> str:
    """The label the writer will publish, applying SOURCE_ALIASES."""
    return SOURCE_ALIASES.get(str(label).strip().lower(), str(label).strip())


def test_the_tables_are_found():
    """If a table is renamed this test must fail loudly, not silently pass."""
    tables = _tables()
    assert len(tables) == 2, [n for n, _ in tables]
    for name, table in tables:
        assert len(table) > 30, (name, len(table))


@pytest.mark.parametrize("name,_i", [(n, i) for i, (n, _) in
                                     enumerate(_tables())])
def test_every_table_label_reaches_a_registry_label(name, _i):
    """A label is acceptable when it IS a registry label, or an alias maps
    it to one, or it is a bare authority name the generic
    `registry_source_label` rule can resolve from the row's Country."""
    from pipeline.merge_master import registry_source_label

    table = dict(_tables())[name]
    offenders = []
    for host, (label, country) in sorted(table.items()):
        if label in NOT_A_REGULATOR:
            continue
        resolved = _resolves_to(label)
        if resolved in REGISTRY_LABELS:
            continue
        # A BARE authority short name ("AESAN", "FSANZ") is resolved by the
        # generic rule from the row's Country, which this table also holds —
        # so the country used here is the table's own, not another table's.
        if registry_source_label(label, country) in REGISTRY_LABELS:
            continue
        offenders.append((host, label, resolved))
    assert not offenders, (
        f"{name}: {len(offenders)} label(s) that no registry uses and no "
        f"alias resolves. Use the label tools/monitored_sources.SOURCES "
        f"gives the regulator, or add an alias to "
        f"merge_master.SOURCE_ALIASES: {offenders[:8]}")


@pytest.mark.parametrize("name,_i", [(n, i) for i, (n, _) in
                                     enumerate(_tables())])
def test_every_table_label_is_known_to_source_canon(name, _i):
    """The label the writer publishes must have a canonical identity.

    This is the clause that was missing on 2026-10-24: a label absent from
    _source_canon is a label verify_urls.HOST_FOR_CANONICAL_SOURCE cannot
    be reached for, so the host check does not weaken — it switches off.
    """
    table = dict(_tables())[name]
    dark = []
    for host, (label, _country) in sorted(table.items()):
        if label in NOT_A_REGULATOR:
            continue
        resolved = _resolves_to(label)
        if resolved not in REGISTRY_LABELS:
            continue                      # the test above owns that failure
        if canonical_source(resolved)[1] != "regulator":
            dark.append((host, resolved))
    assert not dark, (
        f"{name}: {len(dark)} label(s) with no canonical identity — add "
        f"them to pipeline/_source_canon.CANONICAL, or the host check and "
        f"the coverage denominator both go dark for them: {dark[:8]}")


def test_new_zealand_specifically():
    """The row that found this. Pinned so the repair cannot be undone."""
    import pipeline.regulatory_domains as rd
    import pipeline.gap_finder_tavily as pgt

    for table in (rd.REGULATOR_DOMAINS, pgt.HOST_TO_SOURCE):
        assert table["mpi.govt.nz"][0] == "MPI (NZ)", table["mpi.govt.nz"]

    # and the old label still resolves, for rows already in the archive
    assert SOURCE_ALIASES["mpi nz"] == "MPI (NZ)"
    assert canonical_source("MPI NZ") == ("NZ-MPI", "regulator", None)
    assert canonical_source("MPI (NZ)") == ("NZ-MPI", "regulator", None)

    from pipeline.verify_urls import allowed_hosts_for_source
    for label in ("MPI NZ", "MPI (NZ)"):
        assert "mpi.govt.nz" in (allowed_hosts_for_source(label) or ()), label
