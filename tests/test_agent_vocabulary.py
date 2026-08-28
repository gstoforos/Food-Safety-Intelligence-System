"""An agent must not be able to widen the schema by writing into it.

The twelve schema defects found on 2026-08-28 were all made by deterministic
code, which is the easy case: code that classifies wrongly does it the same
way every time and a hand review finds it. A model asked to fill the same
fields fails differently — it invents terms. "refrigerated-RTE",
"meat, cooked", "Class 1 recall" each look plausible, pass every type check,
and silently create a stratum containing one row.

These tests hold the boundary: an agent reports what a notice SAYS, and the
schema concludes what that MEANS. The two must not meet.
"""
from __future__ import annotations

import pytest

from pipeline.agents._vocabulary import (
    AGENT_WRITABLE, DERIVED_NEVER_WRITABLE, PIPELINE_ONLY, VOCABULARY,
    validate_changes, validate_value)


def test_a_clean_correction_is_accepted():
    assert validate_changes({
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Listeria monocytogenes detected in a finished product",
    }) == []


@pytest.mark.parametrize("col", sorted(VOCABULARY))
def test_no_analytical_column_can_be_written_by_an_agent(col):
    """Not "refused if the value is odd" — refused outright. These columns are
    derived from the wording; an agent that writes one is asserting a
    classification that no regulator made."""
    refusals = validate_changes({col: next(iter(VOCABULARY[col]))})
    assert refusals, col
    assert "DERIVED" in refusals[0], refusals


@pytest.mark.parametrize("col", PIPELINE_ONLY)
def test_no_published_statistic_can_be_written_by_an_agent(col):
    refusals = validate_changes({col: "1"})
    assert refusals and "pipeline" in refusals[0], (col, refusals)


def test_an_invented_term_is_refused_and_the_legal_one_is_named():
    """The refusal has to teach. A model that is told 'invalid' tries another
    invention; one that is told the legal term uses it."""
    out = validate_value("ConsumptionState", "refrigerated-RTE")
    assert out and "not a controlled term" in out[0]
    assert "ready-to-eat" in out[0]


def test_the_vocabulary_covers_every_analytical_column_the_schema_writes():
    """A column added to enrich_schema without a vocabulary entry would be
    silently unvalidated — writable by an agent with any value at all."""
    from pipeline.enrich_schema import COLUMNS
    bookkeeping = {"EventID", "EnrichedBy", "EnrichedAt", "EnrichmentTier"}
    missing = [c for c in COLUMNS if c not in VOCABULARY and c not in bookkeeping]
    assert not missing, f"analytical columns with no controlled vocabulary: {missing}"


def test_every_vocabulary_term_is_one_the_schema_can_actually_produce():
    """The reverse leak: a term in the contract that the code never emits is a
    stratum an agent could be talked into that analysis will never see."""
    from pipeline import product_axes as PA
    emitted = set()
    for d in (PA.PROCESS_TERMS, PA.CONSUMPTION_TERMS, PA.CATEGORY_TERMS,
              PA.PACKAGING_TERMS, PA.PRESERVATION_TERMS):
        emitted |= set(d)
    emitted |= {"unknown", "packaged", "unpackaged", "frozen", "chilled",
                "ambient", "loose"}
    # PreservationSystem's structural labels are produced by code, not a dict.
    emitted |= {"chilled-rte", "chilled-raw", "chilled-other", "ambient-stable",
                "ambient-other", "low-moisture-dried", "fermented-acidified",
                "cured-smoked"}
    for col in ("ProcessType", "ConsumptionState", "PreservationSystem"):
        stray = [t for t in VOCABULARY[col] if t not in emitted]
        assert not stray, f"{col}: contract offers terms the code never emits: {stray}"


def test_writable_fields_and_derived_fields_do_not_overlap():
    """The whole boundary in one assertion."""
    assert not (set(AGENT_WRITABLE) & set(DERIVED_NEVER_WRITABLE))
    assert not (set(AGENT_WRITABLE) & set(PIPELINE_ONLY))


def test_the_curator_uses_this_contract_and_not_its_own_list():
    from pipeline.agents import curator
    assert curator.WRITABLE == set(AGENT_WRITABLE)
