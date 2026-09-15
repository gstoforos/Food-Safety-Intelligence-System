"""The Process Authority note is the one paragraph that gives engineering
advice, so it is the one paragraph where a wrong number does damage.

process_authority.py has carried an explicit rule since it was written:
no F-value, D-value, z-value, hold-tube length or temperature target may
appear in published briefing text, because those are engagement
deliverables and mean nothing without per-product validation. The module
now also closes the note with a pointer to the AFTS software that does
this arithmetic — which is exactly the kind of addition that erodes such
a rule, since the tools it names compute those very numbers.

So the rule gets a test rather than a comment. Every deterministic
paragraph, on every trigger category, is scanned for a quoted process
target; and the scanner is itself tested against a deliberate violation,
because a guard that cannot fire is not a guard.

The rest covers the trigger logic these paragraphs hang off, and the two
properties the tool pointer has to have: it names the tool that fits the
hazard, and it never reads as a substitute for a process authority.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
if str(DOCS) not in sys.path:
    sys.path.insert(0, str(DOCS))

import process_authority as pa  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Rule 4 — no quoted process target, anywhere in published text
# ──────────────────────────────────────────────────────────────────────
# Regulation citations are numbers too — 21 CFR 113, Reg. 852/2004, Form
# 2541 — so the scan cannot simply look for digits. It looks for a number
# WEARING A PROCESS UNIT, which is what makes a figure a target: a
# temperature, a time, or a named kinetic value with a figure attached.
_TARGET_PATTERNS = (
    r"\d+(?:\.\d+)?\s*°",                       # 121.1 °C
    r"\d+(?:\.\d+)?\s*(?:degrees|deg)\b",
    r"\d+(?:\.\d+)?\s*(?:min|minutes|sec|seconds)\b",
    r"\bF\s*0?\s*(?:=|of)\s*\d",                # F0 = 3, F of 3
    r"\b[Dd]\s*(?:-|\s)?value\s*(?:=|of)\s*\d",
    r"\b[zZ]\s*(?:-|\s)?value\s*(?:=|of)\s*\d",
    r"\b[zZ]\s*=\s*\d",
)


def quoted_process_targets(text: str):
    """Every substring in `text` that reads as a process target."""
    found = []
    for pattern in _TARGET_PATTERNS:
        found += re.findall(pattern, text)
    return found


def test_the_target_scanner_actually_fires():
    """A guard that cannot fire is not a guard.

    If this test ever fails, the scan below is vacuous and the rule is
    being enforced by nothing at all.
    """
    for violation in ("hold at 121.1 °C", "an F0 of 3 minutes",
                      "a D-value of 0.21", "process for 12 minutes",
                      "z = 10"):
        assert quoted_process_targets(violation), (
            f"the scanner missed a plain violation: {violation!r}")


def test_regulatory_citations_are_not_mistaken_for_targets():
    """The paragraphs are full of numbers that must be allowed."""
    for legitimate in ("21 CFR 113 and 21 CFR 114", "Regulation 852/2004",
                       "Reg. 2073/2005", "FDA Form 2541", "21 CFR 108"):
        assert not quoted_process_targets(legitimate), (
            f"the scanner false-fired on a citation: {legitimate!r}")


# ──────────────────────────────────────────────────────────────────────
# Rows that fire each category
# ──────────────────────────────────────────────────────────────────────
ROWS = {
    "botulinum": [{"Pathogen": "Clostridium botulinum",
                   "Product": "canned green beans", "Class": "Class I"}],
    "lacf": [{"Product": "low-acid canned soup", "Reason": "retort deviation",
              "Pathogen": "", "Class": "Class I"}],
    "aseptic": [{"Product": "UHT oat drink",
                 "Reason": "aseptic zone integrity failure",
                 "Class": "Class I"}],
    "anaerobic": [{"Product": "cold-smoked salmon, vacuum packed",
                   "Pathogen": "Listeria monocytogenes", "Class": "Class I"}],
    "auto": [{"Product": "infant formula powder", "Reason": "recall",
              "Class": "Class I"}],
}


@pytest.fixture(params=sorted(ROWS))
def fired_trigger(request):
    trigger = pa.detect_process_authority_trigger(ROWS[request.param])
    assert trigger["fired"], f"{request.param} rows did not fire the trigger"
    return trigger


def test_no_deterministic_paragraph_quotes_a_process_target(fired_trigger):
    """The rule, enforced on every category including the new pointer."""
    text = pa.deterministic_fallback(fired_trigger)
    offenders = quoted_process_targets(text)
    assert not offenders, (
        f"the PA paragraph quotes a process target: {offenders}")


def test_the_prompt_extension_quotes_no_target_either(fired_trigger):
    """The AI is handed this text verbatim, so it is published text too."""
    offenders = quoted_process_targets(pa.build_prompt_extension(fired_trigger))
    assert not offenders, f"the PA prompt quotes a process target: {offenders}"


# ──────────────────────────────────────────────────────────────────────
# The tool pointer
# ──────────────────────────────────────────────────────────────────────
def test_the_pointer_names_the_tool_that_fits_the_hazard():
    """A retort model is the wrong answer to a hold-tube failure."""
    aseptic = pa.detect_process_authority_trigger(ROWS["aseptic"])
    assert "hold-tube" in pa.tooling_pointer(aseptic)

    lacf = pa.detect_process_authority_trigger(ROWS["lacf"])
    assert "in-container" in pa.tooling_pointer(lacf)
    assert "hold-tube" not in pa.tooling_pointer(lacf)


def test_the_pointer_never_reads_as_a_substitute_for_a_process_authority():
    """The sentence before it asks the reader to engage one. This one must
    not quietly offer them a calculator instead."""
    for trigger in (None, {}, {"categories": []},
                    pa.detect_process_authority_trigger(ROWS["botulinum"])):
        text = pa.tooling_pointer(trigger)
        assert "rather than substituting for one" in text
        assert "measured product data" in text
        assert pa.AFTS_SOFTWARE_URL in text


def test_the_pointer_survives_hazard_classes_this_module_never_models():
    """The weekly note fires on RTE Listeria and low-moisture Salmonella.
    Those must get the general pointer, not a retort model."""
    unknown = {"fired": True, "categories": ["listeria_rte", "nonsense"]}
    text = pa.tooling_pointer(unknown)
    assert pa.AFTS_SOFTWARE_URL in text
    assert "in-container" in text and "hold-tube" in text


def test_the_pointer_is_one_sentence():
    """It is appended to a paragraph with a 3-5 sentence budget."""
    text = pa.tooling_pointer(None)
    assert text.count(". ") == 0, "the pointer grew a second sentence"
    assert text.endswith(".")


# ──────────────────────────────────────────────────────────────────────
# The paragraph it is appended to
# ──────────────────────────────────────────────────────────────────────
def test_the_paragraph_still_identifies_itself(fired_trigger):
    """The HTML renderer finds this note by its label prefix, and the
    weekly builder learned the hard way that phrase-sniffing lets another
    paragraph wear it."""
    text = pa.deterministic_fallback(fired_trigger)
    assert text.startswith(pa.PROCESS_AUTHORITY_LABEL + ":")


def test_the_pointer_closes_the_paragraph(fired_trigger):
    assert pa.deterministic_fallback(fired_trigger).endswith(
        pa.tooling_pointer(fired_trigger))


def test_tooling_can_be_switched_off_without_editing_prose(fired_trigger):
    plain = pa.deterministic_fallback(fired_trigger, include_tooling=False)
    assert pa.AFTS_SOFTWARE_URL not in plain
    assert plain.startswith(pa.PROCESS_AUTHORITY_LABEL + ":")
    assert plain.rstrip().endswith(".")


def test_an_unfired_trigger_produces_nothing_to_append():
    """Callers append unconditionally, so silence has to be empty."""
    quiet = pa.detect_process_authority_trigger(
        [{"Product": "fresh spinach", "Reason": "undeclared allergen"}])
    assert not quiet["fired"]
    assert pa.deterministic_fallback(quiet) == ""
    assert pa.build_prompt_extension(quiet) == ""


def test_the_ai_prompt_carries_the_pointer_verbatim(fired_trigger):
    """Both paths have to close in the same words, or the briefing reads
    differently depending on whether an API key was set."""
    prompt = pa.build_prompt_extension(fired_trigger)
    assert pa.tooling_pointer(fired_trigger) in prompt
    assert "reproduced EXACTLY" in prompt


# ──────────────────────────────────────────────────────────────────────
# Trigger logic the paragraphs hang off
# ──────────────────────────────────────────────────────────────────────
def test_botulinum_fires_on_its_own():
    trigger = pa.detect_process_authority_trigger(
        [{"Pathogen": "Clostridium botulinum", "Product": "olive tapenade"}])
    assert trigger["fired"] and "botulinum" in trigger["categories"]


def test_anaerobic_packaging_alone_is_not_enough():
    """Vacuum packing is not a hazard. Firing on it would put a
    scheduled-process lecture under every chilled yoghurt recall."""
    benign = pa.detect_process_authority_trigger(
        [{"Product": "vacuum packed plain yoghurt", "Reason": "mislabelled",
          "Class": "Class III"}])
    assert not benign["fired"]

    with_severity = pa.detect_process_authority_trigger(
        [{"Product": "vacuum packed plain yoghurt", "Reason": "mislabelled",
          "Class": "Class I"}])
    assert with_severity["fired"]


def test_no_rows_is_not_a_trigger():
    assert pa.detect_process_authority_trigger([])["fired"] is False
