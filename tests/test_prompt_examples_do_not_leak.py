"""An example in a prompt must not be copyable as an answer.

THE INCIDENT (2026-09-27)
=========================
Instruction 10 in the extractor prompt read:

    If the article mentions a recall reference number issued by {authority}
    (e.g. 'allerta 842632', 'numero pratica 12345'), include it in reason_en
    (e.g. 'Recall ID 842632').

The model copied the example VERBATIM onto recalls that printed no reference
number at all. Nine rows in the workbook carry the string "842632", and in
TWO of them it was the entire Reason field:

    Mattilsynet · Brie de Melun AOP · Matcompaniet AS · E. coli STEC · Tier 1
        Reason: "Recall ID 842632"

That row could not be published — `_publish_gate` refuses a Reason that is
only a reference number, correctly, because it describes no hazard. It sat in
Pending until PR #34 cleared the queue, and then it was the one row of 37 that
landed in no sheet at all. A leaked prompt example cost a Tier-1 E. coli STEC
recall its place in the register.

THREE FILES CARRIED THE IDENTICAL LINE — pipeline/extractor.py,
pipeline/gap_finder/extractor.py and pipeline/official_feeds/extractor.py — so
fixing one would have left the leak running in the other two. That is the same
shape as the reviewer heredocs: a rule duplicated across files is a rule that
gets half-fixed.

WHAT THIS TEST PINS
-------------------
1. No literal digit sequence from the old example survives in any prompt.
2. Any prompt that still shows an example of a reference number also carries
   an explicit instruction not to copy it.
3. The rule stays fixed in ALL THREE files, not one.

This does not stop every possible leak — a model can always echo something —
but it stops the specific, measured one, and it stops a future edit from
quietly reintroducing a bare concrete example.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: The three files that carried the identical leaking instruction.
EXTRACTORS = (
    "pipeline/extractor.py",
    "pipeline/gap_finder/extractor.py",
    "pipeline/official_feeds/extractor.py",
)

#: The digits the model actually copied into nine rows.
LEAKED = "842632"


def _prompt_text(path: Path) -> str:
    """The file's source with `#` comment lines removed.

    The comments explaining this incident necessarily quote the leaked digits,
    exactly as the module docstring above does. A test that reads them as
    evidence of the bug would fail on its own explanation — which has happened
    in this repo four times. Only live prompt text is checked.
    """
    return "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    )


@pytest.mark.parametrize("rel", EXTRACTORS)
def test_the_leaked_example_is_gone_from_the_prompt(rel):
    p = ROOT / rel
    if not p.exists():
        pytest.skip(f"{rel} not present")
    body = _prompt_text(p)
    assert LEAKED not in body, (
        f"{rel} still contains the literal example {LEAKED!r} in live prompt "
        f"text. The model copied exactly this string onto nine rows, and on "
        f"two of them it became the whole Reason field — which cost a Tier-1 "
        f"E. coli STEC recall its publication. Describe the FORMAT instead of "
        f"showing a copyable number.")


@pytest.mark.parametrize("rel", EXTRACTORS)
def test_a_reference_number_never_replaces_the_hazard(rel):
    """reason_en must describe the hazard even when no reference exists."""
    p = ROOT / rel
    if not p.exists():
        pytest.skip(f"{rel} not present")
    body = _prompt_text(p)
    if "Recall ID" not in body:
        pytest.skip("this file no longer mentions a recall reference number")
    low = body.lower()
    assert "never copy" in low or "not copy" in low, (
        f"{rel} still shows how to format a recall reference number but never "
        f"tells the model not to copy the example. That is the 2026-09-27 "
        f"leak exactly.")
    assert ("only if" in low) or ("never instead" in low), (
        f"{rel} does not make the reference number conditional on the article "
        f"actually printing one, so reason_en can still end up as nothing but "
        f"a reference.")


def test_all_three_extractors_were_fixed_together():
    """A rule duplicated across files is a rule that gets half-fixed."""
    present = [r for r in EXTRACTORS if (ROOT / r).exists()]
    if len(present) < 2:
        pytest.skip("fewer than two extractors in this checkout")
    leaking = [r for r in present if LEAKED in _prompt_text(ROOT / r)]
    assert not leaking, (
        f"{len(leaking)} of {len(present)} extractors still leak: {leaking}. "
        f"All three carried the identical line; fixing one leaves the other "
        f"two writing the same bad Reason every night.")


def test_no_published_row_has_a_reference_number_as_its_whole_reason():
    """The register-side consequence, measured rather than assumed."""
    pd = pytest.importorskip("pandas")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():
        pytest.skip("no workbook")
    d = pd.read_excel(xlsx, "Recalls")
    pat = re.compile(r"^\s*(recall\s*id|allerta|numero\s*pratica)\s*[:#]?\s*\d+\s*$", re.I)
    bad = [(r["Date"], r["Source"], str(r.get("Product"))[:40], str(r.get("Reason")))
           for _, r in d.iterrows() if pat.match(str(r.get("Reason") or ""))]
    assert not bad, (
        f"{len(bad)} published row(s) have a bare reference number as their "
        f"entire Reason, so they name no hazard: {bad[:5]}")
