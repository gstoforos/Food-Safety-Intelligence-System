"""The bare "hav" needle, pinned in every list that carries it.

WHAT WENT WRONG
===============
"hav" was written into several pathogen keyword lists as the abbreviation
for Hepatitis A virus. Every one of those lists is matched by substring
containment, not by token, so "hav" matches the word "have".

Every USDA FSIS recall notice ends with the same sentence:

    "Consumers who HAVe purchased these products are urged not to consume
     them."

So the needle matched literally every FSIS notice. Any FSIS recall that
named no organism of its own — a recall for lack of federal inspection, or
for an ineligible import — came out of the classifier stamped
"Hepatitis A virus".

It happened three times in August 2026 alone, all three caught only at the
publish gate and parked in Rejected:

    2026-08-26  Shanghai Ravioli Corporation   frozen buffalo chicken
    2026-08-17  Indus Foods / Gangothri Foods  pickled goat and chicken
    2026-08-17  Asian America Trading          Siluriformes fish

None of those three notices contains the word "hepatitis".

WHY IT SURVIVED THE FIRST FIX
=============================
The 2026-08-04 Ukrop's audit found this exact bug and spaced the needle —
in pipeline/_publish_gate.py and in ONE of the two lists in
pipeline/verify_pathogen_in_source.py. Six other copies were missed:

    pipeline/verify_pathogen_in_source.py  (the second list)
    pipeline/gap_finder/rules.py           (twice, plus "hev")
    pipeline/signal_detector.py
    scrapers/fsanz.py
    scrapers/_pathogen_vocab.py
    scrapers/oceania/fsanz.py
    docs/build_weekly_report_afts.py

A fix applied to two of eight copies is not a fix. This test asserts on the
SOURCE FILES, not on behaviour, because that is the only way to stop the
eighth copy being written next time.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# The literal FSIS boilerplate. If a needle matches this, it matches every
# USDA recall ever published.
FSIS_BOILERPLATE = (
    "consumers who have purchased these products are urged not to consume them"
)

# Files allowed to contain a bare "hav"/"hev" token in quotes: the historical
# repair script and this test both quote it while describing the bug.
EXEMPT = {"fix_ukrops_pathogen.py", "test_hav_substring_regression.py"}

BARE = re.compile(r"""['"](hav|hev)['"]""")


def _sources():
    for d in ("pipeline", "scrapers", "docs", "tools"):
        for f in (ROOT / d).rglob("*.py"):
            if f.name not in EXEMPT:
                yield f


def test_no_bare_hav_needle_anywhere_in_the_tree():
    """No file may carry a bare 'hav' or 'hev' string literal.

    Spaced forms — " hav ", "(hav)", "hav virus" — are the correct way to
    write the abbreviation, and are what every list uses now.
    """
    offenders = []
    for f in _sources():
        for i, line in enumerate(f.read_text(encoding="utf-8-sig").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if BARE.search(line):
                offenders.append(f"{f.relative_to(ROOT)}:{i}: {line.strip()[:90]}")
    assert not offenders, (
        "a bare 'hav'/'hev' needle is back. It matches the word 'have', which "
        "appears on every USDA FSIS notice:\n  " + "\n  ".join(offenders)
    )


@pytest.mark.parametrize("modpath,attr", [
    ("pipeline.gap_finder.rules", "PATHOGENS_TIER_2"),
])
def test_no_needle_matches_the_fsis_boilerplate(modpath, attr):
    """The load-bearing assertion: no needle may fire on FSIS boilerplate."""
    import importlib
    needles = getattr(importlib.import_module(modpath), attr)
    hits = [n for n in needles if n and n in FSIS_BOILERPLATE]
    assert not hits, (
        f"{modpath}.{attr} contains needles that match the sentence printed on "
        f"every USDA FSIS recall notice: {hits}"
    )


def test_a_real_hepatitis_a_recall_is_still_matched():
    """The fix must not cost us the true positives it was written for."""
    from pipeline.gap_finder.rules import PATHOGENS_TIER_2
    for text in (
        "recall due to possible hepatitis a virus contamination",
        "testing detected hav virus in the product",
        "rappel : virus de l'hepatite a",
    ):
        assert any(n in text for n in PATHOGENS_TIER_2), text


def test_the_three_august_fsis_notices_no_longer_classify_as_hepatitis():
    """The three rows this bug actually produced, by their own wording."""
    from pipeline.gap_finder.rules import PATHOGENS_TIER_2
    notices = {
        "shanghai-ravioli":
            "shanghai ravioli corporation recalls not ready-to-eat frozen "
            "buffalo chicken products produced without the benefit of "
            "inspection. " + FSIS_BOILERPLATE,
        "gangothri-foods":
            "indus foods llc dba gangothri foods recalls ready-to-eat pickled "
            "goat and chicken products produced without benefit of inspection. "
            + FSIS_BOILERPLATE,
        "asian-america-trading":
            "asian america trading, inc. recalls ineligible siluriformes fish "
            "products imported from the people's republic of china. "
            + FSIS_BOILERPLATE,
    }
    for slug, text in notices.items():
        hits = [n for n in PATHOGENS_TIER_2
                if n in text and ("hepatit" in n or "hav" in n or "hev" in n)]
        assert not hits, f"{slug} still classifies as a hepatitis notice: {hits}"
