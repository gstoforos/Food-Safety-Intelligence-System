# -*- coding: utf-8 -*-
"""Every gap finder must be watched, including the ones already dead.

THE BLIND SPOT (audit 2026-09-23)
=================================
``tools/dispatch_watchdog.py`` watched the scrapers, the reviewers, the
merge, the news feed, the URL guardian and the publication surfaces. It
watched **no gap finder at all** — the entire discovery half of the
system.

Three had already died in that blind spot::

    Central EU gap finder   last commit 2026-06-14   (3 months)
    Nordic gap finder       last commit 2026-05-31   (4 months)
    East EU gap finder      HAS NEVER COMMITTED

Nothing anywhere said so, and the register carries the shape of it:
France alone holds 800 of 1764 rows while Germany sits at 41 and Austria,
Switzerland, Czechia, Hungary and Slovakia are between 0 and 20. That
distribution reads like a map of Europe and is actually a map of which
gap finders are alive.

WHY THE DEAD ONES ARE LISTED TOO
--------------------------------
Their entries are OVERDUE the moment they are added. That is the point: a
watchdog line that only goes green once someone fixes the workflow is a
to-do item that cannot be forgotten. The alternative — leaving them out
until they are fixed — is how they stayed dead for four months.

If a region is retired deliberately, its line should be DELETED with a
note, the way the gemini url-gate line was retired in the same file. A
permanent false OVERDUE trains operators to ignore the real ones, and the
file already records that lesson.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.dispatch_watchdog import WATCHED  # noqa: E402

WF = ROOT / ".github" / "workflows"

#: workflow stem -> the prefix its commit step actually uses.
#: `scandinavian_gap_finder.yml` commits as "Nordic gap finder:" — the file
#: name and the prefix disagree, which is exactly the kind of thing a
#: hand-maintained table gets wrong.
EXPECTED = {
    "italian_gap_finder": "Italian gap finder:",
    "spanish_gap_finder": "Spanish gap finder:",
    "portuguese_gap_finder": "Portuguese gap finder:",
    "greek_gap_finder": "Greek gap finder:",
    "africa_gap_finder": "Africa gap finder:",
    "central_eu_gap_finder": "Central EU gap finder:",
    "scandinavian_gap_finder": "Nordic gap finder:",
    "east_eu_gap_finder": "East EU gap finder:",
}

_PATTERNS = [rx for _, rx, _ in WATCHED]
_NAMES = [n for n, _, _ in WATCHED]


@pytest.mark.parametrize("stem,prefix", sorted(EXPECTED.items()))
def test_every_regional_gap_finder_is_watched(stem, prefix):
    if not (WF / f"{stem}.yml").exists():
        pytest.skip(f"{stem}.yml not present")
    assert any(rx == "^" + prefix for rx in _PATTERNS), (
        f"{stem}.yml commits {prefix!r} and dispatch_watchdog.py does not "
        f"watch it — a gap finder nobody watches can die for four months, "
        f"and three did")


@pytest.mark.parametrize("stem,prefix", sorted(EXPECTED.items()))
def test_the_watched_prefix_matches_what_the_workflow_writes(stem, prefix):
    """The table is hand-maintained; the workflow is the truth.

    scandinavian_gap_finder.yml commits as "Nordic gap finder:". A table
    written from file names rather than from commit steps would watch a
    prefix that is never produced and report a healthy workflow OVERDUE
    forever.
    """
    f = WF / f"{stem}.yml"
    if not f.exists():
        pytest.skip(f"{stem}.yml not present")
    src = f.read_text(encoding="utf-8")
    found = re.findall(r'"([A-Z][A-Za-z ]+ gap finder):', src)
    assert found, f"{stem}.yml has no recognisable commit prefix"
    assert prefix.rstrip(":") in found, (
        f"{stem}.yml writes {sorted(set(found))}, the table expects "
        f"{prefix!r}")


def test_the_dead_three_are_listed_not_omitted():
    """The regression this file exists for."""
    for prefix in ("^Central EU gap finder:", "^Nordic gap finder:",
                   "^East EU gap finder:"):
        assert prefix in _PATTERNS, (
            f"{prefix} was dropped from the watchdog. If that region was "
            f"retired on purpose, say so in a comment where the line was — "
            f"silence is how it died the first time")


def test_no_duplicate_patterns():
    assert len(_PATTERNS) == len(set(_PATTERNS)), "two lines watch one prefix"


def test_no_duplicate_names():
    assert len(_NAMES) == len(set(_NAMES)), "two lines share a display name"


@pytest.mark.parametrize("name,rx,hours",
                         [w for w in WATCHED if "gap finder" in w[0]],
                         ids=[w[0] for w in WATCHED if "gap finder" in w[0]])
def test_thresholds_are_sane(name, rx, hours):
    """Roughly 2x a daily dispatch: one missed slot quiet, two loud."""
    assert 24 <= hours <= 72, (
        f"{name} at {hours}h — under 24 cries wolf on a single slip, over "
        f"72 hides three days of silence")


def test_every_pattern_is_anchored():
    """An unanchored pattern matches a prefix mentioned mid-message."""
    for n, rx, _ in WATCHED:
        assert rx.startswith("^"), f"{n}: {rx!r} is not anchored"
