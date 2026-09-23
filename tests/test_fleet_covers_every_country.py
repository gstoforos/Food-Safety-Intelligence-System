# -*- coding: utf-8 -*-
"""Every registered country must be reachable by the fleet, every week.

THE PROBLEM THIS REPLACES (audit 2026-09-23)
============================================
Coverage lived in eight per-region workflows, each carrying a hand-typed
default country list and each needing its own entry in FsisScheduler.gs —
a file this repository cannot see or test. Three lost their entry::

    at be ch de hu lu nl pl   last run 2026-06-14   Central EU
    dk fi is no se            last run 2026-05-31   Nordic
    ba cz ee hr md mk         never ran             East EU
    eg gh ke                  fully configured, wired to nothing

22 of 28 registered countries dark, with working code. All 22 codes
resolve through ``countries.get()`` today.

The bookkeeping left a second tell: ``africa_gap_finder.yml``'s header
lists *"Verified & wired: cz hr ee mk md ba"* — the EAST EU countries,
pasted into the wrong file's documentation. Anyone checking whether East
EU was covered would have read that and concluded it was.

``tools/fleet_shard.py`` keeps no list. It reads the REGISTRY, which is
true by construction because a config that does not register cannot run,
and deals it into weekday shards.

WHAT IS ASSERTED
----------------
The union of the shards is exactly the registry — no country missing, no
country invented, none run twice in a week — and the shards stay balanced
as countries are added.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.fleet_shard import (N_SHARDS, plan, registered_codes,  # noqa: E402
                               shard_for)

CODES = registered_codes()
WF = ROOT / ".github" / "workflows" / "gap_finder_fleet.yml"


def test_the_registry_is_not_empty():
    """A zero-length registry would make every test below pass vacuously —
    and would make the workflow run nothing, silently, forever."""
    assert len(CODES) >= 20, f"only {len(CODES)} countries registered"


def test_every_country_runs_exactly_once_a_week():
    seen = []
    monday = date(2026, 9, 21)          # a Monday
    for i in range(7):
        seen += shard_for(CODES, monday + timedelta(days=i))
    assert sorted(seen) == sorted(CODES), (
        "the week's shards are not a partition of the registry: "
        f"missing {sorted(set(CODES) - set(seen))}, "
        f"extra {sorted(set(seen) - set(CODES))}")
    assert len(seen) == len(set(seen)), "a country runs twice in one week"


def test_the_previously_dark_countries_are_all_in_the_plan():
    """The 22. Named individually so a regression says which one went."""
    dark = {"at", "be", "ch", "de", "hu", "lu", "nl", "pl",
            "dk", "fi", "is", "no", "se",
            "ba", "cz", "ee", "hr", "md", "mk",
            "eg", "gh", "ke"}
    covered = {c for shard in plan().values() for c in shard}
    missing = sorted(dark - covered)
    assert not missing, f"still unreachable by the fleet: {missing}"


def test_the_currently_live_countries_are_not_dropped():
    covered = {c for shard in plan().values() for c in shard}
    for c in ("it", "pt", "es", "gr", "za", "ng"):
        assert c in covered, f"{c} runs today and the fleet would drop it"


def test_shards_are_balanced():
    sizes = [len(v) for v in plan().values()]
    assert max(sizes) - min(sizes) <= 1, (
        f"shard sizes {sizes} differ by more than one — an unbalanced deal "
        f"puts a long day over the job's 50-minute budget")


def test_a_shard_fits_the_job_budget():
    """~6 min/country observed; the job allows 50 and caps each at 12."""
    biggest = max(len(v) for v in plan().values())
    assert biggest * 6 <= 45, (
        f"{biggest} countries x ~6 min exceeds the 50-minute job budget; "
        f"raise N_SHARDS rather than the timeout")


def test_the_same_weekday_always_draws_the_same_shard():
    """A country that fails must retry on a predictable date."""
    a = shard_for(CODES, date(2026, 9, 21))
    b = shard_for(CODES, date(2026, 9, 28))
    assert a == b


@pytest.mark.parametrize("n", [1, 2, 3, 5, 7, 14])
def test_any_shard_count_still_partitions_the_registry(n):
    seen = []
    for i in range(n):
        seen += [c for j, c in enumerate(sorted(CODES)) if j % n == i]
    assert sorted(seen) == sorted(CODES)


def test_an_empty_registry_yields_an_empty_shard_not_a_crash():
    assert shard_for([], date(2026, 9, 21)) == []


def test_adding_a_country_needs_no_workflow_edit():
    """The whole point. A new config must appear in the plan by itself."""
    extended = sorted(CODES + ["zz"])
    covered = set()
    monday = date(2026, 9, 21)
    for i in range(7):
        covered |= set(shard_for(extended, monday + timedelta(days=i)))
    assert "zz" in covered


# --------------------------------------------------------------------------
# the workflow
# --------------------------------------------------------------------------

def test_the_workflow_exists_and_parses():
    yaml = pytest.importorskip("yaml")
    assert WF.exists(), "gap_finder_fleet.yml is missing"
    doc = yaml.safe_load(WF.read_text(encoding="utf-8"))
    assert "fleet" in doc["jobs"]


def test_the_workflow_has_no_internal_cron():
    """Repository rule: schedules live in FsisScheduler.gs alone. A
    duplicate trigger fired every slot twice into the same writer lane."""
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(WF.read_text(encoding="utf-8"))
    on = doc.get("on") or doc.get(True)
    assert "schedule" not in (on or {}), "fleet workflow carries its own cron"


def test_the_workflow_keeps_no_country_list():
    """A hardcoded list is the defect being removed, not relocated."""
    src = WF.read_text(encoding="utf-8")
    run = src[src.index("Run today's countries"):]
    for bad in ("de,at,ch", "se,no,dk", "cz,hr,ee", "za,ng"):
        assert bad not in run, (
            f"the fleet hardcodes {bad!r} — it must read the registry")
    assert "tools.fleet_shard" in src


def test_an_empty_shard_fails_loudly():
    """Silently running zero countries is how this class of bug hides."""
    src = WF.read_text(encoding="utf-8")
    assert "Empty shard" in src and "exit 1" in src


def test_each_country_has_its_own_timeout():
    """A job-level cap alone lets one hung country skip every country
    after it in the shard, with no record."""
    assert "timeout 12m python -m pipeline.gap_finder.main" in \
        WF.read_text(encoding="utf-8")


def test_the_fleet_signs_its_own_commits_and_is_watched():
    from tools.dispatch_watchdog import WATCHED
    src = WF.read_text(encoding="utf-8")
    assert '"Gap finder fleet:' in src
    assert any(rx == r"^Gap finder fleet:" for _, rx, _ in WATCHED), (
        "reviewer 1 was invisible for weeks because it committed under "
        "another agent's prefix; a new workflow does not repeat that")


def test_the_shard_cli_prints_codes_only_on_stdout():
    """The workflow consumes stdout directly; a stray log line there would
    be passed to the runner as a country code."""
    out = subprocess.run([sys.executable, "-m", "tools.fleet_shard"],
                         cwd=ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    line = out.stdout.strip()
    assert line, "no shard printed"
    for c in line.split(","):
        assert c in CODES, f"stdout carried {c!r}, which is not a country"
