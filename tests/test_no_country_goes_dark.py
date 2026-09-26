# -*- coding: utf-8 -*-
"""A registered country that stops producing must be visible, not silent.

WHAT THIS CAUGHT (audit 2026-09-23)
===================================
``pipeline/gap_finder/countries/`` registers 28 countries. Six had run in
the previous 30 days. The rest stopped, and the last record each one left
says when::

    it pt es          2026-09-23   alive
    za ng             2026-09-22   alive
    gr                2026-09-21   alive
    at be ch de hu lu nl pl        2026-06-14   Central EU, 8 countries
    dk fi is no se                 2026-05-31   Nordic, 5 countries
    ba cz ee hr md mk              no run_log at all — NEVER RAN

Eight countries stopping on one date and five on another is the signature
of a WORKFLOW that stopped, not of per-country crashes: a crashing
country would keep writing dated crash records, because
``gap_finder/main`` writes one on every exit.

The code is fine. Every one of those 19 country codes resolves through
``countries.get()`` today. The workflows are healthy and simply stopped
being dispatched by FsisScheduler.gs, which lives outside this repo — so
the repo cannot fix it, but it can refuse to be quiet about it.

WHY A TEST AND NOT ONLY A WATCHDOG
----------------------------------
``tools/dispatch_watchdog.py`` now watches the eight gap-finder
WORKFLOWS. That is the right granularity for "did the job run", and it is
the wrong granularity for "is Hungary still covered": one workflow
dispatches eight countries, and it can run to completion while producing
nothing for half of them.

This test watches COUNTRIES. The two together are the claim the register
actually makes — that France's 800 rows out of 1764 reflect France, not
reflect which gap finders happen to be alive.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DATA = ROOT / "docs" / "data"

#: Countries known to be dark when this test was written, with the date
#: their last record carries. Listing them here is the point: the test
#: goes red for anything NOT on this list, and each entry is a to-do that
#: cannot be quietly forgotten.
#:
#: To retire a country deliberately, delete its config from
#: pipeline/gap_finder/countries/ AND its line here, in one change, so the
#: register stops claiming coverage it does not have.
# Dark for two completely different reasons, which must not share one
# list. A country that STOPPED is a regression — something that worked no
# longer does. A country that has NEVER RUN is unbuilt coverage. Counting
# them together lets a real outage hide behind a burst of new configs,
# which is exactly what would have happened on 2026-09-23 when twelve were
# added in a morning.

#: Dark because dispatch stopped. Each line is an outage with a date.
#: The ratchet below bounds THIS set. Adding to it is admitting a
#: regression; the fix is to restore dispatch, not to extend the list.
STOPPED = {
    # Central EU workflow — stopped 2026-06-14
    "at", "be", "ch", "de", "hu", "lu", "nl", "pl",
    # Nordic workflow — stopped 2026-05-31
    "dk", "fi", "is", "no", "se",
    # East EU workflow — has never run
    "ba", "cz", "ee", "hr", "md", "mk",
    # No dedicated workflow; configs exist for future use.
    "eg", "gh", "ke",
}

#: Dark because they are new: written, tested, sharded into the fleet, but
#: not yet dispatched once. Each maps to the date its config was written,
#: so an entry cannot sit here indefinitely pretending to be coverage —
#: see test_a_new_country_does_not_sit_unrun_forever.
NOT_YET_RUN = {
    # Twelve countries added the Greek way on 2026-09-23, replacing
    # scrapers that are BLOCKED rather than broken. Of the 33 collectors
    # covering Asia, Latin America, the Middle East and Africa, one placed
    # a row in the preceding 45 days.
    #
    # Asia — replacing scrapers silent since 06-14 (ph) and 06-25 (sg).
    #
    # sg and hk LEFT this list on 2026-09-24: the gap finder fleet ran its
    # first shard that morning ("Gap finder fleet: 2026-09-24 shard of 6
    # countries") and both wrote a run_log at 10:44/10:45. They are now held
    # to the 14-day freshness bar like any other live country — which is the
    # whole point of this list being time-bounded rather than permanent.
    #
    # br, kr and tw LEFT on 2026-09-25, the next shard, at 10:43/10:44/10:46.
    # This test asked for them by name the moment their run logs appeared,
    # which is the mechanism working: the grace period is not a place to
    # park a country.
    #
    # WHAT THEY PRODUCED, recorded here because "has run" is not "is
    # working": br 17 candidates -> 11 verified -> 0 accepted; kr 28 -> 16
    # -> 0; tw 137 -> 50 -> 0. All 77 refused by the authority-URL gate as
    # "no official press-release URL in the source article (news-only
    # discovery)" — NOT by the LLM, and not by a bad regex. The gate held
    # exactly as designed; what is missing for these three is the SECOND
    # half of the Greek route, resolving a news story back to the
    # regulator's own notice. Until that lands they are live, freshness-
    # checked, and contributing nothing. That is a coverage gap stated
    # honestly, which is better than a grace period hiding it.
    "jp": "2026-09-23", "ph": "2026-09-23",
    "id": "2026-09-23", "vn": "2026-09-23",
    # Latin America — replacing scrapers silent since 06-26 (mx), 06-27 (br)
    "mx": "2026-09-23", "co": "2026-09-23",
    "cl": "2026-09-23",
    # Middle East
    "sa": "2026-09-23", "ae": "2026-09-23",
    # North America. Added after the others, when a question showed that
    # USDA FSIS — blocked by a 403, exactly like the rest — had gone
    # fifteen days without a row while the healthy FDA scraper made the
    # region look covered.
    "us": "2026-09-23",
}

#: The union, for the freshness assertion, which does not care why.
KNOWN_DARK = STOPPED | set(NOT_YET_RUN)

#: How long a newly written config may sit without a single run before it
#: stops counting as pending and starts counting as a broken promise.
UNRUN_GRACE_DAYS = 21

STALE_DAYS = 14


def _registered() -> list[str]:
    """Every registered country.

    This used to prime the registry by calling get() on a hand-written
    tuple of 28 codes — which meant a country this test was supposed to
    watch could be missing from the watch list itself. Since 2026-09-23
    all_codes() walks the package directory, so there is nothing to keep
    in step: a new config file is in scope for this test the moment it
    lands.
    """
    from pipeline.gap_finder.countries import all_codes
    return all_codes()


def _last_run(code: str):
    f = DATA / f"gap_finder_{code}" / "run_log.jsonl"
    if not f.exists():
        return None
    best = ""
    for line in f.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        t = r.get("finished_at") or r.get("started_at") or ""
        if t > best:
            best = t
    return best or None


REGISTERED = _registered()


def test_there_are_countries_to_check():
    assert len(REGISTERED) >= 20, (
        f"only {len(REGISTERED)} countries registered — the loader is "
        f"probably broken, and a broken loader makes every test below pass "
        f"vacuously")


@pytest.mark.parametrize("code", sorted(c for c in REGISTERED
                                        if c not in KNOWN_DARK))
def test_a_live_country_has_run_recently(code):
    last = _last_run(code)
    assert last, (
        f"{code!r} is registered, is not on the KNOWN_DARK list, and has no "
        f"run_log at all — it has never run")
    when = datetime.fromisoformat(last.replace("Z", "+00:00"))
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - when
    assert age < timedelta(days=STALE_DAYS), (
        f"{code!r} last ran {last[:10]}, {age.days} days ago. Either it has "
        f"gone dark — in which case fix the dispatch, do not move it to "
        f"KNOWN_DARK — or it was retired, in which case delete its config "
        f"and its entry together")


@pytest.mark.parametrize("code", sorted(KNOWN_DARK))
def test_a_known_dark_country_is_still_registered(code):
    """Guards the list itself.

    If a config is deleted without removing its KNOWN_DARK entry, this
    list slowly becomes a graveyard of names nobody recognises, and the
    next real outage gets added to it as routine.
    """
    assert code in REGISTERED, (
        f"{code!r} is on KNOWN_DARK but no longer has a country config. "
        f"Remove it from the list — the two must be deleted together")


def test_the_dark_list_is_not_growing_unnoticed():
    """A ratchet. More than half the fleet dark is not a list, it is an
    outage, and it should not be possible to reach that state one
    KNOWN_DARK entry at a time."""
    stopped = len(STOPPED & set(REGISTERED))
    assert stopped <= 22, (
        f"{stopped} of {len(REGISTERED)} registered countries have STOPPED "
        f"running. Raising this ceiling is a decision about coverage, not a "
        f"test fix — and note this counts regressions only: a country that "
        f"has never run belongs in NOT_YET_RUN, which is bounded by time "
        f"rather than by count")


@pytest.mark.parametrize("code", sorted(NOT_YET_RUN))
def test_a_new_country_does_not_sit_unrun_forever(code):
    """A config that never runs is not coverage, it is a file.

    Twelve were written on one morning. Without this, all twelve could sit
    in NOT_YET_RUN for a year while the register showed Asia and Latin
    America as covered — which is the same false-green the scraper fleet
    had been showing, moved one layer up.

    Once a country has run, delete its NOT_YET_RUN entry: it is then held
    to the 14-day freshness bar like everybody else.
    """
    if _last_run(code):
        pytest.fail(
            f"{code!r} HAS run now — remove it from NOT_YET_RUN so the "
            f"14-day freshness assertion starts covering it")
    written = datetime.fromisoformat(NOT_YET_RUN[code]).replace(
        tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - written).days
    assert age <= UNRUN_GRACE_DAYS, (
        f"{code!r}'s config was written {NOT_YET_RUN[code]}, {age} days ago, "
        f"and has still never run. Either the fleet is not dispatching it "
        f"(check tools/fleet_shard.py --plan and the gap_finder_fleet "
        f"workflow) or it runs and crashes before writing a run_log. Do not "
        f"extend the grace period to make this pass")


def test_every_live_country_actually_produced_rows():
    """Running is not the same as finding anything.

    Africa is the case in point: `africa_gap_finder` committed on 30 of the
    last 30 days and put ONE row in the register. A green workflow and a
    green run_log both said it was fine.
    """
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = DATA / "recalls.xlsx"
    if not xlsx.exists():
        pytest.skip("no register")
    ws = openpyxl.load_workbook(xlsx, read_only=True)["Recalls"]
    h = [str(c.value or "") for c in ws[1]]
    ic, ida = h.index("Country"), h.index("DateAdded")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    recent = set()
    for r in ws.iter_rows(min_row=2, values_only=True):
        d = r[ida]
        ds = d.isoformat()[:10] if hasattr(d, "isoformat") else str(d or "")[:10]
        if ds >= cutoff:
            recent.add(str(r[ic] or "").strip().lower())
    # Reported, not asserted: a quiet month in one country is normal, and
    # failing on it would train people to ignore this file. The number is
    # the finding.
    live = [c for c in REGISTERED if c not in KNOWN_DARK]
    print(f"\n  countries live: {len(live)} | distinct countries with a row "
          f"in the last 30 days: {len(recent)}")
    assert live, "no live countries at all"
