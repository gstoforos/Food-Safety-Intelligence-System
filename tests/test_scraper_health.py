# -*- coding: utf-8 -*-
"""A scraper that has stopped finding anything must be visible.

WHAT THIS CAUGHT (audit 2026-09-23)
===================================
``pipeline/run_all.py`` runs 66 scrapers every day. Each one printed a
single line to the job log — ``[DONE] SFA (SG)/Singapore -> 0 recalls`` —
and the log dies with the job. So a scraper whose selector broke when the
site was redesigned returns 0 forever and looks exactly like a scraper
having a quiet week.

Measured against the register:

* **34 of 66 scrapers have never placed a single row.** Not "none
  recently" — none ever.
* 12 more placed rows and stopped, most inside one fortnight::

      FDA (PH)  2026-06-14      COFEPRIS (MX) 2026-06-26
      NCC (ZA)  2026-06-14      ANMAT (AR)    2026-06-27
      SFA (SG)  2026-06-25      ANVISA (BR)   2026-06-27

  Six regulators on three continents falling silent within two weeks is
  a pattern, not six coincidences — and nothing in this repository was in
  a position to see it.
* Of the 33 scrapers covering Asia, Latin America, the Middle East and
  Africa, exactly ONE produced a row in the last 45 days.

That is why those regions read as 3% of the register. Not missing
collectors — 33 collectors, silent, with nothing watching them.

TWO OF THE "SILENT" ARE A NAMING MISMATCH, NOT A DEATH
------------------------------------------------------
``Min. Salute (IT)`` and ``Mattilsynet (NO)`` are the scrapers' AGENCY
strings; the register carries their rows as ``Ministero della Salute
(IT)`` and ``Mattilsynet``. Same authority, two spellings, so any
name-keyed health check files them under a name that never matches. They
are aliased below rather than renamed: AGENCY is also the key for
``AGENCIES_FILTER`` and the scraper audit, and renaming it to fix a
report would be fixing the wrong layer.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

HEALTH = ROOT / "docs" / "data" / "scraper_health.json"

#: scraper AGENCY -> the Source string its rows actually carry.
ALIASES = {
    "Min. Salute (IT)": "Ministero della Salute (IT)",
    "Mattilsynet (NO)": "Mattilsynet",
}

#: Agencies known silent when this file was written, with nothing yet
#: proving they can work. Each is a to-do, not an exemption: the test goes
#: red for anything NOT on this list, and the ratchet below stops the list
#: growing quietly.
#:
#: To retire a scraper deliberately, DELETE its module and its entry here
#: in one change, so the fleet stops claiming coverage it does not have.
KNOWN_SILENT = {
    # Asia — 11 of 12
    "BPOM (ID)", "FSSAI (IN)", "KKM (MY)", "MHLW (JP)", "SAMR (CN)",
    "TFDA (TW)", "Thai FDA", "MFDS (KR)", "VFA (VN)", "FDA (PH)",
    "SFA (SG)",
    # Latin America — all 8
    "ARCSA (EC)", "DIGESA (PE)", "ISP (CL)", "INVIMA (CO)", "MSP (UY)",
    "COFEPRIS (MX)", "ANMAT (AR)", "ANVISA (BR)",
    # Middle East — all 5
    "MoCCAE (AE)", "MoH (IL)", "MoPH (QA)", "SFDA (SA)", "TGTHB (TR)",
    # Africa — 6 of 7
    "FDA (GH)", "KEBS (KE)", "NAFDAC (NG)", "NFSA (EG)", "ONSSA (MA)",
    "COMESA", "NCC (ZA)",
    # Europe — these exist as scrapers but the register is fed for these
    # countries by the gap finders instead.
    "ANSVSA (RO)", "ASAE (PT)", "BFSA (BG)", "Fødevarestyrelsen (DK)",
    "HAH (HR)", "Livsmedelsverket (SE)", "MAST (IS)", "Nébih (HU)",
    "PVD (LV)", "Ruokavirasto (FI)", "UVHVVR (SI)", "VMVT (LT)",
    "VTA (EE)", "ŠVPS (SK)", "Min. Salute (IT)", "Mattilsynet (NO)",
}

ZERO_RUN_LIMIT = 14        # consecutive empty runs before it is a defect


def _health() -> dict:
    if not HEALTH.exists():
        pytest.skip("scraper_health.json not written yet — run pipeline.run_all")
    return json.loads(HEALTH.read_text(encoding="utf-8-sig"))


def _discovered() -> set:
    import importlib
    import inspect
    import pkgutil
    from scrapers._base import BaseScraper
    out = set()
    for region in ("north_america", "europe_eu", "europe_non_eu", "eu_wide",
                   "asia", "oceania", "africa", "latam", "middle_east"):
        try:
            pkg = importlib.import_module(f"scrapers.{region}")
        except Exception:                                    # noqa: BLE001
            continue
        for _, m, _ in pkgutil.iter_modules(pkg.__path__):
            try:
                mod = importlib.import_module(f"scrapers.{region}.{m}")
            except Exception:                                # noqa: BLE001
                continue
            for _, c in inspect.getmembers(mod, inspect.isclass):
                if (issubclass(c, BaseScraper) and c is not BaseScraper
                        and c.__module__ == mod.__name__
                        and getattr(c, "AGENCY", None)):
                    out.add(c.AGENCY)
    return out


DISCOVERED = _discovered()


def test_scrapers_are_discovered_at_all():
    """A zero-length discovery makes every test below pass vacuously, and
    would mean run_all scrapes nothing while exiting 0."""
    assert len(DISCOVERED) >= 50, f"only {len(DISCOVERED)} scrapers found"


def test_the_health_file_covers_every_discovered_scraper():
    h = _health()
    missing = sorted(DISCOVERED - set(h))
    assert not missing, (
        f"{len(missing)} scrapers run with no health record: {missing[:8]}. "
        f"A scraper nobody records is a scraper nobody can miss")


@pytest.mark.parametrize("agency", sorted(DISCOVERED - KNOWN_SILENT))
def test_a_scraper_not_on_the_silent_list_is_producing(agency):
    rec = _health().get(agency) or {}
    zeros = int(rec.get("consecutive_zero_runs") or 0)
    assert zeros < ZERO_RUN_LIMIT, (
        f"{agency} has returned nothing for {zeros} consecutive runs. "
        f"Either its selector broke — fix it — or the source genuinely "
        f"went quiet, in which case say so here rather than moving it to "
        f"KNOWN_SILENT by reflex")


@pytest.mark.parametrize("agency", sorted(KNOWN_SILENT))
def test_a_known_silent_scraper_still_exists(agency):
    """Guards the list. If a module is deleted without removing its entry,
    this list becomes a graveyard of names nobody recognises and the next
    real outage gets appended to it as routine."""
    assert agency in DISCOVERED, (
        f"{agency!r} is on KNOWN_SILENT but no scraper declares it. Delete "
        f"the entry — the module and the entry go together")


def test_the_silent_list_is_not_growing_unnoticed():
    """A ratchet. Two thirds of the fleet silent is not a list, it is an
    outage, and it must not be reachable one entry at a time."""
    #: 47 is where this stood on 2026-09-23 — measured, not chosen. Every
    #: scraper fixed should lower it; nothing should raise it without a
    #: decision recorded here.
    n = len(KNOWN_SILENT & DISCOVERED)
    assert n <= 47, (
        f"{n} of {len(DISCOVERED)} scrapers are on the silent list. "
        f"Raising this ceiling is a decision about coverage, not a test fix")


def test_a_scraper_that_throws_is_distinguished_from_one_that_finds_nothing():
    """They were the same `[]` before. A thrown exception means broken; an
    empty list may mean a quiet week."""
    src = (ROOT / "pipeline" / "run_all.py").read_text(encoding="utf-8")
    assert '"last_error"' in src
    assert "Tuple[str, List[Recall], str]" in src, (
        "run_one_scraper must return the error alongside the rows")


def test_health_is_written_even_when_every_scraper_is_empty():
    """The empty case is the one that matters — that is what a broken
    fleet looks like — so the write must not sit behind `if all_rows`."""
    src = (ROOT / "pipeline" / "run_all.py").read_text(encoding="utf-8")
    i = src.index("def run_all_scrapers")
    body = src[i:src.index("\ndef ", i + 10)]
    assert "_save_health(health)" in body
    save_at = body.index("_save_health(health)")
    ret_at = body.index("return all_rows")
    assert save_at < ret_at, "health is saved after the return"


def test_writing_health_can_never_break_the_scrape():
    """Telemetry must not be able to take the product down."""
    src = (ROOT / "pipeline" / "run_all.py").read_text(encoding="utf-8")
    i = src.index("def _save_health")
    body = src[i:src.index("\ndef ", i + 10)]
    assert "except Exception" in body and "log.warning" in body


def test_the_aliased_agencies_are_named_not_silently_renamed():
    """Renaming AGENCY to fix a report would break AGENCIES_FILTER and the
    scraper audit, which key on it."""
    for scraper_name, register_name in ALIASES.items():
        assert scraper_name in DISCOVERED, (
            f"{scraper_name!r} is aliased but no longer exists")
        assert scraper_name != register_name
