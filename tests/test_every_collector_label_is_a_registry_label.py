# -*- coding: utf-8 -*-
"""Every collector's own authority name canonicalises to a registry label.

WHAT THIS CAUGHT (morning-fix 2026-10-07)
=========================================
One defect, three recurrences in five days, each found by a row that had
already PUBLISHED to subscribers:

    2026-10-03  Czechia   "SZPI"   -> registry calls it "SZPI (CZ)"
    2026-10-04  Italy     "Salute" -> "Ministero della Salute (IT)"
    2026-10-07  Hungary   "NÉBIH"  -> "NKFH (HU)"

Each time the repair was one map entry and each time the next country was
left to the next morning. ``registry_source_label`` (2026-10-04) stopped
doing it one at a time for the common case — a bare "X" becomes "X (CC)"
when exactly one such registry label exists for the row's Country — and
that covers 39 of the 46 configs in ``pipeline/gap_finder/countries/``.

The remaining seven are the ones a generic rule cannot derive, and they are
the ones that keep publishing wrong:

    hu.py  NÉBIH       NKFH (HU)     agency merged; hu.py's own docstring
                                     records the 2026-09-30 move to
                                     nkfh.gov.hu
    be.py  FAVV-AFSCA  AFSCA (BE)
    ee.py  PTA         VTA (EE)
    hr.py  HAPIH       HAH (HR)
    gh.py  FDA Ghana   FDA (GH)
    ph.py  FDA PH      FDA (PH)
    kr.py  MFDS        MFDS (KR)     not a rename at all: the registry
                                     index was keyed on "South Korea" and
                                     the caller canonicalises Country to
                                     "Korea, South" BEFORE the Source
                                     lookup, so the entry was unreachable.
                                     Fixed in registry_source_label.

WHY THIS TEST SHAPE
-------------------
Not "is SOURCE_ALIASES complete" — a list cannot be asked whether it is
missing something. Not "is the register clean" — the register only shows
the countries that have published, which is how all three recurrences got
out. This test reads the 46 country configs, takes the label each one
actually stamps on a row, and puts it through the writer's own
canonicalisation. A config whose label does not come out as a label
``tools/monitored_sources.SOURCES`` counts cannot be added at all, so
country 47 is covered before its first recall, not after.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

from pipeline.merge_master import apply_label_aliases
from tools.monitored_sources import SOURCES

REGISTRY_LABELS = {label for label, _, _ in SOURCES}


def _configs():
    """(module name, authority_short, name_en) for every fleet country."""
    from pipeline.gap_finder import countries as pkg

    out = []
    for mod in pkgutil.iter_modules(pkg.__path__):
        if mod.name in ("base",):
            continue
        m = importlib.import_module(f"{pkg.__name__}.{mod.name}")
        for attr in vars(m).values():
            if type(attr).__name__ != "CountryConfig":
                continue
            short = getattr(attr, "authority_short", "") or ""
            name_en = getattr(attr, "name_en", "") or ""
            if short:
                out.append((mod.name, short, name_en))
    return sorted(set(out))


CONFIGS = _configs()


def test_the_fleet_is_still_the_size_this_test_thinks_it_is():
    """If the loader stops finding configs this file must fail loudly
    rather than quietly assert nothing. 46 measured 2026-10-07."""
    assert len(CONFIGS) >= 40, (
        f"only {len(CONFIGS)} country configs discovered; the loader in "
        f"this test has probably stopped matching CountryConfig")


@pytest.mark.parametrize("mod,short,name_en", CONFIGS,
                         ids=[c[0] for c in CONFIGS])
def test_every_collector_label_canonicalises_to_a_registry_label(
        mod, short, name_en):
    row = {"Source": short, "Country": name_en}
    apply_label_aliases([row])
    assert row["Source"] in REGISTRY_LABELS, (
        f"pipeline/gap_finder/countries/{mod}.py stamps Source {short!r} on "
        f"every row it writes, and the writer's canonicalisation leaves it "
        f"as {row['Source']!r} — a label tools/monitored_sources.SOURCES "
        f"does not list. The first recall this collector publishes will "
        f"reach Recalls with it and the dashboard's 'Sources monitored' "
        f"tile will not count the source. Either add an entry to "
        f"merge_master.SOURCE_ALIASES, or name the config's "
        f"authority_short so registry_source_label derives it.")


def test_the_hungarian_agency_resolves_to_its_successor():
    """The 2026-10-07 case, stated as itself.

    NÉBIH was merged into NKFH and the registry entry carries both hosts
    ("nkfh.gov.hu nebih.gov.hu"). The published row that exposed this
    (SZEGA Camembert Kft., STEC, 2026-10-06) has its URL on nkfh.gov.hu,
    so adding "NÉBIH" to the registry — which the failing assertion in
    test_monitored_sources literally asks for — would have been the wrong
    repair.
    """
    for raw in ("NÉBIH", "nébih", "NEBIH"):
        row = {"Source": raw, "Country": "Hungary"}
        apply_label_aliases([row])
        assert row["Source"] == "NKFH (HU)", (raw, row)


def test_a_country_alias_does_not_hide_the_registry_entry():
    """The kr.py case: Country is canonicalised before Source, so the
    registry index has to answer to both spellings of the country."""
    row = {"Source": "MFDS", "Country": "South Korea"}
    apply_label_aliases([row])
    assert row["Source"] == "MFDS (KR)", row
    # ...and from the already-canonical spelling too.
    row = {"Source": "MFDS", "Country": "Korea, South"}
    apply_label_aliases([row])
    assert row["Source"] == "MFDS (KR)", row
