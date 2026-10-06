# -*- coding: utf-8 -*-
"""A config in the wrong directory is a file, not coverage.

WHAT THIS CAUGHT (morning-fix 2026-10-06)
=========================================
India's gap finder was written on 2026-10-05 on an operator request
("build India same concept as Greece"), complete with a verified
news-authority mode, a curated national-press whitelist and an entry in
``tests/test_no_country_goes_dark.NOT_YET_RUN``. It was saved as
``pipeline/gap_finder/india.py`` — one directory above
``pipeline/gap_finder/countries/``, which is the directory
``countries.all_codes()`` walks.

So nothing imported it, ``get("in")`` raised, ``all_codes()`` returned 45
codes without ``"in"``, the fleet sharder could not dispatch it, and the
register claimed India coverage that did not exist. The only reason it
surfaced at all is that
``test_no_country_goes_dark::test_a_known_dark_country_is_still_registered``
reads the KNOWN_DARK list against the registry and said ``'in'`` is on
the list but has no config — which is the right alarm wired to the wrong
diagnosis: the config existed, the registry just could not see it.

``pipeline/gap_finder/us.py`` was the same shape, older and quieter: a
complete US config sitting beside the real one in ``countries/us.py``,
whose content it was a strict subset of (the live file carries the PR #30
merge of 2026-09-24). Nothing imported it either. Both were resolved on
2026-10-06 — India moved into ``countries/``, the orphan ``us.py``
deleted.

WHY A TEST AND NOT A CONVENTION
-------------------------------
Because the failure is silent in both directions. A config in the wrong
place throws no error and produces no run_log, and a run_log that never
appears looks exactly like a country the scheduler has not reached yet —
which is a state the fleet legitimately has nineteen countries in. There
is nothing to notice.

India's own module docstring explains that the file is ``india.py`` and
not ``in.py`` because ``in`` is a Python keyword, exactly as Iceland is
``iceland.py``. That part was right. The directory was the mistake, and a
filename rule is what made it plausible.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

GAP_FINDER = ROOT / "pipeline" / "gap_finder"
COUNTRIES = GAP_FINDER / "countries"


def _modules_defining_a_country_config(where: Path) -> dict[str, str]:
    """{module path -> the code= literal} for every CountryConfig(...) found.

    Parsed, not imported: an orphan module may not even import cleanly, and
    a test that imports it would fail for the wrong reason.
    """
    found: dict[str, str] = {}
    for py in sorted(where.glob("*.py")):
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:                                 # pragma: no cover
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = getattr(fn, "id", None) or getattr(fn, "attr", None)
            if name != "CountryConfig":
                continue
            code = "?"
            for kw in node.keywords:
                if kw.arg == "code" and isinstance(kw.value, ast.Constant):
                    code = str(kw.value.value)
            found[str(py.relative_to(ROOT))] = code
    return found


def test_no_country_config_lives_outside_the_countries_package():
    stray = _modules_defining_a_country_config(GAP_FINDER)
    assert not stray, (
        f"{len(stray)} CountryConfig(...) defined in pipeline/gap_finder/ "
        f"instead of pipeline/gap_finder/countries/, where "
        f"countries.all_codes() walks for them: "
        f"{stray}. A config here is never imported, never dispatched and "
        f"never writes a run_log — it looks exactly like a country the "
        f"scheduler has not reached yet. Move it into countries/.")


def test_every_countries_module_is_registered_under_its_own_code():
    """The directory walk and the registry must agree, both ways."""
    from pipeline.gap_finder.countries import all_codes

    on_disk = _modules_defining_a_country_config(COUNTRIES)
    registered = set(all_codes())

    missing = {m: c for m, c in on_disk.items() if c not in registered}
    assert not missing, (
        f"module(s) under countries/ define a CountryConfig whose code the "
        f"registry does not hold: {missing}. Either register() is not being "
        f"called at import time or the module is not importable")

    assert len(registered) >= len(on_disk), (
        f"{len(on_disk)} config modules on disk but only {len(registered)} "
        f"codes registered")


def test_india_is_registered():
    """Pinned by name. India was written, listed as pending coverage, and
    invisible to the registry for a day; this is the assertion that would
    have said so on 2026-10-05."""
    from pipeline.gap_finder.countries import all_codes, get

    assert "in" in all_codes(), (
        "India's config is not registered — check it is under "
        "pipeline/gap_finder/countries/")
    assert get("in").authority_short == "FSSAI"
