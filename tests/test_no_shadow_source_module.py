# -*- coding: utf-8 -*-
"""A collector has ONE home. A second copy at another path runs never.

WHAT THIS CAUGHT (morning fix 2026-10-12)
=========================================
``pipeline/official_feeds/`` held two files whose names also exist under
``pipeline/official_feeds/sources/``::

    pipeline/official_feeds/uk.py         20034 bytes   imported by nothing
    pipeline/official_feeds/sources/uk.py 19270 bytes   imported by everything

    pipeline/official_feeds/scotland.py         1550 bytes   nothing
    pipeline/official_feeds/sources/scotland.py 1728 bytes   everything

The FSA Food-Alert-for-Action fix (an establishment line is the firm, see
tests/test_an_establishment_line_is_the_firm.py) was written on 2026-10-11
and uploaded to the SHADOW. It diffed cleanly, it imported cleanly, and it
changed nothing: the collector and the test both read ``sources/uk.py``.
The test stayed red for a day inside an otherwise green suite, and the next
FAFA alert would have been held in Pending with Company empty exactly as
before.

The scotland shadow showed the other half of the cost. It was not a
pending fix but a stale one — its header described a page-size rule the
live module had already replaced — so it documented a retired answer at
the path somebody would naturally read.

WHY THE SHAPE AND NOT THE INSTANCE
----------------------------------
Both shadows now re-export from ``sources/``, so they cannot carry
different behaviour. This test holds that: a module at
``pipeline/official_feeds/<name>.py`` whose name also exists under
``sources/`` must be a shim — small, and importing from its real home. The
next upload that lands a real collector at the shadow path goes red here
immediately instead of passing silently.

A GitHub-web upload cannot delete a file, which is why these are shims
rather than deletions: dragging a folder adds and overwrites, it never
removes. A shim is the strongest thing an upload-only workflow can put at
a path it must neutralise.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "pipeline" / "official_feeds"
SOURCES = PKG / "sources"

#: A shim is a docstring plus re-export lines. Anything bigger is a module.
MAX_SHIM_BYTES = 4096


def _shadows() -> list[Path]:
    """Modules directly under official_feeds/ that also exist in sources/."""
    out = []
    for p in sorted(PKG.glob("*.py")):
        if p.name == "__init__.py":
            continue
        if (SOURCES / p.name).is_file():
            out.append(p)
    return out


SHADOWS = _shadows()


def test_the_shadow_set_is_the_one_we_know_about():
    """A NEW shadow is a new instance of the 2026-10-12 bug.

    Listing the known two by name means a third one cannot arrive quietly
    and be waved through by the per-file assertions below.
    """
    assert {p.name for p in SHADOWS} <= {"uk.py", "scotland.py"}, (
        f"new shadow module(s) under pipeline/official_feeds/ that also "
        f"exist under sources/: {sorted(p.name for p in SHADOWS)}. A "
        f"collector has one home — put it in sources/ and leave a shim here, "
        f"or give it a name sources/ does not already use.")


@pytest.mark.parametrize("path", SHADOWS, ids=lambda p: p.name)
def test_a_shadow_is_a_shim_not_a_second_collector(path: Path):
    text = path.read_text(encoding="utf-8")
    size = len(text.encode("utf-8"))
    assert size <= MAX_SHIM_BYTES, (
        f"{path.relative_to(ROOT)} is {size} bytes — too big to be a "
        f"re-export shim. If this is a fix, it belongs in "
        f"pipeline/official_feeds/sources/{path.name}, which is the module "
        f"the collector and the tests actually import. A second copy here "
        f"runs never; see this file's docstring for the day it cost.")
    assert f"from pipeline.official_feeds.sources.{path.stem} import" in text, (
        f"{path.relative_to(ROOT)} must re-export from "
        f"pipeline.official_feeds.sources.{path.stem} so the two cannot "
        f"carry different behaviour")


@pytest.mark.parametrize("path", SHADOWS, ids=lambda p: p.name)
def test_the_shim_resolves_to_the_same_objects(path: Path):
    """Re-export is not enough on its own — it has to be the SAME function.

    `from x import *` plus a stray local def would satisfy the text check
    above and still shadow. Compare identity.
    """
    import importlib

    shim = importlib.import_module(f"pipeline.official_feeds.{path.stem}")
    real = importlib.import_module(f"pipeline.official_feeds.sources.{path.stem}")
    assert shim.fetch is real.fetch, (
        f"pipeline.official_feeds.{path.stem}.fetch is not "
        f"pipeline.official_feeds.sources.{path.stem}.fetch — the shim has "
        f"grown its own implementation")
