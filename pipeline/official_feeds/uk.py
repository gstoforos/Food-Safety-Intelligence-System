# -*- coding: utf-8 -*-
"""SHIM. The UK collector lives in ``pipeline.official_feeds.sources.uk``.

WHY THIS FILE IS A SHIM AND NOT A COLLECTOR (morning fix 2026-10-12)
====================================================================
This path used to hold a full, SECOND copy of the UK collector. Nothing
imported it — every caller and every test reads
``pipeline/official_feeds/sources/uk.py`` — so it was a file that looked
like the collector, diffed plausibly against it, and ran never.

It cost a day. The FSA Food-Alert-for-Action fix (an establishment line is
the firm, FSA-FAFA-03-2026) was written on 2026-10-11 and uploaded HERE,
to the shadow. ``tests/test_an_establishment_line_is_the_firm.py`` imports
from ``sources.uk`` and stayed red through 3,887 passing tests; the live
collector was unchanged, so the next FAFA alert would have put the
establishment line in Product and left Company empty, which is what holds
a Tier-1 alert in Pending.

``pipeline/official_feeds/scotland.py`` was the same shape, and its shadow
was OLDER than the live module — its header still described a paging rule
that ``sources/scotland.py`` had already replaced.

So this file now re-exports, and cannot drift. Writing a real module back
over it is caught by ``tests/test_no_shadow_source_module.py``, which is
the point: the next upload that lands here goes red immediately instead of
passing silently for a day.

NO ``import *``. tests/test_no_undefined_names.py refuses star imports —
they blind pyflakes, which is how ``date`` and ``since`` reached production
undefined in this very package. The names the rest of the tree actually
imports from this path are listed explicitly; anything else resolves
through ``__getattr__`` below, which forwards to the real module and so
cannot carry different behaviour either.
"""
from __future__ import annotations

from typing import Any

from pipeline.official_feeds.sources import uk as _real
from pipeline.official_feeds.sources.uk import (  # noqa: F401
    _establishment,
    _firm,
    _product,
    _product_names,
    fetch,
)


def __getattr__(name: str) -> Any:
    """Forward every other attribute to the module that really has it."""
    try:
        return getattr(_real, name)
    except AttributeError:
        raise AttributeError(
            f"module {__name__!r} is a shim for "
            f"{_real.__name__!r}, which has no attribute {name!r}") from None


def __dir__():
    return sorted(set(globals()) | set(dir(_real)))
