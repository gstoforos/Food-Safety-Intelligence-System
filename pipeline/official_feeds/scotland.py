# -*- coding: utf-8 -*-
"""SHIM. The Scotland collector lives in
``pipeline.official_feeds.sources.scotland``.

Same story as ``pipeline/official_feeds/uk.py`` next to it, and worse in one
way: the copy that used to sit here was OLDER than the live module. Its
header still described page-size clamping as the truncation fix, which
``sources/scotland.py`` had already replaced with date slicing in
``uk.fetch``. Anyone reading this path to find out how Scotland avoids
truncation was reading a retired answer.

Re-export only, and no ``import *`` — see the note in uk.py.
``tests/test_no_shadow_source_module.py`` keeps it this way.
"""
from __future__ import annotations

from typing import Any

from pipeline.official_feeds.sources import scotland as _real
from pipeline.official_feeds.sources.scotland import fetch  # noqa: F401


def __getattr__(name: str) -> Any:
    try:
        return getattr(_real, name)
    except AttributeError:
        raise AttributeError(
            f"module {__name__!r} is a shim for "
            f"{_real.__name__!r}, which has no attribute {name!r}") from None


def __dir__():
    return sorted(set(globals()) | set(dir(_real)))
