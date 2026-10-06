# -*- coding: utf-8 -*-
"""MOVED 2026-10-06 to pipeline/gap_finder/countries/india.py.

This file was where India's gap-finder config was written on 2026-10-05.
That is one directory above ``pipeline/gap_finder/countries/``, which is the
directory ``countries.all_codes()`` walks — so nothing imported it,
``get("in")`` raised, ``all_codes()`` returned 45 codes without ``"in"``, the
fleet sharder could not dispatch India, and the register claimed coverage it
did not have for a day. The config itself was correct; only its directory
was wrong.

THIS FILE IS A SHIM AND CAN BE DELETED. It exists only because this repo is
updated by uploading files through the GitHub web UI, which can add and
overwrite but cannot delete — so the move had to arrive as "new file in
countries/ + this stub here". Deleting it changes nothing.

It deliberately defines NO CountryConfig: a second CountryConfig for "in"
outside countries/ is the exact defect
tests/test_a_country_config_is_where_the_registry_looks.py now refuses.
"""

from .countries.india import INDIA  # noqa: F401  — re-export for any caller

__all__ = ["INDIA"]
