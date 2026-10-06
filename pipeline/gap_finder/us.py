# -*- coding: utf-8 -*-
"""SUPERSEDED 2026-10-06 by pipeline/gap_finder/countries/us.py.

This file held a complete US CountryConfig sitting one directory above
``pipeline/gap_finder/countries/``, where the registry walks. Nothing
imported it, so it was never registered and never ran. The live config is
``countries/us.py``, whose content is a strict superset of what was here —
same authority, same item regex character for character, same timezone, same
cron offsets, plus the PR #30 merge of 2026-09-24 (cidrap.umn.edu,
foodpoisoningbulletin.com, foxnews.com, and the "adulterated / contaminated
/ unsafe" terms). Nothing is lost by this file going away.

Same shape as pipeline/gap_finder/india.py, found in the same sweep: a
country config outside the directory the registry reads is a file, not
coverage, and it fails silently in both directions — see the docstring of
tests/test_a_country_config_is_where_the_registry_looks.py.

THIS FILE IS A SHIM AND CAN BE DELETED. It exists only because this repo is
updated by uploading files through the GitHub web UI, which cannot delete.
It deliberately defines no CountryConfig.
"""

from .countries.us import UNITED_STATES  # noqa: F401  — re-export

__all__ = ["UNITED_STATES"]
