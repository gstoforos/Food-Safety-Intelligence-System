"""Canonical URL-year extractor — source-aware. Locked 2026-04-30.

Used by:
  - pipeline/url_gate_gemini.py
  - pipeline/claude_check.py
  - pipeline/merge_master.py

Returns the year encoded in a URL when one is clearly present.
Returns None when ambiguous — caller MUST NOT treat None as mismatch.
"""
from __future__ import annotations
import re
from datetime import date
from typing import Optional


def url_year(url: str, source: str = "") -> Optional[int]:
    """Extract a year from a URL. Returns None if not reliably extractable."""
    if not url:
        return None
    u = str(url).lower()

    # RappelConso new format: /fiche-rappel/YYYY-MM-NNNN/
    m = re.search(r"/fiche-rappel/(20\d{2})-\d{2}-\d+", u)
    if m:
        return int(m.group(1))

    # RappelConso old numeric fiche ID — sequential, NOT a year
    if "rappel.conso.gouv.fr" in u and re.search(r"/fiche-rappel/\d{4,5}/", u):
        return None

    # produktwarnung.eu, news mirrors: /YYYY/MM/DD/
    m = re.search(r"/(20\d{2})/\d{2}/\d{2}/", u)
    if m:
        return int(m.group(1))

    # FDA recall numbers: F-NNNN-YYYY
    m = re.search(r"f-\d+-(20\d{2})", u)
    if m:
        return int(m.group(1))

    # USDA FSIS recall slugs (only when domain matches)
    if "fsis.usda.gov" in u:
        m = re.search(r"recalls?/[\w-]*?(20\d{2})", u)
        if m:
            return int(m.group(1))

    # Generic /YYYY/ in path with non-digit boundary
    m = re.search(r"/(20\d{2})/(?!\d)", u)
    if m:
        return int(m.group(1))

    # PDF filenames with embedded digits — too noisy, skip
    return None


def is_year_mismatch(row_date: Optional[date], url: str, source: str = "") -> Optional[str]:
    """Returns reason string if URL year clearly conflicts with row's Date year.
    Returns None when no conflict (or no extractable URL year).
    """
    if not row_date:
        return None
    uy = url_year(url, source)
    if uy is None:
        return None
    if uy < 2026:
        return f"URL year {uy} pre-2026; FSIS scope is 2026+"
    if abs(uy - row_date.year) > 1:
        return f"URL year {uy} mismatches Date year {row_date.year} by >1"
    return None
