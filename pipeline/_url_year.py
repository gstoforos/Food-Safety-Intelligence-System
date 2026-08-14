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


# Hosts where a 4-digit year in the slug is the alert's year rather than
# incidental text. Regulator domains only — see the note in url_year().
_SLUG_YEAR_HOSTS = (
    "fda.gov", "fsis.usda.gov", "food.gov.uk", "fsai.ie",
    "recalls-rappels.canada.ca", "inspection.canada.ca", "cfs.gov.hk",
    "foodstandards.gov.au", "mpi.govt.nz", "aesan.gob.es", "efet.gr",
    "lebensmittelwarnung.de", "salute.gov.it", "webgate.ec.europa.eu",
    "sfa.gov.sg", "cfia-acia.canada.ca",
)

# A year preceded by a hyphen/underscore and NOT part of a longer digit run.
_SLUG_YEAR = re.compile(r"[-_](20\d{2})(?![0-9])")


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

    # ------------------------------------------------------------------
    # THE YEAR IS OFTEN IN THE SLUG, NOT A PATH SEGMENT (audit 2026-08-13).
    #
    # Everything above looks for a year between slashes. Regulators put it in
    # the hyphenated slug instead, and that shape was invisible here:
    #
    #   .../recalls-market-withdrawals-safety-alerts/
    #       listeria-announced-top-10-recalls-july-2023#listeria-cheese
    #
    # The NA recall agent produced exactly that URL on 2026-08-12 for a 2026
    # cheese recall — a real FDA page about a THREE-YEAR-OLD recall round-up,
    # offered as the citation for a current one. url_year() returned None, so
    # is_year_mismatch() returned None, so nothing downstream objected. It was
    # stopped only because the agent's separate "not in Searx results"
    # fabrication check fired. That check is about invention, not staleness;
    # a genuinely-indexed stale page would have sailed through.
    #
    # Deliberately narrow, because a slug is noisier than a path:
    #   - regulator hosts ONLY (news slugs carry years for unrelated reasons)
    #   - the year must follow a hyphen or underscore, so a run of digits
    #     like cfs.gov.hk's /press/20260601_12425.html is not misread
    #   - RappelConso is excluded above already: its fiche IDs are sequential
    #
    # The newest year present wins: a slug naming two years is describing a
    # span, and the later one is the one to test the row's Date against.
    # ------------------------------------------------------------------
    if any(h in u for h in _SLUG_YEAR_HOSTS):
        path = u.split("://", 1)[-1]
        path = path.split("/", 1)[1] if "/" in path else ""
        years = [int(y) for y in _SLUG_YEAR.findall(path)]
        if years:
            return max(years)

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
