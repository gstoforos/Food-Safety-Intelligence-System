"""A source the dashboard counts as monitored must have something collecting it.

WHAT WAS WRONG (found 2026-10-11)
=================================
`tools/monitored_sources.py` is the list the dashboard's "Sources monitored"
tile reads, via docs/data/sources.json. One entry on it has no collector
anywhere in the repository:

    ("MAPAQ QC", "Canada", "mapaq.gouv.qc.ca")

`scrapers/mapaq.py` exists and looks like the one — and contains the Qatar
MoPH scraper (`AGENCY = "MoPH (QA)"`, INDEX_URLS on moph.gov.qa). It is one
of the ~50 legacy single-class modules directly under `scrapers/` whose
filenames are permuted against their contents; `tools/move_to_attic.sh` and
the two xfails in tests/test_scraper_registry.py exist to retire that whole
layer, and the live collectors are the ones in the subpackages. For every
other agency the permutation is cosmetic, because the agency IS there under
some other filename. MAPAQ is the one that is simply absent: no module in
`scrapers/` or `pipeline/` mentions mapaq.gouv.qc.ca or quebec.ca as a
collection target, and there is no gap_finder country for Canada.

THE COST, MEASURED. The newest MAPAQ QC row in the register is 2026-04-13 —
181 days before this test was written. On 2026-10-10 two in-scope Québec
notices were published (a Clostridium botulinum risk in Naloune brand
marinade sold by Manas épicerie africaine inc.; E. coli in HUMMUS HALAL
sold by Marché Sultan) and neither reached the register. The four MAPAQ rows
that are in it arrived through the Tavily gap-finder path, which is not part
of the daily scrape fleet.

WHAT THIS TEST PINS
-------------------
Not that MAPAQ is fixed — it is not, and writing a collector is a separate
decision, partly because quebec.ca notice URLs carry an unguessable numeric
id. It pins that MAPAQ is the ONLY such entry, so the next source added to
the monitored list without a collector fails here instead of showing up on
the tile and going quiet for six months.

Remove it from KNOWN_UNCOVERED the moment a collector lands.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.monitored_sources import SOURCES                    # noqa: E402

#: Source labels advertised on the tile with nothing collecting them.
#: Every entry here is a known hole, named so it cannot be forgotten.
KNOWN_UNCOVERED: set = set()
# "MAPAQ QC" LEFT this set on 2026-10-11: pipeline/gap_finder/countries/ca.py
# (operator: "yes" to writing the collector). Empty means no known holes.

#: Where a collector could live.
COLLECTOR_DIRS = ("scrapers", "pipeline")


def _collector_text() -> str:
    parts = []
    for d in COLLECTOR_DIRS:
        for p in (ROOT / d).rglob("*.py"):
            if "__pycache__" in p.parts:
                continue
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts).lower()


def test_every_monitored_source_host_appears_in_a_collector():
    blob = _collector_text()
    uncovered = []
    for row in SOURCES:
        label, hosts = row[0], str(row[2] or "")
        # An entry may declare several hosts; one reachable host is enough.
        candidates = [h for h in hosts.replace(",", " ").split() if h]
        if not candidates:
            continue
        if any(h.lower() in blob for h in candidates):
            continue
        uncovered.append((label, hosts))

    surprises = [u for u in uncovered if u[0] not in KNOWN_UNCOVERED]
    assert not surprises, (
        "These sources are counted on the dashboard's 'Sources monitored' "
        "tile and no module in scrapers/ or pipeline/ targets their host — "
        "the tile overstates coverage and nothing will notice:\n  "
        + "\n  ".join(f"{lab}  ({h})" for lab, h in surprises))


def test_known_uncovered_is_not_stale():
    """When a collector lands, this list must shrink with it."""
    blob = _collector_text()
    by_label = {row[0]: str(row[2] or "") for row in SOURCES}
    fixed = []
    for label in KNOWN_UNCOVERED:
        hosts = [h for h in by_label.get(label, "").replace(",", " ").split() if h]
        if hosts and any(h.lower() in blob for h in hosts):
            fixed.append(label)
    assert not fixed, (
        f"{fixed} now has a collector. Remove it from KNOWN_UNCOVERED so the "
        f"list keeps meaning 'these are the holes'.")


def test_mapaq_is_still_the_qatar_scraper_or_gone():
    """Names the booby trap, so nobody else loses an afternoon to it.

    scrapers/mapaq.py is NOT a MAPAQ collector. If someone writes a real one
    there this assertion flips and should simply be deleted along with the
    KNOWN_UNCOVERED entry.
    """
    p = ROOT / "scrapers" / "mapaq.py"
    if not p.exists():
        return                      # retired to _attic: fine
    body = p.read_text(encoding="utf-8", errors="replace").lower()
    assert "mapaq" not in body.replace("mapaq.py", ""), (
        "scrapers/mapaq.py now mentions MAPAQ. If it is a real collector, "
        "drop 'MAPAQ QC' from KNOWN_UNCOVERED above and delete this test.")
