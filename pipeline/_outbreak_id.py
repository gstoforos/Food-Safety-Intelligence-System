#!/usr/bin/env python3
"""
_outbreak_id.py  —  event identity for outbreak-flagged recalls
================================================================

WHY
---
`Outbreak` is a per-ROW boolean, but a single outbreak generates several rows:
the contaminated ingredient, each downstream manufacturer, the health-agency
investigation page, and any regulator's public-health alert. Counting rows
therefore overstates the number of outbreaks.

Measured on the register (2026-08-11), 35 rows carry Outbreak=1, but:

    2026-05 Salmonella          4 rows  (3 moringa brands + 1 noodle event)
    2026-04 Salmonella          4 rows  (2 pistachio rows + moringa + spring rolls)
    2026-04 S. Bovismorbificans 2 rows  (Good4U Super Sprouts, duplicated)

and the 2026-08 S. Javiana jalapeño event alone spans 4 rows (Coast Citrus,
two CDC pages, one FSIS alert). The headline outbreak KPI is inflated by
roughly 15%.

WHAT THIS DOES
--------------
Derives a stable `OutbreakID` so a report can count DISTINCT EVENTS:

    outbreaks_this_month = len({r["OutbreakID"] for r in rows
                                if r["Outbreak"] == 1 and r["OutbreakID"]})

Identity is taken from the strongest available evidence, in order:

  1. The health agency's own investigation slug, parsed from the URL. This is
     the authoritative event id and is shared by every page of one
     investigation:
        cdc.gov/salmonella/outbreaks/javiana-08-26/...     -> cdc:javiana-08-26
        cdc.gov/listeria/outbreaks/blueberries-07-26/...   -> cdc:blueberries-07-26
        fda.gov/.../outbreak-investigation-salmonella-jalapenos-august-2026
                                                          -> fda:salmonella-jalapenos-august-2026
  2. An explicit id already written into Notes as [outbreak:<id>] — this is the
     operator override and always wins over the derived key below.
  3. A derived key of pathogen + ISO month, used only as a LAST resort. It is
     deliberately coarse: it will group two genuinely separate Salmonella
     events in the same month. That is why `derive()` returns the id together
     with a confidence, and why a low-confidence id should be reviewed rather
     than trusted for a published count.

NOTHING IS GUESSED SILENTLY: every id carries its source, so a report can show
which events were identified from a real investigation page and which were
inferred.

USAGE
-----
    from pipeline._outbreak_id import derive, count_events

    oid, confidence, how = derive(row)
    n = count_events(rows)          # distinct events among Outbreak=1 rows
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

# ── 1. investigation slugs in health-agency URLs ─────────────────────────
# CDC:  /<pathogen>/outbreaks/<slug>/...        slug e.g. "javiana-08-26"
_CDC_RE = re.compile(
    r"cdc\.gov/([a-z0-9\-]+)/outbreaks/([a-z0-9\-]+)", re.I)
# FDA:  /outbreak-investigation-<...>  or  /outbreaks-foodborne-illness/<slug>
_FDA_RE = re.compile(
    r"fda\.gov/[^?#]*?outbreak[-/]investigation[-/]([a-z0-9\-]+)", re.I)
# PHAC / UKHSA style: /outbreak/<slug> or /outbreaks/<slug>
_GENERIC_RE = re.compile(
    r"/outbreaks?/([a-z0-9][a-z0-9\-]{3,})(?:[/?#]|$)", re.I)

# ── 2. operator override written into Notes ──────────────────────────────
_NOTES_RE = re.compile(r"\[outbreak:\s*([A-Za-z0-9:_\-\.]+)\s*\]")

_TRAILING_JUNK = re.compile(r"(index|investigation|details?|home|en|update)$", re.I)


def _clean_slug(s: str) -> str:
    s = str(s or "").strip().strip("-/").lower()
    s = _TRAILING_JUNK.sub("", s).strip("-")
    return s


def derive(row: Dict[str, Any]) -> Tuple[str, str, str]:
    """Return (outbreak_id, confidence, how).

    confidence is "high" when the id came from a real investigation slug or an
    operator override, "low" when it was inferred from pathogen+month.
    Returns ("", "none", "not an outbreak row") when Outbreak is not 1.
    """
    try:
        flag = int(str(row.get("Outbreak", 0)).strip() or 0)
    except (TypeError, ValueError):
        flag = 0
    if flag != 1:
        return "", "none", "not an outbreak row"

    notes = str(row.get("Notes", "") or "")
    m = _NOTES_RE.search(notes)
    if m:
        return m.group(1).lower(), "high", "operator override in Notes"

    url = str(row.get("URL", "") or "")
    m = _CDC_RE.search(url)
    if m:
        slug = _clean_slug(m.group(2))
        if slug:
            return f"cdc:{slug}", "high", "CDC investigation slug"
    m = _FDA_RE.search(url)
    if m:
        slug = _clean_slug(m.group(1))
        if slug:
            return f"fda:{slug}", "high", "FDA investigation slug"
    m = _GENERIC_RE.search(url)
    if m:
        slug = _clean_slug(m.group(1))
        if slug and not slug.isdigit():
            return f"src:{slug}", "high", "investigation slug in URL"

    pathogen = str(row.get("Pathogen", "") or "").strip().lower()
    pathogen = re.sub(r"[^a-z0-9]+", "-", pathogen).strip("-")[:32]
    month = str(row.get("Date", "") or "")[:7]
    if pathogen and re.match(r"\d{4}-\d{2}$", month):
        return (f"inferred:{pathogen}:{month}", "low",
                "inferred from pathogen + month — NOT an investigation id")
    return "", "none", "no identity available"


def annotate(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return the rows with OutbreakID / OutbreakIDSource filled in place."""
    out = []
    for r in rows:
        oid, conf, how = derive(r)
        r["OutbreakID"] = oid
        r["OutbreakIDSource"] = f"{conf}: {how}" if oid else ""
        out.append(r)
    return out


def count_events(rows: Iterable[Dict[str, Any]],
                 high_confidence_only: bool = True) -> int:
    """Distinct outbreak EVENTS among the given rows.

    DEFAULT IS high_confidence_only=True, and that default matters.

    Merging on the inferred pathogen+month key was tested against the register
    and it is NOT safe: `inferred:salmonella:2026-04` collapsed four genuinely
    separate events into one — Canadian pistachios, a second pistachio recall,
    an Ambrosia moringa supplement, and the Kaohsiung spring-roll outbreak.
    Trading a 5-row over-count for a 4-event under-count is a worse error,
    because an under-count hides outbreaks.

    So by default only a real investigation slug (or an operator override)
    merges rows. Everything else counts individually, exactly as today. The
    result is conservative: it removes the duplication we can PROVE
    (multi-page CDC/FDA investigations) and invents nothing.

    Rows whose id could not be derived at all are counted individually — an
    unidentifiable outbreak is still an outbreak, and silently dropping it
    would understate the figure.
    """
    ids, unidentified = set(), 0
    for r in rows:
        oid, conf, _ = derive(r)
        if not oid:
            try:
                if int(str(r.get("Outbreak", 0)).strip() or 0) == 1:
                    unidentified += 1
            except (TypeError, ValueError):
                pass
            continue
        if high_confidence_only and conf != "high":
            unidentified += 1
            continue
        ids.add(oid)
    return len(ids) + unidentified


def report(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Summary for an operator: rows vs events, and which ids are inferred."""
    rows = list(rows)
    flagged = [r for r in rows
               if str(r.get("Outbreak", "")).strip() in ("1", "1.0")]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    low: set = set()
    for r in flagged:
        oid, conf, _ = derive(r)
        key = oid or f"<unidentified:{id(r)}>"
        groups.setdefault(key, []).append(r)
        if conf == "low":
            low.add(key)
    return {
        "outbreak_rows": len(flagged),
        "distinct_events": len(groups),
        "overcount": len(flagged) - len(groups),
        "low_confidence_ids": sorted(low),
        "groups": {k: [f"{str(v.get('Company',''))[:24]} / "
                       f"{str(v.get('Product',''))[:28]}" for v in vs]
                   for k, vs in groups.items() if len(vs) > 1},
    }
