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


# ── ONE EVENT, ONE FLAG ──────────────────────────────────────────────────
# Operator rule (2026-08-13): when several rows cover the SAME outbreak, only
# one carries Outbreak=1, and FDA is the source of record.
#
# The 2026-08 S. Javiana jalapeño event showed why: it reached the register as
# four rows — FDA Coast Citrus, a CDC investigation page, a USDA FSIS public
# health alert, and FDA Taylor Fresh — with three of them flagged. A monthly
# that counts flagged rows reports one outbreak three times.
#
# Priority is FDA first because the FDA notice names the recalling firm and the
# product, which is what the register is for. CDC pages describe the
# investigation rather than a recall; an FSIS alert is the downstream
# consequence. For events with no FDA row (most EU/Canada/Oceania outbreaks)
# the highest-priority source actually present wins, so the flag is never lost.
_SOURCE_PRIORITY = (
    "fda",            # source of record — names firm + product
    "cdc",            # investigation page
    "usda fsis",      # downstream public-health alert
    "phac", "ukhsa", "efsa", "ecdc",
    "cfia", "rasff", "fsa", "fsai", "fsanz", "rappelconso", "aesan",
)


def _source_rank(source: str) -> int:
    s = str(source or "").lower()
    for i, key in enumerate(_SOURCE_PRIORITY):
        if key in s:
            return i
    return len(_SOURCE_PRIORITY)


# Commodity words strong enough to identify an event across agencies. A CDC
# investigation page, an FDA recall and an FSIS alert for one event share the
# pathogen, the food, and the month — but NOT a URL slug, which is why slug
# matching alone missed the jalapeño event (cdc:javiana-08-26 vs an FDA slug
# vs an FSIS alert with no slug at all).
#
# The list is deliberately specific foods, not categories: "jalapeno" is one
# commodity, "vegetable" would merge unrelated recalls. A row with no listed
# commodity gets no cross-source key and is never merged.
_COMMODITY = (
    "jalapeno", "jalapeño", "blueberr", "cantaloupe", "onion", "cucumber",
    "lettuce", "spinach", "sprout", "moringa", "pistachio", "peanut butter",
    "cashew", "tahini", "sesame", "flour", "eggs", "shell egg", "raw milk",
    "cheese", "deli meat", "hot dog", "smoked salmon", "oyster", "clam",
    "ice cream", "infant formula", "peaches", "papaya", "mango", "basil",
    "enoki", "mushroom", "chicken salad", "turkey deli", "ground beef",
)


def _commodity_key(row: Dict[str, Any]) -> str:
    blob = " ".join(str(row.get(k, "") or "") for k in
                    ("Product", "Reason", "Company")).lower()
    for c in _COMMODITY:
        if c in blob:
            return c.replace("ñ", "n")
    return ""


def cross_source_key(row: Dict[str, Any]) -> str:
    """pathogen + commodity + month — used only to link rows across agencies
    that are plainly the same event. Returns "" when the row has no listed
    commodity, so unrelated recalls are never merged."""
    com = _commodity_key(row)
    if not com:
        return ""
    # GENUS ONLY. One outbreak is one organism, but agencies name it at
    # different precision: CDC wrote "Salmonella", FDA wrote "Salmonella
    # Javiana" for the same 2026-08 jalapeño event, and a serovar-sensitive key
    # split them into two events. Reduce to the genus so they match.
    patho = str(row.get("Pathogen", "") or "").strip().lower()
    patho = patho.split("(")[0].strip()
    for _pfx in ("shiga toxin-producing", "verotoxin-producing", "pathogenic"):
        if patho.startswith(_pfx):
            patho = patho[len(_pfx):].strip()
    _genus = re.split(r"[\s,/;:]+", patho)[0] if patho else ""
    _genus = re.sub(r"[^a-z.]+", "", _genus)
    if _genus in ("e.", "e"):          # "E. coli" -> escherichia
        _genus = "escherichia"
    patho = _genus[:14]
    month = str(row.get("Date", "") or "")[:7]
    if not (patho and re.match(r"\d{4}-\d{2}$", month)):
        return ""
    return f"xs:{patho}:{com}:{month}"


def dedupe_outbreak_flags(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return the rows that should LOSE their Outbreak=1 flag.

    Two-tier grouping, in this order of authority:

    1. AGENCY SLUG WINS. If a row carries a real investigation slug, that slug
       IS the event. Two rows with DIFFERENT slugs are DIFFERENT events and are
       never merged — the 2026-05 moringa rows proved why: CDC ran two separate
       investigations that month (supergreenssupplementpowders-1-26 and
       moringa-05-26), and a pathogen+commodity+month key fused them into one.

    2. A row with NO slug may join a slug group via the cross-source key
       (pathogen genus + commodity + month), but ONLY if it matches exactly one
       such group. If it matches two, it is ambiguous and left alone. This is
       what links the FDA recall and the FSIS alert to the CDC jalapeño
       investigation, none of which share a URL pattern.

    Within a group the flag stays on the highest-priority source — FDA first,
    per the operator rule. Rows that cannot be placed keep their flag: an
    unidentifiable outbreak is still an outbreak.
    """
    flagged = []
    for r in rows:
        try:
            if int(str(r.get("Outbreak", 0)).strip() or 0) == 1:
                flagged.append(r)
        except (TypeError, ValueError):
            pass

    slug_groups: Dict[str, List[Dict[str, Any]]] = {}
    slugless: List[Dict[str, Any]] = []
    for r in flagged:
        oid, conf, _ = derive(r)
        if oid and conf == "high":
            slug_groups.setdefault(oid, []).append(r)
        else:
            slugless.append(r)

    # Which cross-source keys does each slug group answer to?
    key_to_slugs: Dict[str, set] = {}
    for slug, members in slug_groups.items():
        for m in members:
            k = cross_source_key(m)
            if k:
                key_to_slugs.setdefault(k, set()).add(slug)

    # MERGE ACROSS AGENCIES, NOT WITHIN ONE.
    # CDC and FDA each publish their own slug for the same event
    # (cdc:javiana-08-26 and fda:salmonella-jalapeno-august-2026 are one
    # outbreak), so slug groups from DIFFERENT agencies sharing a cross-source
    # key are the same event and merge. But two slugs from the SAME agency are
    # two investigations that agency deliberately kept apart — CDC's
    # supergreenssupplementpowders-1-26 and moringa-05-26 in 2026-05 — and must
    # never be fused.
    merged_into: Dict[str, str] = {}
    for k, owners in key_to_slugs.items():
        if len(owners) < 2:
            continue
        prefixes = [s.split(":", 1)[0] for s in owners]
        if len(prefixes) != len(set(prefixes)):
            continue                      # same agency twice — ambiguous
        canonical = sorted(owners)[0]
        for s in owners:
            merged_into[s] = canonical
    if merged_into:
        for src, dst in merged_into.items():
            if src != dst and src in slug_groups:
                slug_groups.setdefault(dst, []).extend(slug_groups.pop(src))
        for k, owners in key_to_slugs.items():
            key_to_slugs[k] = {merged_into.get(s, s) for s in owners}

    for r in slugless:
        k = cross_source_key(r)
        if not k:
            continue
        owners = key_to_slugs.get(k, set())
        if len(owners) == 1:                  # unambiguous — attach
            slug_groups[next(iter(owners))].append(r)
        elif not owners:                      # a group of slugless rows
            slug_groups.setdefault(k, []).append(r)
        # len(owners) > 1 -> ambiguous, leave the row's flag alone

    losers: List[Dict[str, Any]] = []
    for event, members in slug_groups.items():
        if len(members) < 2:
            continue
        members.sort(key=lambda r: (_source_rank(r.get("Source", "")),
                                    str(r.get("Date", ""))))
        keeper = members[0]
        for r in members[1:]:
            r["_outbreak_keeper"] = str(keeper.get("Source", ""))
            r["_outbreak_event"] = event
            losers.append(r)
    return losers
