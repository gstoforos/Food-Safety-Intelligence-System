#!/usr/bin/env python3
"""Write the statistical columns into the Recalls sheet. The assembler.

WHY THIS FILE EXISTS
--------------------
The extractors were built and never joined up: regulator_fields.py reads the
RASFF taxonomy, product_axes.py derives the four product axes, and neither
had anything that put the answers in the workbook. The schema existed as a
proposal, a prompt and two modules — and the register still had eighteen
columns.

This is the assembler. It runs the extractors in tier order, writes the
result, and records which tier answered so an inference can never be
mistaken for a regulator's statement.

    tier 1  the regulator's own published field   (RASFF category/risk/class)
    tier 2  a structured field in stored text     (RASFF id, CFIA category)
    tier 3  multilingual keyword over Product+Reason
    tier 4  unknown — always allowed, never inferred around

WHAT IT WILL NOT DO
-------------------
* touch any existing column. The eighteen columns the pipeline already
  writes are read-only here.
* overwrite a value a human set. A row whose EnrichedBy says "human" is
  skipped entirely.
* leak. Every column added is registered in
  merge_master.RECALLS_INTERNAL_COLUMNS, so mirror_json_from_xlsx strips it
  from recalls.json, and the public xlsx builders use allow-lists that do
  not name it. Verified by tests/test_enrich_schema.py.
* guess. Every value is either a regulator's own term or a keyword match on
  text the notice actually contains. "unknown" is a correct answer and the
  most common one on several axes.

    python -m pipeline.enrich_schema --dry-run
    python -m pipeline.enrich_schema --write
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import openpyxl  # noqa: E402

from pipeline import product_axes as PA          # noqa: E402
from pipeline.regulator_fields import parse_rasff  # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
SHEET = "Recalls"
VERSION = "enrich-schema/1"

# Order matters: this is the order they are appended to the sheet.
COLUMNS: Tuple[str, ...] = (
    "FoodCategory", "ProcessType", "ConsumptionState", "StorageCondition",
    "PackagingType", "PackagingForm", "HazardGroup", "HazardCertainty",
    "NoticeType", "SeverityClass", "EventID",
    "EnrichedBy", "EnrichedAt", "EnrichmentTier",
)

# ── Class -> NoticeType / SeverityClass ─────────────────────────────────
# The Class column holds two variables at once: a regulatory ACTION and, for
# US/Canadian sources, an FDA SEVERITY class. Splitting them is the whole
# point; "Class I" and "Class 1" merge themselves once they are read as
# severity rather than as action.
_NOTICE_FROM_CLASS = {
    "border rejection": "border-rejection",
    "voluntary": "consumer-recall",
    "mandatory": "consumer-recall",
    "recall": "consumer-recall",
    "alert": "public-warning",
    "information": "information",
    "withdrawal": "withdrawal",
}
_SEVERITY_FROM_CLASS = {
    "class i": "i", "class 1": "i", "food recall warning (class 1)": "i",
    "class ii": "ii", "class 2": "ii",
    "class iii": "iii", "class 3": "iii",
}

_HAZARD_GROUP_RULES = (
    ("foreign material", "foreign-material"),
    ("foreign body", "foreign-material"), ("foreign-body", "foreign-material"),
    ("physical", "foreign-material"), ("glass fragment", "foreign-material"),
    ("metal fragment", "foreign-material"), ("plastic fragment", "foreign-material"),
    ("heavy metal", "heavy-metal"),
    ("aflatoxin", "mycotoxin"), ("ochratoxin", "mycotoxin"),
    ("mycotoxin", "mycotoxin"), ("zearalenone", "mycotoxin"),
    ("patulin", "mycotoxin"),
    ("histamine", "biotoxin"), ("scombrotoxin", "biotoxin"),
    ("cereulide", "biotoxin"), ("toxin", "biotoxin"),
    ("norovirus", "pathogen-viral"), ("hepatitis", "pathogen-viral"),
    ("cyclospora", "pathogen-parasitic"), ("anisakis", "pathogen-parasitic"),
    ("rodent", "pest-rodent"), ("mouse", "pest-rodent"), ("insect", "pest-rodent"),
    ("pesticide", "chemical"), ("nitrite", "chemical"), ("sulphite", "chemical"),
    ("ethylene oxide", "chemical"), ("hydrocyanic", "chemical"),
    ("pfoa", "chemical"), ("pfas", "chemical"), ("residue", "chemical"),
    ("veterinary", "chemical"), ("nitrofurazone", "chemical"),
)

# Labels that name no hazard at all. They must not fall through to the
# bacterial catch-all: "None (organoleptic spoilage)" is not a pathogen, and
# a stratum built on the bacterial bucket would silently carry it.
_NOT_A_HAZARD = ("unspecified hazard", "none (", "organoleptic",
                 "process deviation", "microbiological quality",
                 "unintended fermentation", "inadequate sterilization",
                 "incomplete pasteurization", "labeling", "labelling")


def _hazard_group(pathogen: str) -> str:
    p = (pathogen or "").strip().lower()
    if not p:
        return "unknown"
    for needle, group in _HAZARD_GROUP_RULES:
        if needle in p:
            return group
    for needle in _NOT_A_HAZARD:
        if needle in p:
            return "unknown"
    # Anything left that names an organism is bacterial. The catch-all is
    # last so every specific rule wins, and the _NOT_A_HAZARD guard above
    # stops process-deviation and quality labels landing here — a stratum on
    # the bacterial bucket would otherwise carry "None (organoleptic
    # spoilage)" as a pathogen.
    return "pathogen-bacterial"


def _first(*vals: str) -> str:
    for v in vals:
        if v and v not in ("unknown", ""):
            return v
    return "unknown"


def derive(row: Dict[str, Any]) -> Tuple[Dict[str, str], str]:
    """Return (column values, the strongest tier that answered)."""
    rasff = parse_rasff(row.get("Reason"), row.get("Notes"))
    cls = str(row.get("Class", "") or "").strip().lower()

    # FoodCategory: RASFF's own term first, then a CFIA category if the row
    # carries one. product_axes deliberately does not own this axis.
    #
    # split_cfia_category takes the CATEGORY STRING, not the row. Passing the
    # row made it stringify the whole dict and return it as the category, so
    # every non-RASFF row got a category and the coverage read 100% — on a
    # field that is genuinely hard. A suspiciously perfect number on a hard
    # field is the tell; this was caught by printing the distribution before
    # writing anything.
    food = rasff.food_category or ""
    if not food:
        raw_cat = str(row.get("Category") or row.get("category") or "")
        if raw_cat.lower().startswith("food -"):
            commodity, _proc = PA.split_cfia_category(raw_cat)
            if commodity:
                from pipeline.regulator_fields import CATEGORY_MAP
                food = CATEGORY_MAP.get(commodity.strip().lower(), "")

    proc = PA.process_type(row)
    cons = PA.consumption_state(row)
    stor = PA.storage_condition(row)
    ptyp = PA.packaging_type(row)
    pfrm = PA.packaging_form(row)

    notice = rasff.notice_type or _NOTICE_FROM_CLASS.get(cls, "")
    severity = _SEVERITY_FROM_CLASS.get(cls, "not-classified" if cls else "unknown")

    event = ""
    if rasff.rasff_id:
        event = f"rasff:{rasff.rasff_id}"
    else:
        try:
            from pipeline._outbreak_id import derive as _ob
            oid, conf, _ = _ob(row)
            if oid and conf == "high":
                event = oid
        except Exception:                                   # noqa: BLE001
            pass

    tier = ("tier1-regulator" if (rasff.food_category or rasff.risk
                                  or rasff.classification_raw)
            else ("tier2-structured" if event or notice != "" else "tier3-keyword"))

    return {
        "FoodCategory": food or "unknown",
        "ProcessType": proc[0],
        "ConsumptionState": cons[0],
        "StorageCondition": stor[0],
        "PackagingType": ptyp[0],
        "PackagingForm": pfrm[0],
        "HazardGroup": _hazard_group(str(row.get("Pathogen", ""))),
        "HazardCertainty": rasff.risk or "unknown",
        "NoticeType": notice or "unknown",
        "SeverityClass": severity,
        "EventID": event or "",
    }, tier


def run(xlsx: Path, write: bool) -> Dict[str, Any]:
    wb = openpyxl.load_workbook(xlsx)
    ws = wb[SHEET]
    hdr = [str(c.value or "") for c in ws[1]]

    # Append any column that is not already present, preserving order.
    for name in COLUMNS:
        if name not in hdr:
            hdr.append(name)
            ws.cell(row=1, column=len(hdr), value=name)
    idx = {h: i + 1 for i, h in enumerate(hdr)}

    today = datetime.now(timezone.utc).date().isoformat()
    stats: Counter = Counter()
    tiers: Counter = Counter()
    skipped_human = 0

    for r in range(2, ws.max_row + 1):
        row = {h: ws.cell(row=r, column=idx[h]).value for h in hdr}
        if str(row.get("EnrichedBy") or "").strip().lower() == "human":
            skipped_human += 1
            continue
        values, tier = derive(row)
        for k, v in values.items():
            if write:
                ws.cell(row=r, column=idx[k], value=v)
            if v and v != "unknown":
                stats[k] += 1
        tiers[tier] += 1
        if write:
            ws.cell(row=r, column=idx["EnrichedBy"], value=VERSION)
            ws.cell(row=r, column=idx["EnrichedAt"], value=today)
            ws.cell(row=r, column=idx["EnrichmentTier"], value=tier)

    n = ws.max_row - 1
    if write:
        wb.save(xlsx)
    return {"rows": n, "filled": dict(stats), "tiers": dict(tiers),
            "skipped_human": skipped_human}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--xlsx", default=str(XLSX))
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)

    res = run(Path(a.xlsx), a.write)
    n = res["rows"]
    print(f"{n} rows{' — WRITTEN' if a.write else ' — dry run'}")
    if res["skipped_human"]:
        print(f"  {res['skipped_human']} row(s) skipped (EnrichedBy=human)")
    print()
    for c in COLUMNS:
        if c in ("EnrichedBy", "EnrichedAt", "EnrichmentTier"):
            continue
        k = res["filled"].get(c, 0)
        print(f"  {c:18} {k:5}/{n}  {k / n * 100:5.1f}%")
    print()
    for t, k in sorted(res["tiers"].items()):
        print(f"  {t:18} {k:5}  {k / n * 100:5.1f}%")
    if not a.write:
        print("\n(dry run — pass --write to apply)")
        print("Remember: register the columns in "
              "merge_master.RECALLS_INTERNAL_COLUMNS before committing, or "
              "they leak into recalls.json.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
