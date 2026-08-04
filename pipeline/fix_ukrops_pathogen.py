#!/usr/bin/env python3
"""Repair the fabricated Hepatitis A pathogen on the Ukrop's recall.

WHAT WAS PUBLISHED (Recalls, promoted 2026-08-04)
=================================================
    Date      2026-08-01
    Source    FDA
    Company   Ukrops Homestyle Foods
    Product   Baked Spaghetti and Bread Pudding products
    Pathogen  Hepatitis A virus                       <- fabricated
    Reason    "Baked products have potential for presence of aluminum
               slivers from the pans that were used"
    Tier      1
    Notes     [tier-guard: Hepatitis A virus is always Tier 1;
               forced from Tier 2]
    URL       .../ukrops-homestyle-foods-announces-voluntary-recall-due-
              possible-foreign-object

The FDA permalink says "due-possible-foreign-object". Verified against the
live notice on 2026-08-04: the hazard is aluminium slivers from the baking
pans, and the words Hepatitis, Listeria and Salmonella appear nowhere on the
page. Six products are affected, not two:

    Baked Spaghetti (72251528211) and Baked Spaghetti Bulk (72251529211)
    Chicken Cobbler (72251528457) and Chicken Cobbler Bulk (72251529457)
    Bread Pudding with Vanilla Sauce (72251528044) and its KIT (72251591732)

Announced 30 July 2026, published by FDA 1 August 2026, distributed in VA,
NC, WV and KY through Food Lion, Harris Teeter, Kroger, Publix and Wegmans.

WHY IT GOT WORSE AFTER IT ARRIVED
---------------------------------
Three guards should have caught this and each failed in a different way:

  1. The Pathogen/Reason contradiction rule stayed silent, because no entry
     in the physical-hazard vocabulary matched "aluminum slivers" — so the
     Reason was unclassifiable and the rule failed safe, as designed.
  2. Worse, the Reason DID classify as biological — because the keyword list
     carried a bare "hav" (meant as the Hepatitis A virus abbreviation),
     which matched the word "HAVe" in "Baked products have potential". The
     invented pathogen and the reason therefore appeared to AGREE.
  3. The always-Tier-1 guard then read the invented pathogen and escalated
     Tier 2 -> 1, writing "[tier-guard: Hepatitis A virus is always Tier 1]"
     into Notes — so the fabrication acquired a provenance stamp that made
     the row look reviewed.

All three are fixed in code alongside this repair: the physical vocabulary
now carries qualified foreign-object shapes, "hav" is spaced so it only
matches the token, and enforce_tier1() refuses to escalate on a Pathogen the
row's own Reason contradicts.

WHAT THIS SCRIPT CHANGES
------------------------
Pathogen -> "Foreign material (aluminium)"; Tier -> 2, the value the row
carried before the tier-guard escalated it and the dominant convention for
metal foreign-material rows in this workbook; Product -> the six products
named on the notice; Reason -> the FDA wording. The row STAYS published:
foreign material is explicitly in AFTS scope.

Run:  python -m pipeline.fix_ukrops_pathogen --dry-run
      python -m pipeline.fix_ukrops_pathogen
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TODAY = "2026-08-04"
URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
       "ukrops-homestyle-foods-announces-voluntary-recall-due-possible-"
       "foreign-object")

FIX = dict(
    pathogen="Foreign material (aluminium)",
    tier=2,
    product=("Baked Spaghetti (UPC 72251528211) and Bulk (72251529211); "
             "Chicken Cobbler (72251528457) and Bulk (72251529457); Bread "
             "Pudding with Vanilla Sauce (72251528044) and KIT (72251591732)"),
    reason=("Potential presence of a foreign object — aluminium slivers from "
            "the baking pans used in production. Distributed in Virginia, "
            "North Carolina, West Virginia and Kentucky through Food Lion, "
            "Harris Teeter, Kroger, Publix and Wegmans."),
    note=("[audit 2026-08-04: Pathogen was 'Hepatitis A virus' — FABRICATED. "
          "Verified against the live FDA notice, whose permalink itself ends "
          "'due-possible-foreign-object': the hazard is aluminium slivers from "
          "the baking pans and no pathogen is named anywhere on the page. The "
          "invented pathogen also caused the always-Tier-1 guard to escalate "
          "the row from Tier 2 to Tier 1. Pathogen corrected to the real "
          "hazard, Tier restored to 2, Product expanded from 2 to the 6 "
          "products the notice lists. Guards fixed in the same pass: the "
          "physical-hazard vocabulary now matches foreign-object wording, the "
          "bare 'hav' keyword that matched the word 'have' is spaced, and "
          "enforce_tier1 no longer escalates on a contradicted Pathogen.]"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )
    from pipeline._language import englishify_reason, looks_non_english

    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)
    n_path = n_lang = 0
    unresolved = []

    for row in approved:
        if str(row.get("URL") or "").strip() == URL:
            row["Pathogen"] = FIX["pathogen"]
            row["Tier"] = FIX["tier"]
            row["Product"] = FIX["product"]
            row["Reason"] = FIX["reason"]
            # Drop the tier-guard stamp earned by the fabricated pathogen —
            # leaving it would keep asserting the row is an always-Tier-1
            # viral event.
            notes = str(row.get("Notes") or "").replace(
                "[tier-guard: Hepatitis A virus is always Tier 1; "
                "forced from Tier 2]", "").strip()
            if f"audit {TODAY}" not in notes:
                notes = (notes + " " + FIX["note"]).strip()
            row["Notes"] = notes[:1800]
            row["LastUpdated"] = TODAY
            row["LastChecked"] = TODAY
            n_path += 1
            print(f"PATHOGEN  {row.get('Date')}  Hepatitis A virus -> "
                  f"{FIX['pathogen']}  (Tier 1 -> 2)")
            continue

        before = str(row.get("Reason") or "")
        after, changed = englishify_reason(before)
        if changed:
            row["Reason"] = after
            row["LastUpdated"] = TODAY
            n_lang += 1
            print(f"LANGUAGE  {row.get('Date')}  {before[:56]!r}\n"
                  f"          -> {after[:56]!r}")
        elif looks_non_english(before):
            unresolved.append((str(row.get("Source")), before[:90]))

    print(f"\npathogen repaired : {n_path}")
    print(f"reasons translated: {n_lang}")
    print(f"unresolved        : {len(unresolved)}")
    for src, text in unresolved:
        print(f"    [{src}] {text}")

    if args.dry_run:
        print("\n(dry run — nothing written)")
        return 0
    save_xlsx_with_pending(sort_rows(approved), sort_rows(pending), xlsx)
    mirror_json_from_xlsx(xlsx, ROOT / "docs" / "data" / "recalls.json")
    print("\n✓ written + recalls.json mirrored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
