#!/usr/bin/env python3
"""Split the RASFF bilingual notification subjects still sitting in Product.

WHY THEY SURVIVED THE 2026-08-02 PASS
=====================================
fix_language_2026_08_02 only touched Product when looks_non_english(Product)
was true. For a bilingual string the English half pulls the whole value back
toward English, so eight RASFF rows read as "English enough" and were skipped
even though half of each is Spanish, Polish or Italian:

    "Presencia de Salmonela spp en salchichón procedente de España //
     Presence of Salmonella spp. in cured sausage from Spain"

    "pałeczki Salmonelli oraz Tiametoksam w papryce chilli mielonej z Indii //
     Salmonella and thiamethoxam in ground chilli pepper from India"

The right test is not "does this look foreign" but "does this split into two
languages" — which is what split_bilingual already answers. This pass applies
it directly to Product, so the condition matches the question.

Two false-positive classes were fixed in _language.py in the same audit before
running this, because both would have TRUNCATED real product names here:

  · ";" is RappelConso's list separator and ordinary punctuation in a product
    designation. "merguez; chair farce; farce à tomate; chipolatas;
    chipolatas aux herbes" and "Salame Nostrano (~800g; lot L6CCTD)" would
    each have lost most of the product. A split now also requires the
    DISCARDED half to be substantial and identifiably another language.
  · US state codes read as Italian articles. An FDA description ending
    "...in AZ, CA, FL, HI, IL, KS, LA, MD, MO, NC, NH, NV, NY, OK, PA, TX,
    UT (17 states)" was classified Italian because IL and LA lowercase to
    "il" and "la". A marker is now discarded when every occurrence of it in
    the original text is an all-caps two-letter token.

Run:  python -m pipeline.fix_product_bilingual --dry-run
      python -m pipeline.fix_product_bilingual
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TODAY = "2026-08-04"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )
    from pipeline._language import split_bilingual

    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)
    n = 0
    for row in approved:
        # SCOPED TO RASFF. See the note in merge_master's writer guard: RASFF
        # is the only source that puts a bilingual notification subject in
        # Product. Everywhere else Product is a NAME, and running the splitter
        # across all sources truncated an FDA product (lot, dates and importer
        # lost) and an EFET one (the Greek product name replaced by a fragment
        # of the packing address).
        if str(row.get("Source") or "") != "RASFF (EU)":
            continue
        product = str(row.get("Product") or "")
        english = split_bilingual(product)
        if english and english != product:
            row["Product"] = english
            row["LastUpdated"] = TODAY
            n += 1
            print(f"{row.get('Date')}  [{str(row.get('Source'))[:12]}]")
            print(f"   was: {product[:104]}")
            print(f"   now: {english[:104]}")
    print(f"\n{n} Product value(s) reduced to their English half")
    if args.dry_run:
        print("(dry run — nothing written)")
        return 0
    save_xlsx_with_pending(sort_rows(approved), sort_rows(pending), xlsx)
    mirror_json_from_xlsx(xlsx, ROOT / "docs" / "data" / "recalls.json")
    print("✓ written + recalls.json mirrored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
