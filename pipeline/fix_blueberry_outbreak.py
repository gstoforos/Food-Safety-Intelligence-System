#!/usr/bin/env python3
"""Restore the outbreak flag on the frozen-blueberries E. coli O145:H28 recall.

WHY (audit 2026-08-02)
======================
The 2026-08-02 accuracy brief reported "every new approved row is Outbreak=0,
so no false-positive outbreak flags this run", and set the CDC frozen-
blueberries rows aside as pending-sheet noise ("auto-rejected, Company/Product
are headline garble, Product = 'E'").

The garble is real. The conclusion drawn from it was not. Checking the source
instead of the row:

    CDC — E. coli Outbreak Linked to Frozen Blueberries
    https://www.cdc.gov/ecoli/outbreaks/blueberries-07-26/index.html
        12 illnesses · 2 states · 4 hospitalisations · 0 deaths
        Investigation status: OPEN   (last update 6 July 2026)
        Product: GreenWise Organic IQF Blueberries 10 oz,
                 lot 60401, best by 09/02/2028
        Recalling firm: Frutas y Hortalizas del Sur S.A.

    FDA — Outbreak Investigation of E. coli O145:H28: Frozen Blueberries
          (July 2026)

Both agencies run a dedicated outbreak page. The recall is ALREADY in Recalls
— twice, from FDA's two publication channels — and both rows carry
Outbreak=0. This is a false NEGATIVE on a confirmed, still-open multistate
outbreak, and it is the more damaging direction of error: an outbreak row that
reads as an ordinary recall drops out of Phase 0 of the ranker, off the
outbreak section of the weekly and monthly reports, and out of the marketing
one-pager.

It is the Midwest Poultry mistake in reverse. There the outbreak flag was
cleared by trusting a stale note on the row over the world; here it was never
set, and a review trusted the row's own silence over two agency outbreak
pages.

WHAT IS CHANGED
---------------
Outbreak 0 → 1 on ONE row only: the FDA company announcement, which carries
the canonical permalink and the GreenWise brand. Its Reason gains the CDC
burden figures so the burden-aware ranker in rank_top_recalls() can order it
against the other July outbreaks — the ranker reads case counts out of the
Reason text, which is also where a reader sees them.

The second row (2026-07-03, openFDA enforcement report H-1181-2026) is the
SAME event on FDA's other channel. It is left at Outbreak=0 deliberately: the
monthly report counts outbreak ROWS, so flagging both would report two
outbreaks where there is one. It gets a cross-reference in Notes instead.

    OPERATOR DECISION NEEDED: those two rows are arguably a duplicate —
    same firm, same product, same pathogen, three days apart, differing only
    in which FDA channel published them. They are NOT merged here.
    _normalize_url_for_dedup deliberately keeps FDA's search_api_fulltext
    parameter as identity-bearing (three separate California Dairies recalls
    collapsed into one when it did not), so this is a policy call about FDA's
    announcement-vs-enforcement duplication, not a dedup bug to patch.

Run:  python -m pipeline.fix_blueberry_outbreak --dry-run
      python -m pipeline.fix_blueberry_outbreak
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TODAY = "2026-08-02"

CANONICAL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
             "alerts/frutas-y-hortalizas-del-sur-sa-initiates-recall-frozen-"
             "greenwise-organic-iqf-blueberries-due")
ENFORCEMENT = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
               "alerts?search_api_fulltext=H-1181-2026")

REASON = ("Possible E. coli O145:H28 contamination. Confirmed multistate "
          "outbreak: CDC reports 12 laboratory-confirmed cases across 2 states "
          "with 4 hospitalizations and no deaths; the investigation is open "
          "(CDC 'E. coli Outbreak Linked to Frozen Blueberries', last update "
          "2026-07-06). FDA runs a parallel investigation, 'Outbreak "
          "Investigation of E. coli O145:H28: Frozen Blueberries (July 2026)'.")

NOTE_CANON = (
    "[audit 2026-08-02: Outbreak 0 -> 1. Verified against the CDC outbreak "
    "page (12 cases, 2 states, 4 hospitalisations, status OPEN) and the FDA "
    "outbreak investigation page. The row had been published as an ordinary "
    "recall, which kept a confirmed multistate outbreak out of Phase 0 of the "
    "ranker and off the outbreak sections of the weekly and monthly reports. "
    "CDC burden figures added to Reason so the burden-aware ranker can order "
    "it.]")

NOTE_ENFORCE = (
    "[audit 2026-08-02: this is the openFDA ENFORCEMENT report for the same "
    "event as the FDA company announcement of 2026-07-06 (Frutas y Hortalizas "
    "del Sur S.A. / GreenWise Organic IQF Blueberries). The outbreak flag is "
    "set on that row only, so the monthly outbreak COUNT stays at one event. "
    "OPERATOR DECISION: these two rows may warrant merging — same firm, same "
    "product, same pathogen, three days apart, differing only in FDA "
    "publication channel.]")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )

    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)
    n = 0

    for row in approved:
        url = str(row.get("URL") or "").strip()
        if url == CANONICAL:
            row["Outbreak"] = 1
            row["Reason"] = REASON
            if f"audit {TODAY}" not in str(row.get("Notes") or ""):
                row["Notes"] = (str(row.get("Notes") or "").strip() + " "
                                + NOTE_CANON).strip()[:1800]
            row["LastUpdated"] = TODAY
            row["LastChecked"] = TODAY
            n += 1
            print(f"OUTBREAK 0->1  {row.get('Date')}  {row.get('Company')}")
        elif url == ENFORCEMENT:
            if f"audit {TODAY}" not in str(row.get("Notes") or ""):
                row["Notes"] = (str(row.get("Notes") or "").strip() + " "
                                + NOTE_ENFORCE).strip()[:1800]
                row["LastUpdated"] = TODAY
                n += 1
                print(f"CROSS-REF      {row.get('Date')}  {row.get('Company')}")

    print(f"\n{n} row(s) touched")
    if args.dry_run:
        print("(dry run — nothing written)")
        return 0

    save_xlsx_with_pending(sort_rows(approved), sort_rows(pending), xlsx)
    mirror_json_from_xlsx(xlsx, ROOT / "docs" / "data" / "recalls.json")
    print("✓ written + recalls.json mirrored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
