#!/usr/bin/env python3
"""
fix_duplicates_2026_09_26.py  —  one-time corrective (nightly operator review)
===============================================================================

Removes two confirmed duplicate rows from Recalls, flagged by the 2026-09-26
daily-review-agent's Lane B queue and independently verified against the
regulator/source pages before being touched. Matched by EXACT URL (not
pattern) so nothing else can be touched. Modelled on fix_allergen_rows.py.

1. CFIA "charlevoisienne-and-joe-smoked-meat...-listeria" — a second URL
   slug for the SAME 2026-07-10 Boucherie Charcuterie Lyn Tremblay Inc. /
   Charlevoisienne / Joe Smoked Meat Listeria monocytogenes recall already
   published (better-populated) under the "charcuterie-charlevoisienne-..."
   URL. Both pages verified live 2026-09-26: identical date, hazard, lot
   code (2026AU01) and product list. The weaker-populated slug is removed;
   the fuller one (Company/Brand correctly parsed, claude-check 2nd-reviewer
   verified) is kept.

2. CDC "ecoli/outbreaks/blueberries-07-26/index.html" — a CDC epidemiology
   page duplicating the SAME 2026-09-03 Frutas y Hortalizas del Sur S.A. /
   Great Value Organic Triple Berry Blend E. coli O145 recall already
   published under its FDA regulatory recall notice (verified live
   2026-09-26). CDC's own domain was unreachable to re-verify tonight
   (network egress block), so the FDA-sourced row is kept as the
   higher-provenance regulatory notice and the CDC epidemiology mirror is
   removed as the redundant one.

Usage:
    python -m pipeline.fix_duplicates_2026_09_26 --xlsx docs/data/recalls.xlsx --commit false
    python -m pipeline.fix_duplicates_2026_09_26 --xlsx docs/data/recalls.xlsx --commit true
"""
from __future__ import annotations
import argparse
from pathlib import Path

TARGETS = {
    "https://recalls-rappels.canada.ca/en/alert-recall/charlevoisienne-and-joe-smoked-meat-brand-meat-products-recalled-due-listeria":
        "Duplicate: second URL slug for the same 2026-07-10 Boucherie Charcuterie "
        "Lyn Tremblay Inc. / Charlevoisienne & Joe Smoked Meat Listeria "
        "monocytogenes recall (lot 2026AU01), already published under "
        "'charcuterie-charlevoisienne-and-joe-smoked-meat-...' with fuller "
        "Company/Brand data. Verified identical via both live CFIA pages, "
        "2026-09-26.",
    "https://www.cdc.gov/ecoli/outbreaks/blueberries-07-26/index.html":
        "Duplicate: CDC epidemiology mirror of the same 2026-09-03 Frutas y "
        "Hortalizas del Sur S.A. / Great Value Organic Triple Berry Blend "
        "E. coli O145 recall already published under its FDA regulatory "
        "recall notice (verified live 2026-09-26). CDC's own page could not "
        "be independently re-fetched tonight (network egress block on "
        "cdc.gov); the higher-provenance FDA row is kept.",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=Path, default=Path("docs/data/recalls.xlsx"))
    ap.add_argument("--commit", type=str, default="false")
    args = ap.parse_args()
    commit = args.commit.lower() in ("1", "true", "yes", "on")

    import openpyxl
    wb = openpyxl.load_workbook(args.xlsx)
    ws = wb["Recalls"]
    headers = [c.value for c in ws[1]]
    url_i = headers.index("URL")

    to_remove = []
    for ridx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        u = str(row[url_i]).strip() if row[url_i] else ""
        if u in TARGETS:
            to_remove.append((ridx, dict(zip(headers, row))))

    print(f"Matched {len(to_remove)} row(s) in Recalls by exact URL:")
    for ridx, rowd in to_remove:
        print(f"  row {ridx}: {str(rowd.get('Product',''))[:45]} | "
              f"Pathogen={rowd.get('Pathogen','')} | {rowd.get('URL','')}")

    if len(to_remove) != len(TARGETS):
        print(f"\n⚠ Expected {len(TARGETS)} rows, found {len(to_remove)}. "
              f"Aborting so nothing wrong is touched.")
        return 1

    if not commit:
        print("\nDRY RUN — nothing changed. Re-run with --commit true to apply.")
        return 0

    wr = wb["Weekly_Rejected"] if "Weekly_Rejected" in wb.sheetnames else None
    if wr is None:
        print("⚠ Weekly_Rejected sheet not found; aborting.")
        return 1
    wr_headers = [c.value for c in wr[1]]

    def col(name):
        return wr_headers.index(name) if name in wr_headers else None

    for _ridx, rowd in to_remove:
        reason = TARGETS[str(rowd.get("URL", "")).strip()]
        new = [""] * len(wr_headers)
        for k, v in rowd.items():
            if k in wr_headers:
                new[wr_headers.index(k)] = v
        for rc in ("RejectionReason", "Reason"):
            if col(rc) is not None:
                new[col(rc)] = reason
                break
        if col("RejectedBy") is not None:
            new[col("RejectedBy")] = "nightly-operator-2026-09-26"
        wr.append(new)

    for ridx, _rowd in sorted(to_remove, key=lambda x: -x[0]):
        ws.delete_rows(ridx, 1)

    wb.save(args.xlsx)
    print(f"\n✓ Moved {len(to_remove)} duplicate rows Recalls → Weekly_Rejected.")

    try:
        from pipeline.merge_master import mirror_json_from_xlsx
        json_path = args.xlsx.parent / "recalls.json"
        mirror_json_from_xlsx(args.xlsx, json_path)
        print(f"✓ recalls.json mirrored ({json_path}).")
    except Exception as e:                                    # noqa: BLE001
        print(f"  (JSON mirror skipped: {e}; run your normal mirror step.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
