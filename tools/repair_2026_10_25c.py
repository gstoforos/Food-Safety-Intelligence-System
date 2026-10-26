# -*- coding: utf-8 -*-
"""Operator review 2026-10-25 (evening): three things held the suite RED on
main after the Sunday Weekly_Rejected -> Rejected move, plus one recall held
in Pending. NOTHING IS REMOVED FROM Recalls.

A. Two Rejected rows lost the SUPERSEDED banner in the move. The banner had
   been written to Weekly_Rejected.RejectionReason (morning fixes 2026-10-23
   and 2026-10-24); the move copied the rows into Rejected, whose column is
   RejectReason, and left it empty. Both URLs are PUBLISHED in Recalls (IN's
   Mercato tuna, histamine; Brezzi salsa tartufata, suspected botulinum
   toxin). Re-stamped SUPERSEDED; original verdict kept verbatim.
B. Three NAFDAC (NG) Pending rows carry an alert number from another year in
   their own URL (035-2025 amoxicillin suspension, 041-2024 Nivea roll-on
   deodorant, 0047-2021 Lefin paediatric suspension) — old alerts, and none
   of them food (two medicines, one cosmetic). Archived to Rejected.
C. FSIS Imperio Distribution LLC beef jerky (uninspected product, 24 Oct) is
   in Pending with Company empty, the one field the publish gate refuses.
   The firm's name is the opening words of FSIS's own notice title, which is
   the row's own URL slug: "imperio-distribution-llc-recalls-beef-jerky-
   products-produced-without-benefit". Filled from that; nothing else touched.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-25"

SUPERSEDED = {
    "cartello%20richiamo%20filetti%20di%20tonno":
        "IN's Mercato tuna fillets (histamine), published in Recalls 2026-10-20",
    "lotto%20596_1792593268.pdf":
        "Brezzi salsa tartufata (suspected botulinum toxin), published in Recalls",
}
NAFDAC_KEYS = ("public-alert-no-35-2025", "public-alert-no-041-2024",
               "public-alert-no-0047-2021")
NAFDAC_REASON = ("REJECTED — not food and not current: a NAFDAC alert whose own "
                 "URL carries an alert number from an earlier year (medicine or "
                 "cosmetic recall). Operator review 2026-10-25.")
IMPERIO_KEY = "imperio-distribution-llc-recalls-beef-jerky"
IMPERIO_CO = "Imperio Distribution LLC"


def _head(ws):
    return [c.value for c in ws[1]]


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    rej = wb["Rejected"]
    rh = _head(rej)
    ui, ri = rh.index("URL") + 1, rh.index("RejectReason") + 1
    si = rh.index("Status") + 1 if "Status" in rh else None
    a = 0
    for r in range(2, rej.max_row + 1):
        url = str(rej.cell(r, ui).value or "").lower()
        hit = next((v for k, v in SUPERSEDED.items() if k in url), None)
        if not hit:
            continue
        prior = str(rej.cell(r, ri).value or "")
        if "SUPERSEDED" in prior:
            continue
        reason = rej.cell(r, rh.index("Reason") + 1).value or ""
        rej.cell(r, ri).value = (f"[SUPERSEDED {TODAY} — the recall on this URL is "
                                 f"{hit}; this archive verdict came from a "
                                 f"misread news roundup / PDF and does not apply. "
                                 f"Original verdict kept:] {prior or reason}").strip()
        if si:
            rej.cell(r, si).value = "SUPERSEDED"
        a += 1
        print(f"  Rejected row {r}: SUPERSEDED ({hit})")

    pend = wb["Pending"]
    ph = _head(pend)
    pu = ph.index("URL") + 1
    b = 0
    for r in range(pend.max_row, 1, -1):
        url = str(pend.cell(r, pu).value or "").lower()
        if not any(k in url for k in NAFDAC_KEYS):
            continue
        row = {h: pend.cell(r, i + 1).value for i, h in enumerate(ph)}
        out = []
        for h in rh:
            if h == "RejectReason":
                out.append(NAFDAC_REASON)
            elif h == "RejectedBy":
                out.append("operator review " + TODAY)
            elif h == "RejectedAt":
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            else:
                out.append(row.get(h, ""))
        rej.append(out)
        pend.delete_rows(r, 1)
        b += 1
        print(f"  Pending -> Rejected: {url[:70]}")

    c = 0
    ci = ph.index("Company") + 1
    for r in range(2, pend.max_row + 1):
        url = str(pend.cell(r, pu).value or "").lower()
        if IMPERIO_KEY in url and not str(pend.cell(r, ci).value or "").strip():
            pend.cell(r, ci).value = IMPERIO_CO
            if "Notes" in ph:
                ni = ph.index("Notes") + 1
                pend.cell(r, ni).value = (str(pend.cell(r, ni).value or "") +
                    " [operator review 2026-10-25: Company read from FSIS's own "
                    "notice title in this row's URL. SOURCES: FSIS per-recall "
                    "notice (row URL, collected by the pipeline's FSIS scraper); "
                    "the firm name is the title's own opening words. In scope as "
                    "uninspected product under operator review 2026-09-25.]").strip()
            c += 1
            print("  Pending: Imperio Distribution LLC Company filled")
    wb.save(XLSX)
    wb.close()
    print(f"superseded {a} · NAFDAC archived {b} · Imperio filled {c}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
