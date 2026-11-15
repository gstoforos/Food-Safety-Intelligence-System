# -*- coding: utf-8 -*-
"""Review 2026-11-15 (c) — USDA FSIS public health alert of 14.11.2026,
ready-to-eat meat products containing FDA-regulated biscuits, Listeria
monocytogenes (the alert @usdafoodsafety posted 14.11.2026 21:24 UTC).

The published row carries Company "(not named in the FSIS text held on this
row)", empty Brand and a generic Product. fsis.usda.gov and its product-list
PDF (PHA-11142026-Product-List_0.pdf) still return a client error through the
proxy, so the fields below come from reports that quote the FSIS release and
its product list, and every value is attributed in Notes:
  * Brand/Product/size/use-by — efoodalert "Recalls and Alerts: November
    12-14, 2026", quoting the FSIS product list: "Mmm! MEALS MADE IN MINUTES
    brand BISCUIT & SAUSAGE GRAVY (9.5 oz; Use by 11/12/26 -11/18/26)".
    Matched by Yahoo News/KTAL (same brand, 9.5 oz; AR, LA, MS, OK, TN, TX,
    mainly vending machines).
  * Company — Food Safety News 14.11.2026 quoting the FSIS release: a firm
    "notified FSIS that they had received FDA-regulated biscuits that had
    been recalled by C.H. Guenther & Son". The downstream USDA-inspected
    establishment is not named in any text read, so Company names the
    biscuit recaller and says so.
Every other field is kept. Applied to every live sheet holding the URL.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
STAMP = "review 2026-11-15c"
URL = ("https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-health-"
       "alert-ready-eat-meat-products-containing-biscuits-recalled")
FIX = {
    "Company": "C.H. Guenther & Son (biscuit recaller; meat establishment not named)",
    "Brand": "Mmm! Meals Made In Minutes",
    "Product": ("Biscuit & Sausage Gravy, 9.5 oz, use by 11/12/26 to 11/18/26 "
                "(ready-to-eat meat product made with recalled FDA-regulated "
                "biscuits)"),
}
NOTE = (
    f"[{STAMP}: fsis.usda.gov and the FSIS product list PDF still unreadable "
    "from this pass (client_error). Brand/Product/size/use-by from efoodalert "
    "'Recalls and Alerts: November 12-14, 2026' quoting the FSIS list: "
    "\"Mmm! MEALS MADE IN MINUTES brand BISCUIT & SAUSAGE GRAVY (9.5 oz; Use "
    "by 11/12/26 -11/18/26)\"; matched by Yahoo News/KTAL (AR, LA, MS, OK, "
    "TN, TX, mainly vending machines). Company from Food Safety News "
    "14.11.2026 quoting the release: a firm \"notified FSIS that they had "
    "received FDA-regulated biscuits that had been recalled by C.H. Guenther "
    "& Son\"; the USDA-inspected establishment is not named in any text read. "
    "Replace from the FSIS page when the scraper reaches it.]")
LIVE = ("Recalls", "Weekly_Review", "Pending")


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    target = _key(URL)
    fixed = 0
    for name in LIVE:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = [c.value for c in ws[1]]
        if "URL" not in head:
            continue
        iu = head.index("URL") + 1
        nc = head.index("Notes") + 1 if "Notes" in head else None
        for r in range(2, ws.max_row + 1):
            if _key(ws.cell(r, iu).value) != target:
                continue
            notes = str(ws.cell(r, nc).value or "") if nc else ""
            if STAMP in notes:
                continue
            for f, v in FIX.items():
                if f in head:
                    ws.cell(r, head.index(f) + 1).value = v
            if nc:
                ws.cell(r, nc).value = (notes + " " + NOTE).strip()
            fixed += 1
            print(f"  {name}:{r}: FSIS biscuit alert Company/Brand/Product filled")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"FSIS_BISCUIT_FIXED={fixed}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
