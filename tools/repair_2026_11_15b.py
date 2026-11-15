# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-15 (b).

BVL (DE) Ökoland GmbH Nord "Leberwurst fein, 100 Gramm", metal foreign body.
The published row was dated 2026-11-14 and its Product read "Liver sausage,
finely sliced" — "fein" names the fine texture, not slicing. The Meldung
261113_16_NI_Leberwurst_fein,_100_Gramm was read in full on 2026-11-15:
Datum der Erstveröffentlichung 13.11.2026; Hersteller "Franz Ludowig
Fleischerei GmbH & Co. KG"; Inverkehrbringer "Ökoland GmbH Nord"; MHD
04.12.2026 Charge 3825082 and 08.12.2026 Charge 3825083; 100 Gramm in einem
Schlauchbeutel; EAN 4031829233511; Grund "Fremdkörper" / "Metallfremdkörper".
Date and Product are set from that page only; every other field is kept.
Applied to every live sheet holding that URL (archives untouched).
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
STAMP = "daily-global 2026-11-15"
URL = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/"
       "2026/11_November/261113_16_NI_Leberwurst_fein,_100_Gramm/"
       "261113_16_NI_Leberwurst_fein,_100_Gramm.html")
FIX = {
    "Date": "2026-11-13",
    "Product": ("Fine liver sausage (Leberwurst fein), 100 g tube bag, batch "
                "3825082 (best before 04.12.2026) and batch 3825083 (best "
                "before 08.12.2026), EAN 4031829233511"),
}
NOTE = (f"[{STAMP}: Date 2026-11-14 -> 2026-11-13 (Datum der "
        "Erstveröffentlichung 13.11.2026) and Product corrected from the BVL "
        "Meldung read 2026-11-15 — \"fein\" is the texture, not \"finely "
        "sliced\"; manufacturer Franz Ludowig Fleischerei GmbH & Co. KG, "
        "placed on the market by Ökoland GmbH Nord] "
        "[original product: Leberwurst fein, 100 Gramm]")
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
        for r in range(2, ws.max_row + 1):
            if _key(ws.cell(r, iu).value) != target:
                continue
            notes_col = head.index("Notes") + 1 if "Notes" in head else None
            notes = str(ws.cell(r, notes_col).value or "") if notes_col else ""
            if STAMP in notes:
                continue
            for f, v in FIX.items():
                if f in head:
                    ws.cell(r, head.index(f) + 1).value = v
            if notes_col:
                ws.cell(r, notes_col).value = (notes + " " + NOTE).strip()
            fixed += 1
            print(f"  {name}:{r}: Ökoland Leberwurst fein Date/Product corrected")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"OEKOLAND_FIXED={fixed}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
