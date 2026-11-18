# -*- coding: utf-8 -*-
"""Review 2026-11-18 (d) — EFET (GR) 17.11.2026 recall of rice cakes,
press release 5428 "Ανάκληση σε ρυζογκοφρέτες".

The row reached Pending from a kathimerini.gr news hit with Company
"FarmakeioAthens.com" (a pharmacy site, not the firm), no hazard, and a
Reason carrying an invented "Recall ID EFET-2026-1118". efet.gr returns a
client error through the proxy, so the fields below come from EFET's
announcement as reproduced in full by Newsbomb (17.11.2026 12:19) and matched
by Documento, Larissanet and others: "Rice Up Parmesan Rice Cakes", 60 g,
lot "L07 26 27", best before 01.07.2027, imported from Bulgaria by
"Καμπάση ΕΠΕ" (Thessaloniki); EFET Attica inspected after consumer
complaints and found worms ("την παρουσία σκουληκιών"); the company has
withdrawn the lot. Every value is attributed in Notes; the row stays in
Pending and goes through the gate like any other.
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
STAMP = "review 2026-11-18d"
URL = ("https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-"
       "cat/item/5428-deltio-typou-anaklisi-se-ryzogkofretes")
FIX = {
    "Date": "2026-11-17",
    "Company": "Kampasi EPE",
    "Brand": "Rice Up",
    "Product": ("Parmesan rice cakes, 60 g, lot L07 26 27, best before "
                "01.07.2027, imported from Bulgaria"),
    "Pathogen": "Foreign material (pest)",
    "Reason": ("Presence of worms found by EFET's Attica office after consumer "
               "complaints; the company withdrew the lot from the market"),
}
NOTE = (
    f"[{STAMP}: efet.gr unreadable from this pass (client_error). Fields from "
    "EFET's announcement as reproduced by Newsbomb 17.11.2026 12:19 "
    "(newsbomb.gr/ellada/story/1777091), matched by Documento and Larissanet: "
    "\"Rice Up Parmesan Rice Cakes\", \"συσκευασίας 60 γραμμαρίων\", lot "
    "\"L07 26 27\", \"κατά προτίμηση πριν από την 01.07.2027\", imported from "
    "Bulgaria by \"Καμπάση ΕΠΕ\" (Thessaloniki), \"την παρουσία σκουληκιών\". "
    "Company 'FarmakeioAthens.com' and the invented 'Recall ID "
    "EFET-2026-1118' replaced. Re-read from efet.gr when reachable.] "
    "[original product: ρυζογκοφρέτες]")
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
            print(f"  {name}:{r}: EFET rice cakes repaired from the announcement")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"EFET_RICECAKE_FIXED={fixed}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
