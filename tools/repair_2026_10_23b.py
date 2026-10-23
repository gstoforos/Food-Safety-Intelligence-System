# -*- coding: utf-8 -*-
"""Review of the 2026-10-23 morning fix: two recalls the Daily Global Recall
Search found MISSING from all five sheets, each read by the operator's review
session on the authority's own page on 2026-10-23. NOTHING IS REMOVED.

1. EFET (GR), press release item 5424, "Αθήνα, 22 Οκτωβρίου 2026": ΜΕΑΤΚΑ Α.Ε.,
   "ΜΠΙΦΤΕΚΙ ΚΟΤΟΠΟΥΛΟ HAMBURGER (κατεψυγμένο)", 850 g, Lot 15626, frozen
   05/06/2026, use-by 05/06/2027; "Salmonella spp." found by the EFET
   Thessaloniki laboratory during the 2026 official microbiological-criteria
   checks; EFET ordered the immediate recall of the entire lot.
2. AESAN (ES), alert 2026_72, published 22/10/2026, ref ES2026/637: "Mejillón
   de Chile cocido", brand "Marvalmar", lot "387/21096 1809", best before
   20/11/2026, vacuum tray 200 g, chilled; Listeria monocytogenes; distributed
   to Castilla-La Mancha, Castilla y León and Comunidad de Madrid. AESAN names
   no operator; Company carries the register's existing AESAN convention
   "(not specified in AESAN alert ...)".
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

ROWS = [
    {
        "Date": "2026-10-22",
        "Source": "EFET (GR)",
        "Company": "MEATKA S.A.",
        "Brand": "",
        "Product": ("Frozen chicken burger patties, 850 g, "
                    "lot 15626, frozen 05/06/2026, use-by 05/06/2027"),
        "Pathogen": "Salmonella spp.",
        "Reason": ("Salmonella spp. detected by the EFET Food Testing and Research Laboratory of "
                   "Thessaloniki in a sample taken under the 2026 official microbiological "
                   "food-safety-criteria checks. EFET ordered the immediate recall of the entire "
                   "lot; consumers who bought it should not eat it."),
        "Class": "Mandatory",
        "Country": "Greece",
        "URL": ("https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/"
                "5424-deltio-typou-anaklisi-mi-asfaloys-proiontos-bifteki-kotopoulo-katepsygmeno"),
        "Notes": ("[operator review 2026-10-23] EFET press release item 5424 read on its own page, "
                  "dated \"Αθήνα, 22 Οκτωβρίου 2026\" (Central Macedonia Regional Directorate): "
                  "company \"ΜΕΑΤΚΑ Α.Ε.\"; lot 15626; \"Salmonella spp.\". Brand left empty: none "
                  "printed. [original product: ΜΠΙΦΤΕΚΙ ΚΟΤΟΠΟΥΛΟ HAMBURGER (κατεψυγμένο)] "
                  "[original company: ΜΕΑΤΚΑ Α.Ε.]"),
    },
    {
        "Date": "2026-10-22",
        "Source": "AESAN (ES)",
        "Company": "(not specified in AESAN alert 2026_72)",
        "Brand": "Marvalmar",
        "Product": ("Cooked Chilean mussels, vacuum tray 200 g, chilled, "
                    "lot 387/21096 1809, best before 20/11/2026"),
        "Pathogen": "Listeria monocytogenes",
        "Reason": ("Presence of Listeria monocytogenes (AESAN alert ES2026/637). Distributed in "
                   "Castilla-La Mancha, Castilla y León and Comunidad de Madrid. Consumers holding "
                   "the product should not eat it; pregnant women should follow AESAN's advice."),
        "Class": "Alert",
        "Country": "Spain",
        "URL": "https://aesan.gob.es/alertas/2026_72",
        "Notes": ("[operator review 2026-10-23] AESAN alert 2026_72 read on its own page, published "
                  "\"22/10/2026\", ref \"ES2026/637\": brand \"Marvalmar\", lot \"387/21096 1809\", "
                  "\"Peso de unidad: 200 g\". Company: AESAN names no operator (same convention as AESAN alert 22/2026). "
                  "[original product: Mejillón de Chile cocido]"),
    },
]


def _present(url: str) -> bool:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    try:
        for sheet in ("Recalls", "Pending"):
            rows = wb[sheet].iter_rows(values_only=True)
            hdr = list(next(rows))
            i = hdr.index("URL")
            if any(r and str(r[i] or "").strip().lower().rstrip("/") == url.lower() for r in rows):
                return True
    finally:
        wb.close()
    return False


def main() -> int:
    todo = [r for r in ROWS if not _present(r["URL"])]
    for r in ROWS:
        if r not in todo:
            print(f"  already in Recalls or Pending — left alone: {r['URL']}")
    if todo:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as fh:
            json.dump(todo, fh, ensure_ascii=False)
            path = fh.name
        out = subprocess.run([sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
                             cwd=str(ROOT), capture_output=True, text=True)
        print((out.stdout or "").rstrip()[-800:])
        if out.returncode != 0:
            print("  add_manual_row FAILED:", (out.stderr or "")[-800:])
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
