# -*- coding: utf-8 -*-
"""Review of the 2026-10-20 morning fix: the Little Wicomico River oyster
Salmonella Telelkebir recall. NOTHING IS REMOVED from Recalls.

The morning run could not open FDA's alert from its sandbox and rightly did
not write a row off news. The operator's review session read FDA's own page
on 2026-10-20:
  https://www.fda.gov/food/alerts-advisories-safety-information/
      safety-alert-salmonella-oysters-little-wicomico-river-virginia
  posted "Mon, 10/19/2026 - 16:01"; "Recalled whole (shellstock) oysters
  from Little Wicomico Oyster Company (VA-1421-SP)" "harvested in the Little
  Wicomico River from September 18, through October 8, 2026"; distributed to
  "CA, CO, CT, DC, FL, GA, IL, MD, MO, NC, NJ, NV, NY, PA, TX, VA";
  part of "a cluster of 27 Salmonella Telelkebir illnesses" under
  investigation by CDC, FDA and state authorities. In no sheet of the
  register before this pass.
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
URL = ("https://www.fda.gov/food/alerts-advisories-safety-information/"
       "safety-alert-salmonella-oysters-little-wicomico-river-virginia")
ROW = {
    "Date": "2026-10-19",
    "Source": "FDA",
    "Company": "Little Wicomico Oyster Company",
    "Brand": "",
    "Product": ("Whole (shellstock) oysters harvested in the Little Wicomico River, Virginia, "
                "18 Sep – 8 Oct 2026 (shellfish dealer VA-1421-SP)"),
    "Pathogen": "Salmonella Telelkebir",
    "Reason": ("Recalled oysters linked to a cluster of 27 Salmonella Telelkebir illnesses under "
               "investigation by CDC, FDA and state authorities. Distributed to CA, CO, CT, DC, "
               "FL, GA, IL, MD, MO, NC, NJ, NV, NY, PA, TX and VA, possibly further. Do not "
               "serve, sell or eat; discard."),
    "Class": "Recall",
    "Country": "United States",
    "Outbreak": 1,
    "URL": URL,
    "Notes": ("[operator review 2026-10-20] FDA safety alert read on its own page 2026-10-20, "
              "posted Mon 10/19/2026 16:01: firm \"Little Wicomico Oyster Company (VA-1421-SP)\"; "
              "harvested \"September 18, through October 8, 2026\"; \"a cluster of 27 Salmonella "
              "Telelkebir illnesses\". Brand left empty: none printed."),
}


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    present = False
    for sheet in ("Recalls", "Pending"):
        rows = wb[sheet].iter_rows(values_only=True)
        hdr = list(next(rows))
        i = hdr.index("URL")
        present |= any(r and str(r[i] or "").strip().lower() == URL for r in rows)
    wb.close()
    if present:
        print("  oysters already in Recalls or Pending — left alone")
    else:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as fh:
            json.dump([ROW], fh, ensure_ascii=False)
            path = fh.name
        out = subprocess.run([sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
                             cwd=str(ROOT), capture_output=True, text=True)
        print((out.stdout or "").rstrip()[-500:])
        if out.returncode != 0:
            print("  add_manual_row FAILED:", (out.stderr or "")[-500:])
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
