# -*- coding: utf-8 -*-
"""Review of the 2026-10-16 morning fix: two AGES surveillance bulletins.

NOTHING IS REMOVED from Recalls. Two rows move from Pending to
Weekly_Rejected (archives are append-only).

Rebuilding the morning fix on main at 4a3b95d4, promote_gate_passing
published two rows that the Exa daily search had queued on 2026-10-15:

    https://www.ages.at/mensch/krankheit/ages-radar-fuer-infektionskrankheiten/
        detail/ages-radar-fuer-infektionskrankheiten-15-oktober-2026
    https://www.ages.at/en/human/disease/ages-radar-for-infectious-diseases/
        details/ages-radar-for-infectious-diseases-15-october-2026

Company = Brand = the bulletin's own title, Product = its date ("15 October
2026 - AGES"), Pathogen "Salmonella", Outbreak 1, Tier 1, Class "Recall".
The pages are AGES's weekly infectious-disease radar (the extracted text
opens on stepped-up surveillance of acute respiratory infections from week
40) — a surveillance bulletin, German and English editions of the same
page. They name no recalled product and no firm, so they are not recalls.
They passed every publish-gate rule; pipeline/_publish_gate.py now refuses
a surveillance bulletin and a Product that is only a date (rule 2c), and
this pass archives the two rows so they do not sit in Pending.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-16"
NEEDLE = "ages-radar-f"           # matches both the -fuer- and -for- slugs
REASON = ("not a recall: AGES weekly infectious-disease surveillance bulletin "
          "(AGES-Radar für Infektionskrankheiten / Radar for Infectious Diseases, "
          "15 Oct 2026) — no recalled product, no firm; Company was the bulletin "
          "title and Product its date. [operator review 2026-10-16]")


def _head(ws):
    return [c.value for c in ws[1]]


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    pend, arch = wb["Pending"], wb["Weekly_Rejected"]
    ph, ah = _head(pend), _head(arch)
    ucol = ph.index("URL") + 1
    hits = [r for r in range(2, pend.max_row + 1)
            if NEEDLE in str(pend.cell(r, ucol).value or "").lower()]
    for r in reversed(hits):
        row = {h: pend.cell(r, i + 1).value for i, h in enumerate(ph)}
        out = []
        for h in ah:
            if h == "RejectionReason":
                out.append(REASON)
            elif h == "RejectedBy":
                out.append("operator review " + TODAY)
            elif h == "Reviewed":
                out.append("Y")
            elif h == "Week_Added":
                out.append(TODAY)
            else:
                out.append(row.get(h, ""))
        arch.append(out)
        pend.delete_rows(r, 1)
        print(f"  Pending -> Weekly_Rejected: {row.get('URL')}")
    if hits:
        wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
