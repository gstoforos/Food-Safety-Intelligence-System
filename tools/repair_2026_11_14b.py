# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-14 (b).

BVL (DE) 13.11.2026 — Fleischerei Lars Methe Knackwurst, Listeria
monocytogenes. The morning pass queued a clean row for it
(tools/manual_rows_2026_11_14.json), but between that pass and this one a
scraper put its own copy of the same lebensmittelwarnung.de URL into Pending
with page-chrome fields (Company "lebensmittelwarnung.de", Product
"Homepage - Knackwurst ..."). The clean row was deduped against that copy and
never landed, so the recall would have stayed unpublished.

This repairs the Pending copy in place with the fields the morning pass read
on the BVL Meldung (taken verbatim from manual_rows_2026_11_14.json, nothing
new claimed) and returns it to the queue. The four Weekly_Rejected copies of
the news headline ("no official <agency> press-release URL") are marked
SUPERSEDED — append-only, nothing deleted.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
MORNING_ROWS = ROOT / "tools" / "manual_rows_2026_11_14.json"
STAMP = "daily-global 2026-11-14"
HEADLINE = "Knackwurst der Fleischerei Methe"
FIELDS = ("Date", "Source", "Company", "Brand", "Product", "Pathogen",
          "Reason", "Class", "Country", "Outbreak")

SUPERSEDED = (
    f"[SUPERSEDED {STAMP} — \"no official press-release URL\" is false: the "
    "BVL Meldung 261113_17_SN_Knackwurst_Kuemmel_und_Knackwurst_Knoblauch on "
    "lebensmittelwarnung.de is live and was read in full on 2026-11-14. One "
    "German recall; the repaired Pending row is the copy that wins; this row "
    "is kept as the audit trail of the original refusal.] ")


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _morning_row() -> dict:
    rows = json.loads(MORNING_ROWS.read_text(encoding="utf-8"))
    for r in rows:
        if "Methe" in str(r.get("Company", "")):
            return r
    raise SystemExit("Methe row not found in manual_rows_2026_11_14.json")


def _repair(wb, src: dict) -> tuple[int, int]:
    fixed = sup = 0
    target = _key(src["URL"])
    ws = wb["Pending"]
    head = _head(ws)
    iu = head.index("URL") + 1
    rows = [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, iu).value) == target]
    keep = None
    for r in rows:
        notes = str(ws.cell(r, head.index("Notes") + 1).value or "")
        if STAMP in notes:
            keep = r
    for r in reversed(rows):                 # one live copy only
        if keep is None:
            keep = rows[0]
        if r != keep:
            ws.delete_rows(r, 1)
    if keep is not None:
        notes = str(ws.cell(keep, head.index("Notes") + 1).value or "")
        if STAMP not in notes:
            before = {f: ws.cell(keep, head.index(f) + 1).value
                      for f in ("Company", "Product") if f in head}
            for f in FIELDS:
                if f in head and f in src:
                    ws.cell(keep, head.index(f) + 1).value = src[f]
            for f, v in (("Status", "pending"), ("RejectedBy", "")):
                if f in head:
                    ws.cell(keep, head.index(f) + 1).value = v
            note = (f"[{STAMP}: scraper copy carried page-chrome fields "
                    f"(Company {before.get('Company')!r}, Product "
                    f"{before.get('Product')!r}) and blocked the morning "
                    "pass's clean row by URL dedup; fields replaced with those "
                    "the morning pass read on the BVL Meldung] "
                    + str(src.get("Notes", "")))
            ws.cell(keep, head.index("Notes") + 1).value = (notes + " " + note).strip()
            fixed += 1
            print(f"  Pending:{keep}: BVL Methe Knackwurst repaired and re-queued")
    for name in ("Weekly_Rejected", "Rejected"):
        if name not in wb.sheetnames:
            continue
        a = wb[name]
        h = _head(a)
        col = next((h.index(c) + 1 for c in ("RejectionReason", "RejectReason")
                    if c in h), None)
        if col is None:
            continue
        for r in range(2, a.max_row + 1):
            vals = [a.cell(r, c).value for c in range(1, len(h) + 1)]
            text = " ".join(str(v) for v in vals if v)
            same_url = "URL" in h and _key(a.cell(r, h.index("URL") + 1).value) == target
            if not (HEADLINE in text or same_url):
                continue
            cur = str(a.cell(r, col).value or "")
            if f"SUPERSEDED {STAMP}" in cur:
                continue
            a.cell(r, col).value = SUPERSEDED + cur
            if "Reviewed" in h:
                a.cell(r, h.index("Reviewed") + 1).value = "SUPERSEDED"
            sup += 1
            print(f"  {name}:{r}: Methe Knackwurst refusal marked SUPERSEDED")
    return fixed, sup


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    src = _morning_row()
    wb = openpyxl.load_workbook(XLSX)
    fixed, sup = _repair(wb, src)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"METHE_FIXED={fixed} ARCHIVE_SUPERSEDED={sup}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
