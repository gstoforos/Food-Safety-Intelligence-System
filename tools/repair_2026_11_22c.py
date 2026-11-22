# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-22 (c).

Corrected in place (Recalls row keyed by URL; read on the HAPIH page today):
  HAPIH (HR) Mijukić-Prom "Panceta kockice VP, 0,55kg" — Date 2026-11-21 was
  the news discovery day; HAPIH's notice is dated "20. studenoga 2026.".
  Product read "Sausage cubes" — panceta kockice is diced pancetta; the lot
  "LOT 918N33" and "najbolje upotrijebiti do 20.03.2027." are on the page.

Archived out of Pending to Rejected (one archive row each):
  * FDA Greenwich Rx compounded sterile products — a pharmaceutical recall,
    not food (R1);
  * FSAI "Candy Canes" row (Company = Brand = Product = "Candy Canes", no
    Date) — the same Card Factory candy-cane recall is already published
    from FSA PRIN-53-2026 (2026-11-18, pieces of metal).
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
TODAY = "2026-11-22"
STAMP = "daily-global review 2026-11-22c"

FIXES = [
    ("https://www.hapih.hr/opoziv-proizvoda-mijukic-prom-panceta-kockice-vp-055kg/", {
        "Date": "2026-11-20",
        "Product": ("Pancetta cubes VP, 0.55 kg, LOT 918N33, best before "
                    "20.03.2027"),
    }, ("[{s}: Date 2026-11-21 -> 2026-11-20 (HAPIH notice dated '20. "
        "studenoga 2026.'; 11-21 was the index.hr discovery day) and Product "
        "'Sausage cubes' -> pancetta cubes with the lot and best-before read "
        "on the page] [original product: Panceta kockice VP, 0,55kg]")),
]
REPAIRS = []

ARCHIVE = [
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/greenwich-rx-issues-voluntary-nationwide-recall-compounded-sterile-products-due-lack-sterility",
     "pharmaceutical recall (compounded sterile products, lack of sterility "
     "assurance) — not food, out of scope by R1"),
    ("https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-card-factory-candy-canes",
     "DUPLICATE of the published FSA PRIN-53-2026 row (2026-11-18, Card "
     "Factory candy canes, pieces of metal); this copy has Company = Brand = "
     "Product = 'Candy Canes' and no Date"),
]


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _repair(wb):
    n = sup = 0
    ws = wb["Pending"]
    head = _head(ws)
    iu = head.index("URL") + 1
    inotes = head.index("Notes") + 1
    for url, fields, note in REPAIRS:
        k = _key(url)
        rows = [r for r in range(2, ws.max_row + 1) if _key(ws.cell(r, iu).value) == k]
        for r in reversed(rows[1:]):
            ws.delete_rows(r, 1)
        if rows:
            r = rows[0]
            notes = str(ws.cell(r, inotes).value or "")
            if STAMP not in notes:
                for f, v in fields.items():
                    if f in head:
                        ws.cell(r, head.index(f) + 1).value = v
                for f, v in (("Status", "pending"), ("RejectedBy", "")):
                    if f in head:
                        ws.cell(r, head.index(f) + 1).value = v
                ws.cell(r, iu).value = url
                ws.cell(r, inotes).value = note.format(s=STAMP)
                n += 1
                print(f"  Pending:{r}: repaired from {url}")
        for name in ("Weekly_Rejected", "Rejected"):
            if name not in wb.sheetnames:
                continue
            a = wb[name]
            h = _head(a)
            if "URL" not in h:
                continue
            col = next((h.index(c) + 1 for c in ("RejectionReason", "RejectReason") if c in h), None)
            if col is None:
                continue
            for rr in range(2, a.max_row + 1):
                if _key(a.cell(rr, h.index("URL") + 1).value) != k:
                    continue
                cur = str(a.cell(rr, col).value or "")
                if STAMP in cur:
                    continue
                a.cell(rr, col).value = (f"[SUPERSEDED {STAMP} — row repaired from "
                                         "the AESAN page and re-queued] ") + cur
                if "Reviewed" in h:
                    a.cell(rr, h.index("Reviewed") + 1).value = "SUPERSEDED"
                sup += 1
    return n, sup


def _fix(wb):
    n = 0
    for name in ("Recalls", "Weekly_Review", "Pending"):
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        if "URL" not in head or "Notes" not in head:
            continue
        iu = head.index("URL") + 1
        inotes = head.index("Notes") + 1
        for url, fields, note in FIXES:
            k = _key(url)
            for r in range(2, ws.max_row + 1):
                if _key(ws.cell(r, iu).value) != k:
                    continue
                notes = str(ws.cell(r, inotes).value or "")
                if STAMP in notes:
                    continue
                for f, v in fields.items():
                    if f in head:
                        ws.cell(r, head.index(f) + 1).value = v
                ws.cell(r, inotes).value = (notes + " " + note.format(s=STAMP)).strip()
                n += 1
                print(f"  {name}:{r}: corrected from {url}")
    return n


def _archive(wb):
    ws = wb["Pending"]
    head = _head(ws)
    iu = head.index("URL") + 1
    arch = wb["Rejected"]
    ah = _head(arch)
    done = 0
    for url, why in ARCHIVE:
        k = _key(url)
        rows = [r for r in range(2, ws.max_row + 1) if _key(ws.cell(r, iu).value) == k]
        if not rows:
            continue
        first = {h: ws.cell(rows[0], i + 1).value for i, h in enumerate(head)}
        out = []
        for h in ah:
            if h in ("RejectReason", "RejectionReason"):
                out.append(f"{STAMP}: archived — {why}")
            elif h == "RejectedBy":
                out.append(STAMP)
            elif h in ("RejectedAt", "Week_Added"):
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            elif h == "Reviewed":
                out.append("Y")
            else:
                out.append(first.get(h, ""))
        arch.append(out)
        for r in reversed(rows):
            ws.delete_rows(r, 1)
        done += len(rows)
        print(f"  Pending: {len(rows)} row(s) archived — {url}")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    n, sup = _repair(wb)
    fixed = _fix(wb)
    gone = _archive(wb)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"FIXED={fixed} ARCHIVED={gone}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
