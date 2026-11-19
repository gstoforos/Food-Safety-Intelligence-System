# -*- coding: utf-8 -*-
"""Operator review 2026-11-19 (e) — two AESAN items George approved, repaired
in place in Pending from AESAN's own pages, and the Pending junk he approved
archiving.

Repaired (Pending row keyed by URL; fields read on the AESAN page 2026-11-19):
  1. AESAN alert ES2026/208 (www.aesan.gob.es/alertas/2026_31), 15/04/2026
     15:30 — "Norwegian smoked salmon with pistachio", brand "Deluxe", 100 g
     plastic blister, refrigerated, lot LC22606501, expiry 18/04/26, from
     Italy, initial distribution Canary Islands; Listeria monocytogenes.
     The Pending row held "La Agencia Española de Seguridad Alimentaria y
     Nut..." as Product.
  2. AESAN news 16/10/2026 "Información sobre la sospecha de casos de
     salmonelosis asociados a huevos" — suspected salmonellosis cases at a
     hospitality establishment in Vitoria (País Vasco); Salmonella Enteritidis
     found in hen droppings at a farm in Aragón; eggs from the affected sheds
     immobilized and withdrawn as a precaution; eggs distributed to France and
     Portugal (alert network activated); egg samples so far negative. No
     firm, brand, lot or case count on the page, so none claimed; Outbreak
     stays 0 (link suspected, not confirmed — George's rule of 2026-11-15).
Any archive row on the same URL is marked SUPERSEDED (append-only).

Archived out of Pending to Rejected (one archive row each; nothing deleted
from an archive):
  * three AECOSAN legacy-listing rows (2025_06, 2026_12, 2026_24) whose text
    "Alerta por presencia de Salmonella spp" matches none of the alerts their
    URLs resolve to (Morbier STEC 2025; Sanrio konjac jelly choking; Camembert
    STEC) and whose Pathogen contradicts their text;
  * NVWA babyvoeding topic page — a general page, Product is page debris;
  * RASFF notification 470205 — RASFF 2021.1402 sunflower seeds, aflatoxins,
    already in Rejected; Product is the table's column headers.
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
TODAY = "2026-11-19"
STAMP = "operator review 2026-11-19e"

REPAIRS = [
    ("https://www.aesan.gob.es/alertas/2026_31", {
        "Date": "2026-04-15",
        "Company": "(not specified in AESAN alert 2026_31)",
        "Brand": "Deluxe",
        "Product": ("Norwegian smoked salmon with pistachio, 100 g plastic "
                    "blister, refrigerated, lot LC22606501, expiry 18/04/26, "
                    "from Italy"),
        "Pathogen": "Listeria monocytogenes",
        "Reason": ("Presence of Listeria monocytogenes, notified by the "
                   "Italian authorities; initial distribution in the Canary "
                   "Islands, possible redistribution to other regions"),
        "Class": "Alert",
        "Country": "Spain",
        "Outbreak": 0,
    }, ("[{s}: fields read on AESAN alert Ref. ES2026/208, 'Fecha y hora: "
        "15/04/2026 15:30': 'Norwegian smoked salmon with pistachio (Salmón "
        "ahumado noruego con pistacho)', 'Marca: Deluxe', 'Peso de unidad: "
        "100 g', 'Lote: LC22606501', 'Fecha de caducidad: 18/04/26'; the "
        "Pending copy held the agency's name as Product] [original product: "
        "Salmón ahumado noruego con pistacho]")),
    ("https://www.aesan.gob.es/actualidad/actualidad-noticias/Informaci-n-sobre-la-sospecha-de-casos-de-salmonelosis-asociados-a-huevos", {
        "Date": "2026-10-16",
        "Company": "(farm not named by AESAN; hen farm in Aragon)",
        "Brand": "",
        "Product": ("Eggs from several lots of one hen farm in Aragon "
                    "(affected sheds); distributed in Spain, France and "
                    "Portugal"),
        "Pathogen": "Salmonella Enteritidis",
        "Reason": ("Suspected salmonellosis cases at a hospitality "
                   "establishment in Vitoria (Basque Country) associated "
                   "with eggs; Salmonella Enteritidis found in hen droppings "
                   "at the source farm; eggs immobilized and withdrawn as a "
                   "precaution; egg samples negative so far"),
        "Class": "Withdrawal",
        "Country": "Spain",
        "Outbreak": 0,
    }, ("[{s}: fields read on the AESAN news item of 16/10/2026 "
        "'Información sobre la sospecha de casos de salmonelosis asociados a "
        "huevos'; no firm, brand, lot or case count named, none claimed; "
        "Outbreak 0 until the link is confirmed (George 2026-11-15). Possibly "
        "the same event as RASFF 877497 (2026-10-08, Salmonella enteritidis "
        "in eggs from Spain) — not asserted] [original product: huevos]")),
]

ARCHIVE = [
    ("https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2025_06.htm",
     "URL resolves to AESAN ES2025/055 (Morbier, STEC, Feb 2025); row text "
     "'Alerta por presencia de Salmonella spp' and Pathogen Listeria match "
     "neither — legacy-listing scrape"),
    ("https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2026_12.htm",
     "URL resolves to AESAN ES2026/087 (Sanrio konjac jelly, choking, Feb "
     "2026); row text and Pathogen match neither — legacy-listing scrape"),
    ("https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2026_24.htm",
     "URL resolves to AESAN ES2026/161 (Camembert, STEC, Mar 2026); row text "
     "and Pathogen match neither — legacy-listing scrape"),
    ("https://www.nvwa.nl/onderwerpen/voedselveiligheid/babyvoeding-en-andere-speciale-voeding",
     "general NVWA topic page, not a recall; Product is page debris and "
     "NVWA's own text reports no measurable cereulide"),
    ("https://webgate.ec.europa.eu/rasff-window/screen/notification/470205",
     "RASFF 2021.1402 (sunflower seeds, aflatoxins) — a 2021 notification "
     "already in Rejected; Product is the RASFF table's column headers"),
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
                out.append(f"{STAMP} (George): archived — {why}")
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
    gone = _archive(wb)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"REPAIRED={n} SUPERSEDED={sup} ARCHIVED={gone}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
