# -*- coding: utf-8 -*-
"""Operator review 2026-10-27: the daily global search's MISSING list.

The daily-global brief of 2026-10-27 named three in-scope recalls of
26 Oct that no sheet carried as a usable row. Each was read on the
regulator's own page on 2026-10-27 and every field below comes from it.

A. AESAN (ES) alert 2026_75, ref. ES2026/648 — Arkan Pure Sesame Tahini,
   Salmonella. Pending held SIX copies, one per language version of the
   page (/en/, /eu/, /va/, /ca/, /gl/ and bare aesan.gob.es), all with
   Company empty and Product scraped from the page chrome
   ("calendar_today 26/10/2026", or the alert's Spanish headline). None
   could pass the publish gate. The six are archived to Rejected (append-
   only; reason recorded) and ONE clean row is queued.
B. BVL (DE) lebensmittelwarnung.de 261026_24_SH_Sumak — Abido Sumak 50 g,
   chlorpyrifos above the limit and the unauthorised dye E 122. Only in
   Weekly_Rejected, four copies fanned out from produktwarnung.eu under
   BVL/AGES/AFSCA/NVWA and rejected "no-authority-url (news-only
   discovery)". True of the article, not of the world: the BVL's own page
   exists. Queued with that URL.
C. CFIA RA-82850 — Neo Chips Apple & Wild Blueberry freeze-dried fruit
   sticks 12 g, dried stem fragments (Food - Extraneous Material), Class 2.
   In no sheet. Tier is computed (foreign material -> Tier 3).

All three go to Pending through tools/add_manual_row.py and take the normal
review path. NOTHING IS REMOVED FROM Recalls.
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
TODAY = "2026-10-27"

AESAN_KEY = "aesan.gob.es"
AESAN_ID = "/alertas/2026_75"
AESAN_REASON = (
    "REJECTED — duplicate page-copy junk: one of six Pending copies of AESAN alert "
    "2026_75 (ES2026/648), one per language version of the same page, with "
    "Company empty and Product scraped from page chrome. Replaced by one "
    "clean row read from the alert itself (operator review " + TODAY + ").")

NEW_ROWS = [
    {
        "Date": "2026-10-26",
        "Source": "AESAN (ES)",
        "Company": "(not specified in AESAN alert 2026_75)",
        "Brand": "Arkan",
        "Product": "Pure Sesame Tahini, 400 g jar, lot 0257502493, "
                   "best before 12/2027",
        "Pathogen": "Salmonella",
        "Reason": "Presence of Salmonella spp. in sesame tahini from Egypt "
                  "(AESAN ref. ES2026/648). Distributed in Cataluña, Madrid "
                  "and País Vasco; redistribution to other regions not ruled "
                  "out.",
        "Class": "Alert",
        "Country": "Spain",
        "Outbreak": 0,
        "URL": "https://www.aesan.gob.es/alertas/2026_75",
        "Notes": (
            "[operator review 2026-10-27: daily-global MISSING item. Read on "
            "aesan.gob.es/alertas/2026_75: ref. ES2026/648, 26/10/2026, "
            "product 'Pure Sesame Tahini', brand Arkan, lot 0257502493, "
            "best before 12/2027, Salmonella spp., origin Egypt, regions "
            "Cataluña, Madrid, País Vasco. The alert names no company. The "
            "underlying RASFF notification is already in Recalls; this is "
            "Spain's own national action. Six page-copy junk rows of this "
            "alert were archived from Pending the same day.]"),
    },
    {
        "Date": "2026-10-26",
        "Source": "BVL (DE)",
        "Company": "Abido Company for Trade and Industry",
        "Brand": "Abido",
        "Product": "Sumac (Abido Sumak), 50 g, lot 10-3-2618952 OH, "
                   "best before 10.03.2029",
        "Pathogen": "Chlorpyrifos (pesticide residue) and unauthorized "
                    "dye E 122",
        "Reason": "Elevated levels of chlorpyrifos (insecticide and acaricide) "
                  "and the presence of the unauthorized dye E 122 were found. "
                  "Sold in Baden-Württemberg, Bavaria, Brandenburg, Hamburg, "
                  "North Rhine-Westphalia, Saarland and Saxony-Anhalt.",
        "Class": "Recall",
        "Country": "Germany",
        "Outbreak": 0,
        "URL": "https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
               "Meldungen/2026/10_Oktober/261026_24_SH_Sumak/"
               "261026_24_SH_Sumak.html",
        "Notes": (
            "[operator review 2026-10-27: daily-global MISSING item. Read on "
            "lebensmittelwarnung.de 261026_24_SH_Sumak, 26.10.2026: product "
            "'Abido Sumak, 50 Gramm', Hersteller 'Abido Company for Trade "
            "and Industry', Inverkehrbringer 'Tema Fine Foods BV', "
            "Großhändler 'Orient Gate GmbH', Los '10-3-2618952 OH', MHD "
            "10.03.2029. Four news-discovered copies sit in Weekly_Rejected "
            "as no-authority-url; this row carries the authority's own URL.] "
            "[original reason (de): \"Es wurden erhöhte Werte von Chlorpyrifos "
            "(Insektizid und Akarizid) und das Vorhandensein des unzulässigen "
            "Farbstoffs E 122 festgestellt.\"]"),
    },
    {
        "Date": "2026-10-23",
        "Source": "CFIA",
        "Company": "Ellipse Conservation",
        "Brand": "Neo Chips",
        "Product": "Apple & Wild Blueberry Freeze-dried Fruit Sticks, 12 g "
                   "(UPC 6 28634 95522 2; codes 262241, 262281, 262521)",
        "Pathogen": "Foreign material (dried stem fragments)",
        "Reason": "Neo Chips brand Apple & Wild Blueberry Freeze-dried Fruit "
                  "Sticks recalled due to pieces of dried stem fragments "
                  "(CFIA category: Food - Extraneous Material). Sold online "
                  "and in New Brunswick and Quebec.",
        "Class": "Class 2",
        "Country": "Canada",
        "Outbreak": 0,
        "URL": "https://recalls-rappels.canada.ca/en/alert-recall/"
               "neo-chips-brand-apple-wild-blueberry-freeze-dried-fruit-"
               "sticks-recalled-due-pieces",
        "Notes": (
            "[operator review 2026-10-27: daily-global MISSING item. Read on "
            "the CFIA notice: recall date 2026-10-23, RA-82850, Class 2, "
            "recalling firm Ellipse Conservation; distribution online, NB, "
            "QC.]"),
    },
]


def _head(ws):
    return [c.value for c in ws[1]]


def _archive_aesan_copies() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    pend, rej = wb["Pending"], wb["Rejected"]
    ph, rh = _head(pend), _head(rej)
    ui, ci = ph.index("URL") + 1, ph.index("Company") + 1
    n = 0
    for r in range(pend.max_row, 1, -1):
        url = str(pend.cell(r, ui).value or "")
        if AESAN_KEY not in url or not url.rstrip("/").endswith(AESAN_ID):
            continue
        if str(pend.cell(r, ci).value or "").strip():
            continue                       # a usable row: leave it alone
        row = {h: pend.cell(r, i + 1).value for i, h in enumerate(ph)}
        out = []
        for h in rh:
            if h in ("RejectReason", "RejectionReason"):
                out.append(AESAN_REASON)
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
        n += 1
        print(f"  Pending -> Rejected: {url}")
    wb.save(XLSX)
    wb.close()
    return n


def main() -> int:
    archived = _archive_aesan_copies()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), rows_path],
        cwd=str(ROOT), text=True, capture_output=True)
    print(proc.stdout.rstrip())
    if proc.stderr.strip():
        print(proc.stderr.rstrip(), file=sys.stderr)
    Path(rows_path).unlink(missing_ok=True)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row failed ({proc.returncode})")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_ARCHIVED={archived}")
    print(f"ROWS_QUEUED={len(NEW_ROWS)}")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
