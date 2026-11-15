# -*- coding: utf-8 -*-
"""Weekly outbreak verification 2026-11-15 — one missed outbreak, one duplicate.

Scope: Recalls rows dated 2026-10-01 .. 2026-11-15 (last 45 days; it covers
all of October 2026, the most recently closed month).

Flagged rows verified on authority pages this pass (all CONFIRMED, kept):
  * FDA, 2026-10-19, Little Wicomico oysters, Salmonella Telelkebir — FDA
    safety alert "Current Update" October 30, 2026: "part of a cluster of 28
    Salmonella Telelkebir illnesses being investigated by CDC, FDA, and the
    relevant state authorities". The Reason still said 27 (the 10/19 count);
    updated to FDA's own 28.
  * FSA (UK), 2026-10-10, Yasensvit eggs — FSA news release on gov.uk, 10 Oct
    2026: "the strain from the eggs was linked to 36 cases of Salmonella
    infection reported in England in 2026." (unchanged)
  * BLV (CH), 2026-10-07, Le Grand'Joie terrine — Canton du Valais, Service
    de la santé publique: "Deux cas de botulisme d'origine alimentaire ont
    été identifiés en Valais." (unchanged)

1. MISSED OUTBREAK — AGES (AT), 2026-10-16, Sicilian cocktail tomatoes,
   Salmonella Strathcona, published with Outbreak=0. The AGES warning itself
   (Lebensmittel-Warnung 16.10.2026) states: "Diese Tomaten stehen im
   Zusammenhang mit einem jährlich wiederkehrenden, Ausbruch von Salmonella
   Strathcona." / "Auch in Österreich wurden seit Ende Juli 29 Fälle
   registriert." / "Sieben Personen (24 %) mussten hospitalisiert werden."
   The recalling authority states cases associated with the product ->
   Outbreak=1. October's outbreak count in the register goes 3 -> 4.

2. DUPLICATE — FDA, 2026-10-15, Family Farmstead Dairy Raw Mild Cheddar, at
   .../family-farmstead-dairy-announces-recall-raw-mild-cheddar-cheese-due-
   potential-e-coli-o157h7. Read 2026-11-15: that URL serves the same page
   as .../...-due-potential-e-coli-o26h11-and (its canonical link points
   there), whose body reads "This is an update to the company's press
   release, previously issued on 10/15/2026, to correct the distribution
   pattern and serotype of E. coli". Same regulator, same recall, two URLs;
   the canonical row (O26:H11 and O45:H2) stays. October total 390 -> 389.

Nothing is deleted from the archives (append-only). Idempotent: a second run
changes nothing.
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
TODAY = "2026-11-15"
BY = "outbreak check " + TODAY
TAG = "[outbreak check " + TODAY

AGES_URL = ("https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/produkt/"
            "produktwarnung-salmonellen-in-tomaten-aus-zwei-regionen-in-sizilien")
AGES_REASON = ("Salmonella Strathcona — AGES warning against eating raw cocktail "
               "tomatoes from Ragusa and Caltanissetta (Sicily), linked to a "
               "recurring Salmonella Strathcona outbreak; 29 cases in Austria "
               "since end of July 2026, 7 hospitalised.")
AGES_NOTE = (TAG + ": CONFIRMED OUTBREAK, Outbreak 0 -> 1. AGES warning read "
             "2026-11-15 (Lebensmittel-Warnung 16.10.2026): \"Diese Tomaten stehen "
             "im Zusammenhang mit einem jährlich wiederkehrenden, Ausbruch von "
             "Salmonella Strathcona.\" / \"Auch in Österreich wurden seit Ende Juli "
             "29 Fälle registriert.\" / \"Sieben Personen (24 %) mussten "
             "hospitalisiert werden.\" No deaths stated. Reason was the bare "
             "serovar; it now carries the page's own counts.]")

OYSTER_URL = ("https://www.fda.gov/food/alerts-advisories-safety-information/"
              "safety-alert-salmonella-oysters-little-wicomico-river-virginia")
OYSTER_NOTE = (TAG + ": CONFIRMED. FDA safety alert, Current Update October 30, "
               "2026: \"part of a cluster of 28 Salmonella Telelkebir illnesses "
               "being investigated by CDC, FDA, and the relevant state "
               "authorities\"; \"One additional illness has been reported since "
               "October 19, 2026.\" Reason count 27 -> 28. No hospitalisations or "
               "deaths stated on the FDA page.]")

FF_CANON = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
            "family-farmstead-dairy-announces-recall-raw-mild-cheddar-cheese-due-"
            "potential-e-coli-o26h11-and")
REMOVE = [{
    "url": ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
            "family-farmstead-dairy-announces-recall-raw-mild-cheddar-cheese-due-"
            "potential-e-coli-o157h7"),
    "why": ("duplicate_of_published_recall — superseded URL of the same FDA "
            "recall. Read 2026-11-15: this URL serves the same page as "
            + FF_CANON + " (its canonical link points there), whose body reads "
            "\"This is an update to the company's press release, previously "
            "issued on 10/15/2026, to correct the distribution pattern and "
            "serotype of E. coli\" (O26:H11 and O45:H2, not O157:H7). Same "
            "regulator, same recall, same lot 7/22/27, two URLs; the canonical "
            "row is published. Never re-promote."),
}]


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _archive(arch, row, reason):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(row.get(h, ""))
    arch.append(out)


def _find(ws, head, url):
    iu = head.index("URL") + 1
    t = _key(url)
    return [r for r in range(2, ws.max_row + 1) if _key(ws.cell(r, iu).value) == t]


def main() -> int:
    wb = openpyxl.load_workbook(XLSX)
    ws, arch = wb["Recalls"], wb["Rejected"]
    head = _head(ws)
    col = {h: i + 1 for i, h in enumerate(head)}
    changed = removed = 0

    # 1. AGES tomatoes: missed outbreak.
    for r in _find(ws, head, AGES_URL):
        notes = str(ws.cell(r, col["Notes"]).value or "")
        if TAG in notes:
            continue
        ws.cell(r, col["Outbreak"]).value = 1
        ws.cell(r, col["Reason"]).value = AGES_REASON
        ws.cell(r, col["Notes"]).value = (notes.rstrip() + " " + AGES_NOTE).strip()
        if "LastUpdated" in col:
            ws.cell(r, col["LastUpdated"]).value = TODAY
        changed += 1
        print("  Recalls: AGES tomatoes Outbreak 0 -> 1")

    # 2. Oysters: FDA's current count.
    for r in _find(ws, head, OYSTER_URL):
        notes = str(ws.cell(r, col["Notes"]).value or "")
        if TAG in notes:
            continue
        reason = str(ws.cell(r, col["Reason"]).value or "")
        if "cluster of 27 " in reason:
            ws.cell(r, col["Reason"]).value = reason.replace("cluster of 27 ",
                                                             "cluster of 28 ")
        ws.cell(r, col["Notes"]).value = (notes.rstrip() + " " + OYSTER_NOTE).strip()
        if "LastUpdated" in col:
            ws.cell(r, col["LastUpdated"]).value = TODAY
        changed += 1
        print("  Recalls: oysters count 27 -> 28 (FDA 10/30 update)")

    # 3. Duplicate URL to Rejected.
    for spec in REMOVE:
        canon_present = bool(_find(ws, head, FF_CANON))
        rows = _find(ws, head, spec["url"])
        if rows and not canon_present:
            print("  canonical Family Farmstead row missing — duplicate NOT removed")
            continue
        for r in sorted(rows, reverse=True):
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            _archive(arch, row, "Removed from Recalls by the " + BY + " — "
                     + spec["why"])
            ws.delete_rows(r, 1)
            removed += 1
            print(f"  Recalls: REMOVED {row.get('Date')} {row.get('Source')!r} "
                  f"{row.get('Company')!r}")
        if not rows:
            print(f"  Recalls: no row at {spec['url'][:70]} (already gone)")

    if changed or removed:
        wb.save(XLSX)
    wb.close()
    if changed or removed:
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_CHANGED={changed} ROWS_REMOVED={removed}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
