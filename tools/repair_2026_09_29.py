# -*- coding: utf-8 -*-
"""One-shot data repair for the two workbook faults CI found on 2026-09-29.

Run once, then delete this file and .github/workflows/repair-2026-09-29.yml.

Why a script-and-workflow rather than an uploaded workbook: removing a row
from Recalls trips test_register_never_shrinks unless the COMMIT MESSAGE
carries a deletion marker, and a workbook uploaded through the GitHub web UI
commits as "Add files via upload", which carries nothing. The committing job
writes the marker itself. It also means this operates on whatever main holds
at run time rather than on a snapshot that goes stale the moment the next
hourly job writes the workbook.

FAULT A — A REGULATOR'S DISPLAY STRING IN THE DATE COLUMN
---------------------------------------------------------
Two Weekly_Rejected rows carry the date the regulator PRINTS rather than the
date in ISO:

    FDA    GF Blends           "September 18, 2026"
    FSAI   Kilbride Classic    "Friday, 18 September 2026"

Both are 18 September. Neither could ever have been published — the publish
gate refuses a Date that is not YYYY-MM-DD, and did — but every window filter
does a string or datetime comparison on this column, so both sorted and
filtered as nothing at all.

The cause is fixed at the writer (merge_master._write_sheet now canonicalises
Date at the same choke point that already handles Country, Source and Class)
and in the parser (scrapers/_listing.parse_any_date could not read a month
name at all until today). This script repairs the two rows already written.

FAULT B — A ROW THAT THIS REGISTER DELIBERATELY EXCLUDES
--------------------------------------------------------
    USDA FSIS · 2026-09-25 · Sempio Food Services Inc.
    "Korean Ginseng ready-to-eat chicken stew — approx. 4,596 lb"
    Reason: "Imported without reinspection; produced without the benefit of
             import reinspection."

FSIS import re-inspection violations are excluded on purpose. The USDA FSIS
scraper drops them (test_import_violation_dropped), and
test_nrte_is_not_ready_to_eat.py::test_import_violations_are_deliberately_not_included
exists expressly so that the line is only ever moved on purpose. This row was
added by hand during the 2026-09-28 morning sweep — its own Notes say "Placed
in PENDING, not Recalls" — and was then published by the offline auto-promoter
the next morning, because the promoter promotes anything that passes the gate
and the gate has no rule about import violations.

It is REMOVED from Recalls and written to the Rejected archive with a terminal
reason, so nothing re-ingests it. It is not deleted outright: an archive row
is how this register records that something was considered and refused.
"""
from __future__ import annotations

import datetime as dt
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = dt.date.today().isoformat()

#: The one row to remove, identified by its authority URL — the only stable
#: key. Named rather than matched by rule, so this script can never remove
#: anything its author did not look at.
SEMPIO_URL = ("https://www.fsis.usda.gov/recalls-alerts/"
              "sempio-food-services-inc--recalls-ready-eat-chicken-stew-"
              "products-imported-without")

SEMPIO_REASON = (
    "operator review " + TODAY + ": out_of_scope_import_reinspection — FSIS "
    "import re-inspection violations are deliberately excluded from this "
    "register. The USDA FSIS scraper drops them and "
    "tests/test_nrte_is_not_ready_to_eat.py::"
    "test_import_violations_are_deliberately_not_included exists so the line "
    "is only ever moved on purpose. This row was added by hand during the "
    "2026-09-28 morning sweep (its own Notes said 'Placed in PENDING, not "
    "Recalls') and was then published by the offline auto-promoter, which "
    "promotes anything passing the publish gate — and the gate has no rule "
    "about import violations. Removed from Recalls " + TODAY + "."
)

#: Sheets whose Date column drives behaviour. `Rejected` is a terminal archive
#: nothing filters on, and is left alone for the same reason the test that
#: found this leaves it alone: rewriting settled audit rows costs more than
#: it buys.
LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")


# ── FAULT C — A ROW VAGUER THAN ITS OWN REASON ─────────────────────────────
# FDA, 2026-09-29, Sierra Nevada Cheese Company, Graziers raw milk cheese.
# Published with Pathogen "Escherichia coli (generic)" while its own Reason
# read "Potential to be contaminated with Shiga toxin-producing Escherichia
# coli (STEC)". The writer now specialises a family label from the row's own
# text (merge_master._write_sheet), which prevents the next one; this repairs
# the row already written, on every sheet that holds it.
#
# The Outbreak flag is a separate matter and is NOT force-set. It read 0, and
# that was correct given the row's text — the outbreak evidence was on FDA's
# page, not in the row. The flag is evidence-gated on purpose (five rows were
# once published as outbreaks on the strength of a RASFF "risk: serious"
# string). So the EVIDENCE is written into Reason, quoted from the notice this
# row already cites, and the flag then rests on something the row says.
SIERRA_URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
              "alerts/sierra-nevada-cheese-company-recalls-graziers-raw-milk-"
              "cheese-because-possible-health-risk")

SIERRA_PATHOGEN = "Shiga toxin-producing E. coli (STEC)"

#: Verbatim from the FDA notice at SIERRA_URL (company announcement
#: 2026-09-28, FDA publish date 2026-09-29).
SIERRA_REASON = (
    "Potential to be contaminated with Shiga toxin-producing Escherichia coli "
    "(STEC); FDA names Escherichia coli O26:H11. FDA/CDC outbreak "
    "investigation: 13 illnesses identified to date, epidemiologically "
    "associated with consumption of Graziers Raw Milk Medium Cheddar."
)

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def repair_dates(wb) -> int:
    from scrapers._listing import parse_any_date
    fixed = 0
    for name in LIVE_SHEETS:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = [str(c.value) for c in ws[1]]
        if "Date" not in head:
            continue
        dcol = head.index("Date") + 1
        ncol = head.index("Notes") + 1 if "Notes" in head else None
        for r in range(2, ws.max_row + 1):
            raw = ws.cell(r, dcol).value
            if raw in (None, "") or isinstance(raw, (dt.date, dt.datetime)):
                continue
            txt = str(raw).strip()
            if not txt or _ISO.match(txt):
                continue
            iso = parse_any_date(txt)
            if not iso:
                print(f"  {name} row {r}: UNREADABLE Date {txt!r} — left as is, "
                      f"report it rather than guessing")
                continue
            ws.cell(r, dcol).value = iso
            if ncol:
                prior = str(ws.cell(r, ncol).value or "").strip()
                ws.cell(r, ncol).value = (
                    prior + f" [date-normalised {TODAY}: the regulator's display "
                    f"string {txt!r} was in the Date column; it is "
                    f"{iso} and now sorts and filters as one]").strip()
            print(f"  {name} row {r}: {txt!r} -> {iso}")
            fixed += 1
    return fixed


def remove_sempio(wb) -> int:
    if "Recalls" not in wb.sheetnames:
        return 0
    ws = wb["Recalls"]
    head = [str(c.value) for c in ws[1]]
    ucol = head.index("URL") + 1
    target = None
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, ucol).value or "").strip() == SEMPIO_URL:
            target = r
            break
    if target is None:
        print("  Sempio row not in Recalls — nothing to remove")
        return 0

    row = {h: ws.cell(target, i + 1).value for i, h in enumerate(head)}
    print(f"  Recalls row {target}: {row.get('Date')} {row.get('Company')} "
          f"— removing")

    # Archive FIRST, so a crash between the two leaves a duplicate rather
    # than a disappearance.
    if "Rejected" in wb.sheetnames:
        arch = wb["Rejected"]
        ahead = [str(c.value) for c in arch[1]]
        out = []
        for h in ahead:
            if h == "RejectReason":
                out.append(SEMPIO_REASON)
            elif h == "RejectedBy":
                out.append("operator review " + TODAY)
            elif h == "RejectedAt":
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            elif h == "Reviewed":
                out.append("Y")
            else:
                out.append(row.get(h, ""))
        arch.append(out)
        print(f"  Rejected: archived with a terminal reason "
              f"({len(ahead)} columns)")
    else:
        print("  WARNING: no Rejected sheet — the row is removed with no "
              "archive entry, which loses the audit trail")

    ws.delete_rows(target, 1)
    return 1


def repair_sierra_nevada(wb) -> int:
    """Name the organism the row's own Reason already named, on every sheet."""
    fixed = 0
    for name in wb.sheetnames:
        ws = wb[name]
        head = [str(c.value) for c in ws[1]]
        if "URL" not in head or "Pathogen" not in head:
            continue
        ucol = head.index("URL") + 1
        pcol = head.index("Pathogen") + 1
        rcol = head.index("Reason") + 1 if "Reason" in head else None
        ocol = head.index("Outbreak") + 1 if "Outbreak" in head else None
        ncol = head.index("Notes") + 1 if "Notes" in head else None
        tcol = head.index("Tier") + 1 if "Tier" in head else None
        for r in range(2, ws.max_row + 1):
            if str(ws.cell(r, ucol).value or "").strip() != SIERRA_URL:
                continue
            was = str(ws.cell(r, pcol).value or "")
            if was == SIERRA_PATHOGEN and ocol and ws.cell(r, ocol).value in (1, "1"):
                print(f"  {name} row {r}: already repaired")
                continue
            ws.cell(r, pcol).value = SIERRA_PATHOGEN
            if rcol:
                ws.cell(r, rcol).value = SIERRA_REASON
            if ocol:
                ws.cell(r, ocol).value = 1
            if tcol:
                ws.cell(r, tcol).value = 1
            if ncol:
                prior = str(ws.cell(r, ncol).value or "").strip()
                ws.cell(r, ncol).value = (
                    prior + f" [pathogen-repair {TODAY}: Pathogen was {was!r} "
                    f"while this row's own Reason already said 'Shiga "
                    f"toxin-producing Escherichia coli (STEC)'. Set to the "
                    f"organism the row names. Outbreak set to 1 on the FDA "
                    f"notice's own words — FDA/CDC investigation, 13 illnesses "
                    f"epidemiologically associated — which are now quoted in "
                    f"Reason so the flag rests on something the row says]"
                ).strip()
            print(f"  {name} row {r}: {was!r} -> {SIERRA_PATHOGEN!r}; "
                  f"Outbreak -> 1")
            fixed += 1
    return fixed


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1
    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1

    print("FAULT A — non-ISO Date values")
    n_dates = repair_dates(wb)
    print(f"  {n_dates} repaired\n")

    print("FAULT B — a deliberately excluded row in Recalls")
    n_rows = remove_sempio(wb)
    print(f"  {n_rows} removed\n")

    print("FAULT C — a row vaguer than its own Reason")
    n_path = repair_sierra_nevada(wb)
    print(f"  {n_path} repaired\n")

    if not n_dates and not n_rows and not n_path:
        print("nothing to do")
        print("ROWS_REMOVED=0")
        return 0

    wb.save(XLSX)
    after = openpyxl.load_workbook(XLSX)["Recalls"].max_row - 1
    print(f"Recalls {before} -> {after}")

    from pipeline.merge_master import mirror_json_from_xlsx
    n = mirror_json_from_xlsx(XLSX, JSON)
    print(f"recalls.json re-mirrored: {n} rows")

    print(f"ROWS_REMOVED={n_rows}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
