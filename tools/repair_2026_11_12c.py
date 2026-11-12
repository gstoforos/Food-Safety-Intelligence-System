# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-12 (c) — row-merge main's new Pending rows.

While this review ran, origin/main gained one commit (01fd813, "Tavily
gap-finder (standalone): +4 rows to Pending"). Its only change is four new
Pending rows; Recalls, the archives and NEWS are content-identical to the
workbook this review started from. A git rebase cannot merge a binary
workbook, so this script does what the pipeline's own "row-merged" retries
do: it copies the four rows from main's workbook into this review's Pending
sheet, keyed on URL, and then decides the two it can.

    python tools/repair_2026_11_12c.py <path-to-main-recalls.xlsx>

Three of the four are the FSA double-URL trap (every FSA alert is served at
both /news-alerts/alert/<ref> and /article/<id>/<slug>): the register
already publishes both recalls from the /news-alerts/alert/ address, so the
/article/ copies are archived to Rejected as duplicates. The fourth (SFA
raw-milk cheese STEC, Gillot) stays in Pending for a human — sfa.gov.sg
could not be read from this session.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-12"
BY = f"operator review {TODAY}"

REJECTS = [
    ("https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella",
     "DUPLICATE OF A PUBLISHED RECALL — FSA /article/ TWIN",
     "The FSA serves every alert at both /news-alerts/alert/<ref> and "
     "/article/<id>/<slug>. This is the /article/ address of FSA-PRIN-29-2026, "
     "Prepworld fruit products, Salmonella, which is published in Recalls at "
     "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-29-2026 (2026-06-23) "
     "and again from the CFS (HK) re-post of 2026-06-24. The Tavily row's own "
     "text ('Prepworld is recalling several fruit packs after testing identified "
     "Salmonella in apple and kiwi') is that alert; Date is empty. alerts.food.gov.uk "
     "answers HTTP 403 from this session, so the twin was matched on the register."),
    ("https://alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-products-because-of-possible-contamination-with-Listeria-monocytogenes",
     "DUPLICATE OF A PUBLISHED RECALL — FSA /article/ TWIN",
     "The /article/ address of FSA-PRIN-38-2026, Greencore three pasta products "
     "(Asda Cheese & Tomato Pasta Salad 300 g …), Listeria monocytogenes, which "
     "is published in Recalls at https://alerts.food.gov.uk/news-alerts/alert/"
     "fsa-prin-38-2026 (2026-08-07). The Tavily row's own text names the Asda "
     "Cheese & Tomato Pasta; Date is empty. alerts.food.gov.uk answers HTTP 403 "
     "from this session, so the twin was matched on the register."),
    ("https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes",
     "DUPLICATE OF A PUBLISHED RECALL — FSA /article/ TWIN",
     "The /article/ address of FSA-PRIN-32-2026, O'Brien Fine Foods (Brady "
     "Family / Deluxe) sliced ham, Listeria monocytogenes, which is published "
     "in Recalls at https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-32-2026 "
     "(2026-07-01). Two earlier operator verdicts on this URL say the same "
     "(2026-08-20 'fabricated_host_and_duplicate', 2026-08-22 "
     "'duplicate_already_approved'); Date is empty. alerts.food.gov.uk answers "
     "HTTP 403 and the data.food.gov.uk search HTTP 400 from this session, so "
     "the twin was matched on the register."),
]


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    return str(u or "").strip().lower().rstrip("/")


def main(main_xlsx: Path) -> int:
    src = openpyxl.load_workbook(main_xlsx, read_only=True, data_only=True)
    wb = openpyxl.load_workbook(XLSX)
    pend, arch = wb["Pending"], wb["Rejected"]
    ph, ah = _head(pend), _head(arch)
    sp = src["Pending"]
    sh = [str(c.value or "") for c in next(sp.iter_rows(min_row=1, max_row=1))]
    if sh != [str(h or "") for h in ph]:
        print("STOP — Pending headers differ between main and this workbook")
        return 2
    have = {_key(pend.cell(r, ph.index("URL") + 1).value)
            for r in range(2, pend.max_row + 1)}
    # Anything this review already decided TONIGHT must not come back. Only
    # rows carrying tonight's operator stamp count: an older archive copy of
    # the same URL is a previous verdict the re-promotion guard will weigh,
    # not a reason to drop main's row on the floor (first run of this script
    # read the whole archive here and silently left two of main's rows out).
    decided = set()
    for sheet in ("Recalls", "Rejected", "Weekly_Rejected"):
        ws = wb[sheet]
        hd = _head(ws)
        uc = hd.index("URL") + 1
        stamp_cols = [hd.index(c) + 1 for c in ("Notes", "RejectReason",
                                                 "RejectionReason") if c in hd]
        for r in range(2, ws.max_row + 1):
            blob = " ".join(str(ws.cell(r, c).value or "") for c in stamp_cols)
            if BY in blob or f"nightly-operator-{TODAY}" in blob:
                decided.add(_key(ws.cell(r, uc).value))
    merged = 0
    for row in sp.iter_rows(min_row=2, values_only=True):
        if not any(v not in (None, "") for v in row):
            continue
        d = dict(zip(sh, row))
        k = _key(d.get("URL"))
        if not k or k in have:
            continue
        if k in decided:
            # Includes the REJECTS below once they have been archived, so a
            # re-run of this script neither re-merges nor re-archives them.
            print(f"  skipped (already decided tonight): {k[-60:]}")
            continue
        pend.append([d.get(h) for h in sh])
        have.add(k)
        merged += 1
        print(f"  merged from main: {str(d.get('Source'))[:12]:13} {str(d.get('Product'))[:50]!r}")
    print(f"merged {merged} Pending row(s) from main")

    index = {}
    ucol = ph.index("URL") + 1
    for r in range(2, pend.max_row + 1):
        index.setdefault(_key(pend.cell(r, ucol).value), []).append(r)
    todo = [(u, t, r) for u, t, r in REJECTS if _key(u) in index]
    for u, _t, _r in REJECTS:
        if _key(u) not in index:
            print(f"  already archived, nothing to do: {u[-60:]}")
    rows_to_delete = []
    for url, tag, reason in todo:
        r = index[_key(url)][0]
        row = {h: pend.cell(r, i + 1).value for i, h in enumerate(ph)}
        notes = str(row.get("Notes") or "").strip()
        row["Notes"] = (f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: {tag}. "
                        f"{reason}] " + notes).strip()
        out = []
        for h in ah:
            if h == "RejectReason":
                out.append(f"{BY}: {tag}. {reason}")
            elif h == "RejectedBy":
                out.append(BY)
            elif h == "RejectedAt":
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            else:
                out.append(row.get(h, ""))
        arch.append(out)
        rows_to_delete.append(r)
        print(f"  Pending row {r:>3} -> Rejected: {tag:<50} {str(row.get('Product'))[:40]!r}")
    for r in sorted(rows_to_delete, reverse=True):
        pend.delete_rows(r, 1)
    wb.save(XLSX)
    print(f"Pending now {pend.max_row - 1}; Rejected {arch.max_row - 1}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    if len(sys.argv) != 2:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(Path(sys.argv[1])))
