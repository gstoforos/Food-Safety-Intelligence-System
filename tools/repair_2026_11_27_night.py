# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-27. Base: main at cff0a74 (21:11 Athens).

Does only what the curator cannot; never deletes a row from the register
(rows MOVE from Pending/Recalls to Rejected with a written reason).

    python tools/repair_2026_11_27_night.py --phase reject     # Pending -> Rejected
    python tools/repair_2026_11_27_night.py --phase unpublish  # Recalls -> Rejected
    python -m pipeline.promote_gate_passing --apply
    python tools/repair_2026_11_27_night.py --phase sort       # Recalls newest-first
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-27"
BY = "nightly operator review 2026-11-27"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"

FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
FSIS = "https://www.fsis.usda.gov/recalls-alerts/"

SAME = ("Same decision as unmerged PR #57 (and #55, #52-#54), re-applied because "
        "that PR never reached main.")
PENDING_REJECTS = {
    "https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026":
        f"stale / duplicate_of_published_recall — {BY}: FDA page read tonight: status "
        "'Ended', 'Content current as of 10/02/2026'. The Taylor Fresh Foods / Taylor "
        f"Farms de Mexico iceberg lettuce recall is already published (FDA, 2026-07-18). {SAME}",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit MEDICINE (Proguanil tablets), not a food. {SAME}",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: medicines-safety alert (gabapentinoids), not a food. {SAME}",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"out of scope — {BY}: RASFF 2021.1402, aflatoxin in 'sunflower seeds for bird "
        f"feeding' — animal FEED, from 2021. {SAME}",
    FDA + "/green-lumber-holdings-llc-issues-recall-and-consumer-safety-alert":
        f"out of scope — {BY}: FDA page read tonight: product 'Natural Fuel For Men', "
        f"product type 'Drugs', undeclared tadalafil. Not a food-safety hazard. {SAME}",
    FDA + "?search_api_fulltext=H-0198-2027":
        f"search_query_url / downstream of published event — {BY}: CONSTRUCTED FDA "
        "site-search URL, not a notice page (no published FDA row uses that pattern). "
        "D & M Smoked Fish cold smoked sprats is downstream of the published Haifa / D&M "
        f"cold-smoked sprats event (FDA 2026-10-23, 2026-10-27). {SAME}",
    FDA + "?search_api_fulltext=H-0200-2027":
        f"search_query_url — {BY}: CONSTRUCTED FDA site-search URL, not a notice page; "
        f"openFDA-only entry for Country Creek alfalfa sprout seeds with no FDA notice to cite. {SAME}",
    "http://data.food.gov.uk/food-alerts/id.html?_limit=100&_offset=400&_sort=-created&_sort=notation&_view=full":
        f"not_a_notice — {BY}: FSA data-catalogue listing-API page, not a recall notice; "
        "no Product, Company or Date. Already Status 'rejected' and in Rejected for the "
        "same reason.",
    "https://www.cdc.gov/infection-control/hcp/reopen-health-facilities/water-wind-damage.html":
        f"out of scope — {BY}: CDC guidance on reopening healthcare facilities after water "
        "and wind damage; not a food. Already Status 'rejected'.",
}
PENDING_REJECT_ONE_COPY = {
    ("https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-card-factory-candy-canes",
     "Candy Canes"):
        f"duplicate — {BY}: re-promotion guard honoured. URL already in Rejected "
        "('daily-global review 2026-11-22c: archived — DUPLICATE of the published FSA "
        f"PRIN-53-2026 row', Card Factory candy canes, metal). {SAME}",
}
NO_URL_PREFIX_REJECTS = {}
RELEASE = {}
# Un-publishing (Wolverine 031-2024, Olympia kofta ?ref copy) was NOT run
# tonight: the session's permission layer refused moving published rows.
UNPUBLISH = {}
def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _archive_row(arch, src_head, vals, why):
    ah = _head(arch)
    first = dict(zip(src_head, vals))
    out = []
    for h in ah:
        if h in ("RejectReason", "RejectionReason"):
            out.append(why)
        elif h == "RejectedBy":
            out.append(BY)
        elif h in ("RejectedAt", "Week_Added"):
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        elif h == "Reviewed":
            out.append("Y")
        else:
            out.append(first.get(h, ""))
    arch.append(out)


def reject(wb):
    ws, arch = wb["Pending"], wb["Rejected"]
    head = _head(ws)
    iu, isrc, ipr = head.index("URL"), head.index("Source"), head.index("Product")
    keys = {_key(u): w for u, w in PENDING_REJECTS.items()}
    n = 0
    for r in range(ws.max_row, 1, -1):
        vals = [c.value for c in ws[r]]
        why = keys.get(_key(vals[iu])) if vals[iu] else None
        if why is None and not vals[iu]:
            why = next((w for (src, pre), w in NO_URL_PREFIX_REJECTS.items()
                        if str(vals[isrc]) == src and str(vals[ipr]).startswith(pre)), None)
        if why is None and vals[iu]:
            why = next((w for (u, pre), w in PENDING_REJECT_ONE_COPY.items()
                        if _key(vals[iu]) == _key(u) and str(vals[ipr]).startswith(pre)), None)
        if why:
            _archive_row(arch, head, vals, why)
            ws.delete_rows(r, 1)
            n += 1
            print(f"  Pending:{r} -> Rejected: {str(vals[ipr])[:60]}")
    print(f"REJECTED={n}")


def release(wb):
    n = 0
    for name in ("Weekly_Rejected", "Rejected"):
        a = wb[name]
        h = _head(a)
        col = next(h.index(c) + 1 for c in ("RejectionReason", "RejectReason") if c in h)
        ncol = h.index("Notes") + 1
        for rr in range(2, a.max_row + 1):
            k = _key(a.cell(rr, h.index("URL") + 1).value)
            for url, why in RELEASE.items():
                if k != _key(url):
                    continue
                cur = str(a.cell(rr, col).value or "")
                if BY in cur:
                    continue
                a.cell(rr, col).value = f"[SUPERSEDED {TODAY} — {BY}: {why}] " + cur
                if "Reviewed" in h:
                    a.cell(rr, h.index("Reviewed") + 1).value = "SUPERSEDED"
                if "Status" in h:
                    a.cell(rr, h.index("Status") + 1).value = "SUPERSEDED"
                n += 1
                print(f"  {name}:{rr} SUPERSEDED: {url}")
    print(f"RELEASED={n}")


def unpublish(wb):
    ws, arch = wb["Recalls"], wb["Rejected"]
    head = _head(ws)
    iu = head.index("URL")
    n = 0
    for url, why in UNPUBLISH.items():
        for r in range(ws.max_row, 1, -1):
            vals = [c.value for c in ws[r]]
            if _key(vals[iu]) == _key(url):
                _archive_row(arch, head, vals, why)
                ws.delete_rows(r, 1)
                n += 1
                print(f"  Recalls:{r} -> Rejected: {url}")
    if "Weekly_Review" in wb.sheetnames:
        wr = wb["Weekly_Review"]
        wh = _head(wr)
        if "URL" in wh:
            for r in range(wr.max_row, 1, -1):
                if _key(wr.cell(r, wh.index("URL") + 1).value) in {_key(u) for u in UNPUBLISH}:
                    wr.delete_rows(r, 1)
                    print(f"  Weekly_Review:{r} mirror row dropped")
    print(f"UNPUBLISHED={n}")


def sort(wb):
    ws = wb["Recalls"]
    head = _head(ws)
    idate = head.index("Date")
    rows = [[c.value for c in row] for row in ws.iter_rows(min_row=2)]
    keyed = sorted(range(len(rows)), key=lambda i: (str(rows[i][idate] or "")[:10], -i), reverse=True)
    if keyed == list(range(len(rows))):
        print("SORT: already newest-first")
        return
    new = [rows[i] for i in keyed]
    for ri, vals in enumerate(new, start=2):
        for ci, v in enumerate(vals, start=1):
            ws.cell(ri, ci).value = v
    print(f"SORT: {sum(1 for a, b in zip(keyed, range(len(rows))) if a != b)} rows moved")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", required=True,
                    choices=("reject", "release", "unpublish", "sort"))
    a = ap.parse_args()
    wb = openpyxl.load_workbook(XLSX)
    globals()[a.phase](wb)
    wb.save(XLSX)
    return 0




if __name__ == "__main__":
    raise SystemExit(main())
