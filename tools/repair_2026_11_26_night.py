# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-26. Base: main at 50eca06 (21:11 Athens).

Does only what the curator cannot; never deletes a row from the register
(rows MOVE from Pending/Recalls to Rejected with a written reason).

    python tools/repair_2026_11_26_night.py --phase reject     # Pending -> Rejected
    python tools/repair_2026_11_26_night.py --phase unpublish  # Recalls -> Rejected
    python -m pipeline.promote_gate_passing --apply
    python tools/repair_2026_11_26_night.py --phase sort       # Recalls newest-first
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-26"
BY = "nightly operator review 2026-11-26"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"

FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
FSIS = "https://www.fsis.usda.gov/recalls-alerts/"

SAME = "Same decision as unmerged PR #55 (and #52-#54), re-applied because that PR never reached main."
PENDING_REJECTS = {
    "https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026":
        f"stale / duplicate_of_published_recall — {BY}: FDA page read tonight: 'Content "
        "current as of 10/02/2026', status 'Ended', 'FDA has concluded its outbreak "
        "investigation'. The Taylor Fresh Foods / Taylor Farms de Mexico iceberg lettuce "
        f"recall is already published (FDA, 2026-07-18). Row Date is the scrape day. {SAME}",
    FSIS + "fsis-issues-public-health-alert-meat-and-poultry-products-containing-biscuits":
        f"duplicate_of_published_recall — {BY}: the row's own text is the FSIS public "
        "health alert of 'Nov. 14, 2026' for meat and poultry products containing "
        "FDA-regulated biscuits (Listeria), already published as USDA FSIS 2026-11-14 "
        "(C.H. Guenther & Son biscuits). Product is scraper text ('5 days ago · "
        f"WASHINGTON, Nov'). fsis.usda.gov does not resolve from this session. {SAME}",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit MEDICINE (Proguanil tablets), not a food; "
        f"archived eight times before. {SAME}",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: medicines-safety alert (gabapentinoids), not a food. {SAME}",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"out of scope — {BY}: RASFF 2021.1402, aflatoxin in 'sunflower seeds for bird "
        f"feeding' — animal FEED, from 2021. Archived twice before. {SAME}",
    FDA + "/green-lumber-holdings-llc-issues-recall-and-consumer-safety-alert":
        f"out of scope — {BY}: FDA page read tonight: 'Green Lumber Natural Fuel For "
        "Men', dietary-supplement capsules, FDA product type 'Drugs', reason "
        f"'Undeclared Tadalafil'. A drug adulterant, not a food-safety hazard. {SAME}",
    FDA + "?search_api_fulltext=H-0198-2027":
        f"search_query_url / downstream of published event — {BY}: CONSTRUCTED FDA "
        "site-search URL, not a notice page (no published row uses that pattern; earlier "
        "rows with it were archived, H-1404..1409-2026). The openFDA entry (D & M Smoked "
        "Fish, Cold Smoked Sprats, C. botulinum) is the downstream recall of the Haifa / "
        "D&M Smoked Fish cold-smoked sprats event already published (FDA 2026-10-23 and "
        f"2026-10-27). Also in Weekly_Rejected (Confirmer). {SAME}",
    FDA + "?search_api_fulltext=H-0200-2027":
        f"search_query_url — {BY}: CONSTRUCTED FDA site-search URL, not a notice page "
        "(same pattern archived for H-1404..1409-2026); openFDA-only entry for Country "
        "Creek alfalfa sprout seeds 'containing seed recalled from the supplier'; no FDA "
        f"press release exists to cite. Also in Weekly_Rejected (Confirmer). {SAME}",
    "https://alerts.food.gov.uk/article/7000":
        f"duplicate_of_published_recall — {BY}: FSA /article/<id> is the alias of an "
        "alert; this one is Hotel Chocolat pistachio chocolate, Salmonella, already "
        "published from /news-alerts/alert/fsa-prin-54-2026 (2026-11-21). The known "
        f"FSA double-URL trap. {SAME}",
    "https://www.sfa.gov.sg/news-publications/newsroom/recall-of-glenisk-baby-organic-fromage-frais-strawberry-with-banana--vanilla---oats--4x60g--due-to-potential-mould-growth-and-spoilage":
        f"out of scope (quality/spoilage) — {BY}: the notice is a recall 'due to potential "
        "mould growth and spoilage' (its own URL slug); Product and Notes are the SFA "
        "site navigation menu, no Date, no Company; Status already 'rejected' and the URL "
        "is in Rejected. sfa.gov.sg does not resolve from this session.",
}
PENDING_REJECT_ONE_COPY = {
    ("https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-card-factory-candy-canes",
     "Candy Canes"):
        f"duplicate — {BY}: re-promotion guard honoured. This URL is already in Rejected "
        "('daily-global review 2026-11-22c: archived — DUPLICATE of the published FSA "
        "PRIN-53-2026 row', Card Factory candy canes, metal). Whether the Irish notice "
        f"should stand beside the UK one is a policy call for George. {SAME}",
}
NO_URL_PREFIX_REJECTS = {}
RELEASE = {}
UNPUBLISH = {
    FSIS + "wolverine-packing-co--recalls-ground-beef-products-due-possible-e--coli-o157h7?ref=foodsafetynews.com":
        f"stale — UN-PUBLISHED by the {BY}. FSIS recall 031-2024 of NOVEMBER 2024, not "
        "a 2026 recall: the row's own Reason reads '(Recall ID 031-2024)'; the release "
        "'FSIS Recall Release 031-2024' was carried by GlobeNewswire 2024-11-21 and NPR "
        "2024-11-22 (Wolverine Packing Co., ~167,277 lb ground beef, use-by 11/14/2024, "
        "15 cases in Minnesota). It was published 2026-11-25 as Date 2026-11-23 with "
        "Outbreak=1 after a foodsafetynews.com '?ref=' link was ingested, so it is "
        "inflating the 2026 W48 outbreak count. fsis.usda.gov does not resolve from this "
        "session. Second night: PR #55 made this rejection and did not reach main.",
    FSIS + "fsis-issues-public-health-alert-beef-kofta-products-served-kebab-shop-restaurant?ref=foodsafetynews.com":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. The same FSIS public "
        "health alert (Olympia Food Industries beef kofta, STEC O157:H7, 'Recall ID "
        "PHA-05242026-01' = 24 May 2026) is already published at the same URL without the "
        "'?ref=foodsafetynews.com' suffix (USDA FSIS, Date 2026-05-24). The query string "
        "defeated the URL dedup and the copy was re-dated 2026-11-20. Second night: PR #55.",
}
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
