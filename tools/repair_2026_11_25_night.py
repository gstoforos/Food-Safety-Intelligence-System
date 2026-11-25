# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-25. Base: main at e3cbff1 (21:11 Athens).

Does only what the curator cannot; never deletes a row from the register
(rows MOVE from Pending/Recalls to Rejected with a written reason).

    python tools/repair_2026_11_24_night.py --phase reject     # Pending -> Rejected
    python tools/repair_2026_11_24_night.py --phase unpublish  # Recalls -> Rejected
    python tools/repair_2026_11_24_night.py --phase release    # false verdicts SUPERSEDED
    python -m pipeline.promote_gate_passing --apply
    python tools/repair_2026_11_24_night.py --phase sort       # Recalls newest-first
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-25"
BY = "nightly operator review 2026-11-25"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"

FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
FSIS = "https://www.fsis.usda.gov/recalls-alerts/"
SAME = "Same decision as unmerged PR #54 (and #52/#53), re-applied because that PR never reached main."

PENDING_REJECTS = {
    # ── re-applied from unmerged PR #54 (rows still in Pending on main) ──
    "https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026":
        f"stale / duplicate_of_published_recall — {BY}: FDA page 'Investigation "
        "of Multistate Outbreak of Cyclospora Illnesses: Iceberg Lettuce (July "
        "2026)', 'Content current as of 10/02/2026', status 'Ended' (read "
        "2026-11-24); the Taylor Farms de Mexico recall is already published "
        f"(FDA, 2026-07-18). Row Date is the scrape day. {SAME}",
    FSIS + "fsis-issues-public-health-alert-meat-and-poultry-products-containing-biscuits":
        f"duplicate_of_published_recall — {BY}: FSIS public health alert of "
        "Nov. 14, 2026 (biscuits, Listeria), already published (USDA FSIS, "
        "2026-11-14). Product is scraper text ('5 days ago · WASHINGTON, Nov'). "
        f"{SAME}",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit MEDICINE (Proguanil tablets), not a food. {SAME}",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: medicines-safety alert (gabapentinoids), not a food. {SAME}",
    "https://www.mpi.govt.nz/news/media-releases/tip-top-6-pack-jelly-tip-ice-creams-recalled-due-to-possible-presence-of-bacteria":
        f"re-ingestion of a URL archived twice before — {BY}: Product is a "
        "truncated snippet ('due to possible ...'), Company is the product name, "
        "no Date, Outbreak flag with no source support. mpi.govt.nz does not "
        "resolve from this session. The recall itself (Tip Top Jelly Tip "
        "6-pack, Bacillus cereus, ~2026-10-02) is still missing from Recalls and "
        f"needs a human re-add from the MPI recall page. {SAME}",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"out of scope — {BY}: RASFF 2021.1402, aflatoxin in 'sunflower seeds "
        f"for bird' — animal FEED, from 2021. Archived before. {SAME}",
    "https://www.mhlw.go.jp/content/001241427.pdf":
        f"out of scope — {BY}: MHLW notice on Kobayashi Pharmaceutical DIETARY "
        f"SUPPLEMENTS; no Date, no Company. Archived before. {SAME}",
    # ── new tonight ──
    "https://www.fsis.usda.gov/food-safety/alerts":
        f"not a single recall notice — {BY}: the FSIS 'alerts' LISTING page; "
        "Company 'Active', Product 'Food Safety and Inspection Service'. "
        "Ingested by the DDGS gap-finder 2026-11-25T00:11Z.",
    "https://alerts.food.gov.uk/article/7000":
        f"duplicate_of_published_recall — {BY}: FSA /article/7000 is the "
        "article alias of FSA-PRIN-54-2026 (Hotel Chocolat, Salmonella), "
        "published from /news-alerts/alert/fsa-prin-54-2026 (2026-11-21). The "
        "slugged alias was already removed from Recalls by the outbreak check on "
        "2026-11-22 for the same reason. alerts.food.gov.uk returned 403 tonight.",
    FDA + "/green-lumber-holdings-llc-issues-recall-and-consumer-safety-alert":
        f"out of scope — {BY}: FDA page read tonight: 'Green Lumber Natural Fuel "
        "For Men', lot GL01, 'dietary supplement capsules', FDA product type "
        "'Drugs', reason undeclared tadalafil. A drug-adulterated supplement, "
        "not a food-safety hazard in human food.",
    FDA + "?search_api_fulltext=H-0198-2027":
        f"search_query_url / downstream of published event — {BY}: the URL is a "
        "CONSTRUCTED FDA site-search query, not a notice page; no published row "
        "uses that pattern and ~40 earlier rows with it were archived "
        "(H-1404..1409-2026 etc.). openFDA confirms the entry itself: "
        "'H-0198-2027', 'D & M Smoked Fish, Inc.', 'Cold Smoked Sprats, 5 lbs.', "
        "'May be contaminated with Clostridium botulinum', initiated 20261026, "
        "Class I, NY/NJ/PA — a downstream recall of the Haifa Smoked Fish cold "
        "smoked sprats event already published (FDA, 2026-10-23 / 2026-10-27).",
    FDA + "?search_api_fulltext=H-0200-2027":
        f"search_query_url / downstream of published event — {BY}: CONSTRUCTED "
        "FDA site-search URL, not a notice page (same pattern archived for "
        "H-1404..1409-2026). openFDA confirms: 'H-0200-2027', 'Country Creek "
        "LLC', 'Country Creek Acres Alfalfa Sprout Seeds', 'Products contain seed "
        "recalled from the supplier and may be contaminated with E. coli and/or "
        "Salmonella', initiated 20261023, Class I — a downstream seed recall of "
        "the alfalfa-seed event already in Recalls. No FDA press release exists "
        "to cite.",
    RC % "24179":
        f"duplicate_of_published_recall — {BY}: fiche 24179 read tonight "
        "(réf. 2026-11-0185, 25/11/2026): 'Les coquillages récoltés dans l'étang "
        "de Thau à partir du 16 novembre 2026', 'Biotoxines marines PSP (toxines "
        "paralysantes)', declarant COMITE REGIONAL DE LA CONCHYLICULTURE DE LA "
        "MEDITERRANEE. The same harvest block (same lagoon, same 16 Nov start, "
        "same declarant, same hazard) is already published as fiche 24121 (réf. "
        "2026-11-0130, 19/11/2026, 'blocage des coquillages récoltés dans "
        "l'étang de Thau à partir du 16 novembre 2026', 'Biotoxines PSP par "
        "mesure de précaution'). 24179 is the confirmation of that block, not a "
        "new recall.",
}
PENDING_REJECT_ONE_COPY = {
    ("https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-card-factory-candy-canes",
     "Card Factory Candy CanesRecall of a batch"):
        f"duplicate — {BY}: malformed copy (doubled scraper string as Product, "
        "no Company) of the FSAI Card Factory candy canes alert whose clean copy "
        "is already in Rejected (2026-11-22c, 'DUPLICATE of the published FSA "
        "PRIN-53-2026'). Re-promotion guard honoured; whether the Irish notice "
        f"should stand beside the UK one is a policy call for a human. {SAME}",
}
NO_URL_PREFIX_REJECTS = {}
RELEASE = {}

UNPUBLISH = {
    "https://www.aesan.gob.es/alertas/2026_86":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. The row's "
        "own captured title is 'Ampliación de información sobre la alerta por "
        "posible presencia de Salmonella Enteritidis en huevos procedentes de "
        "España. (Ref. ES2026/711)' — an extension of alert ES2026/711, already "
        "published from aesan.gob.es/alertas/2026_87 (same company Eroski, same "
        "eggs, same Date 2026-11-20, same reference). Fourth night: PRs #52–#54 "
        "made this rejection and none reached main.",
    RC % "24152":
        f"duplicate_of_published_recall / fiche withdrawn — UN-PUBLISHED by the "
        f"{BY}. Re-read tonight: /fiche-rappel/24152/interne returns the "
        "'Produits rappelés' LISTING page (fiches 24165–24174), not a fiche. The "
        "same recall is live as fiche 24159 (Top Budget grated Emmental 200 g, "
        "GTIN 3410280141671, glass, FROMAGERIES TERRES D'OR), published and kept. "
        f"{SAME}",
    FSIS + "wolverine-packing-co--recalls-ground-beef-products-due-possible-e--coli-o157h7?ref=foodsafetynews.com":
        f"stale — UN-PUBLISHED by the {BY}. This is FSIS recall 031-2024 of "
        "NOVEMBER 2024, not a 2026 recall: the row's own Reason/Notes carry "
        "'(Recall ID 031-2024)', and the release is 'FSIS Recall Release 031-2024 "
        "- E. coli O157' (GlobeNewswire 2024-11-21; NPR 2024-11-22): Wolverine "
        "Packing Co., ~167,277 lb ground beef, use-by 11/14/2024, 15 cases in "
        "Minnesota. Published on 2026-11-25 as Date 2026-11-23 with Outbreak=1 "
        "after a foodsafetynews.com link (?ref=) was ingested. fsis.usda.gov does "
        "not resolve from this session.",
    FSIS + "fsis-issues-public-health-alert-beef-kofta-products-served-kebab-shop-restaurant?ref=foodsafetynews.com":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. The same "
        "FSIS public health alert (Olympia Food Industries beef kofta, E. coli "
        "O157:H7, 'Recall ID PHA-05242026-01' = 24 May 2026) is already "
        "published at the same URL without the '?ref=foodsafetynews.com' "
        "suffix, Date 2026-05-24. The query string defeated the URL dedup and "
        "the row was re-dated 2026-11-20.",
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
                    choices=("reject", "release", "unpublish", "sort", "holdback"))
    a = ap.parse_args()
    wb = openpyxl.load_workbook(XLSX)
    globals()[a.phase](wb)
    wb.save(XLSX)
    return 0



# ── phase "holdback": a promoted row that would publish a false Outbreak=1 ──
HOLDBACK = {
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/prime-food-processing-llc-issues-safety-warning-regarding-uneviscerated-dried-herring-fish-0":
        "[nightly operator 2026-11-25: HELD IN PENDING — corrected and gate-passing, "
        "but it carries Outbreak=1 from the extractor while FDA says 'At this time, "
        "no illnesses or adverse reactions associated with the affected product have "
        "been reported.' The operator may not write Outbreak; a human must set "
        "Outbreak=0 before this row is published.]",
}


def holdback(wb):
    rs, ps = wb["Recalls"], wb["Pending"]
    rh, ph = _head(rs), _head(ps)
    for url, note in HOLDBACK.items():
        for r in range(rs.max_row, 1, -1):
            vals = dict(zip(rh, [c.value for c in rs[r]]))
            if _key(vals["URL"]) != _key(url):
                continue
            vals["Notes"] = (str(vals.get("Notes") or "") + " " + note).strip()
            vals["Status"] = "pending"
            vals["ScrapedAt"] = vals.get("DateAdded")
            ps.append([vals.get(h) for h in ph])
            rs.delete_rows(r, 1)
            print(f"  Recalls:{r} -> Pending (held): {url}")
        if "Weekly_Review" in wb.sheetnames:
            wr = wb["Weekly_Review"]
            wh = _head(wr)
            if "URL" in wh:
                for r in range(wr.max_row, 1, -1):
                    if _key(wr.cell(r, wh.index("URL") + 1).value) == _key(url):
                        wr.delete_rows(r, 1)
                        print(f"  Weekly_Review:{r} mirror row dropped")


if __name__ == "__main__":
    raise SystemExit(main())
