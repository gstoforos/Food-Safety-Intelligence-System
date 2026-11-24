# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-24. Base: main at 7115586 (21:11 Athens).

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
TODAY = "2026-11-24"
BY = "nightly operator review 2026-11-24"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"

PENDING_REJECTS = {
    "https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026":
        f"stale / duplicate_of_published_recall — {BY}: FDA page read tonight: "
        "'Investigation of Multistate Outbreak of Cyclospora Illnesses: Iceberg "
        "Lettuce (July 2026)', 'Content current as of 10/02/2026', status "
        "'Ended'; recall by Taylor Farms de Mexico on July 17, 2026, which is "
        "already published (FDA, 2026-07-18). The row's Date 2026-11-22 is the "
        "scrape day. Same decision as unmerged PRs #52/#53.",
    "https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-health-alert-meat-and-poultry-products-containing-biscuits":
        f"duplicate_of_published_recall — {BY}: the row's own text is the FSIS "
        "public health alert of 'Nov. 14, 2026' for products containing "
        "FDA-regulated biscuits (Listeria), already published (USDA FSIS, "
        "2026-11-14). Product is scraper text ('3 days ago · WASHINGTON, Nov'). "
        "Same decision as unmerged PRs #52/#53.",
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-formaldehydu-z-kubka-z-melaminy":
        f"out of scope — {BY}: a FOOD-CONTACT ARTICLE (formaldehyde migration "
        "from a melamine cup, per the row's own title), not a food. Same "
        "verdict as unmerged PRs #48–#53.",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit MEDICINE (Proguanil tablets), not a "
        "food. Same verdict as unmerged PRs #50–#53.",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: a medicines-safety alert (gabapentinoids), not a "
        "food; the review agent also rejected it on 2026-11-24. Same verdict as "
        "unmerged PRs #50–#53.",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: re-ingestion through the FSA "
        "/article/<id> alias. Already rejected before; archived reason stands: "
        "'Already in Recalls as FSA-PRIN-32-2026 dated 2026-07-01'. No Date.",
    "https://alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: re-ingestion through the FSA "
        "/article/<id> alias of FSA-PRIN-38-2026, whose published copy wins. "
        "Rejected before. No Date.",
    "https://favv-afsca.be/nl/producten/terugroeping-van-het-favv-98":
        f"not a single recall notice — {BY}: re-ingestion of a FAVV/AFSCA "
        "listing page; Company empty, Product is the Dutch page body, no Date. "
        "Rejected before.",
    "https://www.mpi.govt.nz/news/media-releases/new-reports-highlight-food-recall-system-at-work":
        f"not a recall — {BY}: re-ingestion of an MPI media release about a "
        "report; Company and Product are headline fragments; no Date. "
        "Rejected before.",
    "https://www.mhlw.go.jp/content/001241427.pdf":
        f"out of scope — {BY}: re-ingestion of the MHLW notice on Kobayashi "
        "Pharmaceutical DIETARY SUPPLEMENTS; no Date, no Company. Rejected "
        "before.",
    # ── re-ingested by the Tavily gap-finder at 20:11 UTC on 2026-11-24 ──
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/babyvoeding-en-andere-speciale-voeding":
        f"not a single recall notice — {BY}: re-ingestion of the NVWA TOPIC "
        "page 'Babyvoeding en andere speciale voeding' (/onderwerpen/…); "
        "Product is page debris ('24-03-2026 13:00 Beeld: © PureBudget Lees "
        "verder'), no Company, no Date. Archived before (Rejected, NVWA "
        "babyvoeding, 'Product looks like a headline … Date is empty').",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"out of scope — {BY}: re-ingestion of RASFF notification 2021.1402, "
        "aflatoxin in 'sunflower seeds for bird' — animal FEED, and from 2021. "
        "No Date, no Company. Archived before.",
    "https://www.mpi.govt.nz/news/media-releases/tip-top-6-pack-jelly-tip-ice-creams-recalled-due-to-possible-presence-of-bacteria":
        f"re-ingestion of a URL archived twice before — {BY}: Product is a "
        "truncated search snippet ('due to possible ...'), Company is the "
        "product name, no Date, and Reason claims an outbreak no source on the "
        "row supports. The same URL is in Rejected (2026-10-02, 'Date is "
        "empty'; then the re-promotion guard). mpi.govt.nz does not resolve "
        "from this session. NOTE FOR A HUMAN: the Tip Top Jelly Tip 6-pack "
        "Bacillus cereus recall itself (MPI, ~2026-10-02) is NOT in Recalls "
        "under any URL — it was lost to repairable defects and should be "
        "re-added from the MPI recall page with its real Date.",
}
# Two Pending rows carry the SAME FSAI URL; only the malformed copy goes.
PENDING_REJECT_ONE_COPY = {
    ("https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-card-factory-candy-canes",
     "Card Factory Candy CanesRecall of a batch"):
        f"duplicate_within_pending — {BY}: second copy of the FSAI Card Factory "
        "candy canes alert, same URL as the Pending row with Company 'Card "
        "Factory' / Product 'Candy Canes'. This copy's Product is a doubled "
        "scraper string ('Card Factory Candy CanesRecall of a batch of Candy "
        "Canes') and it has no Company. The clean copy stays in Pending "
        "(fsai.ie could not be read from this session).",
}
NO_URL_PREFIX_REJECTS = {}

RELEASE = {
    RC % "24097": "the verdict 'Official URL is a listing page' is false: fiche "
        "24097 re-read tonight is the specific notice 'Fromage blanc', Belle "
        "France, réf. 2026-11-0107, GTIN 3258561419695/3258561419701/"
        "3258561419718/3258561419725, origine EVEN LAIT INDUSTRIE, risque "
        "'Autres contaminants chimiques'; curator fetch-and-match passed tonight",
}

UNPUBLISH = {
    "https://www.aesan.gob.es/alertas/2026_86":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. The row's "
        "own captured title is 'Ampliación de información sobre la alerta por "
        "posible presencia de Salmonella Enteritidis en huevos procedentes de "
        "España. (Ref. ES2026/711)' — an extension of alert ES2026/711, which "
        "is already published from aesan.gob.es/alertas/2026_87 (captured "
        "title 'Alerta por posible presencia de Salmonella Enteritidis en "
        "huevos procedentes de España. (Ref. ES2026/711)'). Same company "
        "(Eroski), same product (free-range eggs), same Date (2026-11-20), "
        "same alert reference. Third night running: unmerged PRs #52 and #53 "
        "rejected it and the rejection never reached main. aesan.gob.es does "
        "not resolve from the session.",
    RC % "24152":
        f"duplicate_of_published_recall / fiche withdrawn — UN-PUBLISHED by the "
        f"{BY}. Fiche 24152 no longer exists on RappelConso: both /interne and "
        "/Interne now return the 'Produits rappelés' listing, which carries "
        "fiches 24146–24162 but not 24152. The same recall is live as fiche "
        "24159 (réf. 2026-11-0166, 24/11/2026): 'Emmental Râpé Top Budget "
        "200g', GTIN 3410280141671, 'Code lot commençant par 11 323', 'Risque "
        "de corps étranger de type verre', origine 'FROMAGERIES TERRES D'OR', "
        "distributeur INTERMARCHE — the same firm, brand, product, GTIN, lot "
        "and hazard this row carried (corrected by unmerged PR #53 from the "
        "fiche as it then read). The published copy at 24159 is kept and was "
        "translated tonight through the curator.",
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
