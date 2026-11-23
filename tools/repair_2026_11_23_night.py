# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-23. Base: main at 2c8e3a9 (21:11 Athens).

Does only what the curator cannot; never deletes a row from the register.

    python tools/repair_2026_11_22_night.py --phase reject     # Pending -> Rejected
    python tools/repair_2026_11_22_night.py --phase release    # false verdicts SUPERSEDED
    python tools/repair_2026_11_22_night.py --phase unpublish  # Recalls -> Rejected
    python -m pipeline.promote_gate_passing --apply
    python tools/repair_2026_11_22_night.py --phase sort       # Recalls newest-first
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-23"
BY = "nightly operator review 2026-11-23"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"

PENDING_REJECTS = {
    "https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026":
        f"stale / duplicate_of_published_recall — {BY}: the row's own title is "
        "'Multistate Outbreak of Cyclospora Illnesses: Iceberg Lettuce July "
        "2026' — the July outbreak whose recall is published (FDA, 2026-07-18, "
        "Taylor Fresh Foods iceberg lettuce from central Mexico, Cyclospora). "
        "The FDA page read on 2026-11-22 said 'Content current as of "
        "10/02/2026', status 'Ended'; fda.gov returned 401 tonight. The Date "
        "2026-11-22 on the row is the scrape day. Row has no Company. Same "
        "decision as unmerged PR #52.",
    "https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-health-alert-meat-and-poultry-products-containing-biscuits":
        f"duplicate_of_published_recall — {BY}: the row's own text is the FSIS "
        "public health alert of 'Nov. 14, 2026' for products 'containing "
        "FDA-regulated biscuits that may be contaminated with Listeria "
        "monocytogenes'. That PHA is published (USDA FSIS, 2026-11-14, "
        "…/fsis-issues-public-health-alert-ready-eat-meat-products-containing-"
        "biscuits-recalled). No Company; Product is scraper text ('3 days ago "
        "· WASHINGTON, Nov'). Same decision as unmerged PR #52.",
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-formaldehydu-z-kubka-z-melaminy":
        f"out of scope — {BY}: a FOOD-CONTACT ARTICLE (formaldehyde migration "
        "from a melamine cup), not a food. Same verdict as unmerged PRs #48–#52.",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit MEDICINE (Proguanil tablets), not a "
        "food. Same verdict as unmerged PRs #50–#52.",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: a medicines-safety alert (gabapentinoids), not a "
        "food. Same verdict as unmerged PRs #50–#52.",
    # ── re-ingested by the Tavily gap-finder at 20:11 UTC; each was rejected before ──
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: re-ingestion. Already rejected "
        "twice; the archived reason stands: 'O'Brien Fine Foods ham, Listeria. "
        "Already in Recalls as FSA-PRIN-32-2026 dated 2026-07-01.' This copy has "
        "no Date. FSA serves the alert at both /article/<id> and "
        "/news-alerts/alert/<ref>.",
    "https://alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: re-ingestion. Already rejected "
        "twice; archived reason: a third address for FSA-PRIN-38-2026, whose "
        "published copy wins. This copy has no Date.",
    "https://favv-afsca.be/nl/producten/terugroeping-van-het-favv-98":
        f"not a single recall notice — {BY}: re-ingestion. Archived reason "
        "stands: FAVV/AFSCA listing page; Company empty, Product is the Dutch "
        "page body, no Date.",
    "https://www.mpi.govt.nz/news/media-releases/new-reports-highlight-food-recall-system-at-work":
        f"not a recall — {BY}: re-ingestion of an MPI media release about a "
        "report ('A new report by New Zealand Food Safety identifies "
        "Salmonella in imported sesame seed-based products…'); Company and "
        "Product are fragments of the headline; no Date. Rejected before.",
    "https://www.mhlw.go.jp/content/001241427.pdf":
        f"out of scope — {BY}: re-ingestion. MHLW notice on Kobayashi "
        "Pharmaceutical DIETARY SUPPLEMENTS ('Dietary supplements … produced "
        "and sold by Kobayashi Pharmaceutical are under recall'); no Date, no "
        "Company. Rejected before.",
}
NO_URL_PREFIX_REJECTS = {
    ("MFDS", "Ministry of Food and Drug Safety>Information>News&Notice"):
        f"not a recall — {BY}: seasonal MFDS consumer advisory 'Urges Caution "
        "Against Norovirus Food Poisoning During Winter'; no product, no firm, "
        "and the URL-guardian blanked the URL. Same verdict as unmerged PR #52.",
}

RELEASE = {
    RC % "24127": "the verdict 'URL is a dataset page, not an official recall "
        "page' is false: fiche 24127 re-read tonight is the specific notice "
        "'Pain brioché x32 400g', GTIN 3251517091006, lot 6260, origin "
        "COOPERATIVE U ENSEIGNE U ENSEIGNE; curator fetch-and-match passed tonight",
    RC % "24089": "the fiche re-read tonight is the specific notice for the "
        "Bûche/Bûchette pralinée (GTIN 2000000324418, 2000000278902), origin "
        "COOPERATIVE U ENSEIGNE U ENSEIGNE; curator fetch-and-match passed tonight",
    RC % "24109": "the verdict 'Pathogen is empty' is repaired (Chemical "
        "contaminant, fiche category 'Autres contaminants chimiques'); fiche "
        "24109 re-read tonight is the Paysan Breton fromage blanc 5 kg notice "
        "(GTIN 3412290026917, EVEN LAIT INDUSTRIE); curator passed",
    RC % "24097": "the verdict 'Official URL is a listing page' is false: fiche "
        "24097 re-read tonight is the Belle France fromage blanc notice (4 GTINs, "
        "EVEN LAIT INDUSTRIE); curator passed",
}

UNPUBLISH = {
    "https://www.aesan.gob.es/alertas/2026_86":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. The row's "
        "own captured title is 'Ampliación de información sobre la alerta por "
        "posible presencia de Salmonella Enteritidis en huevos procedentes de "
        "España. (Ref. ES2026/711)' — an extension of alert ES2026/711, which "
        "is already published from aesan.gob.es/alertas/2026_87 (Eroski "
        "free-range eggs, Salmonella, 2026-11-20; its title carries the same "
        "'(Ref. ES2026/711)'). Rejected as a duplicate by unmerged PR #52; the "
        "rejection never reached main, so the pipeline promoted it today. "
        "aesan.gob.es does not resolve from the session.",
    FDA + "/fresh-ready-foods-llc-expands-recall-include-fresh-you-brand-sausage-egg-cheese-biscuit-sandwich-and":
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. Read on the "
        "FDA page on 2026-11-22: announcement 'November 17, 2026' expands 'its "
        "November 8th recall of select Fresh To You Sausage, Egg & Cheese "
        "Biscuit Sandwiches'. Both products it adds are already published: FDA "
        "2026-11-08 (same sandwich, same firm) and USDA FSIS 2026-11-14 "
        "(Biscuit & Sausage Gravy, 9.5 oz). Same decision as unmerged PRs "
        "#51/#52, which never reached main.",
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
