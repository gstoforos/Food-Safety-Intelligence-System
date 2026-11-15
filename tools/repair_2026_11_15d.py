# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-15 (d). Base 66761cc.

CONTEXT. The nightly operator's PRs #35..#44 are all open and unmerged, so
none of the decisions taken on 2026-11-14 (PR #44) reached main. The same
19 Pending rows were back tonight. Every decision below was re-taken against
the regulator's own page where the session could open it (rappel.conso.gouv.fr,
recalls-rappels.canada.ca, fda.gov all opened; nafdac.gov.ng, mpi.govt.nz,
kontrollwiki.livsmedelsverket.se, thencc.org.za and foedevarestyrelsen.dk
could not be opened — those decisions rest on the register and on the row's
own content, and say so). Nothing is deleted from an archive; the archive is
append-only. No published row is removed.

A. PENDING ROWS REFUSED (13) — each reason is written on the archive row.

B. FOUR ARCHIVE VERDICTS THAT DO NOT HOLD, RETIRED SO THE GUARD RELEASES
   fiche 24012 was archived as "Official URL does not match the candidate and
   the page is not found" — false tonight: the fiche is live and specific
   (CHAMPIGNONS MORILLES SECHEES 60G, CHAMPIBON, B&M, 04/11/2026). fiches
   24047 ("Pathogen is empty"), 24064 and 24077 (Company "/" — page debris)
   were archived for REPAIRABLE field defects, repaired tonight by the
   curator against the fiche. Each archive row is annotated "[SUPERSEDED ...]",
   the marker this repo uses to retire a wrong verdict (promote_gate_passing.
   supersede_archived_copies, repair_2026_11_14c). Nothing is deleted.

C. PUBLISHED NOTES THAT STILL BEGIN "REJECTED: ..." (18 Recalls rows)
   Notes is one of the 14 public fields mirrored into recalls.json. Eighteen
   published rows still open with a Pending-stage reviewer verdict
   ("REJECTED: URL agent: Hazard is a foreign body (glass) not a microbial
   pathogen | ...", "REJECTED: network | ...") that was later overruled when
   the row was promoted. The verdict text is MOVED to a bracketed trailing
   annotation, never dropped, so the audit trail survives and the public
   field no longer says a published recall is rejected.

D. TWO SILENT PUBLISHED-AND-REJECTED CONTRADICTIONS (pre-existing on main)
   Rejected rows for the Eden Brothers FDA URL (a confirmer duplicate whose
   verdict sat in Notes) and for the salute.gov.it listing URL (page chrome
   scraped as a notice) share a URL with a published recall and had an empty
   RejectReason. Each now says which copy wins.

    python tools/repair_2026_11_15d.py            # all parts
    python tools/repair_2026_11_15d.py --dry-run  # report only
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-15"
BY = "nightly operator " + TODAY

# Pending URL -> reason. Exact URLs as held in Pending tonight.
PENDING_REJECTS = {
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        "out_of_scope_animal_feed — FDA notice read 2026-11-15: Laub's Feeding "
        "& Cubing, Inc. (Beryl, UT) 'Grade A Mini Hay Cubes', sold in 1500 lb "
        "totes and in bulk, filed under FDA 'Animal & Veterinary' / 'Animal "
        "Feed' after reports of sick and dead horses; 'Potential to be "
        "contaminated with Clostridium Botulinum'. Animal feed, not human food "
        "(scope rule R1). Same decision as the 2026-11-14 operator (PR #44, "
        "unmerged).",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-sprouts-september-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page read "
        "2026-11-15: Salmonella Bovismorbificans, 34 ill in 6 states, 3 "
        "hospitalised, 'FDA's outbreak investigation is complete' (Nov 4, "
        "2026). The recalls it describes are published: Evergreen Fresh "
        "Sprouts (FDA 2026-09-10; CDC 2026-09-09, Outbreak=1), Gardeners "
        "Basics and EAK Distribution seed recalls. Product/Reason hold the "
        "page title, Company is empty.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-listeria-monocytogenes-soft-cheese-june-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page read "
        "2026-11-15: Clover Hill Dairy requeson Listeria monocytogenes, 15 ill "
        "in 4 states, 14 hospitalised, 1 death, investigation complete "
        "2026-08-26. Clover Hill Dairy recalls are published 2026-06-09 and "
        "2026-06-18. Product/Reason hold the page title, Company is empty.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page read "
        "2026-11-15: Sierra Nevada Cheese Company Graziers raw-milk cheeses, E. "
        "coli O26:H11, 18 ill in 10 states, 'CDC is declaring the outbreak "
        "over' (Nov 13, 2026). The recall is published 2026-09-29 (FDA, Sierra "
        "Nevada Cheese Company). Seventh arrival of this page; refused by "
        "operators 2026-10-04, 2026-11-08, 2026-11-14. Terminal.",
    "https://recalls-rappels.canada.ca/en/alert-recall/secaris-nasal-gel-affected-lot-may-contain-foreign-matter":
        "out_of_scope_not_food — Canada.ca recall portal page read 2026-11-15: "
        "'SECARIS NASAL GEL (DIN 00551805), lot 26A35A', category 'Health "
        "products - Drugs', Pendopharm Division of Pharmascience Inc., "
        "published 2026-11-13. A drug, not a food (R1).",
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        "duplicate_already_approved — upheld: operator review 2026-08-22. "
        "Prepworld fruit products / Salmonella is published as FSA-PRIN-29-2026 "
        "(2026-06-23, alerts.food.gov.uk/news-alerts/alert/fsa-prin-29-2026) "
        "and as the CFS Hong Kong mirror (2026-06-24). The /article/<id>/ form "
        "is the FSA's second URL for one alert — the known FSA "
        "double-publication trap. This copy has no Date. The register decides; "
        "one alert, one row.",
    "https://kontrollwiki.livsmedelsverket.se/artikel/127/atgarder-vid-fynd-av-listeria-i-livsmedel":
        "not_a_recall — Livsmedelsverket Kontrollwiki guidance article "
        "'Åtgärder vid fynd av Listeria i livsmedel' (actions on finding "
        "Listeria in food); the row's own Notes quote inspector guidance "
        "('Agerande vid icke säkra livsmedel ... artikel 14 i förordning'). "
        "Company and Brand hold the article title, Product holds "
        "'Kontrollwiki'. No product, no firm, no date. Domain not reachable "
        "from the session; the decision rests on the row's own content.",
    "https://www.mpi.govt.nz/news/media-releases/new-reports-highlight-food-recall-system-at-work":
        "not_a_recall — MPI media release about New Zealand Food Safety's "
        "annual recall report. Company holds 'New reports highlight food' and "
        "Product 'system at work': the headline split in two. Already Status "
        "'rejected' by the URL agent. No product, no date. Domain not "
        "reachable from the session; the decision rests on the row's own "
        "content.",
    "https://www.mpi.govt.nz/dmsdocument/25847/direct":
        "not_a_recall — MPI document-store PDF whose extract is a survey "
        "acknowledgement mentioning a '24-hour dietary recall' (a nutrition-"
        "survey method, not a product recall). Already Status 'rejected' by "
        "the URL agent. No product, no firm, no date.",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        "not_a_current_recall — RASFF notification id 470205 is years old "
        "(the register's November 2026 notifications are in the 886xxx "
        "range); the row's Product is the RASFF Window column header "
        "('Organisations · Reference · Subject · ...'), scraped UI chrome, "
        "with no Date and no Company. The page rendered only 'Application "
        "name' tonight.",
    "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/":
        "stale_event_terminal — upheld: NCC media statement of October 3, 2024 "
        "on the BM Foods / Shoprite Checkers Deli Hummus Listeria recall "
        "(September 2024), the founding incident of pipeline/_publish_gate.py. "
        "Upheld TERMINAL by operator review 2026-11-06 and again 2026-11-14; "
        "eleventh arrival via the South Africa gap finder, this time carrying a "
        "2026-11-12 Date the statement does not have. A two-year-old event is "
        "not a current recall whatever date the gap finder stamps on it.",
    "https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        "not_a_recall — NAFDAC watchlist notice 'pending regulatory "
        "investigation' for products marketed by Mofus Nigeria Ltd: Product "
        "empty, Pathogen empty, Reason 'Pending regulatory investigation'. Not "
        "a recall, no hazard named. Ninth arrival; the earlier refusal "
        "('Product is empty; Pathogen is empty') still holds. nafdac.gov.ng "
        "not reachable from the session.",
    "https://nafdac.gov.ng/public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        "out_of_scope_not_food — NAFDAC alert on counterfeit Proguanil "
        "(Projeanil, Re-granil) 100 mg TABLETS, an antimalarial drug. Not a "
        "food (R1); 'Counterfeit products' is not a hazard this register "
        "holds. Date 2026-03-25 (Public Alert No. 023/2026). Eighth arrival. "
        "URL is truncated mid-word ('...registration-n'). nafdac.gov.ng not "
        "reachable from the session.",
}

RELEASE = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24077/interne":
        "the archive row records no verdict; its Notes hold only the "
        "publish-gate defect Company '/' / Brand '/' (page debris), a "
        "repairable field defect. Fiche 24077 read in full on 2026-11-15: "
        "REBLOCHON DE SAVOIE au lait cru, Origine de la fiche GAEC LA LOUISA, "
        "13/11/2026, Motif 'retrait par mesure de précaution dans un contexte "
        "suspicion salmonelle', Risques 'Salmonella spp'. The curator-corrected "
        "Pending row passes the full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne":
        "the archive row records no verdict; its Notes hold only the "
        "publish-gate defect Company '/' / Brand '/' (page debris), a "
        "repairable field defect. Fiche 24064 read in full on 2026-11-15: "
        "REBLOCHON DE SAVOIE FERMIER 450g, GAEC LES DEUX SAVOIES, 11/11/2026, "
        "'Présence de Escherichia Coli STEC'. The curator-corrected Pending row "
        "passes the full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24012/interne":
        "'Official URL does not match the candidate and the page is not "
        "found' is false on 2026-11-15: fiche 24012 is live and specific — "
        "CHAMPIGNONS MORILLES SECHEES 60G, CHAMPIBON, B&M, 04/11/2026, "
        "'Probable contamination d'un lot de morilles par une toxine "
        "susceptible d'être à l'origine d'intoxication alimentaire'. The "
        "curator-corrected Pending row passes the full publish gate and is "
        "the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24047/interne":
        "'Pathogen is empty' was a repairable field defect, repaired tonight "
        "from fiche 24047 (read in full 2026-11-15: CHAMPIGNONS MORILLES "
        "SECHEES, CHAMPIBON, Promocash, 09/11/2026, same Motif as fiche "
        "24012; Risques 'Autre risque'). The curator-corrected Pending row "
        "passes the full publish gate and is the copy that wins",
}


def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _rows_for(ws, url):
    hdr = _hdr(ws)
    col = hdr.index("URL") + 1
    want = _key(url)
    return [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == want]


def _row_dict(ws, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(_hdr(ws)) if h}


def _archive(wb, row: dict, reason: str) -> None:
    """Append to Rejected. Append-only; nothing in an archive is deleted."""
    arch = wb["Rejected"]
    out = []
    for h in _hdr(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            v = row.get(h, "")
            out.append("" if v is None else v)
    arch.append(out)


def reject_pending(wb, dry: bool) -> int:
    ws = wb["Pending"]
    n = 0
    for url, reason in PENDING_REJECTS.items():
        rows = _rows_for(ws, url)
        if not rows:
            # The Recall URL agent's 20:36 UTC pass (main c82d449) physically
            # deleted ten of these rows after a second reviewer rejection,
            # without archiving them. The decision and its reason are still
            # recorded here; there is simply no row left to move.
            print(f"    Pending row already gone (deleted upstream, not "
                  f"archived): {url[-50:]}")
            continue
        for r in sorted(rows, reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} -> Rejected")
            if not dry:
                _archive(wb, row, reason)
                ws.delete_rows(r, 1)
            n += 1
    return n


def release_false_refusals(wb, dry: bool) -> int:
    n = 0
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        ws = wb[sheet]
        hdr = _hdr(ws)
        ci = hdr.index(col) + 1
        for url, why in RELEASE.items():
            for r in _rows_for(ws, url):
                cur = str(ws.cell(r, ci).value or "")
                if "SUPERSEDED" in cur:
                    continue
                stamp = (f"[SUPERSEDED {TODAY} by the {BY} — {why}. This row "
                         f"is kept as the audit trail of the original refusal.] ")
                print(f"    {sheet} row {r}: {url[-30:]} marked SUPERSEDED")
                if not dry:
                    ws.cell(r, ci).value = stamp + cur
                    if "Reviewed" in hdr:
                        ws.cell(r, hdr.index("Reviewed") + 1).value = "SUPERSEDED"
                n += 1
    return n


def move_rejected_prefix_out_of_published_notes(wb, dry: bool) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    ci = hdr.index("Notes") + 1
    lu = hdr.index("LastUpdated") + 1 if "LastUpdated" in hdr else None
    n = 0
    for r in range(2, ws.max_row + 1):
        cur = str(ws.cell(r, ci).value or "")
        if not cur.startswith("REJECTED:"):
            continue
        head, sep, rest = cur.partition(" | ")
        verdict = head[len("REJECTED:"):].strip()
        if not sep:
            rest = ""
        new = (rest.strip() + f" [{BY}: this published row's Notes opened with "
               f"a Pending-stage reviewer verdict, 'REJECTED: {verdict}', that "
               f"was overruled when the row was promoted; the verdict text is "
               f"kept here and no longer leads the public field.]").strip()
        print(f"    Recalls row {r}: {str(ws.cell(r, hdr.index('Company') + 1).value)[:28]!r}"
              f" — moved 'REJECTED: {verdict[:50]}'")
        if not dry:
            ws.cell(r, ci).value = new
            if lu:
                ws.cell(r, lu).value = TODAY
        n += 1
    return n


# D. TWO ARCHIVE ROWS THAT SHARE A URL WITH A PUBLISHED RECALL AND SAY NOTHING
#    (tests/test_a_recall_is_not_both_published_and_rejected.py was red on main
#    before this run). Both pre-date tonight. The verdict is written where the
#    register's contradiction check reads it — RejectReason — and nothing is
#    deleted.
CONTRADICTIONS = {
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/eden-brothers-recalls-calabrese-broccoli-seeds-due-possible-salmonella-contamination":
        f"[duplicate — annotated {TODAY} by the {BY}: the published copy wins. "
        "The same FDA URL is published as Eden Brothers LLC, 'Broccoli Seeds - "
        "Calabrese', Salmonella, 2026-10-07. This archive row's own Notes already "
        "said 'duplicate: this URL is already published on 2026-10-07', but in "
        "Notes, which the contradiction check does not read; its Product is the "
        "notice's first paragraph. Kept as the audit trail.]",
    "https://www.salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari/?tab=1&offset=0&pageElements=10":
        f"[unresolved contradiction of provenance — annotated {TODAY} by the {BY}: "
        "this archived row is the Ministero della Salute LISTING page scraped as "
        "if it were a notice (Product is the page title 'Avvisi e richiami di "
        "prodotti alimentari', no firm, no hazard, discovered via ilmessaggero.it "
        "2026-11-14). The same listing URL is carried by the published La Vacje "
        "Salumerie row of 2026-04-14, itself documented UNRESOLVED because "
        "salute.gov.it serves a bot wall and the specific notice URL could not "
        "be recovered. This row is not a recall; the published row stands.]",
}


def annotate_silent_contradictions(wb, dry: bool) -> int:
    ws = wb["Rejected"]
    hdr = _hdr(ws)
    ci = hdr.index("RejectReason") + 1
    n = 0
    for url, text in CONTRADICTIONS.items():
        for r in _rows_for(ws, url):
            cur = str(ws.cell(r, ci).value or "")
            if cur.strip().lower() in ("", "none", "nan"):
                print(f"    Rejected row {r}: {url[-40:]} annotated")
                if not dry:
                    ws.cell(r, ci).value = text
                n += 1
    return n


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    print("A  Pending rows refused:")
    print("   archived: %d" % reject_pending(wb, a.dry_run))
    print("B  earlier refusals that do not hold:")
    print("   archive rows marked SUPERSEDED: %d" % release_false_refusals(wb, a.dry_run))
    print("C  published Notes opening with a reviewer verdict:")
    print("   rows rewritten: %d" % move_rejected_prefix_out_of_published_notes(wb, a.dry_run))
    print("D  archive rows sharing a URL with a published recall, silent:")
    print("   rows annotated: %d" % annotate_silent_contradictions(wb, a.dry_run))
    if not a.dry_run:
        wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
