# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-14 (c). Base 308eff0.

Every decision below was taken against the regulator's own page where the
page could be opened from the operator's session, and against the register
itself where the question was a register question (duplicate, re-arrival of
a terminal rejection). Nothing is deleted from an archive; the archive is
append-only. One published row is REMOVED from Recalls and archived, and the
commit that carries this script says so in as many words
("deliberate row removal", tests/test_register_never_shrinks.py).

A. A PUBLISHED ROW THAT IS NOT A FOOD
   Recalls held BVL (DE) 2026-11-12, Company "Rossmann", Brand "Nuby",
   Product "Baby spoons", Pathogen "Bisphenol AF exceeding limit", URL
   .../261112_13_NI_Waermesensorloeffel/... — a heat-sensing baby SPOON,
   a food-contact article announced through lebensmittelwarnung.de's
   consumer-goods channel. Scope rule R1 is human FOOD; tableware and
   kitchen utensils have been refused before (RappelConso ceramic plates
   2026-08-21, GIS kitchen-utensil set 2026-11-01, NKFH electric shaver
   2026-11-06). The derived HazardGroup on the row read "pathogen-bacterial",
   which is what the schema does with a hazard it cannot place. The row is
   moved to Rejected. lebensmittelwarnung.de could not be opened from the
   operator's session; the decision rests on the row's own product name and
   the notice slug, both of which name a spoon, not on a hazard reading.

B. A PENDING ROW THE PROMOTER WOULD HAVE PUBLISHED AS FOOD
   pipeline.promote_gate_passing listed "Laub's Feeding & Cubing — Grade A
   Mini Hay Cubes" (FDA, Clostridium botulinum) as passing the FULL publish
   gate. The FDA notice, read today, files it under Animal & Veterinary:
   hay cubes sold in 1500 lb totes after reports of sick and dead horses.
   Animal feed is out of scope. Its only earlier refusal was "Date is empty",
   a repairable defect, so the re-promotion guard would have let it through
   at the next 4-hourly run. Archived now.

C. PENDING ROWS THAT ARE NOT RECALLS, NOT FOOD, OR ALREADY PUBLISHED
   Each reason is written on the archive row. The three FDA outbreak-
   investigation pages describe outbreaks whose recalls are PUBLISHED
   (Evergreen Fresh Sprouts 2026-09-10 + CDC; Clover Hill Dairy 2026-06-09
   and 06-18; Nara Organics 2026-06-13 + CDC). Secaris nasal gel is a health
   product; ANVISA's produce-washing advice is advice; MPI's report and
   survey PDF and Livsmedelsverket's Kontrollwiki are guidance pages; the
   RASFF 470205 row is scraped UI chrome of an old notification; the three
   FSA rows are terminal duplicates upheld by operators on 2026-08-22 and
   08-27; the NCC hummus statement is the 2024 founding incident, upheld
   TERMINAL on 2026-11-06; the NAFDAC watchlist names no product and no
   hazard, and the NAFDAC Proguanil alert is a counterfeit antimalarial.
   The BLV public warning BRX4VP3-xxIP is the same Aldi Suisse
   Mini-Hähnchenbrust Listeria recall already published from
   recallswiss.admin.ch/#Recalls/1191 on 2026-11-13 (same firm, product,
   hazard, date; FSVO title on both rows).

D. THREE EARLIER REFUSALS THAT DO NOT HOLD, RETIRED SO THE GUARD RELEASES
   promote_approved's re-promotion guard blocked three rows the curator had
   just corrected against the fiche itself:
     * fiche 24077 (Reblochon de Savoie au lait cru, Salmonella, 13/11) and
       fiche 24064 (Reblochon fermier 450 g, STEC, 11/11) — the archive row
       for each says "NO_REASON_RECORDED ... treat as UNAUDITED, not as a
       confirmed rejection". An unaudited non-verdict is not a reason to
       keep a Tier-1 recall out once a person has read the fiche.
     * fiche 24012 (Champibon dried morels, 04/11) — archived as "not a
       specific recall page" and "page is not found". Both false on
       2026-11-14: the fiche is live, specific, and was read in full.
   Each archive row is annotated "[SUPERSEDED ...]" — the marker this repo
   uses to retire a wrong verdict (promote_gate_passing.supersede_archived_
   copies, repair_2026_11_14b). Nothing is deleted.

NOT TOUCHED (needs a human): the Fødevarestyrelsen chicken-gyros row and
the SFA raw-milk-cheese row — both domains unreachable from this session,
and the curator's own fetch failed on the first.

HOW THIS RAN ON 2026-11-14. Parts B and C were applied by the nightly
operator with ``--pending-only``. Part A — removing a PUBLISHED row — was
refused by the operator session's permission layer, twice, and was NOT
applied. It is left here, complete and runnable, for George:

    python tools/repair_2026_11_14c.py --published-only

in a commit whose message carries "deliberate row removal" so that
tests/test_register_never_shrinks.py accepts the shrink, followed by
``python -m pipeline.enrich_schema --write`` and a recalls.json re-mirror.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-14"
BY = "nightly operator " + TODAY

SPOON_URL = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
             "Meldungen/2026/11_November/261112_13_NI_Waermesensorloeffel/"
             "261112_13_NI_Waermesensorloeffel.html")
SPOON_REASON = (
    f"[REMOVED FROM Recalls {TODAY} by the {BY} — OUT OF SCOPE: NOT FOOD "
    "(R1). Nuby heat-sensing baby spoons sold by Rossmann, Bisphenol AF above "
    "the limit: a food-contact utensil announced through lebensmittelwarnung.de, "
    "the same class as the ceramic plates (2026-08-21), the GIS kitchen-utensil "
    "set (2026-11-01) and the NKFH electric shaver (2026-11-06) this register "
    "already refused. The row's own Product (\"Baby spoons\") and the notice "
    "slug (Waermesensorloeffel) both name a spoon. lebensmittelwarnung.de could "
    "not be opened from the operator's session, so the hazard text was not "
    "re-read; the scope decision does not depend on it.]")

# Pending URL -> reason. Exact URLs as held in Pending tonight.
PENDING_REJECTS = {
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        "out_of_scope_animal_feed — FDA notice read 2026-11-14: Laubs Feeding and "
        "Cubing, Inc. (Beryl, UT) Grade A Mini Hay Cubes, 1500 lb totes and bulk, "
        "filed under Animal & Veterinary after reports of sick and dead horses; "
        "Clostridium botulinum. Animal feed, not human food. It PASSED the full "
        "publish gate tonight and would have been promoted at the next run; the "
        "earlier refusal ('Date is empty') was repairable and would not have held it.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-sprouts-september-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page (34 ill, "
        "3 hospitalised, outbreak declared over 2026-11-04) for the broccoli-sprout "
        "Salmonella outbreak whose recalls are published: Evergreen Fresh Sprouts "
        "2026-09-10 (FDA) and 2026-09-09 (CDC, Outbreak=1), plus the Gardeners "
        "Basics (10-08) and EAK Distribution (10-02) seed recalls. Refused three "
        "times before for the same headline-as-product shape.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-listeria-monocytogenes-soft-cheese-june-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page (15 ill, "
        "14 hospitalised, 1 death; investigation complete 2026-08-26) for the "
        "Clover Hill Dairy requeson Listeria outbreak, published 2026-06-09 and "
        "2026-06-18 (expanded recall). Company empty; Product is the page title.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-infant-botulism-powdered-infant-formula-june-2026":
        "duplicate_of_published_recall — FDA outbreak-investigation page for the "
        "Nara Organics powdered infant formula botulism event, published "
        "2026-06-13 from the FDA recall notice and from CDC (Outbreak=1). Already "
        "marked rejected by the review agent 2026-11-14.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026":
        "duplicate_of_published_recall — sixth arrival of the FDA outbreak page "
        "for the Sierra Nevada Cheese Company raw-milk cheese E. coli O26:H11 "
        "event, published 2026-09-29; refused by operators 2026-10-04, the "
        "outbreak check 2026-11-08 and the morning fix pass 2026-11-14. Terminal.",
    "https://recalls-rappels.canada.ca/en/alert-recall/secaris-nasal-gel-affected-lot-may-contain-foreign-matter":
        "out_of_scope_not_food — Health Canada 'Health products - Contamination' "
        "notice for Secaris NASAL GEL (the row's own extract), carried on Canada's "
        "consolidated recall portal. Not a food; Company and Brand hold the "
        "headline and Product holds 'Canada.ca'. Earlier refusal recorded no reason.",
    "https://www.gov.br/anvisa/en/updates/to-disinfect-produce-use-water-and-a-proper-solution-specifically-designed-for-food":
        "not_a_recall — ANVISA consumer advice page ('Vinegar, dish soap or "
        "detergent DO NOT disinfect fruits and vegetables', 13/11/2026). No "
        "product, no firm, no recall. Outbreak=1 was set with no human cases "
        "named anywhere on the row.",
    "https://www.blv.admin.ch/de/newnsb/BRX4VP3-xxIP":
        "duplicate_of_published_recall — FSVO public warning 'Listerien in "
        "Mini-Hähnchenbrust-Filetstücken 150g, verkauft bei ALDI SUISSE AG' "
        "(13.11.2026). The same recall is published from "
        "recallswiss.admin.ch/customer-access/#Recalls/1191 (BLV (CH), Aldi "
        "Suisse AG, Mini-Hähnchenbrust-Filetstücke, Listeria monocytogenes, "
        "2026-11-13) with the same FSVO title on its Notes. One recall, one row.",
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        "duplicate_already_approved — upheld: operator review 2026-08-22, "
        "Prepworld fruit products Salmonella is published as FSA-PRIN-29-2026 "
        "(2026-06-23) and the CFS Hong Kong mirror (2026-06-24). This copy has no "
        "Date. The /article/<id>/ form is the FSA's second URL for one alert.",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        "duplicate_already_approved — upheld: operator review 2026-08-22, O'Brien "
        "Fine Foods ham Listeria is published as FSA-PRIN-32-2026 (2026-07-01). "
        "No Date on this copy. The /article/<id>/ form is the FSA's second URL "
        "for one alert.",
    "https://alerts.food.gov.uk/article/6841/Tesco-recalls-Tesco-Finest-Mozzarella-Pearls--Pesto-Orzo-Salad-because-of-possible-contamination-with-E.-coli-STEC":
        "duplicate_already_approved — upheld: operator review 2026-08-27, Tesco "
        "Finest Mozzarella Pearls & Pesto Orzo Salad STEC is published 2026-08-25 "
        "as FSA-PRIN-41-2026. No Date on this copy.",
    "https://kontrollwiki.livsmedelsverket.se/artikel/127/atgarder-vid-fynd-av-listeria-i-livsmedel":
        "not_a_recall — Livsmedelsverket Kontrollwiki guidance article 'Åtgärder "
        "vid fynd av Listeria i livsmedel' (actions on finding Listeria in food). "
        "Guidance for inspectors, no product, no firm, no date.",
    "https://www.mpi.govt.nz/news/media-releases/new-reports-highlight-food-recall-system-at-work":
        "not_a_recall — MPI media release about New Zealand Food Safety's annual "
        "recall report. Company holds 'New reports highlight food' and Product "
        "'system at work': the headline split in two. No product, no date.",
    "https://www.mpi.govt.nz/dmsdocument/25847/direct":
        "not_a_recall — MPI document store PDF whose extract is a survey "
        "acknowledgement mentioning a '24-hour dietary recall' (a nutrition-"
        "survey method, not a product recall). No product, no firm, no date.",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        "not_a_current_recall — RASFF notification 470205 is a years-old "
        "notification id (the register's November 2026 notifications are in "
        "the 886xxx range); the row's Product is the RASFF Window column "
        "header ('Organisations · Reference · Subject · ...'), scraped UI "
        "chrome, with no Date and no Company.",
    "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/":
        "stale_event_terminal — upheld: NCC media statement dated October 3, 2024 "
        "(read today) on the BM Foods / Shoprite Checkers Deli Hummus Listeria "
        "recall notified 16 September 2024. Upheld TERMINAL by operator review "
        "2026-11-06; tenth arrival via the South Africa gap finder with a 2026 "
        "Date it does not have. It PASSED the publish gate tonight.",
    "https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        "not_a_recall — NAFDAC watchlist notice 'pending regulatory investigation' "
        "for products marketed by Mofus Nigeria Ltd: no product named (Product "
        "is empty), no hazard named (Pathogen is empty), no recall. Eighth "
        "arrival; the earlier refusal ('Product is empty; Pathogen is empty') "
        "still holds. nafdac.gov.ng could not be opened from this session.",
    "https://nafdac.gov.ng/public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        "out_of_scope_not_food — NAFDAC alert on counterfeit Proguanil "
        "(Projeanil, Re-granil) 100 mg TABLETS, an antimalarial drug. Not a "
        "food (R1); 'Fake NAFDAC Registration Number' is not a hazard this "
        "register holds. Seventh arrival.",
}

SUPERSEDED_SPOON_NOTE = (
    f" [{BY}: the published copy this row deferred to was itself REMOVED from "
    "Recalls tonight as OUT OF SCOPE — a baby spoon is not a food. This row's "
    "refusal stands, on scope rather than on its original no-reason.]")


RELEASE = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24077/interne":
        "the only recorded refusal is NO_REASON_RECORDED (unaudited). Fiche "
        "24077 read in full on 2026-11-14: REBLOCHON DE SAVOIE au lait cru, "
        "GAEC LA LOUISA, 13/11/2026, Salmonella spp; the corrected Pending "
        "row passes the full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne":
        "the only recorded refusal is NO_REASON_RECORDED (unaudited). Fiche "
        "24064 read in full on 2026-11-14: REBLOCHON DE SAVOIE FERMIER 450g, "
        "GAEC LES DEUX SAVOIES, 11/11/2026, Escherichia coli STEC; the "
        "corrected Pending row passes the full publish gate and is the copy "
        "that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24012/interne":
        "'not a specific recall page' and 'page is not found' are both false "
        "on 2026-11-14: fiche 24012 is live and specific — CHAMPIGNONS "
        "MORILLES SECHEES 60G, CHAMPIBON, B&M, 04/11/2026, 'probable "
        "contamination d'un lot de morilles par une toxine'. The corrected "
        "Pending row passes the full publish gate and is the copy that wins",
}


def release_false_refusals(wb) -> int:
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
                ws.cell(r, ci).value = stamp + cur
                if "Reviewed" in hdr:
                    ws.cell(r, hdr.index("Reviewed") + 1).value = "SUPERSEDED"
                print(f"    {sheet} row {r}: {url[-30:]} marked SUPERSEDED")
                n += 1
    return n


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


def remove_the_spoon_row(wb) -> int:
    ws = wb["Recalls"]
    rows = _rows_for(ws, SPOON_URL)
    if len(rows) != 1:
        raise SystemExit(f"expected exactly one Recalls row for the spoon "
                         f"notice, found {len(rows)}")
    r = rows[0]
    row = _row_dict(ws, r)
    if "spoon" not in str(row.get("Product", "")).lower():
        raise SystemExit("Recalls row at the spoon URL does not describe "
                         "spoons any more — refusing to remove it")
    print(f"    Recalls row {r}: {row['Source']} {row['Company']!r} "
          f"{row['Product']!r} -> Rejected")
    _archive(wb, row, SPOON_REASON)
    ws.delete_rows(r, 1)
    return 1


def annotate_the_superseded_archive_row(wb) -> int:
    ws = wb["Weekly_Rejected"]
    hdr = _hdr(ws)
    n = 0
    for r in _rows_for(ws, SPOON_URL):
        c = ws.cell(r, hdr.index("RejectionReason") + 1)
        cur = str(c.value or "")
        if "SUPERSEDED" in cur and SUPERSEDED_SPOON_NOTE not in cur:
            c.value = cur + SUPERSEDED_SPOON_NOTE
            n += 1
    return n


def reject_pending(wb) -> int:
    ws = wb["Pending"]
    n = 0
    for url, reason in PENDING_REJECTS.items():
        rows = _rows_for(ws, url)
        if not rows:
            raise SystemExit(f"Pending row not found for {url}")
        for r in sorted(rows, reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} -> Rejected")
            _archive(wb, row, reason)
            ws.delete_rows(r, 1)
            n += 1
    return n


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pending-only", action="store_true",
                    help="run parts B and C (the Pending queue) only")
    ap.add_argument("--published-only", action="store_true",
                    help="run part A (remove the published spoon row) only")
    ap.add_argument("--release-only", action="store_true",
                    help="run part D (retire the three false refusals) only")
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    removed = n = 0
    if a.release_only:
        print("D  earlier refusals that do not hold:")
        print("   archive rows marked SUPERSEDED: %d" % release_false_refusals(wb))
        wb.save(XLSX)
        wb.close()
        print("ROWS_REMOVED=0")
        return 0
    if not a.pending_only:
        print("A  a published row that is not a food:")
        removed = remove_the_spoon_row(wb)
        print("   removed from Recalls: %d" % removed)
        print("   archive rows annotated: %d"
              % annotate_the_superseded_archive_row(wb))
    if not a.published_only:
        print("B+C  Pending rows refused:")
        n = reject_pending(wb)
        print("   archived: %d" % n)
        print("D  earlier refusals that do not hold:")
        print("   archive rows marked SUPERSEDED: %d" % release_false_refusals(wb))
    wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=%d" % removed)
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
