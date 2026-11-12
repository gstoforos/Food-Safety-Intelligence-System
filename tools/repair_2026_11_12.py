# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-12 — clear Pending.

Fourteen Pending rows are archived to the permanent Rejected sheet with a
written, terminal reason each. Every verdict below was taken at the
regulator's page on 2026-11-12 where the page was reachable from the
session, and from the register's own archive where it was not; the reason
says which. Nothing is deleted from Recalls; the three rows that were
promotable (RappelConso 24064, CFIA Molson beer, AESAN 2026_81) were
repaired through the curator and are promoted by
pipeline.promote_gate_passing AFTER this script has run, so that the rows
archived here cannot ride along.

Rows are matched by exact URL. If any URL is missing from Pending the
script stops before writing: main has moved under it and the list must be
re-read, not half-applied.
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

# (URL, short tag, reason)
REJECTS = [
    ("https://www.fsai.ie/news-and-alerts/latest-news/four-enforcement-orders-served-on-food-busines-(2)",
     "NOT A RECALL — ENFORCEMENT ORDERS",
     "FSAI news item 'Four Enforcement Orders served on food businesses in "
     "October' (11/11/2026): closure orders on food premises, not a product "
     "recall. Company empty, Product is the date line of the page, Pathogen "
     "'Rodent contamination' is the inspection finding, not a recalled "
     "product's hazard. Already marked rejected by the URL agent on the row "
     "and archived to Weekly_Rejected 2026-11-11."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-infant-botulism-powdered-infant-formula-june-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page read 2026-11-12: 'Outbreak Investigation "
     "of Infant Botulism: Powdered Infant Formula (June 2026)', recalled "
     "product Nara Organics Whole Milk Organic Powdered Infant Formula, 4 "
     "illnesses, content current as of 08/27/2026. The event is published in "
     "Recalls twice already — Nara Organics from the FDA recall notice "
     "(2026-06-13) and from the CDC investigation (2026-06-13, Outbreak=1). "
     "Company and Brand empty, Product is the page title, Date 2026-11-11 is "
     "the feed's re-listing date, not the notice's."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Cyclospora iceberg-lettuce event. "
     "Published in Recalls from the recalling firm's notice (Taylor Fresh Foods, "
     "2026-07-18, Outbreak=1). Company and Brand empty, Product is the page "
     "title. Archived to Weekly_Rejected 2026-11-07 and rejected by the "
     "operator review of 2026-11-11 (PR #40, unmerged)."),
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium",
     "ANIMAL FEED — OUT OF SCOPE",
     "Laub's Feeding & Cubing, Inc. recalls Grade A Mini Hay Cubes for "
     "potential Clostridium botulinum; horse feed listed under FDA Animal & "
     "Veterinary, illnesses reported in horses only (notice read by the "
     "operator review of 2026-11-11). Animal feed is outside the human-food "
     "scope. Archived to Weekly_Rejected before (Confirmer, Date empty) and "
     "rejected 2026-11-11 (PR #40, unmerged)."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the E. coli O26:H11 raw-milk cheese "
     "event. Published in Recalls from the firm's own FDA notice (Sierra Nevada "
     "Cheese Company, 2026-09-29, Outbreak=1). This URL has been archived five "
     "times before for the same reason (2026-09-01, 09-26, 10-01, 10-09, "
     "11-08); Company and Brand are empty and the Product is the page title."),
    ("https://aesan.gob.es/actualidad/actualidad-noticias/La-EFSA-actualizacion",
     "NOT A RECALL — EFSA RISK-ASSESSMENT NEWS",
     "AESAN news item 'La EFSA actualiza la evaluación de riesgos de las "
     "toxinas de Alternaria en los alimentos y piensos' (11/11/2026): an EFSA "
     "scientific-opinion announcement, not a recall of any product. Company "
     "empty, Product is the scraper string 'calendar_today 11/11/2026', "
     "Pathogen is the opinion's subject."),
    ("https://www.aesan.gob.es/en/alertas/2026_81",
     "DUPLICATE PENDING ROW — SAME ALERT UNDER THE www HOST",
     "Second Pending row for AESAN alert 2026_81 (Priégola dairy, Listeria "
     "monocytogenes, Ref. ES2026/678). The same alert sits in Pending at "
     "https://aesan.gob.es/en/alertas/2026_81, which was read at source on "
     "2026-11-12, repaired through the curator and is promoted tonight; the "
     "www host does not resolve from this session. Two Pending rows per URL "
     "held tests/test_pending_holds_one_row_per_url RED on main."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24047/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24047 (Champibon dried morels, 09/11/2026): Motif "
     "'probable contamination d'un lot de morilles par une toxine susceptible "
     "d'être à l'origine d'intoxication alimentaire', Risques 'Autre risque'. "
     "The fiche names no toxin, no chemical and no micro-organism; Pathogen "
     "is empty and the register names its hazard. Same verdict as the "
     "operator reviews of 2026-11-08, 11-10 and 11-11 (PRs #38, #39, #40, "
     "unmerged) and the Confirmer archive of 2026-11-09."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24016/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24016 (Champibon dried morels, 05/11/2026): same Motif "
     "as fiche 24047 — 'une toxine', Risques 'Autre risque' — no toxin, "
     "chemical or micro-organism named; Pathogen empty. Archived 2026-11-05 "
     "and 2026-11-09 (Confirmer: Pathogen is empty) and rejected by the "
     "operator reviews of 2026-11-08, 11-10 and 11-11."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24012/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24012 (Champibon dried morels, 04/11/2026): same Motif "
     "as fiche 24047 — 'une toxine', Risques 'Autre risque' — no toxin, "
     "chemical or micro-organism named; Pathogen empty; Product is a GTIN and "
     "a date. Archived 2026-11-04 and rejected by the operator reviews of "
     "2026-11-08, 11-10 and 11-11."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-sprouts-september-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Salmonella sprouts event, with a "
     "sentence lifted from the page as Product. The recall it describes is "
     "published in Recalls from the firm's own FDA notice (Eden Brothers LLC, "
     "2026-10-07). Archived before on 2026-09-04, 09-09 and 10-09 and "
     "rejected by the operator review of 2026-11-11."),
    ("https://favv-afsca.be/nl/themas/controles/interne-controles-door-bedrijven/meldingsplicht",
     "NOT A RECALL — ANIMAL FEED GUIDANCE",
     "AFSCA reporting-obligation page carrying a circular on Salmonella control "
     "in ANIMAL FEED (PCCB/S1/483559). Not a recall notice, no product, no "
     "date, and feed is outside the human-food scope. Terminal operator "
     "verdict of 2026-08-15 stands; rejected again 2026-11-10 and 2026-11-11."),
    ("https://kontrollwiki.livsmedelsverket.se/artikel/127/atgarder-vid-fynd-av-listeria-i-livsmedel",
     "NOT A RECALL — INSPECTOR GUIDANCE",
     "Livsmedelsverket Kontrollwiki article 127, 'Åtgärder vid fynd av Listeria "
     "i livsmedel', is guidance for inspectors on what to do when Listeria is "
     "found. The headline sits in Company and Brand, 'Kontrollwiki' in Product, "
     "no date. Not a recall of any product. Rejected by the operator review "
     "of 2026-11-11."),
    ("https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/",
     "STALE ALERT — 2024 STATEMENT",
     "NCC media statement 'Date Posted' 3 October 2024; 'On 16 September 2024' "
     "Shoprite Checkers notified the NCC of the BM Foods Deli Hummus recall "
     "(read by the operator review of 2026-11-11). A September-2024 recall "
     "carried with Date 2026-11-12. Eleventh archive copy; terminal verdict "
     "of 2026-11-06 ('TERMINAL, UPHELD') stands."),
]


def _head(ws):
    return [c.value for c in ws[1]]


def main() -> int:
    wb = openpyxl.load_workbook(XLSX)
    pend, arch = wb["Pending"], wb["Rejected"]
    ph, ah = _head(pend), _head(arch)
    ucol = ph.index("URL") + 1

    def _key(u):
        return str(u or "").strip().lower().rstrip("/")

    index = {}
    for r in range(2, pend.max_row + 1):
        index.setdefault(_key(pend.cell(r, ucol).value), []).append(r)

    missing = [u for u, _t, _r in REJECTS if _key(u) not in index]
    if missing:
        print("STOP — not in Pending (main has moved; re-read the sheet):")
        for u in missing:
            print("   ", u)
        return 2
    multi = [u for u, _t, _r in REJECTS if len(index[_key(u)]) != 1]
    if multi:
        print("STOP — more than one Pending row per URL:", multi)
        return 2

    before_p, before_a = pend.max_row - 1, arch.max_row - 1
    rows_to_delete = []
    for url, tag, reason in REJECTS:
        r = index[_key(url)][0]
        row = {h: pend.cell(r, i + 1).value for i, h in enumerate(ph)}
        full = f"{BY}: {tag}. {reason}"
        notes = str(row.get("Notes") or "").strip()
        row["Notes"] = (f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: {tag}. "
                        f"{reason}] " + notes).strip()
        out = []
        for h in ah:
            if h == "RejectReason":
                out.append(full)
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
        print(f"  Pending row {r:>3} -> Rejected: {tag:<45} "
              f"{str(row.get('Product') or row.get('URL'))[:50]!r}")

    for r in sorted(rows_to_delete, reverse=True):
        pend.delete_rows(r, 1)

    wb.save(XLSX)
    print(f"Pending {before_p} -> {pend.max_row - 1}; "
          f"Rejected {before_a} -> {arch.max_row - 1}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
