# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-11 — clear Pending.

Sixteen Pending rows are archived to the permanent Rejected sheet with a
written, terminal reason each. Every verdict below was taken at the
regulator's page on 2026-11-11 where the page was reachable from the
session, and from the register's own archive where it was not; the reason
says which. Nothing is deleted from Recalls; the three rows that were
promotable were repaired through the curator and are promoted by
pipeline.promote_gate_passing AFTER this script has run, so that the
rows archived here cannot ride along.

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
TODAY = "2026-11-11"
BY = f"operator review {TODAY}"

# (URL, short tag, reason)
REJECTS = [
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the E. coli O26:H11 raw-milk cheese "
     "event. The event is published in Recalls from the firm's own FDA notice "
     "(Sierra Nevada Cheese Company, 2026-09-29, Outbreak=1). This URL has been "
     "archived four times before for the same reason (2026-09-01, 09-26, 10-01, "
     "10-09); the Company and Brand are empty and the Product is the page title."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24047/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24047 (Champibon dried morels, 09/11/2026) read 2026-11-11: "
     "Motif 'probable contamination d'un lot de morilles par une toxine susceptible "
     "d'être à l'origine d'intoxication alimentaire', Risques 'Autre risque'. The "
     "fiche names no toxin, no chemical and no micro-organism; Pathogen is empty "
     "and the register names its hazard. Same verdict as the operator reviews of "
     "2026-11-08 and 2026-11-10 (PR #38, #39, unmerged)."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Cyclospora iceberg-lettuce event. "
     "Published in Recalls from the recalling firm's notice (Taylor Fresh Foods, "
     "2026-07-18, Outbreak=1). Company and Brand empty, Product is the page title; "
     "reviewer 3 already rejected it on 2026-11-11."),
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/ramar-foods-recalls-mint-chocolate-chip-hidden-spinach-ice-cream-because-possible-health-risk",
     "STALE NOTICE ATTACHED TO A 2026 HEADLINE",
     "The fda.gov page is the Ramar Foods 'Peekaboo' Mint Chocolate Chip ice cream "
     "recall of 2020 (Best Before 10/08/2021), which the page itself says has been "
     "completed and terminated. The row was discovered from a 2026-11-06 Google "
     "News headline that recirculated it; the morning fix pass 2026-11-11 wrote "
     "'DO NOT PUBLISH THIS ROW AGAINST THIS URL' on the row. Not a 2026 recall."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24016/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24016 (Champibon dried morels, 05/11/2026): same Motif as "
     "fiche 24047 — 'une toxine', Risques 'Autre risque' — no toxin, chemical or "
     "micro-organism named; Pathogen empty. Already archived 2026-11-05 "
     "(Confirmer: Pathogen is empty) and rejected by the operator reviews of "
     "2026-11-08 and 2026-11-10."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24012/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24012 (Champibon dried morels, 04/11/2026): same Motif as "
     "fiche 24047 — 'une toxine', Risques 'Autre risque' — no toxin, chemical or "
     "micro-organism named; Pathogen empty; Product is a GTIN and a date. Already "
     "archived 2026-11-04 and rejected by the operator reviews of 2026-11-08 and "
     "2026-11-10."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/investigations-foodborne-illness-outbreaks",
     "FABRICATED ATTRIBUTION — LISTING PAGE",
     "The FDA outbreak-investigations TABLE, filed as 'Clostridium botulinum — "
     "outbreak' with Outbreak=1, Company and Product empty. The table is a listing, "
     "not a notice; its Oct/Nov 2026 rows carry no botulinum entry (the operator "
     "review of 2026-11-10 read it: the 10/28 row is Salmonella Javiana, ref "
     "1424). Archived before on 2026-10-20 and 2026-10-28 as a listing page."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-sprouts-september-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Salmonella sprouts event, with a "
     "sentence lifted from the page as Product. The recall it describes is "
     "published in Recalls from the firm's own FDA notice (Eden Brothers LLC, "
     "2026-10-07). Archived before on 2026-09-04, 09-09 and 10-09."),
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts?search_api_fulltext=H-0177-2027",
     "DUPLICATE OF A PUBLISHED RECALL",
     "An fda.gov SEARCH URL (openFDA recall H-0177-2027) for the Eden Brothers "
     "calabrese broccoli seeds recall, which is published in Recalls from the "
     "firm's own FDA press release of 2026-10-07. Archived before on 2026-10-07."),
    ("https://nafdac.gov.ng/public-alert-no-042-2026-alert-on-the-seizure-of-suspected-substandard-and-falsified-bppl-artemether-lumefantrine-80mg-480mg/",
     "NOT A FOOD",
     "NAFDAC Public Alert 042/2026 is the seizure of a falsified ANTIMALARIAL "
     "(artemether/lumefantrine 80/480 mg), a medical product. Terminal verdict "
     "recorded by the morning fix pass of 2026-10-31 ('not a food (R1)') and "
     "upheld by the operator review of 2026-11-10; it still holds."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-eggs-july-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Salmonella Enteritidis shell-egg "
     "event. Published in Recalls twice already — Midwest Poultry Services, L.P. "
     "from the FDA notice (2026-07-22, Outbreak=1) and from the CDC investigation "
     "(2026-09-03, Outbreak=1). The Google-News headline is the Reason. Archived "
     "before on 2026-09-03."),
    ("https://favv-afsca.be/nl/themas/controles/interne-controles-door-bedrijven/meldingsplicht",
     "NOT A RECALL — ANIMAL FEED GUIDANCE",
     "AFSCA reporting-obligation page carrying a circular on Salmonella control in "
     "ANIMAL FEED (PCCB/S1/483559). Not a recall notice, no product, no date, and "
     "feed is outside the human-food scope. Archived before (operator review "
     "2026-08-15) and rejected by the operator review of 2026-11-10."),
    ("https://kontrollwiki.livsmedelsverket.se/artikel/127/atgarder-vid-fynd-av-listeria-i-livsmedel",
     "NOT A RECALL — INSPECTOR GUIDANCE",
     "Livsmedelsverket Kontrollwiki article 127, 'Åtgärder vid fynd av Listeria i "
     "livsmedel', is guidance for inspectors on what to do when Listeria is "
     "found. The headline sits in Company and Brand, 'Kontrollwiki' in Product, no "
     "date. Not a recall of any product."),
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium",
     "ANIMAL FEED — OUT OF SCOPE",
     "FDA notice read 2026-11-11: Laub's Feeding & Cubing, Inc. (Beryl, Utah) "
     "recalls Grade A Mini Hay Cubes sold 3–31 Oct 2026 for potential Clostridium "
     "botulinum; listed under Animal & Veterinary > Animal Feed, the affected "
     "animals are horses, illnesses and deaths reported in horses only. Horse "
     "feed is outside the human-food scope."),
    ("https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/",
     "STALE ALERT — 2024 STATEMENT",
     "NCC media statement read 2026-11-11: 'Date Posted' 3 October 2024; "
     "'On 16 September 2024' Shoprite Checkers notified the NCC of the BM Foods "
     "Deli Hummus recall; sell-by dates 10 Sep – 8 Oct 2024. A September-2024 "
     "recall carried with Date 2026-11-11. Tenth archive copy; terminal verdict "
     "of 2026-11-06 ('TERMINAL, UPHELD') stands."),
    ("https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/",
     "NOT A RECALL — WATCHLIST, NO HAZARD",
     "NAFDAC Public Alert 043/2026 places products marketed by Mofus Nigeria Ltd on "
     "a WATCHLIST 'pending regulatory investigation'. The row names no product, no "
     "company, no hazard (Pathogen empty; Reason 'Pending regulatory "
     "investigation'). A watchlist notice with no finding is not a recall and "
     "carries nothing the register could publish. nafdac.gov.ng is not reachable "
     "from this session; the verdict rests on the alert's own title and the empty "
     "row. Archived seven times before (2026-08-20 … 2026-11-04)."),
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
        print(f"  Pending row {r:>3} -> Rejected: {tag:<40} "
              f"{str(row.get('Product') or row.get('URL'))[:50]!r}")

    for r in sorted(rows_to_delete, reverse=True):
        pend.delete_rows(r, 1)

    wb.save(XLSX)
    print(f"Pending {before_p} -> {pend.max_row - 1}; "
          f"Rejected {before_a} -> {arch.max_row - 1}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
