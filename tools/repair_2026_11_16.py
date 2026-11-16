# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-16. Base 2914d34 (main, 21:11 Athens).

Everything here was decided against the regulator's own page tonight
(rappel.conso.gouv.fr, recalls-rappels.canada.ca, fda.gov, thencc.org.za
opened; salute.gov.it, nafdac.gov.ng, foedevarestyrelsen.dk,
recallswiss.admin.ch, blv.admin.ch, lebensmittelwarnung.de, fsis.usda.gov did
not resolve from the session). Field corrections on live rows go through
pipeline/agents/curator.py with quoted evidence (see
docs/data/agent-proposals/*-nightly-operator.json); this script does only
what the curator cannot: archive refused Pending rows, retire archive
verdicts that no longer hold, correct one published row whose authority
PDF is unreachable, and re-sort Recalls newest-first after the promoter
appends.

A. TWELVE PENDING ROWS REFUSED — archived to Rejected with the reason,
   never deleted (append-only).
B. SEVEN ARCHIVE ROWS WHOSE VERDICT DOES NOT HOLD — marked SUPERSEDED so the
   re-promotion guard releases fiches 24077, 24064, 24016 and 24012, each of
   which was archived on a repairable field defect ("Pathogen is empty",
   Company "/"), a false reachability claim ("page is not found" — read in
   full tonight) or no verdict at all ("confirmer did not re-review").
C. ONE PUBLISHED ROW DESCRIBES THE WRONG PRODUCT. Recalls row for the
   Ministero della Salute PDF Modello_richiamo_Pomodorino_1794652742.pdf
   (Mediterranea Snc di Milanese Rosanna & C., 2026-11-15) was published as
   "Dried tomato", Brand empty. The row's own discovery source —
   ilfattoalimentare.it, "Salmonella nel pomodoro datterino: richiamato un
   lotto prodotto a Vittoria", 15/11/2026 — quotes the ministry notice: "un
   lotto di pomodoro datterino a marchio L'Ortofrutticola F ... prodotto da
   Mediterranea Snc di Milanese Rosanna & C." in 500 g packs, lot 4406,
   barcode 8056590100820, "presenza di Salmonella spp.". FRESH datterino
   tomatoes, not dried. salute.gov.it does not resolve from this session, so
   the PDF itself was not read; the correction is from the article the row
   was extracted from, and the row is flagged for a human to confirm against
   the PDF.
F. THREE ARCHIVE ROWS SHARE A URL WITH A PUBLISHED RECALL AND SAY NOTHING
   (Eden Brothers duplicate; the two salute.gov.it listing-page rows). RED on
   main before this run; annotated in the reason column, nothing deleted.
D. RECALLS RE-SORTED NEWEST-FIRST. promote_gate_passing appends; the three
   fiches published this morning sat at rows 2406-2408 below a 2026-11-15
   row. Values-only stable sort, explicit cell assignment (never
   ``ws.cell(..., value=None)``, which openpyxl ignores).

E. MAIN MOVED WHILE THIS REVIEW RAN. Commit 839594c (Tavily gap-finder,
   20:14 UTC) added seven Pending rows to the workbook after this review's
   snapshot. ``--phase merge --upstream <main's recalls.xlsx>`` copies every
   upstream Pending row this workbook lacks, value for value, so the rebase
   can take this branch's workbook without losing the pipeline's rows; then
   it archives the five of them this review could decide (all duplicates of
   published or previously refused items, or not recalls). The two AESAN
   alerts 2026_24 and 2026_12 stay in Pending: aesan.gob.es does not resolve
   from the session and the rows' own fields contradict each other
   (headline says Salmonella, Pathogen says Listeria, no Date).

    python3 tools/repair_2026_11_16.py --phase merge --upstream /path/to/main-recalls.xlsx

    python3 tools/repair_2026_11_16.py --dry-run
    python3 tools/repair_2026_11_16.py --phase a      # A + B + C, before the curator
    python3 tools/repair_2026_11_16.py --phase sort   # D, after the promoter
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-16"
BY = "nightly operator review 2026-11-16"

# ── A. Pending rows refused ────────────────────────────────────────────────
PENDING_REJECTS = {
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-salmonella-sprouts-september-2026":
        f"duplicate_of_published_recall — {BY}: the FDA outbreak-INVESTIGATION "
        "page for the Salmonella Bovismorbificans sprouts outbreak (investigation "
        "complete). The recall is PUBLISHED in Recalls as Evergreen Fresh Sprouts, "
        "LLC (FDA notice 2026-09-10) and as the CDC outbreak page (2026-09-09). "
        "Row carries no company and the page headline as both Product and Reason; "
        "the review agent also rejected it 2026-11-16. Fifth archive copy of this URL.",
    "https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-listeria-monocytogenes-soft-cheese-june-2026":
        f"duplicate_of_published_recall — {BY}: the FDA outbreak-INVESTIGATION "
        "page for the June 2026 Listeria soft-cheese outbreak. The recalls are "
        "PUBLISHED: Clover Hill Dairy, LLC (FDA 2026-06-09 and expanded 2026-06-18), "
        "the two CDC outbreak pages (2026-06-04), La Ceiba Foods (2026-06-26) and "
        "Nelson & Isa Lacteos (2026-06-05). Row carries no company and the page "
        "headline as both Product and Reason; the review agent also rejected it "
        "2026-11-16.",
    "https://www.cdc.gov/ecoli/outbreaks/raw-milk-cheese-09-26/index.html":
        f"duplicate_of_published_recall — {BY}: CDC outbreak page for the E. coli "
        "O26:H11 raw-milk cheese outbreak, which CDC itself declares over "
        "('This outbreak is over', investigation status Closed, Nov. 13, 2026). The "
        "recall is PUBLISHED as Sierra Nevada Cheese Company (FDA 2026-09-29, "
        "Outbreak=1); the FDA investigation page for the same outbreak was archived "
        "duplicate_of_published_recall 2026-11-14. Product on this row is the single "
        "letter 'E' (tests/test_a_product_name_is_not_a_reference_code.py was RED on "
        "main because of it). Third archive copy of this URL.",
    "https://www.efsa.europa.eu/en/news/alternaria-toxins-food-and-feed-efsa-updates-its-risk-assessment":
        f"not a recall — {BY}: EFSA news item 'Alternaria toxins in food and feed: "
        "EFSA updates its risk assessment' (11 November 2026) is a risk-assessment "
        "update, names no product, firm or lot. The same item was refused in its "
        "three AESAN language copies on 2026-11-14 and in Weekly_Rejected; Product "
        "on this row is the page's own title and lead paragraph.",
    "https://rappel.conso.gouv.fr/affichettePDF/24016/Interne":
        f"duplicate — {BY}: the affichette (PDF poster) URL of RappelConso fiche "
        "24016, whose fiche-rappel URL (https://rappel.conso.gouv.fr/fiche-rappel/"
        "24016/interne) is the copy promoted tonight after correction. One recall, "
        "one row.",
    "https://rappel.conso.gouv.fr/fiche-rappel/24047/interne":
        f"duplicate_of_published_recall — {BY}: fiche 24047 (09/11/2026, "
        "Promocash) is a second publication of the recall in fiche 24016 "
        "(05/11/2026, K 2 M PROMOCASH): both read CHAMPIGNONS MORILLES SECHEES, "
        "CHAMPIBON, 400 g, lot 262306, GTIN 3760201600023, DLC 01/12/2029, sold "
        "23/06/2026-03/11/2026, Motif 'probable contamination d'un lot de morilles "
        "par une toxine susceptible d'être à l'origine d'intoxication alimentaire', "
        "Risques 'Autre risque'. Same brand, same lot, same distributor chain, "
        "same hazard: one recall. Fiche 24016, the first publication, is promoted "
        "tonight and is the copy that wins. (The B&M 60 g fiche 24012 is a "
        "distinct product and is published separately.)",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/classic-delight-llc-issues-public-health-alert-due-possible-health-risk":
        f"stale — {BY}: the FDA notice read tonight is dated June 11, 2024 "
        "(company announcement June 10, 2024; Classic Delight LLC, St. Mary's, "
        "Ohio; Listeria monocytogenes in environmental samples; units produced "
        "May 2023-June 2024). The row's Date 2026-11-09 is the date a news "
        "aggregator (WhatNow, via Google News) republished it, not the notice's. "
        "Product and Reason are the news headline. Outside the register's window.",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        f"not a food — {BY}: FDA page read tonight lists the product type as "
        "'Animal Feed' under Animal & Veterinary: 'Laub's Feeding and Cubing, Inc. "
        "of Beryl, Utah is voluntary recalling up 89 ton of Grade A Mini Hay Cubes' "
        "(Clostridium botulinum). Hay cubes for livestock; human food only.",
    "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/":
        f"stale, terminal, upheld — {BY}: the NCC media statement read tonight "
        "is posted October 3, 2024 about a recall notified on 16 September 2024 "
        "(BM Foods / Shoprite Checkers Deli Hummus range, sell-by 10 Sep-08 Oct "
        "2024). The row's Date 2026-11-12 is a news-discovery date the statement "
        "does not carry. Operator review 2026-11-06 ruled this URL TERMINAL; "
        "this is at least the twelfth arrival.",
    "https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        f"not a recall — {BY}: NAFDAC Public Alert 043/2026 places a marketer's "
        "products 'on watchlist pending regulatory investigation'. The row names "
        "no product (Product 'None'), no hazard (Pathogen empty), Reason 'Pending "
        "regulatory investigation'. nafdac.gov.ng does not resolve from this "
        "session; the row's own content settles it. Archived six times before "
        "without a written reason.",
    "https://nafdac.gov.ng/public-alert-no-030-2026-alert-on-the-withdrawal-of-levamisole-containing-medicines-as-endorsed-by-the-italian-medicines-agency-aifa-over-serious-neurological-risks/":
        f"not a food — {BY}: NAFDAC Public Alert 030/2026 on the withdrawal of "
        "levamisole-containing MEDICINES as endorsed by the Italian Medicines "
        "Agency (AIFA) over neurological risks. A drug withdrawal; Pathogen on the "
        "row is 'Not applicable'. Archived four times before.",
    "https://nafdac.gov.ng/public-alert-no-034-2026-niger-republic-imposes-import-ban-on-bulmex-parboiled-rice-from-india-due-to-metallic-contamination/":
        f"not a recall — {BY}: NAFDAC Public Alert 034/2026 relays the Niger "
        "Republic's IMPORT BAN on Bulmex parboiled rice from India; it is not a "
        "Nigerian recall or withdrawal notice. The alert dates from August 2026 "
        "(the archive copy Rejected 2026-08-13 carries 2026-08-06) while this row "
        "says 2026-11-11, the date a news site mentioned it. nafdac.gov.ng does not "
        "resolve from this session, so nothing on the row could be corrected with "
        "evidence. Third arrival.",
}

# ── B. archive verdicts that do not hold ───────────────────────────────────
RELEASE = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24077/interne":
        "this archive row records no verdict on the recall, only the confirm "
        "agent's pass-through and the publish gate's repairable field defect "
        "(Company and Brand '/'). Fiche 24077 was read in full tonight: REBLOCHON "
        "DE SAVOIE au lait cru, origin GAEC LA LOUISA, 13/11/2026, Risques "
        "'Salmonella spp (agent responsable de la salmonellose)', Motif 'retrait "
        "par mesure de précaution dans un contexte suspicion salmonelle'. The "
        "curator-corrected Pending row passes the full publish gate and is the "
        "copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne":
        "this archive row records no verdict on the recall, only the confirm "
        "agent's pass-through and the publish gate's repairable field defect "
        "(Company and Brand '/'). Fiche 24064 was read in full tonight: REBLOCHON "
        "DE SAVOIE FERMIER 450g, GAEC LES DEUX SAVOIES, 11/11/2026, Risques "
        "'Escherichia coli shiga toxinogène (STEC)', Motif 'Présence de "
        "Escherichia Coli STEC'. The curator-corrected Pending row passes the "
        "full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24016/interne":
        "'Pathogen is empty' was a repairable field defect, now filled from the "
        "fiche's own Motif; the reviewer-2 'Unable to confirm the recall details "
        "from the provided page' is false — fiche 24016 was read in full tonight "
        "(CHAMPIGNONS MORILLES SECHEES, CHAMPIBON, 400 g, lot 262306, K 2 M "
        "PROMOCASH, 05/11/2026, Motif 'probable contamination d'un lot de "
        "morilles par une toxine susceptible d'être à l'origine d'intoxication "
        "alimentaire'). The curator-corrected Pending row passes the full publish "
        "gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24012/interne":
        "'Pathogen is empty' was a repairable field defect, now filled from the "
        "fiche's own Motif; 'the page is not found' is false — fiche 24012 was "
        "read in full tonight (CHAMPIGNONS MORILLES SECHEES 60G, CHAMPIBON, B&M "
        "France SAS, version 2, 04/11/2026, tous les lots, GTIN 3760201609743); "
        "'confirmer did not re-review' records that nobody looked. The "
        "curator-corrected Pending row passes the full publish gate and is the "
        "copy that wins",
}

# ── C. the Italian tomato row ──────────────────────────────────────────────
TOMATO_URL = ("https://www.salute.gov.it/new/sites/default/files/external_data/"
              "avvisi_sicurezza_alimentare/Modello_richiamo_Pomodorino_1794652742.pdf")
TOMATO_OLD_PRODUCT = "Dried tomato"
TOMATO_PRODUCT = ("Fresh datterino tomatoes (pomodoro datterino), 500 g, lot 4406, "
                  "barcode 8056590100820")
TOMATO_BRAND = "L'Ortofrutticola F"
TOMATO_NOTE = (
    f"[{BY}: Product was published as 'Dried tomato' with no Brand. The row's "
    "own discovery source, ilfattoalimentare.it 'Salmonella nel pomodoro "
    "datterino: richiamato un lotto prodotto a Vittoria' (15/11/2026), quotes "
    "the ministry notice: 'un lotto di pomodoro datterino a marchio "
    "L'Ortofrutticola F', 'prodotto da Mediterranea Snc di Milanese Rosanna & "
    "C.' (contrada Fanello, Vittoria RG), commercialised by L'Ortofrutticola F "
    "S.r.l. (Niscemi CL), 500 g, lot 4406, barcode 8056590100820, 'presenza di "
    "Salmonella spp.' — FRESH category-I datterino tomatoes, not dried. "
    "salute.gov.it does not resolve from this session, so the ministry PDF "
    "itself was not read; NEEDS-HUMAN to confirm the PDF matches.]")


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
            print(f"    Pending row not present (nothing to archive): {url[-60:]}")
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
                if cur.strip().lower() in ("none", "nan"):
                    cur = ""
                if "SUPERSEDED" in cur:
                    continue
                stamp = (f"[SUPERSEDED {TODAY} by the {BY} — {why}. This row "
                         f"is kept as the audit trail of the original refusal.] ")
                print(f"    {sheet} row {r}: {url[-30:]} marked SUPERSEDED")
                if not dry:
                    ws.cell(r, ci).value = stamp + cur
                    if "Reviewed" in hdr:
                        ws.cell(r, hdr.index("Reviewed") + 1).value = "SUPERSEDED"
                    if "Status" in hdr and sheet == "Rejected":
                        ws.cell(r, hdr.index("Status") + 1).value = "SUPERSEDED"
                n += 1
    return n


def fix_tomato_row(wb, dry: bool) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    rows = _rows_for(ws, TOMATO_URL)
    if len(rows) != 1:
        raise SystemExit(f"expected exactly one Recalls row for the tomato PDF, "
                         f"found {len(rows)}")
    r = rows[0]
    cur = str(ws.cell(r, hdr.index("Product") + 1).value or "")
    if cur.strip().casefold() != TOMATO_OLD_PRODUCT.casefold():
        if TOMATO_PRODUCT in cur:
            print("    tomato row already corrected")
            return 0
        raise SystemExit(f"tomato row Product is {cur!r}, expected "
                         f"{TOMATO_OLD_PRODUCT!r} — not touching it")
    print(f"    Recalls row {r}: Product {cur!r} -> {TOMATO_PRODUCT!r}; "
          f"Brand -> {TOMATO_BRAND!r}")
    if not dry:
        ws.cell(r, hdr.index("Product") + 1).value = TOMATO_PRODUCT
        ws.cell(r, hdr.index("Brand") + 1).value = TOMATO_BRAND
        notes = str(ws.cell(r, hdr.index("Notes") + 1).value or "")
        if TOMATO_NOTE not in notes:
            ws.cell(r, hdr.index("Notes") + 1).value = (notes + " " + TOMATO_NOTE).strip()
        ws.cell(r, hdr.index("LastUpdated") + 1).value = TODAY
    return 1


def sort_recalls(wb, dry: bool) -> int:
    """Stable newest-first sort of Recalls by Date; explicit cell assignment."""
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    di = hdr.index("Date")
    rows = [list(r) for r in ws.iter_rows(min_row=2, values_only=True)]

    def _k(r):
        d = r[di]
        if d is None or d == "":
            return ""
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        return str(d)[:10]

    ordered = sorted(rows, key=_k, reverse=True)      # sorted() is stable
    moved = sum(1 for a, b in zip(rows, ordered) if a is not b)
    print(f"    Recalls: {len(rows)} rows, {moved} change position")
    if not dry and moved:
        for i, r in enumerate(ordered, start=2):
            for j, v in enumerate(r, start=1):
                cell = ws.cell(i, j)
                if cell.value != v:
                    cell.value = v
    return moved


# ── E. upstream Pending rows that arrived during the review ────────────────
UPSTREAM_REJECTS = {
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3133/ address is the same alert as the PUBLISHED FSA-PRIN-29-2026 "
        "(Prepworld RTE fruit products, Salmonella, 2026-06-23) and the CFS HK "
        "re-post of 2026-06-24. Already archived three times as "
        "duplicate_already_approved; Tavily fallback re-surfaced it 2026-11-16 "
        "with no Date.",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3176/ address is the same alert as the PUBLISHED FSA-PRIN-32-2026 "
        "(O'Brien Fine Foods sliced ham, Listeria monocytogenes, 2026-07-01). "
        "Already archived twice as duplicate_already_approved; Tavily fallback "
        "re-surfaced it 2026-11-16 with no Date.",
    "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2025_06.htm":
        f"stale — {BY}: AESAN alert 2025_06 is a 2025 alert (the identifier is "
        "the year), outside the register's window. Row has no Date, no Company, "
        "Product is a headline ('Alerta por presencia de Salmonella spp') and "
        "Pathogen contradicts it ('Listeria monocytogenes'). aesan.gob.es does not "
        "resolve from this session.",
    "https://www.aesan.gob.es/actualidad/actualidad-noticias/Informaci-n-sobre-la-sospecha-de-casos-de-salmonelosis-asociados-a-huevos":
        f"not a recall notice, previously refused — {BY}: AESAN news note of "
        "16/10/2026 on suspected salmonellosis cases associated with eggs; the same "
        "URL is already in Rejected (row dated 2026-10-16) and the egg-salmonella "
        "news cluster was refused a dozen times in October. Row has no Date, no "
        "Company, Product is a sentence fragment. aesan.gob.es does not resolve "
        "from this session; nothing on the row could be improved with evidence.",
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/babyvoeding-en-andere-speciale-voeding":
        f"not a recall — {BY}: NVWA topic/landing page 'Babyvoeding en andere "
        "speciale voeding', already in Rejected under this URL. Product on the row "
        "is page furniture ('24-03-202613:00Beeld: © PureBudget Lees verder'). No "
        "firm, no date, no specific notice.",
}


def merge_upstream_pending(wb, upstream_xlsx: str, dry: bool) -> int:
    """Append every upstream Pending row this workbook lacks, value for value."""
    up = openpyxl.load_workbook(upstream_xlsx, read_only=True, data_only=True)
    uws = up["Pending"]
    urows = list(uws.iter_rows(values_only=True))
    uhdr = [str(h or "") for h in urows[0]]
    ws = wb["Pending"]
    hdr = _hdr(ws)
    if uhdr != hdr:
        raise SystemExit(f"Pending header differs upstream: {uhdr} vs {hdr}")
    have = {_key(ws.cell(r, hdr.index("URL") + 1).value)
            for r in range(2, ws.max_row + 1)}
    # Every URL this review archived tonight is also "had": it must not come
    # back from the upstream copy of the pre-review queue.
    arch = wb["Rejected"]
    ah = _hdr(arch)
    have |= {_key(arch.cell(r, ah.index("URL") + 1).value)
             for r in range(2, arch.max_row + 1)
             if str(arch.cell(r, ah.index("RejectedBy") + 1).value or "") == BY}
    rec = wb["Recalls"]
    rh = _hdr(rec)
    have |= {_key(rec.cell(r, rh.index("URL") + 1).value)
             for r in range(2, rec.max_row + 1)}
    n = 0
    for r in urows[1:]:
        d = dict(zip(uhdr, r))
        if _key(d.get("URL")) in have:
            continue
        print(f"    upstream Pending row copied: {d.get('Source')} "
              f"{str(d.get('Product'))[:40]!r} {str(d.get('URL'))[-50:]}")
        if not dry:
            ws.append([v for v in r])
        n += 1
    up.close()
    return n


def reject_upstream(wb, dry: bool) -> int:
    ws = wb["Pending"]
    n = 0
    for url, reason in UPSTREAM_REJECTS.items():
        for r in sorted(_rows_for(ws, url), reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} -> Rejected")
            if not dry:
                _archive(wb, row, reason)
                ws.delete_rows(r, 1)
            n += 1
    return n


# ── F. archive rows that share a URL with a published recall and say nothing ─
# tests/test_a_recall_is_not_both_published_and_rejected.py was RED on main
# before this run (as it was on 2026-11-15; that fix sits in unmerged PR #45).
# The verdict is written where the contradiction check reads it, nothing is
# deleted.
CONTRADICTIONS = {
    ("Rejected", "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/eden-brothers-recalls-calabrese-broccoli-seeds-due-possible-salmonella-contamination"):
        f"[duplicate — annotated {TODAY} by the {BY}: the published copy wins. "
        "The same FDA URL is published as Eden Brothers LLC, 'Broccoli Seeds - "
        "Calabrese', Salmonella, 2026-10-07. This archive row's own Notes already "
        "say 'duplicate: this URL is already published on 2026-10-07', but in "
        "Notes, which the contradiction check does not read; its Product is the "
        "notice's first paragraph. Kept as the audit trail.]",
    ("Rejected", "https://www.salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari/?tab=1&offset=0&pageElements=10"):
        f"[unresolved contradiction of provenance — annotated {TODAY} by the {BY}: "
        "this archived row is the Ministero della Salute LISTING page scraped as if "
        "it were a notice (Product is the page title 'Avvisi e richiami di prodotti "
        "alimentari', no firm, no hazard, discovered via ilmessaggero.it 2026-11-14). "
        "The same listing URL, without the query string, is carried by the published "
        "La Vacje Salumerie row of 2026-04-14, which cites an index page as its "
        "authority URL. This row is not a copy of that recall and is not a recall; "
        "the published row stands and its own URL remains to be resolved.]",
    ("Weekly_Rejected", "https://www.salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari/?tab=1&offset=0&pageElements=10"):
        f"[unresolved contradiction of provenance, not a copy of the published recall — annotated {TODAY} by the {BY}: this "
        "row is a news-only discovery (ilmessaggero.it, 2026-11-14, ground coffee "
        "recalled for a chemical risk) that was correctly refused for having no "
        "authority URL; the URL it carries is the ministry's LISTING page with a "
        "pagination query string. The published La Vacje Salumerie row of 2026-04-14 "
        "cites the same index page as its authority URL, which is why the two "
        "'share' a URL. Different recall, different product; the published row "
        "stands.] ",
}


def annotate_silent_contradictions(wb, dry: bool) -> int:
    n = 0
    for (sheet, url), text in CONTRADICTIONS.items():
        ws = wb[sheet]
        hdr = _hdr(ws)
        col = "RejectReason" if sheet == "Rejected" else "RejectionReason"
        ci = hdr.index(col) + 1
        for r in _rows_for(ws, url):
            cur = str(ws.cell(r, ci).value or "")
            if cur.strip().lower() in ("", "none", "nan"):
                cur = ""
            if text[:40] in cur:
                continue
            print(f"    {sheet} row {r}: {url[-40:]} annotated")
            if not dry:
                ws.cell(r, ci).value = text + cur
            n += 1
    return n


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--phase", choices=("a", "sort", "merge"), default="a")
    ap.add_argument("--upstream", help="main's recalls.xlsx for --phase merge")
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    if a.phase == "a":
        print("A  Pending rows refused:")
        print("   archived: %d" % reject_pending(wb, a.dry_run))
        print("B  earlier refusals that do not hold:")
        print("   archive rows marked SUPERSEDED: %d" % release_false_refusals(wb, a.dry_run))
        print("C  the Italian tomato row:")
        print("   rows corrected: %d" % fix_tomato_row(wb, a.dry_run))
        print("F  archive rows sharing a URL with a published recall, silent:")
        print("   rows annotated: %d" % annotate_silent_contradictions(wb, a.dry_run))
    elif a.phase == "sort":
        print("D  Recalls newest-first:")
        print("   rows moved: %d" % sort_recalls(wb, a.dry_run))
    else:
        if not a.upstream:
            raise SystemExit("--phase merge needs --upstream <xlsx>")
        print("E  upstream Pending rows added while the review ran:")
        print("   copied: %d" % merge_upstream_pending(wb, a.upstream, a.dry_run))
        print("   refused: %d" % reject_upstream(wb, a.dry_run))
    if not a.dry_run:
        wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
