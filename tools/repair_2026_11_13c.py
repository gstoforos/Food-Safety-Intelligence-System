# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-13 — clear Pending, retire one archive
verdict, move one out-of-scope published row to the archive, re-sort.

CONTEXT. The operator reviews of 2026-11-08, 11-10, 11-11 and 11-12 (PRs
#38, #39, #40, #42) were never merged, so main still carries the Pending
rows every one of them cleared. Each verdict below was taken again on
2026-11-13: at the regulator's page where the page is reachable from this
session (rappel.conso.gouv.fr, recalls-rappels.canada.ca), and from the
register's own archive and the row's own content where the host is not
(fda.gov, fsai.ie, aesan.gob.es, favv-afsca.be, livsmedelsverket.se,
alerts.food.gov.uk, thencc.org.za, nafdac.gov.ng, lebensmittelwarnung.de).
The reason says which.

PHASE supersede — Weekly_Rejected holds the confirm-agent's 2026-11-11 copy
of RappelConso fiche 24064 with RejectionReason "NO_REASON_RECORDED …
treat as UNAUDITED". The defect it names in Notes (Company '/' and Brand
'/' are page debris) is repaired through the curator tonight from the fiche
itself; the archive row is marked SUPERSEDED so the re-promotion guard
no longer reads it as a live verdict. Nothing is deleted.

PHASE reject — eighteen Pending rows go to the permanent Rejected sheet
with a terminal reason each. Rows are matched by exact URL; if any URL is
missing the script stops before writing.

PHASE unpublish — one PUBLISHED row (Recalls, lebensmittelwarnung.de
"Baby spoons", Bisphenol AF) describes a food-contact article, not a food.
Non-food articles are outside the AFTS scope printed on every report. The
row is moved, whole, to Rejected with its reason; nothing is deleted.

PHASE sort — Recalls newest-first via merge_master.sort_rows, content
multiset verified identical before saving.

    python tools/repair_2026_11_13c.py supersede
    python tools/repair_2026_11_13c.py reject
    python tools/repair_2026_11_13c.py unpublish
    python tools/repair_2026_11_13c.py sort
    python tools/repair_2026_11_13c.py merge_main <path-to-main-recalls.xlsx>
    python tools/repair_2026_11_13c.py reject_tavily

PHASE merge_main — while this review ran, main received "Tavily gap-finder
(standalone): +4 rows to Pending" (4a45177). Those four rows are appended
to this workbook's Pending by URL so the rebase drops nothing of the
pipeline's; nothing else differed between the two workbooks (every other
sheet had identical row counts).

PHASE reject_tavily — the four merged rows, decided the same way as the
rest: a duplicate FSA /article/ twin, an MPI media release about a report,
an MPI dietary-survey PDF and a RASFF table-header scrape of a notification
whose id (470205) predates the register's range (814459+).
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-13"
BY = f"operator review {TODAY}"

URL_24064 = "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne"
BANNER_24064 = (
    f"[SUPERSEDED {TODAY} — the defect behind this refusal (publish gate: "
    f"Company '/' and Brand '/' are page debris, named in Notes) was repaired "
    f"through the curator on {TODAY} from fiche 24064 itself (Fiche réf. "
    f"2026-11-0077, 11/11/2026, GAEC LES DEUX SAVOIES, 'REBLOCHON DE SAVOIE "
    f"FERMIER 450g', Motif 'Présence de Escherichia Coli STEC', Risques "
    f"'Escherichia coli shiga toxinogène (STEC)'). The row passes the full "
    f"publish gate and is PUBLISHED in Recalls; this archive row is kept for "
    f"the trail only. Operator review {TODAY}.] ")

# (URL, short tag, reason)
REJECTS = [
    ("https://www.fsai.ie/news-and-alerts/latest-news/four-enforcement-orders-served-on-food-busines-(2)",
     "NOT A RECALL — ENFORCEMENT ORDERS",
     "FSAI news item 'Four Enforcement Orders served on food businesses in "
     "October' (11/11/2026): closure orders on food premises, not a product "
     "recall. Company empty, Product is the page's date line, Pathogen "
     "'Rodent contamination' is an inspection finding. The row's own Notes "
     "carry the page text and the URL agent's rejection; archived to "
     "Weekly_Rejected 2026-11-11 and rejected by the operator review of "
     "2026-11-12 (PR #42, unmerged). fsai.ie is not reachable from this "
     "session; verdict taken from the row's captured page text."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-infant-botulism-powdered-infant-formula-june-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the June-2026 infant botulism event "
     "(Nara Organics powdered infant formula). The event is published in "
     "Recalls twice (FDA recall notice and CDC investigation, 2026-06-13, "
     "Outbreak=1). Company and Brand empty, Product is the page title, Date "
     "2026-11-11 is the feed's re-listing date. Same verdict as the operator "
     "review of 2026-11-12 (PR #42, unmerged), which read the page."),
    ("https://www.aesan.gob.es/actualidad/actualidad-noticias/La-EFSA-actualizacion",
     "NOT A RECALL — EFSA RISK-ASSESSMENT NEWS",
     "AESAN news item 'La EFSA actualiza la evaluación de riesgos de las "
     "toxinas de Alternaria en los alimentos y piensos' (11/11/2026): an EFSA "
     "scientific-opinion announcement, not a recall. Company empty, Product "
     "is the headline plus the scraper string 'calendar_today 11/11/2026'. "
     "Archived to Weekly_Rejected by the URL agent; rejected by the operator "
     "review of 2026-11-12 (PR #42, unmerged)."),
    ("https://www.aesan.gob.es/ca/actualidad/actualidad-noticias/La-EFSA-actualizacion",
     "NOT A RECALL — EFSA RISK-ASSESSMENT NEWS (CATALAN HOST PATH)",
     "Same AESAN news item as the /actualidad/ row, served under the /ca/ "
     "language path. Not a recall; see the sibling row's reason."),
    ("https://aesan.gob.es/eu/actualidad/actualidad-noticias/La-EFSA-actualizacion",
     "NOT A RECALL — EFSA RISK-ASSESSMENT NEWS (BASQUE HOST PATH)",
     "Same AESAN news item as the /actualidad/ row, served under the /eu/ "
     "language path. Not a recall; see the sibling row's reason."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24016/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24016 (Champibon dried morels, 05/11/2026): Motif "
     "'probable contamination d'un lot de morilles par une toxine susceptible "
     "d'être à l'origine d'intoxication alimentaire', Risques 'Autre risque'. "
     "No toxin, chemical or micro-organism is named; Pathogen is empty and "
     "the register names its hazard. Sibling fiche 24047 read at source "
     f"{TODAY} carries the identical Motif. Archived 2026-11-05 and 2026-11-09 "
     "(Confirmer) and rejected by the operator reviews of 2026-11-08, 11-10, "
     "11-11 and 11-12."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24047/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     f"RappelConso fiche 24047 read at source {TODAY}: Fiche réf. 2026-11-0060, "
     "09/11/2026, CHAMPIBON dried morels 400 g, Motif 'probable contamination "
     "d'un lot de morilles par une toxine susceptible d'être à l'origine "
     "d'intoxication alimentaire', Risques 'Autre risque'. The fiche names no "
     "toxin, chemical or micro-organism; Pathogen is empty and the register "
     "names its hazard. Same verdict as the operator reviews of 2026-11-08, "
     "11-10, 11-11 and 11-12 and the Confirmer archive of 2026-11-09."),
    ("https://rappel.conso.gouv.fr/fiche-rappel/24012/interne",
     "HAZARD NOT NAMED BY THE REGULATOR",
     "RappelConso fiche 24012 (Champibon dried morels, 04/11/2026): same Motif "
     "as fiche 24047 ('une toxine', Risques 'Autre risque'); no hazard named, "
     "Pathogen empty, Product is a GTIN and a date. Archived 2026-11-04 and "
     "rejected by the operator reviews of 2026-11-08, 11-10, 11-11 and 11-12."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the E. coli O26:H11 raw-milk cheese "
     "event, published in Recalls from the firm's own FDA notice (Sierra "
     "Nevada Cheese Company, 2026-09-29, Outbreak=1). This URL has been "
     "archived six times for the same reason (2026-09-01, 09-26, 10-01, "
     "10-04, 10-09, 11-08) and rejected 2026-11-12 (PR #42, unmerged)."),
    ("https://www.fda.gov/food/outbreaks-foodborne-illness/investigation-multistate-outbreak-cyclospora-illnesses-iceberg-lettuce-july-2026",
     "DUPLICATE OF A PUBLISHED RECALL",
     "FDA outbreak-investigation page for the Cyclospora iceberg-lettuce "
     "event, published in Recalls from the recalling firm's notice (Taylor "
     "Fresh Foods, 2026-07-18, Outbreak=1). Company and Brand empty, Product "
     "is the page title. Archived to Weekly_Rejected 2026-11-07; rejected by "
     "the operator reviews of 2026-11-11 and 11-12 (unmerged)."),
    ("https://favv-afsca.be/nl/themas/controles/interne-controles-door-bedrijven/meldingsplicht",
     "NOT A RECALL — ANIMAL FEED GUIDANCE",
     "AFSCA reporting-obligation page carrying a circular on Salmonella "
     "control in ANIMAL FEED (PCCB/S1/483559, 'bestrijding van Salmonella in "
     "diervoeders' in the row's own captured text). Not a recall notice, no "
     "product, no date, and feed is outside the human-food scope. Terminal "
     "operator verdict of 2026-08-15 stands; rejected again 2026-11-10, "
     "11-11 and 11-12."),
    ("https://kontrollwiki.livsmedelsverket.se/artikel/127/atgarder-vid-fynd-av-listeria-i-livsmedel",
     "NOT A RECALL — INSPECTOR GUIDANCE",
     "Livsmedelsverket Kontrollwiki article 127, 'Åtgärder vid fynd av "
     "Listeria i livsmedel', is guidance for inspectors (the row's own Notes "
     "quote 'Agerande vid icke säkra livsmedel … artikel 14 i förordning (EG) "
     "nr 178/2002'). The headline sits in Company and Brand, 'Kontrollwiki' "
     "in Product, no date. Not a recall. Rejected by the operator reviews of "
     "2026-11-11 and 11-12."),
    ("https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella",
     "DUPLICATE — FSA /article/ TWIN OF A PUBLISHED ALERT",
     "The FSA serves every alert at both /news-alerts/alert/<ref> and "
     "/article/<id>/<slug>. Prepworld RTE fruit products, Salmonella, is "
     "published in Recalls as FSA-PRIN-29-2026 (2026-06-23) and via the CFS "
     "Hong Kong mirror (2026-06-24). This copy has no Date. Same verdict as "
     "the operator review of 2026-08-22 (Rejected sheet) and 2026-11-12."),
    ("https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes",
     "DUPLICATE — FSA /article/ TWIN OF A PUBLISHED ALERT",
     "O'Brien Fine Foods ham, Listeria monocytogenes, is published in Recalls "
     "as FSA-PRIN-32-2026 (2026-07-01). This /article/ copy has no Date. Same "
     "verdict as the operator review of 2026-08-22 and 2026-11-12."),
    ("https://alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-products-because-of-possible-contamination-with-Listeria-monocytogenes",
     "DUPLICATE — FSA /article/ TWIN OF A PUBLISHED ALERT",
     "Greencore pasta salads, Listeria monocytogenes, is published in Recalls "
     "as FSA-PRIN-38-2026 (2026-08-07, Asda Cheese & Tomato Pasta Salad). "
     "Third address for the same alert; the morning fix pass of 2026-11-08 "
     "recorded the duplicate in the Rejected sheet. No Date on this copy."),
    ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium",
     "ANIMAL FEED — OUT OF SCOPE",
     "Laub's Feeding & Cubing, Inc. recalls Grade A Mini Hay Cubes for "
     "potential Clostridium botulinum: horse feed under FDA Animal & "
     "Veterinary, illnesses in horses only (notice read by the operator "
     "review of 2026-11-11). Company is the headline fragment, Pathogen and "
     "Date empty. Animal feed is outside the human-food scope. Rejected "
     "2026-11-11 and 11-12 (unmerged)."),
    ("https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/",
     "STALE ALERT — 2024 STATEMENT",
     "NCC media statement dated 3 October 2024 on the September-2024 BM Foods "
     "Deli Hummus recall, carried in Pending with Date 2026-11-12. Twelfth "
     "archive copy; the terminal verdict of 2026-11-06 ('TERMINAL, UPHELD') "
     "and the 2026-11-12 rejection stand."),
    ("https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/",
     "NOT A RECALL — WATCHLIST PENDING INVESTIGATION, NO HAZARD, NO PRODUCT",
     "NAFDAC Public Alert No. 043/2026 places products marketed by Mofus "
     "Nigeria Ltd on a watchlist 'pending regulatory investigation' (the "
     "row's own Reason). Product and Pathogen are empty: no hazard is named "
     "and no product is identified, so the register cannot hold it. Archived "
     "by gap_finder/ng/rules.py seven times and by the Confirmer ('Product is "
     "empty; Pathogen is empty'). nafdac.gov.ng is not reachable from this "
     "session; if the investigation names a hazard later, the row can be "
     "re-queued from that notice."),
]

REJECTS_TAVILY = [
    ("https://alerts.food.gov.uk/article/6841/Tesco-recalls-Tesco-Finest-Mozzarella-Pearls--Pesto-Orzo-Salad-because-of-possible-contamination-with-E.-coli-STEC",
     "DUPLICATE — FSA /article/ TWIN OF A PUBLISHED ALERT",
     "Tesco Finest Mozzarella Pearls & Pesto Orzo Salad, STEC, is published in "
     "Recalls as FSA-PRIN-41-2026 (2026-08-25). The FSA serves every alert at "
     "both /news-alerts/alert/<ref> and /article/<id>/<slug>; this /article/ "
     "copy has no Date. Same verdict as the operator review of 2026-08-27 "
     "(Rejected sheet). Arrived from the Tavily gap-finder at 20:11Z while "
     "this review ran."),
    ("https://www.mpi.govt.nz/news/media-releases/new-reports-highlight-food-recall-system-at-work",
     "NOT A RECALL — MEDIA RELEASE ABOUT A REPORT",
     "MPI media release 'New reports highlight food recall system at work': "
     "a New Zealand Food Safety report on recall statistics (the row's own "
     "Notes: 'identifies Salmonella in imported sesame seed-based products as "
     "the most significant food safety…'). Headline sits in Company and "
     "Brand, Product is 'system at work', no date, no product recalled. "
     "Archived before (Rejected, no reason recorded). mpi.govt.nz is not "
     "reachable from this session; verdict from the row's captured text."),
    ("https://www.mpi.govt.nz/dmsdocument/25847/direct",
     "NOT A RECALL — DIETARY SURVEY DOCUMENT",
     "MPI document 25847 is a food-consumption survey report; the scraper "
     "matched the phrase '24-hour dietary recall' (the row's own Product "
     "text). No product, no hazard, no date. Archived before by the URL "
     "agent ('URL is a PDF and not the official recall page')."),
    ("https://webgate.ec.europa.eu/rasff-window/screen/notification/470205",
     "SCRAPER DEBRIS — RASFF TABLE HEADER, NOTIFICATION OUTSIDE THE REGISTER'S RANGE",
     "Product and Notes are the RASFF Window column headers ('Organisations · "
     "Reference · Subject · Product type · …'); Company and Date empty. "
     "Notification id 470205 predates every RASFF id the register holds "
     "(814459–886588, 2026 only). Archived before by the Confirmer ('Date is "
     "empty; Company is empty'). The RASFF Window is a JavaScript shell and "
     "cannot be read from this session."),
]

UNPUBLISH = [
    ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/2026/11_November/261112_13_NI_Waermesensorloeffel/261112_13_NI_Waermesensorloeffel.html",
     "OUT OF SCOPE — NON-FOOD ARTICLE (FOOD-CONTACT MATERIAL)",
     "lebensmittelwarnung.de notice 261112_13_NI 'Wärmesensorlöffel' (heat-"
     "sensor baby spoons, Nuby, sold by Rossmann), hazard 'Bisphenol AF "
     "exceeding limit'. The product is a utensil, not a food: the register's "
     "scope is pathogens, biotoxins, mycotoxins, foreign material, pest and "
     "chemical hazards IN HUMAN FOOD, and non-food articles are excluded. "
     "Published 2026-11-13 by the news-discovery path (n-tv.de) without a "
     "scope check. lebensmittelwarnung.de is not reachable from this session; "
     "the verdict rests on the row's own Product ('Baby spoons') and the "
     "notice slug, which are unambiguous."),
]


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    return str(u or "").strip().lower().rstrip("/")


def supersede() -> int:
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Weekly_Rejected"]
    hdr = _head(ws)
    ucol, rcol, mcol = (hdr.index("URL") + 1, hdr.index("RejectionReason") + 1,
                        hdr.index("Reviewed") + 1)
    hits = [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, ucol).value) == URL_24064.lower()]
    if len(hits) != 1:
        print(f"STOP — expected exactly one Weekly_Rejected row for 24064, found {len(hits)}")
        return 2
    r = hits[0]
    prior = str(ws.cell(r, rcol).value or "")
    if "SUPERSEDED" in prior:
        print("already superseded; nothing written")
        return 0
    ws.cell(r, rcol).value = BANNER_24064 + prior
    ws.cell(r, mcol).value = "SUPERSEDED"
    wb.save(XLSX)
    print(f"Weekly_Rejected row {r} marked SUPERSEDED")
    return 0


def _archive(wb, src_name: str, items, tag_prefix: str) -> int:
    src, arch = wb[src_name], wb["Rejected"]
    sh, ah = _head(src), _head(arch)
    ucol = sh.index("URL") + 1
    index = {}
    for r in range(2, src.max_row + 1):
        index.setdefault(_key(src.cell(r, ucol).value), []).append(r)
    missing = [u for u, _t, _r in items if _key(u) not in index]
    if missing:
        print(f"STOP — not in {src_name} (main has moved; re-read the sheet):")
        for u in missing:
            print("   ", u)
        return 2
    multi = [u for u, _t, _r in items if len(index[_key(u)]) != 1]
    if multi:
        print(f"STOP — more than one {src_name} row per URL:", multi)
        return 2
    before_s, before_a = src.max_row - 1, arch.max_row - 1
    to_delete = []
    for url, tag, reason in items:
        r = index[_key(url)][0]
        row = {h: src.cell(r, i + 1).value for i, h in enumerate(sh)}
        full = f"{BY}: {tag}. {reason}"
        notes = str(row.get("Notes") or "").strip()
        row["Notes"] = (f"[{tag_prefix} {src_name} {TODAY} — TERMINAL: {tag}. "
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
        to_delete.append(r)
        print(f"  {src_name} row {r:>4} -> Rejected: {tag[:48]:<48} "
              f"{str(row.get('Product') or row.get('URL'))[:45]!r}")
    for r in sorted(to_delete, reverse=True):
        src.delete_rows(r, 1)
    print(f"{src_name} {before_s} -> {src.max_row - 1}; "
          f"Rejected {before_a} -> {arch.max_row - 1}")
    return 0


def reject() -> int:
    wb = openpyxl.load_workbook(XLSX)
    rc = _archive(wb, "Pending", REJECTS, "ARCHIVED FROM")
    if rc == 0:
        wb.save(XLSX)
    return rc


def unpublish() -> int:
    wb = openpyxl.load_workbook(XLSX)
    rc = _archive(wb, "Recalls", UNPUBLISH, "REMOVED FROM")
    if rc == 0:
        wb.save(XLSX)
    return rc


def reject_tavily() -> int:
    wb = openpyxl.load_workbook(XLSX)
    rc = _archive(wb, "Pending", REJECTS_TAVILY, "ARCHIVED FROM")
    if rc == 0:
        wb.save(XLSX)
    return rc


def _archived_tonight(wb) -> set:
    from pipeline.merge_master import _normalize_url_for_dedup as N
    w = wb["Rejected"]
    h = _head(w)
    ucol, bcol, acol = (h.index("URL") + 1, h.index("RejectedBy") + 1,
                        h.index("RejectedAt") + 1)
    return {N(str(w.cell(r, ucol).value or "")) for r in range(2, w.max_row + 1)
            if str(w.cell(r, bcol).value or "") == BY
            and str(w.cell(r, acol).value or "")[:10] == TODAY}


def prune_rearchived() -> int:
    """Drop from Pending any row whose URL this review already archived to
    Rejected tonight (first merge_main run re-added 18 of them from main's
    older Pending). The Rejected copy, with tonight's reason, is the record."""
    from pipeline.merge_master import _normalize_url_for_dedup as N
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    ucol = _head(ws).index("URL") + 1
    done = _archived_tonight(wb)
    hits = [r for r in range(2, ws.max_row + 1)
            if N(str(ws.cell(r, ucol).value or "")) in done]
    for r in sorted(hits, reverse=True):
        print(f"  pruned re-added Pending row {r}: {str(ws.cell(r, ucol).value)[:80]}")
        ws.delete_rows(r, 1)
    wb.save(XLSX)
    print(f"Pending -> {ws.max_row - 1} row(s)")
    return 0


def merge_main(main_xlsx: str) -> int:
    """Append to Pending every Pending row of main's workbook whose URL this
    workbook does not hold in Pending or Recalls. Archived copies do not
    count: the row arrived on main tonight and tonight's verdict on it must
    be recorded, not inferred from an older one."""
    from pipeline.merge_master import _normalize_url_for_dedup as N
    src = openpyxl.load_workbook(main_xlsx, read_only=True, data_only=True)["Pending"]
    shdr = [str(c.value) for c in next(src.iter_rows(min_row=1, max_row=1))]
    srows = [dict(zip(shdr, r)) for r in src.iter_rows(min_row=2, values_only=True)
             if any(v not in (None, "") for v in r)]
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    hdr = _head(ws)
    if hdr != shdr:
        print("STOP — Pending headers differ between main and this workbook")
        return 2
    have = set()
    for name in ("Pending", "Recalls"):
        w = wb[name]
        ucol = _head(w).index("URL") + 1
        for r in range(2, w.max_row + 1):
            have.add(N(str(w.cell(r, ucol).value or "")))
    # Rows this review archived tonight are decided; re-adding them from
    # main's older Pending would undo the verdict just written.
    have |= _archived_tonight(wb)
    added = 0
    for r in srows:
        if N(str(r.get("URL") or "")) in have:
            continue
        ws.append([r.get(h) for h in hdr])
        added += 1
        print(f"  merged from main: {str(r.get('URL'))[:90]}")
    wb.save(XLSX)
    print(f"Pending +{added} row(s) from main")
    return 0


def sort_recalls() -> int:
    from pipeline.merge_master import sort_rows
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Recalls"]
    hdr = _head(ws)
    rows = [list(r) for r in ws.iter_rows(min_row=2, values_only=True)
            if any(v not in (None, "") for v in r)]
    before = Counter(tuple(str(v) for v in r) for r in rows)
    ordered = sort_rows([dict(zip(hdr, r)) for r in rows])
    after = Counter(tuple(str(d.get(h)) for h in hdr) for d in ordered)
    if before != after:
        print("STOP — content multiset changed during sort; nothing saved")
        return 2
    if len(ordered) != ws.max_row - 1:
        print(f"STOP — {len(ordered)} rows vs sheet {ws.max_row - 1}; nothing saved")
        return 2
    moved = 0
    for i, d in enumerate(ordered, start=2):
        for j, h in enumerate(hdr, start=1):
            cell = ws.cell(i, j)
            if cell.value != d.get(h):
                cell.value = d.get(h)
                moved += 1
    wb.save(XLSX)
    print(f"Recalls re-sorted newest-first: {len(ordered)} rows, {moved} cells moved")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "merge_main" and len(sys.argv) > 2:
        raise SystemExit(merge_main(sys.argv[2]))
    fn = {"supersede": supersede, "reject": reject, "reject_tavily": reject_tavily,
          "prune_rearchived": prune_rearchived,
          "unpublish": unpublish, "sort": sort_recalls}.get(cmd)
    if fn is None:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(fn())
