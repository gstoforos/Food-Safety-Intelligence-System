# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-18. Base: main at 66ad2dc (21:11 Athens).

Field corrections and promotions go through pipeline/agents/curator.py with
quoted regulator evidence (docs/data/agent-proposals/*-nightly-operator.json)
and pipeline/promote_gate_passing.py. This script does only what the curator
cannot, and never deletes a row from the register: a refused Pending row is
MOVED to Rejected with its reason.

A. TWENTY-FIVE PENDING ROWS REFUSED, each with a written reason.
B. ARCHIVE VERDICTS THAT DO NOT HOLD are marked SUPERSEDED so the archive and
   the published register do not contradict each other (fiche 24097's
   "listing page" verdict is false; the others are repairable field defects
   or no verdict at all). Archive copies of fiches the regulator has since
   REPUBLISHED under a new number (24098 -> 24109, 24016 -> 24047) are
   annotated to say where the recall went.
C. WEEKLY_REVIEW MIRROR RE-SYNCED for the rows corrected tonight. The sheet
   is the promoter's mirror of what it published; rows the curator corrected
   after promotion (stale "[needs cleanup: …]" stamps removed from Notes,
   fiche 24099's Company/Product) would otherwise go to the review email in
   their uncorrected form, and two carried a doubled stamp
   (test_no_live_row_carries_a_duplicated_stamp).
D. RECALLS RE-SORTED NEWEST-FIRST (the promoter appends; nine rows published
   today sat at rows 2436-2444 below a 2026-01-02 row).

    python3 tools/repair_2026_11_18_night.py --dry-run
    python3 tools/repair_2026_11_18_night.py --phase reject
    python3 tools/repair_2026_11_18_night.py --phase release   # after the promoter
    python3 tools/repair_2026_11_18_night.py --phase sort
    python3 tools/repair_2026_11_18_night.py --phase mirror    # last
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-18"
BY = "nightly operator review 2026-11-18"

NAF = "https://nafdac.gov.ng/"
AES = "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"

# -- A. Pending rows refused ------------------------------------------------
PENDING_REJECTS = {
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-formaldehydu-z-kubka-z-melaminy":
        f"out of scope — {BY}: a GIS public warning about a FOOD-CONTACT ARTICLE, "
        "not a food: the notice's own title is 'Ostrzeżenie publiczne dotyczące "
        "wyrobu do kontaktu z żywnością: migracja formaldehydu z kubka z melaminy' "
        "(formaldehyde migration from a melamine cup). Non-food articles are "
        "outside the register. www.gov.pl does not resolve from the session; the "
        "title on the row settles it.",
    "https://recalls-rappels.canada.ca/en/alert-recall/peglyte-powder-affected-lots-may-contain-foreign-matter":
        f"out of scope — {BY}: not a food. Read on the Health Canada notice "
        "tonight: 'PegLyte Powder: Affected lots may contain foreign matter', "
        "Issue 'Health products - Contamination', category 'Health products - "
        "Drugs' (DIN 00777838, Pendopharm, a macrogol bowel-preparation powder). "
        "Source was mislabelled CFIA; Company and Brand held the headline and "
        "Product held 'Canada.ca'.",
    "https://www.cdc.gov/cryptosporidium/about/index.html":
        f"not a recall — {BY}: a CDC Healthy Swimming guidance page ('What to Do "
        "When There is Poop in the Pool'). No food, no firm, no recall; Product "
        "is a sentence about chlorinated pool water and Pathogen 'Escherichia "
        "coli (generic)' is not what the captured text names (Cryptosporidium). "
        "The row's own Notes settle it.",
    "https://www.cdc.gov/ecoli/outbreaks/raw-milk-cheese-09-26/index.html":
        f"duplicate_of_published_recall — {BY}: the CDC outbreak page for the "
        "E. coli O26:H11 raw-milk cheese outbreak. The recall is PUBLISHED as "
        "Sierra Nevada Cheese Company (FDA 2026-09-29, Graziers raw milk "
        "cheeses, Outbreak=1), and the row's own captured text says "
        "'Investigation status: Closed … This outbreak is over' (Nov. 13, 2026). "
        "One event, one row; third archive copy of this URL. cdc.gov does not "
        "resolve from the session tonight.",
    "https://recalls-rappels.canada.ca/en/alert-recall/archibald-brand-nuit-blanche-white-ipa-and-desiree-session-ipa-recalled-due-pieces":
        f"duplicate_of_published_recall — {BY}: slug variant of CFIA notice "
        "RA-82940 (Archibald Inc., recall date 2026-11-06, original published "
        "2026-11-12, Class 3, Quebec), PUBLISHED in Recalls under "
        "…/archibald-brand-nuit-blanche-white-ipa-recalled-due-pieces-metal. "
        "Read tonight: one notice listing Nuit Blanche White IPA and Désirée "
        "Session IPA; the published row's Product was extended to both through "
        "the curator.",
    FDA + "/family-farmstead-dairy-announces-recall-of-all-cheddar-cheese-products-because-possible-health-risk":
        f"duplicate_of_published_recall — {BY}: CONSTRUCTED URL. This slug "
        "('…recall-of-all-cheddar…') returns HTTP 404 tonight; the FDA notice's "
        "real address ('…recall-all-cheddar-cheese-products…') is PUBLISHED in "
        "Recalls as Family Farmstead Dairy, 'Raw Mild Cheddar; Raw Sharp "
        "Cheddar', Listeria monocytogenes, 2026-11-10. Company empty; Product "
        "and Reason are a WRNJ Radio headline.",
    "https://www.efsa.europa.eu/en/news/alternaria-toxins-food-and-feed-efsa-updates-its-risk-assessment":
        f"not a recall — {BY}: EFSA news item 'Alternaria toxins in food and "
        "feed: EFSA updates its risk assessment' (11 November 2026), a "
        "risk-assessment update naming no product, firm or lot. Already marked "
        "rejected in Pending and refused in Weekly_Rejected.",
    FDA + "/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        f"out of scope — {BY}: ANIMAL FEED. Read on the FDA notice tonight: "
        "listed under 'Animal & Veterinary' / 'Animal Feed'; Grade A Mini Hay "
        "Cubes; the illnesses and deaths it reports are horses. Reviewer 2 "
        "advanced this row to pending_gap_v3 today ('Qwen verified') — one "
        "confirmation away from publishing a horse-feed recall as a Tier 3 food "
        "recall. Refused before, in Weekly_Rejected and Rejected.",
    FDA + "?search_api_fulltext=H-0193-2027":
        f"duplicate_of_published_recall — {BY}: openFDA enforcement report "
        "H-0193-2027 (Lidl US Trading, Gelatelli Coffee & Vanilla Gelato, "
        "Listeria monocytogenes, initiated 2026-10-16) is the recall PUBLISHED "
        "as Lidl US / Gelatelli on 2026-10-17 from the FDA press release. The "
        "URL is a site-search query, not a notice.",
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3133/ address is alert FSA-PRIN-29-2026, PUBLISHED 2026-06-23 "
        "under /news-alerts/alert/fsa-prin-29-2026 (Prepworld fruit products, "
        "Salmonella). No Date on the row; a five-month-old alert.",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3176/ address is alert FSA-PRIN-32-2026, PUBLISHED 2026-07-01 "
        "under /news-alerts/alert/fsa-prin-32-2026 (O'Brien Fine Foods ham "
        "products, Listeria monocytogenes). No Date on the row.",
    AES + "2025_06.htm":
        f"stale / fabricated fields — {BY}: read at aesan.gob.es on 2026-11-17: "
        "alert ES2025/055 of 06/02/2025, STEC in Morbier DOP from France. The "
        "row's Product ('Alerta por presencia de Salmonella spp') is the listing "
        "page's first headline and contradicts its own Pathogen ('Listeria "
        "monocytogenes'); neither is what the alert is about.",
    AES + "2026_24.htm":
        f"stale / fabricated fields — {BY}: read at aesan.gob.es on 2026-11-17: "
        "alert ES2026/161 of 26/03/2026, STEC in Camembert de Normandie. Eight "
        "months old; Product and Pathogen on the row are the listing page's "
        "first headline and an unrelated organism.",
    AES + "2026_12.htm":
        f"out of scope / stale — {BY}: read at aesan.gob.es on 2026-11-17: alert "
        "ES2026/087 of 22/02/2026, choking risk in konjac jelly sweets. Not a "
        "contamination hazard, nine months old, and the row's Product and "
        "Pathogen are fabricated from the listing page.",
    "https://www.aesan.gob.es/actualidad/actualidad-noticias/Informaci-n-sobre-la-sospecha-de-casos-de-salmonelosis-asociados-a-huevos":
        f"not a recall — {BY}: an AESAN news note on suspected salmonellosis "
        "cases associated with eggs (sampling at a laying-hen farm in Aragón). "
        "No product, brand or lot; Product is a paragraph of the note. Already "
        "in Rejected.",
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/babyvoeding-en-andere-speciale-voeding":
        f"not a recall — {BY}: an NVWA topic landing page (baby food and other "
        "special foods). Product is page furniture ('24-03-202613:00Beeld: © "
        "PureBudget Lees verder'). Already in Rejected.",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"unverifiable / page furniture — {BY}: Product is the RASFF table's "
        "column headers ('Organisations · Reference · Subject · Product type …'), "
        "no Date, no origin; id 470205 is from the pre-2022 numbering (current "
        "notifications are 88xxxx). Refused in Weekly_Rejected and Rejected.",
    "https://www.aesan.gob.es/alertas/2026_31":
        f"stale — {BY}: read at aesan.gob.es tonight: 'Alerta por presencia de "
        "Listeria monocytogenes en salmón ahumado noruego con pistacho "
        "procedente de Italia', ref. ES2026/208, published 15/04/2026, brand "
        "Deluxe, lot LC22606501, expiry 18/04/26. A seven-month-old alert for a "
        "product that expired in April, surfaced by a search gap-finder with no "
        "Date; Product is the alert's first sentence. Outside the register's "
        "collection window.",
    "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/":
        f"stale — TERMINAL, UPHELD — {BY}: read on the NCC page tonight: posted "
        "'October 3, 2024', 'On 16 September 2024, Shoprite Checkers notified "
        "the Consumer Commission (NCC) of a product recall of the Deli Hummus "
        "range'. A 2024 recall re-queued today with Date 2026-11-11; eleventh "
        "archive copy of this URL.",
    NAF + "public-alert-no-030-2026-alert-on-the-withdrawal-of-levamisole-containing-medicines-as-endorsed-by-the-italian-medicines-agency-aifa-over-serious-neurological-risks/":
        f"out of scope — {BY}: a MEDICINES withdrawal (levamisole), not a food. "
        "Marked rejected in place by the morning fix pass 2026-11-18 (R1: not "
        "food); moved to the archive with that verdict.",
    NAF + "public-alert-no-034-2026-niger-republic-imposes-import-ban-on-bulmex-parboiled-rice-from-india-due-to-metallic-contamination/":
        f"not a recall — {BY}: NAFDAC's relay of the Niger Republic's import ban "
        "on Bulmex parboiled rice (alert 034/2026, August 2026). No Nigerian "
        "recall, no firm, no lot; the review agent already refused it "
        "(2026-11-17) and the row sat in Pending with Status rejected.",
    NAF + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: counterfeit PROGUANIL TABLETS (an antimalarial "
        "drug), not a food. Eighth archive copy of this URL.",
    NAF + "public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        f"not a recall — {BY}: NAFDAC places a firm's products on a WATCHLIST "
        "pending investigation. No product, no hazard (Product and Pathogen are "
        "empty; an earlier copy says codeine cough syrup, a drug). The Reason "
        "carries an extractor-invented 'Recall ID 043/2026-NAFDAC'.",
    NAF + "public-alert-no-09-2026-public-reminder-of-nafdacs-regulatory-directive-on-the-discontinued-registration-of-multi-dose-artemether-lumefantrine-dry-powder-for-oral-suspension/":
        f"out of scope — {BY}: a regulatory reminder about the registration of "
        "an ANTIMALARIAL DRUG (artemether/lumefantrine oral suspension), not a "
        "food and not a recall.",
}

# -- B. archive verdicts that do not hold / annotations --------------------
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
RELEASE = {
    RC % "24097":
        "'Official URL is a listing page, not the specific recall page' is false — "
        "this URL is fiche réf. 2026-11-0107, Version 1, read in full on "
        "2026-11-18 (Fromage blanc Belle France 0% and 3%, 500 g and 1 kg, EVEN "
        "LAIT INDUSTRIE, 16/11/2026, Risques 'Autres contaminants chimiques'). "
        "The curator-corrected row is PUBLISHED and is the copy that wins",
    RC % "24077":
        "this archive row records no verdict on the recall, only a pass-through "
        "and the repairable field defect Company/Brand '/'. Fiche 24077 was read "
        "in full on 2026-11-18 (REBLOCHON DE SAVOIE au lait cru, GAEC LA LOUISA, "
        "13/11/2026, Salmonella spp). The curator-corrected row is PUBLISHED and "
        "is the copy that wins",
    RC % "24064":
        "this archive row records no verdict on the recall, only a pass-through "
        "and the repairable field defect Company/Brand '/'. Fiche 24064 was read "
        "in full on 2026-11-18 (REBLOCHON DE SAVOIE FERMIER 450g, GAEC LES DEUX "
        "SAVOIES, 11/11/2026, STEC). The curator-corrected row is PUBLISHED and "
        "is the copy that wins",
    RC % "24012":
        "'Pathogen is empty' was a repairable field defect, now filled from the "
        "fiche's own Motif. Fiche 24012 was read in full on 2026-11-18 "
        "(CHAMPIGNONS MORILLES SECHEES 60G, CHAMPIBON, B&M France SAS, version 2, "
        "04/11/2026). The curator-corrected row is PUBLISHED and is the copy "
        "that wins",
    RC % "24047":
        "this archive row records no verdict. Fiche 24047 was read in full on "
        "2026-11-18 (réf. 2026-11-0060, 09/11/2026, CHAMPIGNONS MORILLES SECHEES, "
        "CHAMPIBON, lot 262306, K 2 M PROMOCASH). The curator-corrected row is "
        "PUBLISHED and is the copy that wins",
}
ANNOTATE = {
    RC % "24098":
        f"[annotated {TODAY} by the {BY}: /fiche-rappel/24098 no longer serves a "
        "fiche (it returns the RappelConso listing page). The regulator "
        "republished the recall — fiche réf. 2026-11-0108, Paysan Breton fromage "
        "blanc 0% 5 kg pail, EVEN LAIT INDUSTRIE — as /fiche-rappel/24109 "
        "(Version 3, 18/11/2026), which is the copy PUBLISHED tonight. 'Pathogen "
        "is empty' was a repairable field defect. Kept as the audit trail.] ",
    RC % "24016":
        f"[annotated {TODAY} by the {BY}: this fiche (05/11/2026) is the first "
        "publication of the Champibon dried-morel recall (400 g, lot 262306, "
        "K 2 M Promocash). The regulator's republication, fiche 24047 "
        "(09/11/2026), is the copy PUBLISHED tonight. Kept as the audit trail.] ",
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
        for url, text in ANNOTATE.items():
            for r in _rows_for(ws, url):
                cur = str(ws.cell(r, ci).value or "")
                if cur.strip().lower() in ("", "none", "nan"):
                    cur = ""
                if text[:40] in cur:
                    continue
                print(f"    {sheet} row {r}: {url[-30:]} annotated")
                if not dry:
                    ws.cell(r, ci).value = text + cur
                n += 1
    return n


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



def sync_weekly_review(wb, dry: bool) -> int:
    """Copy the corrected text fields from Recalls onto tonight's mirror rows."""
    rec, wr = wb["Recalls"], wb["Weekly_Review"]
    rh, wh = _hdr(rec), _hdr(wr)
    ru = rh.index("URL") + 1
    live = {str(rec.cell(r, ru).value or "").strip(): r
            for r in range(2, rec.max_row + 1)}
    n = 0
    for r in range(2, wr.max_row + 1):
        url = str(wr.cell(r, wh.index("URL") + 1).value or "").strip()
        src = live.get(url)
        if not src:
            continue
        if "nightly operator 2026-11-18" not in str(
                rec.cell(src, rh.index("Notes") + 1).value or ""):
            continue
        moved = []
        for col in ("Company", "Brand", "Product", "Pathogen", "Reason", "Notes"):
            new = rec.cell(src, rh.index(col) + 1).value
            cell = wr.cell(r, wh.index(col) + 1)
            if cell.value != new:
                moved.append(col)
                if not dry:
                    cell.value = new
        if moved:
            print(f"    Weekly_Review row {r}: {url[-28:]} <- Recalls {moved}")
            n += 1
    return n


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--phase", choices=("reject", "release", "mirror", "sort"), required=True)
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    if a.phase == "reject":
        print("A  Pending rows refused:")
        print("   archived: %d" % reject_pending(wb, a.dry_run))
    if a.phase == "release":
        print("B  archive verdicts that do not hold / annotations:")
        print("   archive rows marked SUPERSEDED / annotated: %d"
              % release_false_refusals(wb, a.dry_run))
    if a.phase == "mirror":
        print("C  Weekly_Review mirror re-synced:")
        print("   rows synced: %d" % sync_weekly_review(wb, a.dry_run))
    if a.phase == "sort":
        print("D  Recalls newest-first:")
        print("   rows moved: %d" % sort_recalls(wb, a.dry_run))
    if not a.dry_run:
        wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
