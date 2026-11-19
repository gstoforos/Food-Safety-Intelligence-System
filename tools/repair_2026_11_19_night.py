# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-19. Base: main at c38544b (22:13 Athens; re-applied after the Tavily gap-finder commit landed mid-review).

Field corrections and promotions go through pipeline/agents/curator.py with
quoted regulator evidence (docs/data/agent-proposals/20261119T203000Z-nightly-operator.json)
and pipeline/promote_gate_passing.py. This script does only what the curator
cannot, and never deletes a row from the register: a refused Pending row is
MOVED to Rejected with its reason.

A. NINE PENDING ROWS REFUSED, each with a written reason.
B. ARCHIVE VERDICT THAT DOES NOT HOLD marked SUPERSEDED (fiche 24097's
   "listing page" verdict is false; the fiche was read in full tonight).
C. WEEKLY_REVIEW MIRROR RE-SYNCED for rows the curator corrected after promotion.
D. RECALLS RE-SORTED NEWEST-FIRST (the promoter appends).

    python3 tools/repair_2026_11_19_night.py --phase reject
    python3 tools/repair_2026_11_19_night.py --phase release   # before the promoter
    python3 tools/repair_2026_11_19_night.py --phase sort
    python3 tools/repair_2026_11_19_night.py --phase mirror    # last
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-19"
BY = "nightly operator review 2026-11-19"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"

# -- A. Pending rows refused ------------------------------------------------
PENDING_REJECTS = {
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-formaldehydu-z-kubka-z-melaminy":
        f"out of scope — {BY}: a GIS public warning about a FOOD-CONTACT ARTICLE "
        "('Ostrzeżenie publiczne dotyczące wyrobu do kontaktu z żywnością: migracja "
        "formaldehydu z kubka z melaminy' — formaldehyde migration from a melamine "
        "cup), not a food. Non-food articles are outside the register. Same "
        "verdict as unmerged PR #48; www.gov.pl does not resolve from the session, "
        "the notice title on the row settles it.",
    RC % "24077":
        f"duplicate_of_published_recall — {BY}: fiche 24077 (réf. 2026-11-0090, "
        "13/11/2026, GAEC La Louisa raw-milk Reblochon de Savoie, lots 1609-2309, "
        "suspected Salmonella) was re-issued by RappelConso tonight as fiche 24111 "
        "(réf. 2026-11-0120, 19/11/2026, same firm, product and motif, lots "
        "extended with 1409, 1509, 2809). Both fiches read tonight. Fiche 24111 "
        "is PUBLISHED; one event, one row.",
    FDA + "?search_api_fulltext=H-0193-2027":
        f"duplicate_of_published_recall — {BY}: openFDA enforcement report "
        "H-0193-2027 (Lidl US Trading, Gelatelli Coffee & Vanilla Gelato, "
        "Listeria monocytogenes, initiated 2026-10-16) is the recall PUBLISHED "
        "as Lidl US / Coffee & Vanilla Gelato on 2026-10-17 from the FDA press "
        "release. The URL is a site-search query, not a notice. Already refused "
        "in Weekly_Rejected and Rejected.",
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3133/ address is alert FSA-PRIN-29-2026, PUBLISHED 2026-06-23 "
        "under /news-alerts/alert/fsa-prin-29-2026 (Prepworld fruit products, "
        "Salmonella). Refused twice before (Rejected, operator review "
        "2026-08-22, 'duplicate_already_approved'); the reason still holds.",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3176/ address is alert FSA-PRIN-32-2026, PUBLISHED 2026-07-01 "
        "under /news-alerts/alert/fsa-prin-32-2026 (O'Brien Fine Foods ham "
        "products, Listeria monocytogenes). No Date on the row.",
    FDA + "/fresh-ready-foods-llc-expands-recall-include-fresh-you-brand-sausage-egg-cheese-biscuit-sandwich-and":
        f"duplicate_of_published_recall — {BY}: read on the FDA page tonight "
        "(company announcement November 17, 2026): an EXPANSION of Fresh & "
        "Ready Foods' November 8 recall to 'Fresh To You Sausage, Egg & Cheese "
        "Biscuit Sandwich' 5.7 oz (Fresh Thru 11/12-11/22) and 'Mmm! Meals Made "
        "in Minutes Biscuit & Sausage Gravy' 9.5 oz (EXP 11/12-11/18), "
        "Listeria monocytogenes, following C.H. Guenther & Son's biscuit recall. "
        "Both products are PUBLISHED: Fresh & Ready Foods LLC Sausage, Egg & "
        "Cheese Biscuit Sandwich (2026-11-08) and C.H. Guenther & Son Biscuit & "
        "Sausage Gravy 9.5 oz use by 11/12/26-11/18/26 (2026-11-14). Row was a "
        "headline with no Date or Pathogen.",
    FDA + "/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        f"out of scope — {BY}: ANIMAL FEED. Read on the FDA notice tonight: "
        "listed under 'Animal & Veterinary' / 'Animal Feed'; Grade A Mini Hay "
        "Cubes in 1,500 lb totes; 'Illnesses and deaths in horses were "
        "reported'. Third refusal (Weekly_Rejected, Rejected, PR #48).",
    "https://alerts.food.gov.uk/article/6920/Greencore-recalls-several-products-due-to-the-presence-of-Salmonella":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap again. "
        "This /article/6920/ address is alert FSA-PRIN-48-2026 (Greencore, "
        "Salmonella, 2026-10-08), PUBLISHED under "
        "/news-alerts/alert/fsa-prin-48-2026 (and FSAI's copy of the same "
        "recall). Re-ingested by the Tavily gap-finder at 20:11 UTC tonight; "
        "already refused in Rejected on 2026-11-04 as a duplicate — the reason "
        "still holds. Product was 'several products'.",
    "https://www.mhlw.go.jp/content/001241427.pdf":
        f"stale / out of scope — {BY}: an MHLW PDF on Kobayashi Pharmaceutical "
        "'Foods with Function Claims' dietary supplements ('Request for "
        "discontinuation of use'), the 2024 beni-koji event; no Date on the "
        "row, Product is two bullet points of the notice. Refused before "
        "(Rejected, same URL); the reason still holds. www.mhlw.go.jp does not "
        "resolve from the session; the row's own captured text settles it.",
}

# -- B. archive verdicts that do not hold ----------------------------------
RELEASE = {
    RC % "24097":
        "the verdict 'Official URL is a listing page' is false: fiche 24097 "
        "(réf. 2026-11-0107, 16/11/2026, Even Lait Industrie / Belle France "
        "fromage blanc, cleaning-product residue) was read in full tonight and "
        "is published",
}
ANNOTATE = {}

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
        if "nightly operator 2026-11-19" not in str(
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
