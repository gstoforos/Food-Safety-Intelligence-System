# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-21. Base: main at 501fcd4 (21:11 Athens).

Replays the still-valid parts of unmerged PR #50 (tools/repair_2026_11_20_night.py)
on top of current main, plus tonight's decisions.

Field corrections go through pipeline/agents/curator.py with quoted regulator
evidence (docs/data/agent-proposals/20261121T203000Z-nightly-operator.json)
and promotions through pipeline/promote_gate_passing.py. This script does only
what the curator cannot, and never deletes a row from the register: a refused
row is MOVED to Rejected with its reason.

A. PENDING ROWS REFUSED, each with a written reason.
B. ARCHIVE VERDICT THAT DOES NOT HOLD marked SUPERSEDED (fiche 24097).
E. TWO PUBLISHED ROWS UN-PUBLISHED to Rejected (stale 2024 NCC hummus
   statement dated today; FSA /article/6920/ double-URL duplicate), and the
   SUPERSEDED stamps that released them withdrawn so the re-promotion guard
   holds again.
C. WEEKLY_REVIEW MIRROR RE-SYNCED for corrected rows.
D. RECALLS RE-SORTED NEWEST-FIRST (the promoter appends).

    python3 tools/repair_2026_11_20_night.py --phase reject
    python3 tools/repair_2026_11_20_night.py --phase release
    python3 tools/repair_2026_11_20_night.py --phase unpublish
    python3 -m pipeline.promote_gate_passing --apply
    python3 tools/repair_2026_11_20_night.py --phase sort
    python3 tools/repair_2026_11_20_night.py --phase mirror
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-21"
BY = "nightly operator review 2026-11-21"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
NG = "https://nafdac.gov.ng/"

# -- A. Pending rows refused ------------------------------------------------
PENDING_REJECTS = {
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-formaldehydu-z-kubka-z-melaminy":
        f"out of scope — {BY}: a GIS public warning about a FOOD-CONTACT ARTICLE "
        "('Ostrzeżenie publiczne dotyczące wyrobu do kontaktu z żywnością: migracja "
        "formaldehydu z kubka z melaminy' — formaldehyde migration from a melamine "
        "cup), not a food. Same verdict as unmerged PRs #48–#50; www.gov.pl does "
        "not resolve from the session, the notice title on the row settles it.",
    FDA + "/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        f"out of scope — {BY}: ANIMAL FEED. Re-read on the FDA notice tonight: "
        "Product Type 'Animal & Veterinary' / 'Animal Feed', 'Grade A Mini Hay "
        "Cubes', 'Illnesses and deaths in horses were reported'. Fifth refusal "
        "(Weekly_Rejected, Rejected, unmerged PRs #48–#50).",
    FDA + "/ridley-block-operations-recalls-batch-crystalyxrhe-20-beef-cattle-and-bison-due-excess-levels-non":
        f"out of scope — {BY}: ANIMAL FEED. Read on the FDA notice tonight: Product "
        "Type 'Animal & Veterinary', 'Crystalyx HE-20%', 'levels of non-protein "
        "nitrogen (NPN) from urea that are high enough to cause adverse health "
        "effects in cattle'. A cattle/bison feed block, not human food.",
    FDA + "/greenwich-rx-issues-voluntary-nationwide-recall-compounded-sterile-products-due-lack-sterility":
        f"out of scope — {BY}: a MEDICINE. Read on the FDA notice tonight: Product "
        "Type 'Drugs', 'Compounded sterile products', 'Potential lack of "
        "sterility assurance'. Not a food.",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/814523":
        f"stale / duplicate event — {BY}: read on the RASFF public API tonight: "
        "reference 2026.0027, 'Cereulide in infant formula from Germany', EC "
        "validation date 05-01-2026, measures linked to nestle.de / nestle.hu / "
        "nestle.fi (BEBA, Alfamino). This is the January 2026 Nestlé cereulide "
        "infant-formula event, already published many times over (FSA, FSANZ, "
        "COFEPRIS, CFS rows, January–February 2026). The row has no Date and "
        "Company is the notification number.",
    "https://www.ages.at/en/human/disease/pathogens-from-a-to-z/salmonella":
        f"not a recall — {BY}: the URL is AGES' general disease-information page "
        "('pathogens from A to Z / Salmonella'), not a notice. The row names no "
        "product, no firm and no date (Company and Brand both read 'Salmonella', "
        "Product reads 'AGES').",
    NG + "public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/":
        f"out of scope — {BY}: a NAFDAC alert on COUNTERFEIT MEDICINE "
        "('Proguanil (Projeanil and Re-granil) Tablet B.P 100mg', fake NAFDAC "
        "registration number), not a food. Refused nine times before; same "
        "reason as unmerged PR #50. nafdac.gov.ng does not resolve from the "
        "session; the product on the row settles it.",
    NG + "public-alert-no-039-2026-alert-on-the-risk-of-drug-poisoning-associated-with-concurrent-use-of-gabapentinoids-and-other-medicines/":
        f"out of scope — {BY}: a NAFDAC alert on DRUG POISONING from "
        "'concurrent use of gabapentinoids and other medicines' — a medicines "
        "safety notice, not a food. Refused four times before; same reason as "
        "unmerged PR #50.",
    NG + "public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        f"not a recall / no hazard — {BY}: NAFDAC places 'products marketed by "
        "Mofus Nigeria Ltd' on a WATCHLIST 'pending regulatory investigation'. "
        "No product, no hazard; a watchlist is not a recall. Refused nine times "
        "before; same reason as unmerged PR #50.",
}
# Rows with no URL, matched on Source + Product instead.
NO_URL_REJECTS = {
    ("MFDS", "MFDS Urges Caution Against Norovirus Food Poisoning During Winter"):
        f"not a recall — {BY}: a seasonal MFDS consumer advisory ('Urges Caution "
        "Against Norovirus Food Poisoning During Winter'). No product, no firm, no "
        "notice URL ('URL agent: No official regulator URL found'). Two copies.",
}

# -- B. archive verdicts that do not hold ----------------------------------
RELEASE = {
    RC % "24097":
        "the verdict 'Official URL is a listing page' is false: fiche 24097 "
        "(réf. 2026-11-0107, 16/11/2026, Even Lait Industrie / Belle France "
        "fromage blanc, cleaning-product residue) was re-read in full tonight and "
        "is published",
    RC % "24127":
        "the verdict 'provenance: page does not mention any of …' is false: "
        "fiche 24127 (20/11/2026) was re-read tonight and reads 'Pain brioché "
        "x32 400g', GTIN 3251517091006, lot 6260, 'COOPERATIVE U ENSEIGNE U "
        "ENSEIGNE', 'Risque physique (suspicion de corps étranger)'; the "
        "curator's own fetch-and-match check passed on it tonight (proposal "
        "a528b3e7bf2586ed)",
}
ANNOTATE = {}

# -- E. published rows that must come out of Recalls -----------------------
HUMMUS = "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/"
GREENCORE_6920 = ("https://alerts.food.gov.uk/article/6920/Greencore-recalls-several-"
                  "products-due-to-the-presence-of-Salmonella")
FRESH_READY_EXP = (FDA + "/fresh-ready-foods-llc-expands-recall-include-fresh-you-"
                   "brand-sausage-egg-cheese-biscuit-sandwich-and")
UNPUBLISH = {
    HUMMUS:
        f"stale — UN-PUBLISHED by the {BY}. Read on the NCC page tonight: the "
        "statement was posted October 3, 2024 ('Shoprite Checkers notified the "
        "NCC of the recall on September 16, 2024'; sell-by dates 10 September "
        "2024 to 08 October 2024). The row carried Date 2026-11-20 and was "
        "published today by the confirm agent (commit e06c6bae, 14:27 UTC) into "
        "W47 as a Tier-1 Listeria recall. This is the founding incident of "
        "pipeline/_publish_gate.py; it was refused EIGHT+ times, last as "
        "'TERMINAL, UPHELD' by operator review 2026-11-06. It got through because "
        "a sweep put a release stamp on its 'no reason recorded' archive rows, which "
        "the re-promotion guard then read as a release. Those stamps are "
        "withdrawn. Same decision as unmerged PR #50; NCC page re-read "
        "2026-11-21 ('October 3, 2024').",
    GREENCORE_6920:
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}: the FSA "
        "double-URL trap. /article/6920/ is FSA alert FSA-PRIN-48-2026 (Greencore, "
        "Salmonella in pistachio ingredient, 8 October 2026), which is PUBLISHED "
        "under https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026 "
        "with all five products named. This copy said only 'several products' "
        "and its Notes were the FSA page's boilerplate; the archived copy's own "
        "captured page text reads 'already published at "
        "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026 | Food "
        "Alert , 8 October 2026' and 'Greencore is recalling five products "
        "because Salmonella has been found in pistachio nuts'. Re-published "
        "today (DateAdded 2026-11-20) because the 2026-11-04 archive stamp "
        "'DUPLICATE — the published copy wins' carried the release-stamp word "
        "and the re-promotion guard read it as a release. Same decision as "
        "unmerged PR #50.",
    FRESH_READY_EXP:
        f"duplicate_of_published_recall — UN-PUBLISHED by the {BY}. Read on the "
        "FDA page tonight: company announcement 'November 17, 2026', FDA publish "
        "'November 18, 2026'; Fresh & Ready Foods EXPANDS 'its November 8th "
        "recall of select Fresh To You Sausage, Egg & Cheese Biscuit Sandwiches' "
        "to add 'Mmm! Meals Made in Minutes Biscuit & Sausage Gravy, 9.5 oz.' "
        "(SKU 90013), Listeria monocytogenes. Both products are already "
        "PUBLISHED: Fresh & Ready Foods LLC 'Sausage, Egg & Cheese Biscuit "
        "Sandwich' (2026-11-08, /fresh-ready-foods-llc-recalls-fresh-you-brand-"
        "...) and C.H. Guenther & Son 'Biscuit & Sausage Gravy, 9.5 oz, use by "
        "11/12/26 to 11/18/26' (USDA FSIS, 2026-11-14). This copy repeated the "
        "first product under a second URL and date. It was refused in Pending "
        "by unmerged PR #50 and published by the pipeline on 2026-11-21 because "
        "that PR is not merged.",
}
# Each of these archive texts is folded into the re-promotion guard's
# description, and merge_master._is_terminal_rejection reads ANY occurrence of
# the release word as "not terminal" — that is how both rows came back. So the
# word is retracted (re-spelled, the sentence kept for the audit trail) on every
# archive row of the two URLs, and a parseable operator verdict is prepended.
RELEASE_WORD = r"(?i)superseded"
RETRACTED = "release-stamped [retracted 2026-11-21]"
WITHDRAW = {
    HUMMUS:
        f"[operator review {TODAY}: REJECTED — out of scope stale notice: the "
        "September 2024 NCC statement, NOT published; the Recalls copy was "
        "un-published tonight and every release stamp on this URL retracted. "
        "The refusal STANDS.] ",
    GREENCORE_6920:
        f"[operator review {TODAY}: REJECTED — duplicate_of_published_recall: "
        "/article/6920/ is FSA-PRIN-48-2026, published at "
        "/news-alerts/alert/fsa-prin-48-2026; the Recalls copy was un-published "
        "tonight and the release stamp retracted. The refusal STANDS.] ",
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
    hdr = _hdr(ws)
    si, pi, ui = hdr.index("Source") + 1, hdr.index("Product") + 1, hdr.index("URL") + 1
    for (src, prod), reason in NO_URL_REJECTS.items():
        rows = [r for r in range(2, ws.max_row + 1)
                if str(ws.cell(r, si).value or "").strip() == src
                and str(ws.cell(r, pi).value or "").strip() == prod
                and str(ws.cell(r, ui).value or "").strip().lower() in ("", "none")]
        for r in sorted(rows, reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {src} {prod[:50]!r} (no URL) -> Rejected")
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


def unpublish(wb, dry: bool) -> int:
    """Move a published row to Rejected (append-only) and withdraw the
    SUPERSEDED stamps that let the re-promotion guard release it."""
    rec = wb["Recalls"]
    n = 0
    for url, reason in UNPUBLISH.items():
        for r in sorted(_rows_for(rec, url), reverse=True):
            row = _row_dict(rec, r)
            print(f"    Recalls row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} ({row['Date']}) -> Rejected")
            if not dry:
                _archive(wb, row, reason)
                rec.delete_rows(r, 1)
            n += 1
        wr = wb["Weekly_Review"]
        wh = _hdr(wr)
        for r in _rows_for(wr, url):
            print(f"    Weekly_Review row {r}: {url[-40:]} Reviewed -> REJECTED")
            if not dry:
                wr.cell(r, wh.index("Reviewed") + 1).value = "REJECTED"
                ni = wh.index("Notes") + 1
                wr.cell(r, ni).value = (str(wr.cell(r, ni).value or "")
                                        + f" [{BY}: un-published — see Rejected]").strip()
    import re
    for sheet, col, mark in (("Weekly_Rejected", "RejectionReason", "Reviewed"),
                             ("Rejected", "RejectReason", "Status")):
        ws = wb[sheet]
        hdr = _hdr(ws)
        ci, mi = hdr.index(col) + 1, hdr.index(mark) + 1
        ni = hdr.index("Notes") + 1
        for url, banner in WITHDRAW.items():
            for r in _rows_for(ws, url):
                cur = str(ws.cell(r, ci).value or "")
                if cur.strip().lower() in ("none", "nan"):
                    cur = ""
                notes = str(ws.cell(r, ni).value or "")
                has_word = re.search(RELEASE_WORD, cur + notes)
                if banner[:40] in cur and not has_word:
                    continue
                print(f"    {sheet} row {r}: {url[-40:]} release stamp retracted, verdict stamped")
                if not dry:
                    new = re.sub(RELEASE_WORD, RETRACTED, cur)
                    if banner[:40] not in new:
                        new = banner + new
                    ws.cell(r, ci).value = new
                    ws.cell(r, ni).value = re.sub(RELEASE_WORD, RETRACTED, notes) or None
                    if str(ws.cell(r, mi).value or "").upper() == "SUPERSEDED":
                        ws.cell(r, mi).value = "UPHELD" if sheet == "Weekly_Rejected" else "rejected"
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
        if "proposal" not in str(rec.cell(src, rh.index("Notes") + 1).value or "") \
                or "applied 2026-11-21" not in str(rec.cell(src, rh.index("Notes") + 1).value or ""):
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
    ap.add_argument("--phase", choices=("reject", "release", "unpublish", "mirror", "sort"), required=True)
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    if a.phase == "reject":
        print("A  Pending rows refused:")
        print("   archived: %d" % reject_pending(wb, a.dry_run))
    if a.phase == "release":
        print("B  archive verdicts that do not hold / annotations:")
        print("   archive rows marked SUPERSEDED / annotated: %d"
              % release_false_refusals(wb, a.dry_run))
    if a.phase == "unpublish":
        print("E  published rows un-published to Rejected:")
        print("   rows moved / stamps withdrawn: %d" % unpublish(wb, a.dry_run))
    if a.phase == "mirror":
        print("C  Weekly_Review mirror re-synced:")
        print("   rows synced: %d" % sync_weekly_review(wb, a.dry_run))
    if a.phase == "sort":
        print("D  Recalls newest-first:")
        print("   rows moved: %d" % sort_recalls(wb, a.dry_run))
    if not a.dry_run:
        wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=%d" % (len(UNPUBLISH) if a.phase == "unpublish" and not a.dry_run else 0))
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
