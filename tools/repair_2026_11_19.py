# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-19 — every data edit of the run, in one script.

Run against main's CURRENT docs/data/recalls.xlsx. Idempotent: each edit
checks the state it expects and reports NOOP when the value is already right,
so tools/rebase_and_verify.py can re-run it on a workbook that has moved.

    python3 -m tools.repair_2026_11_19            # report only
    python3 -m tools.repair_2026_11_19 --apply    # write

WHAT IT DOES AND WHY
====================

1. Recalls — RappelConso fiche 24099, GRILLO PARTY, Product "saucisse
   italienne" -> "Italian sausage".

   This single row was turning TWO tests red on main this morning:
     tests/test_product_and_firm_names_are_english.py
       ::test_no_published_product_reads_in_another_language
     tests/test_an_agency_headline_is_not_a_firm_and_a_food.py
       ::TestTheLanguageDetectorSeesGerman
       ::test_no_published_product_is_left_in_german
   It is the only published Product left in another language, and it carries
   the live stamp "[needs cleanup: Product not in English]" written by
   reviewer 3, which has no model and cannot translate.

   Read on the authority page today — https://rappel.conso.gouv.fr/
   fiche-rappel/24099/interne, Fiche réf. 2026-11-0109 Version 1, published
   18/11/2026: title "SAUCISSE ITALIENNE", "Noms des modèles ou références"
   "SAUCISSE ITALIENNE", "Motif du rappel" "détection salmonelle spp",
   "Risques encourus" "Salmonella spp (agent responsable de la
   salmonellose)", GTIN 3700502708950, lot 26313-4569, DLC 17/11/2026. The
   Reason on the row ("Detection of Salmonella spp.") is already a faithful
   English rendering, so only the Product changes. Nothing is added that the
   fiche does not say: "saucisse italienne" is "Italian sausage" and no more.

2. Pending — the CFIA beer recall, Region "National" -> "North America".

   pipeline/promote_gate_passing reported exactly one blocker on this row:
   "Region 'National' is not one of ['Africa', 'Asia', 'Europe', 'Latin
   America', 'Middle East', 'North America', 'Oceania', 'Unknown']". The
   recall itself is sound — CFIA's own per-recall URL, "Food - Extraneous
   Material", Pathogen "Pieces Of Metal", seven brands of alcoholic and
   non-alcoholic beer, and reviewer 1 has twice refused to reject it ("the
   official URL is on the row"). "National" is the notice's DISTRIBUTION
   AREA, not a region bucket; the row's own Country is Canada.

   The code half of this fix is in pipeline/merge_master.apply_label_aliases,
   which now maps any Region outside the publish gate's eight buckets through
   scrapers._models.infer_region using the row's own Country, so no writer
   can mint this defect again. This script repairs the rows already in the
   workbook.

3. Weekly_Rejected — the Archibald Inc. beer recall, Region "Quebec" ->
   "North America".

   Same defect, same CFIA event family, archived instead of pending. The
   confirm agent archived it with "NO_REASON_RECORDED — rejected by
   confirm-agent from CFIA, status pending. The writer supplied no verdict
   and none could be parsed from Notes; treat as UNAUDITED, not as a
   confirmed rejection", and its Notes carry the publish gate's "Region
   'Quebec' is not one of [...]". That phrase was the ONE publish-gate field
   verdict in this workbook that merge_master.REPAIRABLE_DEFECTS did not
   match, so the row could never be re-promoted however well it was
   repaired; the tuple (and promote_gate_passing.SUPERSEDE_IF with it) gained
   it today. The row is REPAIRED here and left where it is — nothing in this
   script promotes it. The archive is append-only: the old value is recorded
   in Notes.

4. Pending — GIS (PL) ochratoxin-A coffee: fill Company, translate Product
   and Reason into English.

   The row was held by the publish gate on "Company is empty" and stamped
   by the confirm agent "[confirm-agent hold: 2026-11-19: no recalling firm
   on the row (Brand '—'); not published and not rejected — reviewer 2 to
   read the firm from the notice]". The notice was read today:
   https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-
   stwierdzenie-przekroczenia-najwyzszego-dopuszczalnego-poziomu-
   ochratoksyny-a-w-mielonej-kawie, published 18.11.2026. Its "Producent:"
   field reads "Bialetti Industrie Spa Via Fogliano, Coccaglio, 25030 Italy";
   the products are "Perfetto Moka Caffé Macinato Nocciola 250 g" (lots
   LC5 BD160B, LC5 BD161B; best before 09/06/2027 and 10/06/2027) and
   "Perfetto Moka Caffé Macinato Caramello 250 g" (lot LC5 BD149T; best
   before 29/05/2027); the hazard sentence is "Wskazane partie kawy zawierają
   ochratoksynę A w stężeniu przekraczającym najwyższy poziom określony w
   przepisach prawa."

   Company is the page's own "Producent" value, not a guess. The firm name
   and the product names are Latin-script and are kept as the notice writes
   them; only the Polish description and the hazard sentence are translated,
   with both originals kept in Notes.

   NOTE FOR THE READER OF THE NEXT PASS: the register already carries this
   Bialetti ochratoxin recall from three other regulators — FSAI (IE)
   2026-10-23, RappelConso (FR) 2026-10-15 and Ministero della Salute (IT)
   2026-11-16. This is Poland's own notice of the same event, on GIS's own
   URL. If the near-duplicate rule refuses it, that refusal is correct and
   should be accepted, not worked around.

WHAT IS DELIBERATELY NOT TOUCHED
================================
* Pending, FSA (UK) Prepworld fruit/Salmonella and O'Brien Fine Foods
  ham/Listeria — both held on "Date is empty" and stamped "[date-unknown:
  claude-check please verify Date]". alerts.food.gov.uk refused every fetch
  this morning (client error, no content), and data.food.gov.uk/food-alerts/
  serves only 50 records, all created in 2018, with no pagination or search
  form reachable from the page. There is therefore no authority page this
  run could read, and a Date not read is a Date not written.
* Pending, GIS (PL) melamine-cup formaldehyde migration — the notice
  (read today) is about "Kubek z melaminy", a food-CONTACT article, importer
  Grupa PSB Handel S.A., lot 874197. That is out of scope by R1 (food only),
  so the right outcome is a scope rejection by a reviewer, not a field
  repair by this script.
* Pending, RappelConso fiches 24109 (Paysan Breton) and 24097 (Belle
  France) fromage blanc — "Possible contamination par des produits de
  nettoyage à des doses ne présentant pas de risque pour la santé du
  consommateur". RappelConso classes the risk as "Autres contaminants
  chimiques". Whether cleaning-product residue is in AFTS scope is a
  standing decision, not a defect: no published row carries such a Pathogen,
  nothing in this repo speaks the word, and the repo's own reviewer
  precedent treats it as out of scope ("URL agent: The hazard is not a
  microbial pathogen, but possible contamination by cleaning products", in
  tests/test_every_field_defect_is_repairable.py). Left for the operator.
* The RASFF Company format ("Origin: … | Notifying: …") — out of bounds.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

# tools/rebase_and_verify.py runs this file as `python3 tools/repair_*.py`,
# not as `-m tools.repair_*`, so the repo root is NOT on sys.path and
# `from scrapers._models import ...` raises ModuleNotFoundError at the gate
# while working perfectly from the command line. Put the root on the path
# before any repo import.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STAMP = "repair 2026-11-19"

#: set by main() so the post-edits know which workbook is in play.
_XLSX_IN_USE = [XLSX]


def _norm(url: str) -> str:
    return str(url or "").strip().rstrip("/").lower()


def _urls_in_workbook() -> set:
    """Every URL in every sheet, normalised. Used to tell "queued" from
    "already there" without trusting an exit code."""
    out = set()
    wb = load_workbook(_XLSX_IN_USE[0], read_only=True, data_only=True)
    try:
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            it = ws.iter_rows(values_only=True)
            try:
                hdr = list(next(it))
            except StopIteration:
                continue
            if "URL" not in hdr:
                continue
            i = hdr.index("URL")
            for row in it:
                if i < len(row) and row[i]:
                    out.add(_norm(row[i]))
    finally:
        wb.close()
    return out


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hdr(ws) -> dict:
    return {str(c.value): i + 1
            for i, c in enumerate(next(ws.iter_rows(min_row=1, max_row=1)))
            if c.value is not None}


def _get(ws, hdr, row: int, col: str) -> str:
    i = hdr.get(col)
    if not i:
        return ""
    v = ws.cell(row=row, column=i).value
    return "" if v is None else str(v).strip()


def _set(ws, hdr, row: int, col: str, value) -> None:
    ws.cell(row=row, column=hdr[col], value=value)


def _append_note(ws, hdr, row: int, text: str) -> None:
    cur = _get(ws, hdr, row, "Notes")
    _set(ws, hdr, row, "Notes", (cur + " " + text).strip())


def _find(ws, hdr, *, url_contains: str, product_contains: str = "",
          company_contains: str = "") -> list[int]:
    """Row numbers whose URL (and optionally Product/Company) match."""
    out = []
    for r in range(2, ws.max_row + 1):
        u = _get(ws, hdr, r, "URL").lower()
        if url_contains.lower() not in u:
            continue
        if product_contains and product_contains.lower() not in \
                _get(ws, hdr, r, "Product").lower():
            continue
        if company_contains and company_contains.lower() not in \
                _get(ws, hdr, r, "Company").lower():
            continue
        out.append(r)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# the edits
# ─────────────────────────────────────────────────────────────────────────────

def fix_1_saucisse_italienne(wb, log) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    rows = _find(ws, hdr, url_contains="rappel.conso.gouv.fr/fiche-rappel/24099")
    if not rows:
        log("1. SKIP  — no Recalls row on fiche 24099 (already gone?)")
        return 0
    n = 0
    for r in rows:
        prod = _get(ws, hdr, r, "Product")
        if prod.lower() != "saucisse italienne":
            log(f"1. NOOP  — Recalls row {r} Product is already {prod!r}")
            continue
        _set(ws, hdr, r, "Product", "Italian sausage")
        notes = _get(ws, hdr, r, "Notes")
        notes = notes.replace("[needs cleanup: Product not in English]", "").strip()
        notes = " ".join(notes.split())
        _set(ws, hdr, r, "Notes", notes)
        _append_note(
            ws, hdr, r,
            f"[{STAMP}: Product was published as the French "
            f"\"saucisse italienne\" and carried reviewer 3's "
            f"\"[needs cleanup: Product not in English]\" stamp, which has no "
            f"model and could not translate it. Read today on "
            f"https://rappel.conso.gouv.fr/fiche-rappel/24099/interne "
            f"(Fiche rereferences 2026-11-0109, Version 1, published "
            f"18/11/2026): title and \"Noms des modeles ou references\" both "
            f"\"SAUCISSE ITALIENNE\", brand \"GRILLO PARTY\", \"Motif du "
            f"rappel\" \"detection salmonelle spp\", GTIN 3700502708950, lot "
            f"26313-4569, DLC 17/11/2026. Translated faithfully and nothing "
            f"added; the stamp is removed because the defect it named is "
            f"repaired. [original product: saucisse italienne]")
        log(f"1. FIXED — Recalls row {r} Product 'saucisse italienne' -> "
            f"'Italian sausage'; stale needs-cleanup stamp dropped")
        n += 1
    return n


def fix_2_pending_region_national(wb, log) -> int:
    ws = wb["Pending"]
    hdr = _hdr(ws)
    rows = _find(
        ws, hdr,
        url_contains="recalls-rappels.canada.ca/en/alert-recall/"
                     "various-brands-alcoholic-and-non-alcoholic-beer")
    if not rows:
        log("2. SKIP  — the CFIA beer row is no longer in Pending")
        return 0
    n = 0
    for r in rows:
        cur = _get(ws, hdr, r, "Region")
        country = _get(ws, hdr, r, "Country")
        if cur == "North America":
            log(f"2. NOOP  — Pending row {r} Region is already 'North America'")
            continue
        if country != "Canada":
            log(f"2. SKIP  — Pending row {r} Country is {country!r}, not "
                f"'Canada'; not repairing Region by hand")
            continue
        _set(ws, hdr, r, "Region", "North America")
        _append_note(
            ws, hdr, r,
            f"[{STAMP}: Region {cur!r} -> 'North America'. This was the row's "
            f"ONLY publish-gate blocker (\"Region {cur!r} is not one of "
            f"['Africa', 'Asia', 'Europe', 'Latin America', 'Middle East', "
            f"'North America', 'Oceania', 'Unknown']\"). {cur!r} is the "
            f"notice's distribution area, not a region bucket; the value "
            f"written here is read from this row's own Country ('Canada') "
            f"through scrapers._models.infer_region. No fetch, no model. "
            f"pipeline/merge_master.apply_label_aliases now does this at the "
            f"writer so no collector can mint the defect again.]")
        log(f"2. FIXED — Pending row {r} Region {cur!r} -> 'North America' "
            f"(CFIA beer, pieces of metal — its only gate blocker)")
        n += 1
    return n


def fix_3_archived_region_quebec(wb, log) -> int:
    ws = wb["Weekly_Rejected"]
    hdr = _hdr(ws)
    rows = _find(
        ws, hdr,
        url_contains="recalls-rappels.canada.ca/en/alert-recall/"
                     "archibald-brand-nuit-blanche-white-ipa")
    if not rows:
        log("3. SKIP  — the Archibald row is not in Weekly_Rejected")
        return 0
    n = 0
    for r in rows:
        cur = _get(ws, hdr, r, "Region")
        country = _get(ws, hdr, r, "Country")
        if cur == "North America":
            log(f"3. NOOP  — Weekly_Rejected row {r} Region already fixed")
            continue
        if country != "Canada":
            log(f"3. SKIP  — Weekly_Rejected row {r} Country is {country!r}")
            continue
        _set(ws, hdr, r, "Region", "North America")
        _append_note(
            ws, hdr, r,
            f"[{STAMP}: Region {cur!r} -> 'North America', read from this "
            f"row's own Country ('Canada') through "
            f"scrapers._models.infer_region. No fetch, no model. The archive "
            f"entry is NOT removed and this pass does not promote it: the "
            f"row was archived by the confirm agent with "
            f"\"NO_REASON_RECORDED ... treat as UNAUDITED, not as a confirmed "
            f"rejection\", and its publish-gate verdict \"Region {cur!r} is "
            f"not one of [...]\" was the one FIELD verdict in this workbook "
            f"that merge_master.REPAIRABLE_DEFECTS did not match, so it could "
            f"never be re-promoted however well it was repaired. That tuple "
            f"and promote_gate_passing.SUPERSEDE_IF both gained the phrase "
            f"today. Original Region value recorded here: {cur!r}.]")
        log(f"3. FIXED — Weekly_Rejected row {r} Region {cur!r} -> "
            f"'North America' (Archibald Inc. beer, pieces of metal)")
        n += 1
    return n


_PL_PRODUCT_EN = ("Ground coffee: Perfetto Moka Caffe Macinato Nocciola 250 g "
                  "(lots LC5 BD160B, LC5 BD161B; best before 09/06/2027 and "
                  "10/06/2027) and Perfetto Moka Caffe Macinato Caramello "
                  "250 g (lot LC5 BD149T; best before 29/05/2027)")
_PL_REASON_EN = ("The named batches of coffee contain ochratoxin A at a "
                 "concentration exceeding the maximum level laid down in law.")


#: The register's existing spelling for this firm. The GIS notice writes
#: "Bialetti Industrie Spa"; FSAI (IE) 2026-10-23 published it as
#: "Bialetti Industrie SpA" and pipeline/_firm_names.fold() collapses the two
#: to one key, so writing the notice's casing turns
#: tests/test_firm_names_are_uniform.py RED on a name the register already
#: has. One spelling per firm (operator 2026-10-01) wins over the notice's
#: capitalisation; the notice's own string is recorded in Notes.
_BIALETTI = "Bialetti Industrie SpA"


def fix_4_gis_ochratoxin_coffee(wb, log) -> int:
    # Sheet-agnostic: once the row passes the gate the promoter moves it to
    # Recalls (and copies it into Weekly_Review), so a re-run has to find it
    # wherever it now lives rather than only in Pending.
    n = 0
    for sheet in ("Pending", "Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        n += _fix_4_on_sheet(wb[sheet], sheet, log)
    if not n:
        log("4. NOOP  — the GIS ochratoxin coffee row needs no repair")
    return n


def _fix_4_on_sheet(ws, sheet, log) -> int:
    hdr = _hdr(ws)
    rows = _find(ws, hdr, url_contains="ochratoksyny-a-w-mielonej-kawie")
    if not rows:
        return 0
    n = 0
    for r in rows:
        if _get(ws, hdr, r, "Company") == _BIALETTI:
            continue
        orig_product = _get(ws, hdr, r, "Product")
        orig_reason = _get(ws, hdr, r, "Reason")
        _set(ws, hdr, r, "Company", _BIALETTI)
        if _get(ws, hdr, r, "Brand") in ("", "—", "-"):
            _set(ws, hdr, r, "Brand", "Perfetto Moka")
        _set(ws, hdr, r, "Product", _PL_PRODUCT_EN)
        _set(ws, hdr, r, "Reason", _PL_REASON_EN)
        notes = _get(ws, hdr, r, "Notes")
        for stale in ("[needs cleanup: Reason not in English]",
                      "[needs cleanup: Product not in English]"):
            notes = notes.replace(stale, "")
        _set(ws, hdr, r, "Notes", " ".join(notes.split()).strip())
        _append_note(
            ws, hdr, r,
            f"[{STAMP}: Company was EMPTY and Brand was '-', which was this "
            f"row's publish-gate blocker, and the confirm agent asked "
            f"reviewer 2 to read the firm from the notice. Read today on the "
            f"GIS notice at the URL on this row (published 18.11.2026): the "
            f"\"Producent:\" field gives \"Bialetti Industrie Spa Via "
            f"Fogliano, Coccaglio, 25030 Italy\" - written here as "
            f"\"{_BIALETTI}\", the spelling FSAI (IE) 2026-10-23 already "
            f"published for this firm, because pipeline/_firm_names.fold() "
            f"collapses the two casings to one key and the register keeps "
            f"one spelling per firm; the products are "
            f"\"Perfetto Moka Caffe Macinato Nocciola 250 g\" (Numery partii "
            f"LC5 BD160B, LC5 BD161B; Daty minimalnej trwalosci 09/06/2027, "
            f"10/06/2027) and \"Perfetto Moka Caffe Macinato Caramello "
            f"250 g\" (Numer partii LC5 BD149T; Data minimalnej trwalosci "
            f"29/05/2027); the hazard sentence is \"Wskazane partie kawy "
            f"zawieraja ochratoksyne A w stezeniu przekraczajacym najwyzszy "
            f"poziom okreslony w przepisach prawa.\" Brand is the product "
            f"line's own name as the notice writes it. Product and Reason "
            f"translated from the Polish; brand and lot/date details kept as "
            f"written; nothing added. The needs-cleanup stamps are removed "
            f"because the defects they named are repaired. The register "
            f"already carries this Bialetti ochratoxin recall from FSAI (IE) "
            f"2026-10-23, RappelConso (FR) 2026-10-15 and Ministero della "
            f"Salute (IT) 2026-11-16 — this is Poland's own notice of the "
            f"same event; if the near-duplicate rule refuses it, that "
            f"refusal is correct. "
            f"[original product: {orig_product}] "
            f"[original Reason (pl): \"{orig_reason}\"]")
        log(f"4. FIXED — {sheet} row {r}: Company <- {_BIALETTI!r}, "
            f"Product and Reason translated from Polish")
        n += 1
    return n


def fix_5_cfia_beer_company(wb, log) -> int:
    """CFIA "various brands of beer" — fill the empty Company.

    Repairing the Region in fix_2 moved this row's publish-gate blocker from
    "Region 'National' is not one of [...]" to "Company is empty": the
    collector had put the seven brand names in Brand and left Company blank.

    Read today on the CFIA's own per-recall page at the URL on this row
    (Identification number RA-82913, CFIA ID 17626): the "Companies" field
    reads "Recalling firm: Molson Canada 2005". Issue "Food - Extraneous
    Material"; Issue body "Various brands of alcoholic and non-alcoholic beer
    recalled due to pieces of metal."; Recall class "Class 3"; Distribution
    "National"; 31 affected products across the seven brands already on the
    row.

    The Date is NOT touched. The page carries three conflicting dates of its
    own — "Recall date" 2026-11-02, "Last updated" 2026-11-09 and "Original
    published date" 2026-11-10 — and the row's existing 2026-11-09 is one of
    them. Picking a different one would move the row between weekly reports
    on no better evidence than it already has.
    """
    ws = wb["Pending"]
    hdr = _hdr(ws)
    rows = _find(
        ws, hdr,
        url_contains="recalls-rappels.canada.ca/en/alert-recall/"
                     "various-brands-alcoholic-and-non-alcoholic-beer")
    if not rows:
        log("5. SKIP  — the CFIA beer row is no longer in Pending")
        return 0
    n = 0
    for r in rows:
        cur = _get(ws, hdr, r, "Company")
        if cur:
            log(f"5. NOOP  — Pending row {r} Company is already {cur!r}")
            continue
        _set(ws, hdr, r, "Company", "Molson Canada 2005")
        _append_note(
            ws, hdr, r,
            f"[{STAMP}: Company was EMPTY — the collector put the seven brand "
            f"names in Brand and left Company blank, which became this row's "
            f"publish-gate blocker once the Region defect was repaired. Read "
            f"today on the CFIA's own per-recall page at the URL on this row "
            f"(Identification number RA-82913, CFIA ID 17626): the "
            f"\"Companies\" field reads \"Recalling firm: Molson Canada "
            f"2005\". The page also gives Issue \"Food - Extraneous "
            f"Material\", Recall class \"Class 3\", Distribution "
            f"\"National\" and 31 affected products across the seven brands "
            f"already on this row. Date left as published: the page carries "
            f"three conflicting dates of its own (Recall date 2026-11-02, "
            f"Last updated 2026-11-09, Original published date 2026-11-10) "
            f"and this row's 2026-11-09 is one of them.]")
        log(f"5. FIXED — Pending row {r} Company <- 'Molson Canada 2005' "
            f"(CFIA recalling-firm field)")
        n += 1
    return n


def fix_6_sweep_every_bad_region(wb, log) -> int:
    """Every remaining Region outside the publish gate's eight buckets.

    fixes 2 and 3 repaired the two CFIA rows by name because each one is a
    recall with its own story. This is the rest of the shape, swept: three
    rows in the permanent Rejected archive carrying a distribution area in
    the Region column —

        Rejected  GIS       2026-09-19  'Poland'
        Rejected  NCC (ZA)  2026-09-28  'Not specified in the article'
        Rejected  FDA       2026-09-26  'Nationwide and online'

    None is blocking anything today: all three are archived and the first
    and third are already marked SUPERSEDED by the rows published at their
    URLs. They are repaired anyway because
    tests/test_a_region_is_a_bucket_not_a_distribution_area.py now asserts
    that NO sheet carries a Region outside the vocabulary, and a guard with
    three standing exceptions is not a guard. Nothing is deleted and the old
    value is recorded on each row.

    The replacement comes from scrapers._models.infer_region on the row's own
    Country — the same deterministic map merge_master.apply_label_aliases now
    applies at the writer. The FDA row is the one that found the second
    defect of the morning: infer_region did not know the label "United
    States" and would have written 'Unknown' over a US recall.
    """
    from scrapers._models import infer_region
    from pipeline._publish_gate import VALID_REGIONS

    n = 0
    for sheet in ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected",
                  "Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        hdr = _hdr(ws)
        if "Region" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            cur = _get(ws, hdr, r, "Region")
            if not cur or cur in VALID_REGIONS:
                continue
            country = _get(ws, hdr, r, "Country")
            bucket = infer_region(country)
            _set(ws, hdr, r, "Region", bucket)
            _append_note(
                ws, hdr, r,
                f"[{STAMP}: Region {cur!r} -> {bucket!r}. {cur!r} is not one "
                f"of the publish gate's eight buckets — it is the notice's "
                f"distribution area, not a region. The value written here is "
                f"read from this row's own Country ({country!r}) through "
                f"scrapers._models.infer_region. No fetch, no model. Nothing "
                f"else on the row is changed and the original value is "
                f"recorded here.]")
            log(f"6. FIXED — {sheet} row {r} Region {cur!r} -> {bucket!r} "
                f"(Country {country!r})")
            n += 1
    if not n:
        log("6. NOOP  — no sheet carries a Region outside the vocabulary")
    return n


# ─────────────────────────────────────────────────────────────────────────────
# THE THREE VERIFIED MISSED RECALLS
# ─────────────────────────────────────────────────────────────────────────────
#
# Found by the 2026-11-19 missed-recall sweep, each read on the regulator's own
# per-recall page, each checked first against all five sheets by URL, company,
# product and a distinctive word. They are queued through
# tools/add_manual_row.py — the real code path — so they enter Pending with
# Status 'pending' and take the same dedup, the same validation and the same
# two review stages as any scraped row. Nothing here writes to Recalls.
#
#   CAA (JP) rcl=00000036155  keisaibi 2026/11/19  metal fragments
#   CAA (JP) rcl=00000036156  keisaibi 2026/11/19  mould
#   FSANZ (AU) Woodbridge Smokehouse Smoked Salmon Spread 150 g
#                             published 19 November 2026  L. monocytogenes
#
# The four CAA notices of keisaibi 2026/11/18 (rcl 36145-36148) were ALREADY
# in Recalls, collected by the Japanese scraper — only the two posted this
# morning were missing, and the newest FSANZ row in the register was
# 2026-10-30.
MANUAL_ROWS = json.loads(r"""
[
    {
        "Date": "2026-11-19",
        "Source": "CAA (JP)",
        "Company": "Nozomikai",
        "Brand": "—",
        "Product": "Genkotsu sweets (plain and kinako roasted-soybean-flour)",
        "Pathogen": "Foreign material (metal)",
        "Reason": "Risk of foreign matter (metal fragments) in the product",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": "https://www.recall.caa.go.jp/result/detail.php?rcl=00000036155&screenkbn=01",
        "Notes": "Added by the morning fix pass 2026-11-19 missed-recall sweep. Read on the CAA listing https://www.recall.caa.go.jp/result/index.php?screenkbn=01&category=1 (keisaibi / publication date 2026/11/19) and on the CAA's own detail page at the URL on this row. The listing's keisaibi is 2026/11/19; the detail page gives only taiou-kaishi-bi (response start date) 2026年11月11日 and does not repeat the publication date. Every field above was read on those two pages: company 「のぞみ会」, product 「げんこつ（白、きな粉）」, hazard 「異物（金属片）混入のおそれ」. Sold at ソレーネ周南, 道の駅蛍街道西ノ市, 道の駅きくがわ, 豊田農業公園みのりの丘. No lot or best-before is on the page. The company name is the standard romanisation of 「のぞみ会」; CAA publishes no English name for it. [original company: のぞみ会] [original product: げんこつ（白、きな粉）] [original Reason (ja): 異物（金属片）混入のおそれ]"
    },
    {
        "Date": "2026-11-19",
        "Source": "CAA (JP)",
        "Company": "Soma Pan",
        "Brand": "—",
        "Product": "Fresh marshmallows",
        "Pathogen": "Mold",
        "Reason": "Contamination by mould",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": "https://www.recall.caa.go.jp/result/detail.php?rcl=00000036156&screenkbn=01",
        "Notes": "Added by the morning fix pass 2026-11-19 missed-recall sweep. Read on the CAA listing https://www.recall.caa.go.jp/result/index.php?screenkbn=01&category=1 (keisaibi / publication date 2026/11/19) and on the CAA's own detail page at the URL on this row. The detail page gives taiou-kaishi-bi (response start date) 2026年11月12日 and no publication date. Every field above was read on those two pages: company 「相馬パン」, product 「生マシュマロ」, hazard 「カビによる汚染」. No lot or best-before is on the page. The company name is the standard romanisation of 「相馬パン」; CAA publishes no English name for it. [original company: 相馬パン] [original product: 生マシュマロ] [original Reason (ja): カビによる汚染]"
    },
    {
        "Date": "2026-11-19",
        "Source": "FSANZ (AU)",
        "Company": "Pure Foods Tasmania",
        "Brand": "Woodbridge Smokehouse",
        "Product": "Woodbridge Smokehouse Smoked Salmon Spread - 150g (batch number 26300, use by 28/12/2026)",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Microbial (Listeria monocytogenes) contamination.",
        "Class": "Recall",
        "Country": "Australia",
        "Outbreak": 0,
        "URL": "https://www.foodstandards.gov.au/food-recalls/recall-alert/woodbridge-smokehouse-smoked-salmon-spread-150g",
        "Notes": "Added by the morning fix pass 2026-11-19 missed-recall sweep of https://www.foodstandards.gov.au/food-recalls/recall-alert - the newest FSANZ (AU) row in the register was 2026-10-30, and this alert was published today. Every field above was read on the FSANZ recall-alert page at the URL on this row: 'Recall published 19 November 2026' (page metadata article:published_time 2026-11-19T15:57:11+1100); Business 'Pure Foods Tasmania'; product 'Woodbridge Smokehouse Smoked Salmon Spread - 150g'; recall reason 'Microbial contamination'; Problem 'Microbial (Listeria monocytogenes) contamination.'; 'Date Marking: Use By 28/12/2026'; 'Batch Number: 26300'; 'Distributed in: TAS', available in independent food retailers including IGA in TAS. FSANZ gives no separate brand field - 'Woodbridge Smokehouse' is read from the product name itself. The page's food-safety-hazard paragraph names Listeria monocytogenes explicitly, so the Pathogen is the page's own word and not an inference from 'microbial contamination'."
    }
]
""")


def fix_7_queue_the_missed_recalls(wb_unused, log) -> int:
    """Queue MANUAL_ROWS through tools/add_manual_row.py.

    Runs as a subprocess on purpose: that script IS the supported entry point
    and it applies merge_master.append_to_pending with every guard —
    already-approved, already-pending and the terminal-rejection guard. A
    re-run is therefore a no-op by construction, and this function reports it
    as one rather than treating "already there" as a failure.

    Takes no workbook handle: the subprocess opens and saves the workbook
    itself, so main() closes its own handle before calling this.
    """
    import subprocess
    import tempfile

    present = _urls_in_workbook()
    wanted = [r for r in MANUAL_ROWS
              if _norm(r["URL"]) not in present]
    for r in MANUAL_ROWS:
        if _norm(r["URL"]) not in present:
            continue
        log(f"7. NOOP  — already in the workbook: {r['Product'][:48]}")
    if not wanted:
        return 0

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(wanted, fh, ensure_ascii=False)
        tmp = fh.name
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), tmp,
         "--xlsx", str(_XLSX_IN_USE[0])],
        cwd=str(ROOT), capture_output=True, text=True)
    for line in (proc.stdout or "").splitlines():
        if line.strip().startswith(("[", "Queued", "re-promotion")):
            log("7.   add_manual_row: " + line.strip()[:150])
    if proc.returncode != 0:
        log(f"7. WARN  — add_manual_row exited {proc.returncode}; stderr: "
            f"{(proc.stderr or '').strip()[:300]}")

    landed = _urls_in_workbook()
    n = 0
    for r in wanted:
        if _norm(r["URL"]) in landed:
            log(f"7. FIXED — queued to Pending: {r['Date']} {r['Source']} "
                f"{r['Product'][:44]}")
            n += 1
        else:
            log(f"7. FAILED — did NOT land, investigate: {r['URL']}")
    return n


EDITS = (fix_1_saucisse_italienne,
         fix_2_pending_region_national,
         fix_3_archived_region_quebec,
         fix_4_gis_ochratoxin_coffee,
         fix_5_cfia_beer_company,
         fix_6_sweep_every_bad_region)

def fix_8_mirror_recalls_json(wb_unused, log) -> int:
    """Re-mirror docs/data/recalls.json from the Recalls sheet.

    recalls.json is what the dashboard reads and what several tests read —
    tests/test_firm_names_are_uniform.py among them. A field repaired
    directly in the workbook leaves it stale, so the suite would still be
    judging the pre-repair data. merge_master.mirror_json_from_xlsx is the
    only sanctioned writer; it is called here rather than reimplemented.
    """
    from pipeline.merge_master import mirror_json_from_xlsx
    xlsx = _XLSX_IN_USE[0]
    n = mirror_json_from_xlsx(xlsx, xlsx.parent / "recalls.json")
    log(f"8. MIRRORED — docs/data/recalls.json rebuilt from the Recalls "
        f"sheet ({n} row(s))")
    return 0


#: fix_7 writes through a subprocess that opens the workbook itself, so it
#: runs after main() has saved and closed its own handle. fix_8 mirrors
#: recalls.json last, once every row edit above is on disk.
POST_EDITS = (fix_7_queue_the_missed_recalls,
              fix_8_mirror_recalls_json)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--xlsx", default=str(XLSX))
    # WRITES BY DEFAULT, on purpose. tools/rebase_and_verify.rebuild_register
    # re-runs every tools/repair_*.py in the code zip with NO arguments —
    # `py(self.repo, r)` — so a script that needs a flag to write silently
    # rebases to a workbook with none of its repairs in it, and the gate
    # zips that. Found the hard way on this run's first gate attempt.
    # --dry-run is the way to look without writing.
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change and write nothing")
    args = ap.parse_args(argv)

    path = Path(args.xlsx)
    if not path.exists():
        print(f"no workbook at {path}", file=sys.stderr)
        return 2

    lines: list[str] = []

    def log(msg: str) -> None:
        lines.append(msg)
        print("  " + msg)

    _XLSX_IN_USE[0] = path
    wb = load_workbook(path)
    print(f"repair_2026_11_19 on {path}")
    total = sum(fn(wb, log) for fn in EDITS)
    if not args.dry_run:
        wb.save(path)
        print(f"  saved field repairs: {path}")
    wb.close()

    # The post-edits write through tools/add_manual_row.py, which opens and
    # saves the workbook itself — so they run only after the handle above is
    # closed, and only when writing was asked for.
    if not args.dry_run:
        total += sum(fn(None, log) for fn in POST_EDITS)
    else:
        for r in MANUAL_ROWS:
            log(f"7. WOULD QUEUE (report only): {r['Date']} {r['Source']} "
                f"{r['Product'][:44]}")

    print(f"  ── {total} edit(s)"
          f"{' (dry run — nothing written)' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
