# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-13. Seven faults plus one missed recall.

Run on a FRESH clone of main by tools/rebase_and_verify.py, which then runs
pipeline.promote_gate_passing --apply behind it.

NOTHING IS REMOVED from Recalls, and nothing from Pending. The commit
message therefore needs no deletion marker.

Every fact written below was read on an authority page during this pass, or
is in the row's own text/URL. Each Notes stamp says which page and what it
said. Where a page could not be opened, the row is left alone and the
reason is reported — not guessed around.


FAULT A — AN AUGUST WARNING PUBLISHED AS A 12 OCTOBER RECALL
============================================================
A GIS (PL) row reached Recalls on 2026-10-13 (confirm agent, 03:27 UTC)
with Date 2026-10-12:

    Company  PPHU Balser Marian Górz
    Product  Cheese
    Reason   "Presence of Listeria monocytogenes (Recall ID GIS-2026-08-19-001)"
    Notes    "Discovered via news: portalspozywczy.pl"
    URL      .../wykrycie-obecnosci-bakterii-listeria-monocytogenes-w-
             jednej-partii-sera-podpuszczkowego

The notice on gov.pl is dated **19.08.2026**. Read this pass: product
"SER GAŁKA Z ZAKOPANEGO 280g", producer "PPHU Balser Marian Górz
Wróblówka, 102B, 34-408 Czarny Dunajec", batch "07286", use-by
"25.09.2026", publication "19.08.2026". The row's own Reason already
carried the regulator's dated identifier — GIS-**2026-08-19**-001 — and
it disagreed with Date by 54 days.

So the DISCOVERY date was written into Date. The recall joined W42's page,
today's daily brief and October's running total, and was absent from
August's — the month whose report and subscriber email are already out.

Date is corrected to the notice date and report_week recomputed (W42 ->
W34) through merge_master.compute_report_week. tests/
test_a_dated_recall_id_agrees_with_date.py holds the shape.

AUGUST'S TOTAL THEREFORE CHANGES BY ONE ROW. That is reported to the
operator, not acted on: rebase_and_verify does not rebuild a closed month,
and republishing August relabels its report "UPDATED" and rewrites the
mailer summary. OPERATOR DECISION.


FAULT B — A PRODUCT TITLE PARSED AS THE COMPANY
===============================================
The FSANZ (AU) row of 2026-10-12 split its own alert title down the middle:

    Company  "Tweedvale Dairies Reduced Fat Milk"     <- the product
    Product  "1L and 2L"                              <- the pack sizes
    Notes    "nan [promote 2026-10-12: ...]"          <- a float, stringified

Read on the FSANZ alert this pass: company "Tweedvale Dairies PTY LTD",
product "Tweedvale Dairies Reduced Fat Milk - 1L and 2L", "Use By
21/10/2026", "Recall published 12 October 2026". The alert's own URL slug
is the title the parser split.

FoodCategory was "unknown" as a direct consequence — no term in "1L and
2L" names a food. With the product restored, enrich_schema.derive() returns
"dairy-other", so the axis is written here too rather than waiting for
tomorrow's schema sweep.

The literal string "nan" at the head of Notes is cleared in the same edit.


FAULT C — THE SIXTH TIME THE HAZARD-GROUP CATCH-ALL LIED
========================================================
The MAPAQ QC row of 2026-10-06 (IGA Grenville ground beef) published with

    Pathogen     "Plastic"                 <- a bare material noun
    HazardGroup  "pathogen-bacterial"

the only one of the register's 63 foreign-material rows in the bacterial
bucket. Two gates were blind to it at once:

  * tools/alert_vocab.py offers "plastic fragment" and "hard plastic" but
    no bare "plastic", so NO subscriber alert could reach the row —
    tests/test_alert_vocab.py::test_every_row_matches_at_least_one_pathogen_term
    was RED.
  * pipeline/_publish_gate's physical class is qualified-forms-only (by
    design: a bare token there would classify "plastic packaging"), so the
    curator refused the hazard of a published row —
    tests/test_curator_scope_2026_09_09.py was RED.

Both are repaired by writing the label the register already uses. Pathogen
becomes "Foreign material (plastic pieces)" — the wording is the row's OWN
Reason, "Presence of possible plastic pieces", and its own URL slug,
"presence-possible-de-morceaux-de-plastique"; the form is the register's,
used once already (idx 1074). Nothing is added that the row did not say.

The quebec.ca notice itself could NOT be read this pass: the agent proxy
rejected the URL with "url exceeds the maximum fetchable length" (HTTP
403). That is a fetch limit, not a site block, and no fact below depends
on the page — the hazard wording is in the row's own two fields.

pipeline/enrich_schema._hazard_group is fixed for the SHAPE, not this
label: bare "Glass", "Metal", "Plastic pieces", "Rubber", "Wood",
"Stones" and "Bone fragments" all reached the bacterial catch-all on main
at 98a7746b, as did "Mercury exceeding limit", "1, 3-Dimethylamylamine
(DMAA)" and "Deoxynivalenol (DON)".


FAULT D — FOUR FRENCH ROWS PUBLISHED IN FRENCH
==============================================
Four RappelConso rows published 2026-10-12. Three carry reviewer 3's
"[needs cleanup: Product not in English]" stamp; the fourth, fiche 23742
("petit saucisson parmesan 75g"), does NOT — the detector's two-hit
threshold cannot see a short name, exactly as it could not see "foie de
poulet". The threshold is deliberately NOT loosened (the docstring of
tests/test_reason_is_not_wholly_foreign.py says why); the morning read of
every Product published in the last 26 h is what catches these.

Three Reason values were also French and were failing two tests:

    23720  "Presence of un ou plusieurs germes pathogènes dans les échantillons"
    23747  "Détéction de listéria"            (the regulator's own typo)
    23742  "Suite à la présence de la Salmonella spp. (agent responsable ...)"

Each fiche was read this pass and each original is kept in Notes.

ALSO ON FICHE 23720. Its "Risques encourus par le consommateur" names TWO
hazards — "Escherichia coli" AND "Salmonella spp (agent responsable de la
salmonellose)" — and the row published with Pathogen "Salmonella" alone,
so a subscriber alerting on E. coli could not be reached. Pathogen takes
the register's existing two-hazard form ("Shiga toxin-producing E. coli
(STEC) and Salmonella", idx 229, is the precedent). Both terms then match
tools/alert_vocab; Tier is 1 either way.


FAULT E — A RECALL BOTH PUBLISHED AND THROWN AWAY
=================================================
Weekly_Rejected holds the same gov.pl URL as Fault A's published row, with
RejectionReason "llm-extraction-failed: Llama timeout or circuit-open;
title-only fallback could not produce clean fields. Not a content verdict
— re-extract when the box is up." The box came up and the row IS now
published, so the archive copy sits there contradicting the register.
tests/test_a_recall_is_not_both_published_and_rejected.py was RED.

Marked SUPERSEDED. NOT deleted — the archives are append-only and this row
is the audit trail of the original extraction failure.


FAULT F — TWO PENDING ROWS BLOCKED ON A FACT PRINTED ON THEIR OWN NOTICE
========================================================================
pipeline.promote_gate_passing refused nine Pending rows, seven of them on
"Company is empty". Reviewer 2 (the "Recall review agent") did not run its
03:xx UTC pass today — reviewers 1 and 3 both did (13f8f94f, 1811d734) —
so two in-window recalls sat held with the firm printed on their own
notice. Both notices were read this pass:

  GIS (PL), .../w-jednej-partii-serow-rozne-rodzaje, published 12.10.2026
      producer "Mlecz Ser Górskie Sp. z o. o. ul. ks. Juliana Łyska 7A,
      34-480 Jabłonka", batch "261.2026/M", eight regional cheeses with
      best-before 18.10.2026 / 07.11.2026. A DIFFERENT notice from the
      same producer's 2026-09-09 warning already in Recalls (batch
      223.2026/M, idx 350) — different batch, different URL, not a
      duplicate.

  FSAI (IE), .../recall-of-a-batch-of-smoked-slamon, published
      "Monday, 12 October 2026" (the slug's typo is FSAI's own)
      "Recall of specific batches of Skellig Bay Smoked Salmon Slices due
      to the presence of Listeria monocytogenes", pack size 200 g, use-by
      "04/11/26 and 06/11/26", "The Good Fish Company are recalling the
      above batches".

The remaining five stay in Pending and are reported, not guessed:
  * USDA FSIS Grace Vienna sausages (#PHA-10062026-01) — the firm is not
    in the row's text and fsis.usda.gov would not open for this pass
    (CLIENT_ERROR from the target; it is Akamai-protected and the pipeline
    reaches it with curl-impersonate from Actions, which this sandbox has
    no route to). Reviewer 2's job, with the page in hand.
  * FAVV, MHLW, CDC and FDA import-alert rows — listing pages, a
    monitoring spreadsheet and two search snippets, none of them a
    regulator's own per-recall notice. They fail R3, not a field check.


FAULT G — ONE RECALL THE PIPELINE MISSED IN THE LAST 24 H
=========================================================
CFS (HK) press release 20261012_12665, Monday 12 October 2026: "CFS finds
excessive Bacillus cereus in sample of preserved bean curd", 140 000 per
gram, retailer instructed to stop sale, remove from shelves and recall the
affected batch. Searched by URL, by "12665", by "腐乳", by "bean curd" and
by "公和" across all five sheets: absent from every one.

Not a stale source. CFS's newest register row is 2026-10-09 (three days;
its normal gap bar is 30), today's scrape-fleet commit exists (e411b2b5),
and the register already holds three press releases of exactly this shape
(20260930_12635, 20261007_12650, 20261009_12660). A single miss.

Queued through tools/add_manual_row.py, as a person would, so it enters
Pending under the same dedup and the same two review stages.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-13"

# ── the rows this pass touches, keyed by the regulator's own URL ──────────
GIS_AUGUST = (
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-"
    "wykrycie-obecnosci-bakterii-listeria-monocytogenes-w-jednej-partii-"
    "sera-podpuszczkowego")
GIS_OCTOBER = (
    "https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-"
    "wykrycie-obecnosci-bakterii-listeria-monocytogenes-w-jednej-partii-"
    "serow-rozne-rodzaje")
FSANZ_TWEEDVALE = ("https://www.foodstandards.gov.au/food-recalls/"
                   "recall-alert/tweedvale-dairies-reduced-fat-milk-1l-and-2l")
FSAI_SKELLIG = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                "recall-of-a-batch-of-smoked-slamon")
MAPAQ_PLASTIC_SLUG = "presence-possible-de-morceaux-de-plastique"
FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"

_SRC = ("read this pass on the regulator's own notice")


def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _urls(ws):
    """(row number, URL) for every data row."""
    hdr = _hdr(ws)
    if "URL" not in hdr:
        return []
    col = hdr.index("URL") + 1
    return [(r, str(ws.cell(row=r, column=col).value or "").strip())
            for r in range(2, ws.max_row + 1)]


def _find(ws, url):
    """Row numbers whose URL is EXACTLY `url`, ignoring a trailing slash.

    Exact, not substring. gov.pl re-uses a slug stem and appends a digit:
    ``...-w-jednej-partii-sera-podpuszczkowego`` is the 19.08.2026 warning
    and ``...-sera-podpuszczkowego2`` is a DIFFERENT producer's 2026-09-09
    warning. A substring match on the first would have re-dated the second.
    """
    want = url.rstrip("/")
    return [r for r, u in _urls(ws) if u.rstrip("/") == want]


def _find_containing(ws, needle):
    """Row numbers whose URL contains `needle`. For unique slugs only."""
    return [r for r, u in _urls(ws) if needle in u]


def _set(ws, row, field, value):
    hdr = _hdr(ws)
    if field not in hdr:
        return False
    cell = ws.cell(row=row, column=hdr.index(field) + 1)
    if str(cell.value or "") == str(value):
        return False
    cell.value = value
    return True


def _get(ws, row, field):
    hdr = _hdr(ws)
    if field not in hdr:
        return ""
    v = ws.cell(row=row, column=hdr.index(field) + 1).value
    return "" if v is None else str(v)


def _note(ws, row, text, *, drop=()):
    """Append to Notes, first dropping any substring in `drop`."""
    prev = _get(ws, row, "Notes").strip()
    if prev.lower() == "nan":
        prev = ""
    elif prev.lower().startswith("nan "):
        prev = prev[4:].strip()
    for d in drop:
        prev = prev.replace(d, "").strip()
    while "  " in prev:
        prev = prev.replace("  ", " ")
    _set(ws, row, "Notes", (prev + " " + text).strip())


# ── FAULT A ───────────────────────────────────────────────────────────────
def fix_august_warning_dated_october(wb) -> int:
    from pipeline.merge_master import compute_report_week
    ws = wb["Recalls"]
    n = 0
    for r in _find(ws, GIS_AUGUST):
        if _get(ws, r, "Date")[:10] == "2026-08-19":
            continue
        was = _get(ws, r, "Date")[:10]
        was_week = _get(ws, r, "report_week")
        _set(ws, r, "Date", "2026-08-19")
        _set(ws, r, "report_week", compute_report_week("2026-08-19"))
        _set(ws, r, "Product",
             "Regional ball cheese from Zakopane (ser gałka z Zakopanego), "
             "280 g — lot 07286, use by 25.09.2026")
        _note(ws, r,
              "[date-fix %s: Date was %s, the date this row was DISCOVERED "
              "in a portalspozywczy.pl article, not the date of the notice. "
              "The GIS warning at %s is dated 19.08.2026 — %s — and the "
              "row's own Reason already carried the regulator's dated "
              "identifier GIS-2026-08-19-001. Date -> 2026-08-19, "
              "report_week %s -> %s. NOTE FOR THE OPERATOR: this moves one "
              "row out of October/W42 and into August, a CLOSED month whose "
              "report and subscriber email are already out. The August "
              "report is NOT rebuilt here.] [product read on the same page: "
              "\"SER GAŁKA Z ZAKOPANEGO 280g\", batch \"07286\", use-by "
              "\"25.09.2026\"; producer \"PPHU Balser Marian Górz "
              "Wróblówka, 102B, 34-408 Czarny Dunajec\"] "
              "[original product: SER GAŁKA Z ZAKOPANEGO 280g]"
              % (TODAY, was, GIS_AUGUST, _SRC, was_week or "(empty)",
                 compute_report_week("2026-08-19")))
        print(f"  Recalls row {r}: Date {was} -> 2026-08-19, "
              f"report_week {was_week} -> {compute_report_week('2026-08-19')}")
        n += 1
    return n


# ── FAULT B ───────────────────────────────────────────────────────────────
def fix_tweedvale_title_split(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for r in _find(ws, FSANZ_TWEEDVALE):
        if _get(ws, r, "Company") == "Tweedvale Dairies PTY LTD":
            continue
        co, pr = _get(ws, r, "Company"), _get(ws, r, "Product")
        _set(ws, r, "Company", "Tweedvale Dairies PTY LTD")
        _set(ws, r, "Product",
             "Tweedvale Dairies Reduced Fat Milk, 1 L and 2 L "
             "(use by 21/10/2026)")
        _set(ws, r, "FoodCategory", "dairy-other")
        _note(ws, r,
              "[field-fix %s: the alert title was split down the middle — "
              "Company held %r (the product) and Product held %r (the pack "
              "sizes), and FoodCategory was 'unknown' because no term in "
              "'1L and 2L' names a food. The FSANZ alert, %s, prints "
              "company \"Tweedvale Dairies PTY LTD\", product \"Tweedvale "
              "Dairies Reduced Fat Milk - 1L and 2L\", \"Use By "
              "21/10/2026\", \"Recall published 12 October 2026\", sold "
              "\"in independent retailers including IGA in SA\". "
              "FoodCategory -> dairy-other, which is what "
              "enrich_schema.derive() now returns for the restored product. "
              "The literal string 'nan' at the head of Notes — a "
              "stringified float from the writer — is cleared in the same "
              "edit.]" % (TODAY, co, pr, FSANZ_TWEEDVALE))
        print(f"  Recalls row {r}: Company {co!r} -> 'Tweedvale Dairies "
              f"PTY LTD'; Product {pr!r} -> full title")
        n += 1
    return n


# ── FAULT C ───────────────────────────────────────────────────────────────
def fix_bare_plastic_hazard(wb) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    pcol = hdr.index("Pathogen") + 1
    n = 0
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(row=r, column=pcol).value or "").strip() != "Plastic":
            continue
        url = _get(ws, r, "URL")
        reason = _get(ws, r, "Reason")
        if MAPAQ_PLASTIC_SLUG not in url or "plastic" not in reason.lower():
            print(f"  Recalls row {r}: Pathogen 'Plastic' but the wording is "
                  f"not in its own Reason/URL — LEFT ALONE, reported")
            continue
        _set(ws, r, "Pathogen", "Foreign material (plastic pieces)")
        _set(ws, r, "HazardGroup", "foreign-material")
        _note(ws, r,
              "[hazard-label %s: Pathogen was the bare material noun "
              "'Plastic', which no term in tools/alert_vocab.py can reach "
              "and which pipeline/_publish_gate's qualified-forms-only "
              "physical class refuses, so this published row was "
              "unreachable by every subscriber alert AND refused by the "
              "curator. HazardGroup was 'pathogen-bacterial' — the only "
              "one of the register's 63 foreign-material rows in the "
              "bacterial bucket. Relabelled to the register's own form "
              "using this row's OWN wording: Reason %r and the URL slug "
              "'%s'. Nothing is added that the row did not say. The "
              "quebec.ca notice could not be opened this pass (agent proxy: "
              "'url exceeds the maximum fetchable length', HTTP 403) and no "
              "fact here needs it. pipeline/enrich_schema._hazard_group is "
              "fixed for the shape, not the label.]"
              % (TODAY, reason, MAPAQ_PLASTIC_SLUG))
        print(f"  Recalls row {r}: Pathogen 'Plastic' -> 'Foreign material "
              f"(plastic pieces)', HazardGroup -> foreign-material")
        n += 1
    return n


# ── FAULT D ───────────────────────────────────────────────────────────────
#: fiche -> (English Product, English Reason or None, Notes stamp pieces)
FRENCH_ROWS = {
    "23763": dict(
        product=("Various organic cheeses: winegrower cheese, medium-aged "
                 "vintner cheese, Geifertshofener Classics, Swabian "
                 "Trollinger cheese, Swabian cream cheese, Geifertshofen "
                 "pepper cheese, Geifertshofen blue-clover cheese"),
        reason=None,
        read=('fiche title "Divers fromages BIO Dorfkäserei Geifertshofen", '
              '"Nom de la marque du produit" "Dorfkäserei Geifertshofen", '
              '"Motif du rappel" "Listeria", "Risques encourus par le '
              'consommateur" "Listeria monocytogenes (agent responsable de '
              'la listériose)", date 12/10/2026'),
    ),
    "23720": dict(
        product="Pork cheek",
        reason="Presence of one or more pathogenic germs in the samples",
        pathogen="Escherichia coli and Salmonella spp.",
        read=('"Nom du produit" "JOUE DE PORC", "Marque" "DISTRIBUTION '
              'VALLEGRAIN", "Motif du rappel" "Présence d\'un ou plusieurs '
              'germes pathogènes dans les échantillons", "Risques encourus '
              'par le consommateur" "Escherichia coli" AND "Salmonella spp '
              '(agent responsable de la salmonellose)", distributor '
              '"BOUCHERIE ETWEIN", date 12/10/2026'),
    ),
    "23747": dict(
        product="Ground beef",
        reason="Detection of Listeria",
        read=('fiche title "Haché de boeuf", "Nom de la marque du produit" '
              '"Hachage au niveau du rayon traditionnel", "Motif du rappel" '
              '"Détéction de Listéria" (the regulator\'s own spelling), '
              '"Risques encourus" "Listeria monocytogenes (agent responsable '
              'de la listériose)", distributor "Grand frais situé à '
              'Lieusaint", date 12/10/2026'),
    ),
    "23742": dict(
        product="Small Parmesan dry sausage, 75 g",
        reason=("Following the presence of Salmonella spp. "
                "(the agent responsible for salmonellosis)"),
        read=('"Noms des modèles ou références" "PETIT SAUCISSON PARMESAN '
              '75G", "Nom de la marque du produit" "AUCHAN", "Origine de la '
              'fiche" "AUCHAN RETAIL SERVICES AUCHAN", "Motif du rappel" '
              '"Suite à la présence de la Salmonella spp (agent responsable '
              'de la salmonellose)", date 12/10/2026'),
    ),
}

STAMPS = ("[needs cleanup: Product not in English]",
          "[needs cleanup: Company/Brand not in English]")


def translate_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, spec in FRENCH_ROWS.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            print(f"  fiche {fiche}: no row on main — skipped")
            continue
        for r in rows:
            if _get(ws, r, "Product") == spec["product"]:
                continue
            was_p = _get(ws, r, "Product")
            was_r = _get(ws, r, "Reason")
            bits = ["[translated %s: Product -> English. Read on %s: %s.]"
                    % (TODAY, FICHE % fiche, spec["read"]),
                    "[original product: %s]" % was_p]
            _set(ws, r, "Product", spec["product"])
            if spec.get("reason"):
                _set(ws, r, "Reason", spec["reason"])
                bits.append('[original Reason (fr): "%s"]' % was_r)
            if spec.get("pathogen"):
                was_h = _get(ws, r, "Pathogen")
                _set(ws, r, "Pathogen", spec["pathogen"])
                bits.append(
                    "[hazard %s: Pathogen was %r, but the fiche's \"Risques "
                    "encourus par le consommateur\" names BOTH Escherichia "
                    "coli and Salmonella spp., so no E. coli alert could "
                    "reach this row. Two-hazard form as used at idx 229. "
                    "Tier is 1 on either hazard.]" % (TODAY, was_h))
            unstamped = all(s not in _get(ws, r, "Notes") for s in STAMPS)
            if unstamped:
                bits.append(
                    "[detector-miss %s: reviewer 3 did NOT stamp this row "
                    "'[needs cleanup: Product not in English]' — "
                    "detect_language's two-hit threshold cannot see a name "
                    "this short, the same way it could not see 'foie de "
                    "poulet'. The threshold is deliberately not loosened "
                    "(see tests/test_reason_is_not_wholly_foreign.py); the "
                    "morning read of every Product published in the last "
                    "26 h is what caught it.]" % TODAY)
            _note(ws, r, " ".join(bits), drop=STAMPS)
            print(f"  Recalls row {r} (fiche {fiche}): Product {was_p[:40]!r} "
                  f"-> {spec['product'][:40]!r}"
                  + ("  [unstamped by the detector]" if unstamped else ""))
            n += 1
    return n


# ── FAULT E ───────────────────────────────────────────────────────────────
def supersede_contradicting_archive_row(wb) -> int:
    """The archive copy of Fault A's row says nothing about which copy wins."""
    recalls = {u.rstrip("/").lower() for _, u in _urls(wb["Recalls"]) if u}
    n = 0
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        if col not in _hdr(ws):
            continue
        for r in range(2, ws.max_row + 1):
            url = _get(ws, r, "URL").strip().rstrip("/").lower()
            if not url or url not in recalls:
                continue
            blob = _get(ws, r, col) + " " + _get(ws, r, "Notes")
            if any(w in blob.upper() for w in
                   ("SUPERSEDED", "UNRESOLVED", "DUPLICATE", "RESOLVED")):
                continue
            prev = _get(ws, r, col).strip()
            if "extraction-failed" not in prev.lower():
                # Only the extraction-failure shape is understood here. Any
                # other silent contradiction is a different verdict and gets
                # reported for a decision rather than given this wording.
                print(f"  {sheet} row {r}: silently contradicts a PUBLISHED "
                      f"row but its refusal is not an extraction failure — "
                      f"LEFT ALONE, reported: {prev[:120]!r}")
                continue
            _set(ws, r, col,
                 ("[SUPERSEDED %s — this archive row refused the recall for "
                  "an EXTRACTION failure, not on its content (\"Not a "
                  "content verdict — re-extract when the box is up\"), and "
                  "the same URL is now PUBLISHED in Recalls with the fields "
                  "read off the regulator's page. The Recalls copy is what "
                  "the register says. Kept, not deleted: the archives are "
                  "append-only and this row is the audit trail of the "
                  "original failure.] %s" % (TODAY, prev)).strip())
            print(f"  {sheet} row {r}: marked SUPERSEDED ({url[:70]})")
            n += 1
    return n


# ── FAULT F ───────────────────────────────────────────────────────────────
def unblock_pending_rows(wb) -> int:
    ws = wb["Pending"]
    n = 0

    for r in _find(ws, GIS_OCTOBER):
        if _get(ws, r, "Company"):
            continue
        was_p = _get(ws, r, "Product")
        _set(ws, r, "Company", "Mlecz Ser Górskie Sp. z o. o.")
        _set(ws, r, "Product",
             "Regional cheeses, batch 261.2026/M: Gałka Naturalna 250 g; "
             "Gałka with wild garlic 250 g; Gałka with paprika 250 g; Gałka "
             "Smoked 250 g; \"Tabliczka\" Smoked 250 g; \"Kukurydza\" "
             "Smoked 280 g; \"Korbacz\" Natural 1 kg; \"Sietniok\" Smoked "
             "180 g")
        _set(ws, r, "Reason",
             "Detection of Listeria monocytogenes in one batch of cheeses "
             "(various kinds)")
        _note(ws, r,
              "[reviewer-2 stand-in %s: the Recall review agent did not run "
              "its 03:xx UTC pass today (reviewers 1 and 3 both did), so "
              "this row sat held on an empty Company with the producer "
              "printed on its own notice. Read on %s: producer \"Mlecz Ser "
              "Górskie Sp. z o. o. ul. ks. Juliana Łyska 7A, 34-480 "
              "Jabłonka\", hazard \"wykrycie obecności bakterii Listeria "
              "monocytogenes\", batch \"261.2026/M\", publication "
              "\"12.10.2026\", eight products with best-before 18.10.2026 / "
              "07.11.2026. A DIFFERENT notice from the same producer's "
              "2026-09-09 warning already in Recalls (batch 223.2026/M) — "
              "different batch, different URL. Product and Reason "
              "translated; product designations (Gałka, Tabliczka, "
              "Kukurydza, Korbacz, Sietniok) kept as printed.] "
              "[original product: %s]" % (TODAY, GIS_OCTOBER, was_p),
              drop=STAMPS)
        print(f"  Pending row {r}: GIS (PL) — Company filled, "
              f"Product/Reason translated")
        n += 1

    for r in _find(ws, FSAI_SKELLIG):
        if _get(ws, r, "Company") and _get(ws, r, "Pathogen"):
            continue
        _set(ws, r, "Company", "The Good Fish Company")
        _set(ws, r, "Pathogen", "Listeria monocytogenes")
        _set(ws, r, "Date", "2026-10-12")
        _set(ws, r, "Product",
             "Skellig Bay Smoked Salmon Slices, 200 g "
             "(use by 04/11/26 and 06/11/26)")
        _set(ws, r, "Reason",
             "Recall of specific batches due to the presence of Listeria "
             "monocytogenes")
        _note(ws, r,
              "[reviewer-2 stand-in %s: row was held with Pathogen, Date "
              "and Reason empty — \"FSAI HTML listing fallback\". Read on "
              "%s: \"Recall of specific batches of Skellig Bay Smoked "
              "Salmon Slices due to the presence of Listeria "
              "monocytogenes\", pack size 200 g, use-by \"04/11/26 and "
              "06/11/26\", \"The Good Fish Company are recalling the above "
              "batches\", published \"Monday, 12 October 2026\". The "
              "misspelling in the URL slug is FSAI's own.]"
              % (TODAY, FSAI_SKELLIG))
        print(f"  Pending row {r}: FSAI (IE) — Company, Pathogen, Date, "
              f"Reason filled from the alert")
        n += 1
    return n


# ── FAULT G ───────────────────────────────────────────────────────────────
MISSED = [{
    "Date": "2026-10-12",
    "Source": "CFS (HK)",
    "Company": "Kung Wo Dou Bun Chong",
    "Brand": "Kung Wo Dou Bun Chong",
    "Product": "Rose preserved bean curd",
    "Pathogen": "Bacillus cereus",
    "Reason": ("Bacillus cereus at 140 000 per gram in a sample of "
               "preserved bean curd collected from a retailer in Shum Shui "
               "Po. The CFS instructed the retailer to stop sale, remove "
               "from shelves and recall the affected batch, and the "
               "retailer surrendered the remaining stock for disposal. "
               "Affected batch sold 24 September to 9 October 2026."),
    "Class": "Recall",
    "Country": "Hong Kong",
    "Outbreak": 0,
    "URL": "https://www.cfs.gov.hk/english/press/20261012_12665.html",
    "Notes": (
        "[manual 2026-10-13: AFTS morning fix pass, missed-recall sweep. "
        "Read on the CFS press release "
        "https://www.cfs.gov.hk/english/press/20261012_12665.html, dated "
        "\"Monday, October 12, 2026\": headline \"CFS finds excessive "
        "Bacillus cereus in sample of preserved bean curd\"; level \"the "
        "sample contained Bacillus cereus at a level of 140 000 per "
        "gram\"; sample \"collected from a retailer in Sham Shui Po\"; the "
        "CFS \"instructed it to stop sale, remove from shelves and recall "
        "the affected batch of the product\" and \"The retailer has "
        "voluntarily surrendered the remaining products from the same "
        "batch to the CFS for disposal\". Absent from all five sheets on "
        "base 98a7746b — searched by URL, by \"12665\", by \"腐乳\", by "
        "\"bean curd\" and by \"公和\".] "
        "[original product: 玫瑰腐乳 — the CFS notes \"(No product name in "
        "English)\", so the English above is a faithful translation of the "
        "Chinese plus the CFS's own words \"preserved bean curd\"] "
        "[original company: 公和荳品廠 — the CFS prints no English name for "
        "the firm; rendered as its standard romanization, per the operator "
        "rule of 2026-10-01] "
        "[the CFS does not name the retailer, only \"a retailer in Shum "
        "Shui Po\"; the firm above is the maker named on the product line] "
        "[no lot code or best-before is printed on the notice — the CFS "
        "gives only the batch's sales period]"),
}]


def _fill_existing_pending(rows) -> list:
    """A missed recall may have reached Pending in the meantime through a
    collector, with a headline for Product and "Press Release" for Company
    (CFS (HK) 20261012_12665 did, on 2026-10-13 — add_manual_row then skips
    it as a duplicate URL and the junk row stays held). Such a row is
    corrected IN PLACE from the fields read on the authority page; only
    rows absent from every sheet go to add_manual_row."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    col = {h: i for i, h in enumerate(_hdr(ws))}
    left, changed = [], 0
    for rec in rows:
        hit = None
        for row in ws.iter_rows(min_row=2):
            if str(row[col["URL"]].value or "").strip().lower() == rec["URL"].lower():
                hit = row
                break
        if hit is None:
            left.append(rec)
            continue
        old = {k: hit[col[k]].value for k in ("Source", "Company", "Brand", "Product")
               if k in col}
        for k, v in rec.items():
            if k in col and k != "Notes":
                hit[col[k]].value = v
        hit[col["Notes"]].value = (rec["Notes"] + " [Pending row corrected in place; "
                                   "collector values were " + json.dumps(old, ensure_ascii=False)
                                   + "]").strip()
        changed += 1
        print(f"  Pending: {rec['Source']} {rec['URL']} corrected in place from the authority page")
    if changed:
        wb.save(XLSX)
    wb.close()
    return left


def add_missed_recalls() -> int:
    rows = _fill_existing_pending(MISSED)
    done = len(MISSED) - len(rows)
    if not rows:
        from pipeline.merge_master import mirror_json_from_xlsx
        mirror_json_from_xlsx(XLSX, JSON)
        return done
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip())
    if out.returncode != 0:
        print("  add_manual_row FAILED:", (out.stderr or "")[-900:])
        return done
    return done + len(rows)


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    before_pending = wb["Pending"].max_row - 1

    print("FAULT A — an August GIS warning published as a 12 October recall")
    n_a = fix_august_warning_dated_october(wb)
    print(f"  {n_a} row(s) re-dated\n")

    print("FAULT B — an FSANZ alert title parsed as the company")
    n_b = fix_tweedvale_title_split(wb)
    print(f"  {n_b} row(s) repaired\n")

    print("FAULT C — a bare material noun as the hazard label")
    n_c = fix_bare_plastic_hazard(wb)
    print(f"  {n_c} row(s) relabelled\n")

    print("FAULT D — four RappelConso rows published in French")
    n_d = translate_french_rows(wb)
    print(f"  {n_d} row(s) translated\n")

    print("FAULT E — a recall both published and thrown away")
    n_e = supersede_contradicting_archive_row(wb)
    print(f"  {n_e} archive row(s) marked SUPERSEDED\n")

    print("FAULT F — Pending rows blocked on a fact on their own notice")
    n_f = unblock_pending_rows(wb)
    print(f"  {n_f} row(s) unblocked\n")

    if n_a or n_b or n_c or n_d or n_e or n_f:
        wb.save(XLSX)
    wb.close()

    print("FAULT G — one recall the pipeline missed in the last 24 h")
    n_g = add_missed_recalls()
    print(f"  {n_g} row(s) queued into Pending\n")

    total = n_a + n_b + n_c + n_d + n_e + n_f + n_g
    if not total:
        print("nothing to do")
        print("ROWS_REMOVED=0")
        return 0

    wb2 = openpyxl.load_workbook(XLSX, read_only=True)
    after = wb2["Recalls"].max_row - 1
    after_pending = wb2["Pending"].max_row - 1
    wb2.close()
    print(f"Recalls {before} -> {after}")
    print(f"Pending {before_pending} -> {after_pending}")

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")

    print("ROWS_REMOVED=0")      # nothing is removed from Recalls here
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
