# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-14.

Run on a FRESH clone of main by tools/rebase_and_verify.py, which then runs
pipeline.promote_gate_passing --apply behind it.

NOTHING IS REMOVED from Recalls or from Pending. The commit message needs
no deletion marker.

Every fact written below was read on an authority page during this pass, or
is in the row's own text/URL. Each Notes stamp says which page and what it
said. Where a page could not be opened, the row is LEFT ALONE and the
reason is reported — not guessed around.


FAULT 0 — YESTERDAY'S WHOLE PASS WAS SILENTLY OVERWRITTEN
=========================================================
This is why faults A-F of tools/repair_2026_10_13.py are re-applied here
instead of being assumed done.

The operator uploaded the 2026-10-13 pass between 19:12:01 and 19:14:14
UTC (7 commits, "Add files via upload"). Measured on that upload:

    Recalls          2022 -> 2024 rows
    GIS Balser row   Date 2026-10-12 -> 2026-08-19, report_week W42 -> W34
    MAPAQ row        Pathogen "Plastic" -> "Foreign material (plastic pieces)"
    FSANZ row        Company/Product un-split
    4 French rows    Product + Reason translated, needs-cleanup stamps dropped
    docs/2026-W34.html, docs/2026-M08.html, docs/2026-W42.html rebuilt

The NEXT commit to touch the workbook,

    bfcb954e  2026-10-13 19:15:05 UTC  FSIS Bot
    "Tavily gap-finder (standalone): +4 rows to Pending (2026-10-13T19:12:30Z)"

has a6b733b1 — the last upload commit — as its PARENT, and carries Recalls
back at 2022 rows with every field repair gone. Verified on main at base
b80a59a4: the two rows the pass added (CFS (HK) 20261012_12665 and the GIS
(PL) "serów (różne rodzaje)" notice) are absent from Recalls and are
sitting in Pending again, and the same five tests are red again.

Three minutes. The cause is in pipeline/commit_github.py and is fixed in
the code half of this pass (see tests/test_commit_github_path_aliasing.py):
nine callers pass an ABSOLUTE path to git_commit_and_push, every
cumulative-file guard compares against the RELATIVE constant, and the
alias let _restore_non_cumulative copy a stale workbook over the
just-pulled remote. Until that fix is on main, EVERY upload is exposed to
the same loss, so it is the first thing in the code zip.

A LIVE SITE DEFECT follows from the clobber and is repaired by re-applying
fault A: docs/2026-W34.html and docs/2026-M08.html were rebuilt by
yesterday's upload and still carry the Balser recall, while the workbook
has it back on 2026-10-12 — so docs/2026-W42.html carries it too. One
recall, two weekly pages.


FAULT H — TWO MORE RAPPELCONSO ROWS PUBLISHED IN FRENCH
=======================================================
Fiches 23767 and 23768 were promoted on 2026-10-13, after yesterday's
pass had run, and published with French Product and French Reason:

    23767  "crème fraiche. la ferme du bignon; lot n°39"
           Reason "Detection of listéria"
    23768  "salade nordique barquettes de 250g,500g,800g lots
            f2627216,f2627217,f2627218 dlc au 14/10/26"
           Reason "Presence of listéria monocytogénès"

NEITHER carries reviewer 3's "[needs cleanup: Product not in English]"
stamp — the same detector miss as "foie de poulet" and "petit saucisson
parmesan 75g": detect_language's two-hit threshold cannot see a short
name, and both Reason values open with an English word, which is enough to
keep them under test_reason_is_not_wholly_foreign's threshold too. The
threshold is deliberately NOT loosened; the morning read of every Product
published in the last 26 h is what catches these.

Read this pass on the fiches themselves:
  23767 — fiche 2026-10-0096 is the "creme crue" listing entry; the fiche
          page gives brand "La ferme du bignon", lot "39", publication
          13/10/2026, motif "Détection de listéria".
  23768 — fiche 2026-10-0097: "SALADE NORDIQUE", "BARQUETTES DE
          250g,500g,800g", lots "F2627216" "F2627217" "F2627218", "Date
          limite de consommation 14/10/2026", brand "EMB 94073 Le", risk
          "Listeria monocytogenes (agent responsable de la listériose)",
          motif "présence de listéria monocytogénès" (the regulator's own
          spelling), distributors "magasins marché frais", publication
          13/10/2026.

"EMB 94073 Le" is NOT a firm: it is the French packing-centre health mark
printed on the pack. The fiche states no "raison sociale" at all — its
only company-ish field is "Origine de la fiche : ALPHAPRIM", which is the
notice's submitter, not the recalling firm. So Company/Brand are LEFT AS
PUBLISHED and the finding is reported; inventing a firm here is the one
unforgivable error, and promoting "ALPHAPRIM" to Company would be exactly
that.


FAULT I — FOUR PENDING ROWS BLOCKED ON A FACT PRINTED ON THEIR OWN NOTICE
=========================================================================
pipeline.promote_gate_passing on base b80a59a4 held 18 of 20 Pending rows.
Four of them were held on a field that is in the row's own text or on the
regulator page, and all four are in scope:

I1  RappelConso 23765 — "filet de maquereau", Pathogen EMPTY, Reason
    "Presence histamine >200mg". Two defects at once:

      * pipeline/enrich_pending_offline.RULES had no histamine rule, so
        the offline enricher reported "no hazard term in the row's own
        text" on a register that has published histamine five times. Fixed
        in the code half.
      * reviewer 2 then REJECTED the twin row that carries the canonical
        URL with "The provided URL is a listing page, not the specific
        recall page". That is factually wrong:
        /fiche-rappel/23765/interne IS the per-recall page — read this
        pass, it is fiche 2026-10-0094, "filet de maquereau", lot 115553,
        GTIN 0203750000000, "Date limite de consommation 09/10/2026",
        risk "contaminants biologiques — toxines endogènes: histamine",
        motif "PRESENCE HISTAMINE >200MG", distributor "SUPERU ST CIERS
        SUR GIRONDE", department Gironde (33), published 13/10/2026.

    The surviving row's URL is RappelConso's own static copy of that same
    fiche (/static/fiches/a289da48-...html); it is moved to the canonical
    /fiche-rappel/23765/interne so the published row cites the page a
    reader can navigate to, and the rejected twin and its two
    Weekly_Rejected copies are annotated SUPERSEDED — never deleted, the
    archives are append-only.

I2  RappelConso 23764 — "paupiette chorizo", Company EMPTY. The fiche
    (2026-10-0093, read this pass) prints brand "Sans marque", lot
    "26272056064", use-by 09/10/2026, risk "Listeria monocytogenes",
    motif "NON CONFORMITE MICROBIOLOGIQUE", distributors "Magasins U",
    publication 13/10/2026, and NO "nom juridique du fabricant". So the
    firm is genuinely not published. Company is written in the register's
    own existing wording for that case — "Not named on the notice (sold
    at ...)", as already published on the 2026-10-07 Savoy-style pork
    paupiette row — which states what the notice says and invents nothing.

I3  GIS (PL) bottled water — Pathogen EMPTY because the whole
    foreign-material block of enrich_pending_offline spoke only English
    and French, and the notice is Polish: "możliwa obecność fragmentów
    szkła". Read this pass on the notice (13.10.2026): producer
    "Uzdrowisko Ciechocinek S.A. - Zakłady Produkcji Zdrojowej", product
    "KRYSTYNKA naturalna woda mineralna gazowana, 330 ml", batch
    "FS224/3". Multilingual rules added in the code half; Company,
    Product and Reason filled here.

I4  FSAI (IE) Ready brek — Pathogen, Date and Reason EMPTY: "FSAI HTML
    listing fallback". The hazard is on the alert, not in the row, so no
    enricher could have found it. Read this pass on FSAI alert 2026.65,
    "Tuesday, 13 October 2026", "Category 1: For Action", country of
    origin United Kingdom: "possible presence of flour beetle", "The
    presence of flour beetles would render the products unfit for human
    consumption"; Ready Brek Original 750 g and 450 g and Tesco Super
    Smooth Porridge 750 g, identified by best-before date only — the
    alert lists NO batch codes. "flour beetle" is also added to the
    offline enricher's pest vocabulary, which knew only "insect", "pest"
    and "nuisible".

The other 14 held rows are LEFT ALONE and reported: nine are Google-News
and Tavily artefacts whose Product is a headline or a paragraph of FDA
symptom boilerplate, and the remaining five (USDA FSIS Grace Vienna
sausages, CFS (HK) preserved bean curd, SFA, MHLW, Livsmedelsverket) need
a page this sandbox cannot open. See the run report.


FAULT J — RECALLS THE PIPELINE MISSED
=====================================
Swept this pass with the R1-R6 rules. Every row below was searched for in
ALL FIVE sheets by URL, by fiche/management number, by company and by a
distinctive product word before being called missing.

J1  RappelConso fiche 23772 (13/10/2026, ref 2026-10-0101) — "Chrysanthemum
    Bud", brand "Nature's best harvest", lot/DDM 17/04/2028, risk
    "Dépassement des limites autorisées de pesticides", motif "Présence
    de pesticides", sold in ten French departments. IN WINDOW. The fiche
    names no pesticide, so the hazard is the register's generic
    "Pesticide residues" and no substance is invented.

J2  NVWA (NL) 13-10-2026 09:36 — "Cook korianderzaad Gemalen", brand
    Cook, "Het product bevat mogelijk salmonella", THT 08-2028, barcode
    3417960022619, Odin article 10749, sold at Ekoplaza and other organic
    shops. The warning is issued by Udea and Odin Groothandel; the notice
    names NO producer, so Company is the two firms that issued it. IN
    WINDOW.

J3  NVWA (NL) 13-10-2026 14:55 — "Cook Kerrie mild", brand Cook, "Het
    product bevat mogelijk salmonella", THT 08-2028, barcode
    3417960022657, Odin article 10743, issued by Odin Groothandel, no
    producer named. IN WINDOW.

J4  CAA (JP) 掲載日 2026/10/13, management number 00000035958 —
    さんれいフーズ "島根県産牛肉入りメンチカツ", 事由
    "異物（金属片）混入のおそれ" (possible foreign matter, metal
    fragments), 対応開始日 2026年09月18日, 返金／回収. IN WINDOW by the
    regulator's publication date; the earlier 対応開始日 is recorded in
    Notes. Firm and product are romanised/translated per the operator
    rule of 2026-10-01 with the originals kept in Notes. recall.caa.go.jp
    has NO module under scrapers/ — these rows reach the register only
    through the JP gap finder, which ran 2026-10-13 and appended nothing.

J5  RappelConso fiche 23745 (12/10/2026, ref 2026-10-0074) — "TENDERS
    POULET EPICES STRIPS OUMATY (2,5KGx4) 10KG", brand OUMATY, lots
    1009260101 / 1009260103 / 1009260105, GTIN 3701479402261, risk
    "Résidus de médicaments vétérinaires", motif "antibiotique détecté
    dans de la viande de poulet utilisée pour la fabrication du produit",
    distributors METRO, KHADISPAL, LACORP LA REUNION.

J6  RappelConso fiche 23702 (12/10/2026, ref 2026-10-0031) — "MERGUEZ
    MAISON", brand "E.LECLERC SEBADIS", internal lot 46, use-by
    04/10/2026, risk "Listeria monocytogenes", motif "Détection de
    Listéria Monocytogènes", distributor "E.LECLERC SEBADIS".

J5 and J6 are 12/10, one day OUTSIDE the 24-hour window, but both were
absent from all five sheets and both fall in W42 — the OPEN week, whose
page is never shipped by this pass and whose numbers no subscriber email
has carried. They are added.

NOT ADDED, reported for a decision instead:

  * RappelConso 23741 (09/10/2026) "RAISIN ROSE RED GLOBE CAT I ORIGINE
    ITALIE", brand CONTE MAX, lot 52, motif "dépassement LMR Abamectin",
    distributors SCANORMANDE-LECASUD-ALDI SAUVIAN. Verified absent from
    all five sheets and in scope. It falls in W41, whose report and
    subscriber email are already out, so adding it would silently change
    a number the operator has already sent. OPERATOR DECISION.
  * RappelConso 23732 (09/10) and 23758 (12/10), Munster, Ferme du
    Pichet, Listeria — extensions of the Ferme du Pichet recall already
    published on 2026-10-02. Left to the near-duplicate rule.
  * RappelConso 23753 and 23754 (10/10), Vallegrain paupiettes, Listeria
    — extensions of the Vallegrain paupiette recall already published on
    2026-10-09. Left to the near-duplicate rule.
  * RappelConso 23757 (12/10) "Thon Entier au Naturel", brand U — a can
    sealing/packaging defect. Out of scope (packaging).
  * BVL (DE) / AGES (AT) / AFSCA (BE) "Kaufland ruft erneut bereits
    zurückgerufene BEBA Babynahrung zurück" (13.10.2026, cereulide) —
    already captured and correctly refused in Weekly_Rejected: "No
    official BVL press-release URL found in the source article —
    discovery via news only". R3 holds; it is an extension of the
    cereulide infant-formula event the register already carries 37 rows
    of.
  * Japan CAA 2026/10/13 management numbers 00000035956 (amazake,
    off-odour), 00000035957 (pork offal slices, 賞味期限/消費期限 label
    error) and 00000035959 (nugget platters, missing egg allergen
    declaration) — quality and labelling only. Out of scope.
  * UK FSA 13/10 Halo Foods peanut bars (undeclared peanut) and Waitrose
    Tuna Pate (incorrect use-by date) — allergen-only and labelling. Out
    of scope.
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
TODAY = "2026-10-14"

FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
FICHE_23765_STATIC = ("https://rappel.conso.gouv.fr/static/fiches/"
                      "a289da48-5ca3-4ce2-8c79-d45e046f518d.html")
GIS_WATER = ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-"
             "zywnosci-mozliwa-obecnosc-fragmentow-szkla-w-jednej-partii-"
             "wody-butelkowanej")
FSAI_READYBREK = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                  "recall-of-specific-batches-of-ready-brek-original")

# Reviewer 3 / offline-promoter stamps that a translated row no longer needs.
STAMPS = ("[needs cleanup: Product not in English]",
          "[needs cleanup: Company/Brand not in English]")


# ── shared helpers (same shape as tools/repair_2026_10_13.py) ─────────────
def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _urls(ws):
    hdr = _hdr(ws)
    if "URL" not in hdr:
        return []
    col = hdr.index("URL") + 1
    return [(r, str(ws.cell(row=r, column=col).value or "").strip())
            for r in range(2, ws.max_row + 1)]


def _find(ws, url):
    """Row numbers whose URL is EXACTLY `url`, ignoring a trailing slash
    and case. Exact, never substring: gov.pl re-uses a slug stem and
    appends a digit, so a substring match on one notice hits another."""
    want = url.rstrip("/").lower()
    return [r for r, u in _urls(ws) if u.rstrip("/").lower() == want]


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


# ── FAULT 0 — re-apply yesterday's pass ──────────────────────────────────
def reapply_2026_10_13(wb) -> int:
    """Re-run faults A-F of the 2026-10-13 pass against the current
    workbook. Every one of those functions is idempotent (each checks the
    repaired value first and returns 0 when it is already there), so this
    is a no-op on a main that kept the upload and a repair on a main that
    did not."""
    from tools import repair_2026_10_13 as y
    n = 0
    for label, fn in (
            ("A  August GIS warning dated October", y.fix_august_warning_dated_october),
            ("B  FSANZ alert title parsed as the company", y.fix_tweedvale_title_split),
            ("C  bare material noun as the hazard label", y.fix_bare_plastic_hazard),
            ("D  four RappelConso rows published in French", y.translate_french_rows),
            ("E  a recall both published and thrown away", y.supersede_contradicting_archive_row),
            ("F  Pending rows blocked on their own notice", y.unblock_pending_rows)):
        got = fn(wb)
        print(f"    {label}: {got}")
        n += got
    return n


def reapply_2026_10_13_missed() -> int:
    """The two rows yesterday's pass added and the clobber removed."""
    from tools import repair_2026_10_13 as y
    return y.add_missed_recalls()


# ── FAULT H — two more French rows ───────────────────────────────────────
FRENCH_ROWS = {
    "23767": dict(
        product="Raw cream (crème crue), La Ferme du Bignon, lot 39",
        reason="Detection of Listeria monocytogenes",
        read=('brand "La ferme du bignon", lot "39", risk "Listeria '
              'monocytogenes (agent responsable de la listériose)", motif '
              '"Détection de listéria", publication 13/10/2026; the listing '
              'entry for this fiche reads "creme crue"'),
        orig_reason="fr",
    ),
    "23768": dict(
        product=("Nordic salad (salade nordique), trays of 250 g, 500 g and "
                 "800 g, lots F2627216, F2627217, F2627218, use-by "
                 "14/10/2026"),
        reason="Presence of Listeria monocytogenes",
        read=('fiche ref 2026-10-0097, "SALADE NORDIQUE", "BARQUETTES DE '
              '250g,500g,800g", lots "F2627216" "F2627217" "F2627218", '
              '"Date limite de consommation 14/10/2026", brand "EMB 94073 '
              'Le", risk "Listeria monocytogenes (agent responsable de la '
              'listériose)", motif "présence de listéria monocytogénès", '
              'distributors "magasins marché frais", publication 13/10/2026'),
        orig_reason="fr",
        firm_note=(
            '[firm NOT corrected %s: Company/Brand read "EMB 94073 Le" on '
            "main. That is the French packing-centre health mark printed on "
            "the pack, not a firm. The fiche states no \"nom juridique du "
            "fabricant\"; its only company field is \"Origine de la fiche : "
            "ALPHAPRIM\", which is the notice's submitter. Left exactly as "
            "published rather than invent a recalling firm.]" % TODAY),
    ),
}


def translate_the_two_new_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, spec in FRENCH_ROWS.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            print(f"    fiche {fiche}: no row on main — skipped")
            continue
        for r in rows:
            if _get(ws, r, "Product") == spec["product"]:
                continue
            was_p = _get(ws, r, "Product")
            was_r = _get(ws, r, "Reason")
            bits = ["[translated %s: Product and Reason -> English. Read on "
                    "%s: %s.]" % (TODAY, FICHE % fiche, spec["read"]),
                    "[original product: %s]" % was_p]
            _set(ws, r, "Product", spec["product"])
            _set(ws, r, "Reason", spec["reason"])
            bits.append('[original Reason (%s): "%s"]'
                        % (spec["orig_reason"], was_r))
            if all(s not in _get(ws, r, "Notes") for s in STAMPS):
                bits.append(
                    "[detector-miss %s: reviewer 3 did NOT apply its "
                    "Product-not-in-English cleanup stamp to this row, and "
                    "the Reason opened with an English word, so "
                    "test_reason_is_not_wholly_foreign's two-hit threshold "
                    "did not see it either. The threshold is deliberately "
                    "not loosened; the morning read of every Product "
                    "published in the last 26 h is what caught it. (The "
                    "stamp is described, not quoted: a note that quotes it "
                    "verbatim makes a translated row look stamped to the "
                    "next pass — see strip_quoted_cleanup_stamps.)]" % TODAY)
            if spec.get("firm_note"):
                bits.append(spec["firm_note"])
            _note(ws, r, " ".join(bits), drop=STAMPS)
            print(f"    Recalls row {r} (fiche {fiche}): "
                  f"{was_p[:44]!r} -> {spec['product'][:44]!r}")
            n += 1
    return n


# ── FAULT H bis — a note that quotes the stamp it says is absent ─────────
def strip_quoted_cleanup_stamps(wb) -> int:
    """A translated row must not still look stamped.

    FOUND 2026-10-14, in this pass's own output. The "[detector-miss ...]"
    note explains that reviewer 3 did NOT stamp the row — and, until the
    rewording above, it did so by QUOTING the stamp verbatim:

        [detector-miss 2026-10-13: reviewer 3 did NOT stamp this row
         '[needs cleanup: Product not in English]' — ...]

    `_note(..., drop=STAMPS)` removes the real stamp and then appends a
    note containing the same substring, so any search of Notes for the
    stamp — the next morning's pass, reviewer 3, the offline promoter —
    finds three rows that were translated hours earlier and are already
    correct. Measured on this pass's draft workbook: fiches 23742
    (yesterday), 23767 and 23768 (today), all three fully translated, all
    three still matching "needs cleanup".

    The wording is fixed at the source in both repair scripts; this
    function cleans the rows that already carry the old form, so the
    count goes to zero instead of decaying. It only ever edits a Notes
    value that ALSO carries a "[translated " or "[detector-miss " marker —
    a genuinely stamped, untranslated row is never touched.
    """
    n = 0
    for sheet in ("Recalls", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        if "Notes" not in _hdr(ws):
            continue
        for r in range(2, ws.max_row + 1):
            notes = _get(ws, r, "Notes")
            if not any(s in notes for s in STAMPS):
                continue
            if "[detector-miss " not in notes and "[translated " not in notes:
                continue        # a real, untranslated stamp — leave it
            fixed = notes
            for s in STAMPS:
                label = s[1:-1].replace("needs cleanup: ", "")
                fixed = fixed.replace(
                    "'%s'" % s, "its %s cleanup stamp" % label)
                fixed = fixed.replace(
                    '"%s"' % s, "its %s cleanup stamp" % label)
                fixed = fixed.replace(s, "its %s cleanup stamp" % label)
            if fixed != notes:
                _set(ws, r, "Notes", fixed)
                print(f"    {sheet} row {r}: unquoted the cleanup stamp in a "
                      f"detector-miss note")
                n += 1
    return n


# ── FAULT I — unblock four Pending rows ──────────────────────────────────
def unblock_pending_rows(wb) -> int:
    ws = wb["Pending"]
    n = 0

    # I1 — the histamine mackerel fillet (fiche 23765).
    for r in _find(ws, FICHE_23765_STATIC):
        if _get(ws, r, "URL").lower() == (FICHE % "23765").lower():
            continue
        _set(ws, r, "URL", FICHE % "23765")
        _set(ws, r, "Pathogen", "Histamine")
        _set(ws, r, "Product",
             "Fresh packed mackerel fillet (filet de maquereau), lot 115553, "
             "use-by 09/10/2026")
        _set(ws, r, "Reason",
             "Histamine above 200 mg detected (endogenous biotoxin)")
        _note(ws, r,
              "[reviewer-2 stand-in %s: held on an empty Pathogen with the "
              "hazard written in its own Reason (\"Presence histamine "
              ">200mg\"); pipeline/enrich_pending_offline had no histamine "
              "rule, fixed in the code half of this pass. URL moved from "
              "RappelConso's static copy of the fiche (%s) to the canonical "
              "per-recall page. Read on %s: fiche 2026-10-0094, \"filet de "
              "maquereau\", lot \"115553\", GTIN \"0203750000000\", \"Date "
              "limite de consommation 09/10/2026\", risk \"contaminants "
              "biologiques — toxines endogènes: histamine\", motif "
              "\"PRESENCE HISTAMINE >200MG\", distributor \"SUPERU ST CIERS "
              "SUR GIRONDE\", Gironde (33), published 13/10/2026.] "
              "[original product: filet de maquereau frais emballe] "
              "[original Reason (fr/en mix): \"Presence histamine >200mg\"]"
              % (TODAY, FICHE_23765_STATIC, FICHE % "23765"))
        print(f"    Pending row {r}: RappelConso 23765 — URL canonicalised, "
              f"Pathogen/Product/Reason filled")
        n += 1

    # I2 — paupiette chorizo (fiche 23764): the notice names no firm.
    for r in _find(ws, FICHE % "23764"):
        if _get(ws, r, "Company"):
            continue
        _set(ws, r, "Company", "Not named on the notice (sold at Magasins U)")
        _set(ws, r, "Product",
             "Chorizo paupiette (paupiette chorizo), lot 26272056064, "
             "use-by 09/10/2026")
        _set(ws, r, "Reason",
             "Microbiological non-conformity: Listeria monocytogenes")
        _note(ws, r,
              "[reviewer-2 stand-in %s: held on an empty Company. Read on "
              "%s: fiche 2026-10-0093, brand \"Sans marque\", lot "
              "\"26272056064\", \"Date limite de consommation 09/10/2026\", "
              "risk \"Listeria monocytogenes (agent responsable de la "
              "listériose)\", motif \"NON CONFORMITE MICROBIOLOGIQUE\", "
              "distributors \"Magasins U\", published 13/10/2026, and NO "
              "\"nom juridique du fabricant\" anywhere on the page. Company "
              "is therefore the register's own existing wording for an "
              "unnamed firm (as published on the 2026-10-07 Savoy-style "
              "pork paupiette row), not a guess. The url-guard already "
              "refused reviewer 2's attempt to reject this row.] "
              "[original product: paupiette chorizo] "
              "[original Reason (fr): \"Microbiological non-conformity\" — "
              "the regulator's own French is \"NON CONFORMITE "
              "MICROBIOLOGIQUE\"]" % (TODAY, FICHE % "23764"))
        print(f"    Pending row {r}: RappelConso 23764 — Company filled "
              f"(notice names no firm), Product/Reason translated")
        n += 1

    # I3 — GIS (PL) glass fragments in bottled mineral water.
    for r in _find(ws, GIS_WATER):
        if _get(ws, r, "Company"):
            continue
        was_p = _get(ws, r, "Product")
        _set(ws, r, "Company",
             "Uzdrowisko Ciechocinek S.A. - Zakłady Produkcji Zdrojowej")
        _set(ws, r, "Pathogen", "Foreign material (glass)")
        _set(ws, r, "Product",
             "KRYSTYNKA naturalna woda mineralna gazowana (sparkling natural "
             "mineral water), 330 ml, batch FS224/3")
        _set(ws, r, "Reason",
             "Possible presence of glass fragments in one batch of bottled "
             "water")
        _note(ws, r,
              "[reviewer-2 stand-in %s: held on an empty Pathogen because "
              "the whole foreign-material block of "
              "pipeline/enrich_pending_offline spoke only English and "
              "French and this notice is Polish; multilingual rules added "
              "in the code half of this pass. Read on %s, published "
              "13.10.2026: producer \"Uzdrowisko Ciechocinek S.A. - Zakłady "
              "Produkcji Zdrojowej\", product \"KRYSTYNKA naturalna woda "
              "mineralna gazowana, 330 ml\", batch \"FS224/3\", hazard "
              "\"możliwa obecność fragmentów szkła\".] "
              "[original product: %s] "
              "[original Reason (pl): \"możliwa obecność fragmentów szkła w "
              "jednej partii wody butelkowanej\"]"
              % (TODAY, GIS_WATER, was_p))
        print(f"    Pending row {r}: GIS (PL) bottled water — Company, "
              f"Pathogen, Product, Reason filled")
        n += 1

    # I4 — FSAI alert 2026.65, flour beetle.
    for r in _find(ws, FSAI_READYBREK):
        if _get(ws, r, "Pathogen"):
            continue
        was_p = _get(ws, r, "Product")
        _set(ws, r, "Company", "Ready Brek and Tesco")
        _set(ws, r, "Pathogen", "Foreign material (pest)")
        _set(ws, r, "Date", "2026-10-13")
        _set(ws, r, "Product",
             "Ready Brek Original 750 g and 450 g, and Tesco Super Smooth "
             "Porridge 750 g (identified by best-before date; the alert "
             "lists no batch codes)")
        _set(ws, r, "Reason",
             "Possible presence of flour beetle; the presence of flour "
             "beetles would render the products unfit for human consumption")
        _note(ws, r,
              "[reviewer-2 stand-in %s: held with Pathogen, Date and Reason "
              "empty — \"FSAI HTML listing fallback\". The hazard is on the "
              "alert, not in the row, so no offline enricher could have "
              "found it; \"flour beetle\" is nevertheless added to the pest "
              "vocabulary in the code half, which knew only \"insect\", "
              "\"pest\" and \"nuisible\". Read on %s: FSAI alert "
              "notification 2026.65, \"Tuesday, 13 October 2026\", "
              "\"Category 1: For Action\", country of origin \"United "
              "Kingdom\", \"possible presence of flour beetle\", \"The "
              "presence of flour beetles would render the products unfit "
              "for human consumption\"; Ready Brek Original 750 g (8 "
              "best-before dates) and 450 g (9 best-before dates) and Tesco "
              "Super Smooth Porridge 750 g (\"All best before dates "
              "30/06/2027 to 25/08/2027 inclusive\"). The alert names Ready "
              "Brek and Tesco as the recalling parties and names NO "
              "manufacturer, so Company is those two and nothing is "
              "invented.] [original product: %s]"
              % (TODAY, FSAI_READYBREK, was_p))
        print(f"    Pending row {r}: FSAI (IE) Ready brek — Company, "
              f"Pathogen, Date, Reason filled")
        n += 1
    return n


def supersede_the_wrongly_rejected_mackerel(wb) -> int:
    """Fiche 23765 is about to be PUBLISHED under the canonical URL while
    a rejected twin and two Weekly_Rejected copies carry the same URL with
    a factually wrong refusal. Annotate, never delete."""
    want = (FICHE % "23765").rstrip("/").lower()
    marker = "[SUPERSEDED %s" % TODAY
    n = 0
    for sheet, col in (("Pending", "Status"),
                       ("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        hdr = _hdr(ws)
        for r in range(2, ws.max_row + 1):
            if _get(ws, r, "URL").rstrip("/").lower() != want:
                continue
            if sheet == "Pending" and _get(ws, r, "Status") != "rejected":
                continue          # the live row this pass is publishing
            if marker in _get(ws, r, "Notes"):
                continue
            prev = _get(ws, r, col).strip() if col in hdr else ""
            _note(ws, r,
                  "%s — this row was refused with \"The provided URL is a "
                  "listing page, not the specific recall page\". That is "
                  "factually wrong: %s IS the per-recall fiche, read this "
                  "pass as fiche 2026-10-0094 (filet de maquereau, lot "
                  "115553, motif \"PRESENCE HISTAMINE >200MG\", published "
                  "13/10/2026). The recall is published in Recalls from the "
                  "same URL with the fields read off that page; the Recalls "
                  "copy is what the register says. Kept, not deleted: the "
                  "archives are append-only and this row is the audit trail "
                  "of the wrong refusal. Prior verdict field (%s): %s]"
                  % (marker, FICHE % "23765", col, prev or "(empty)"))
            print(f"    {sheet} row {r}: marked SUPERSEDED "
                  f"(wrong refusal of fiche 23765)")
            n += 1
    return n


# ── FAULT J — missed recalls ─────────────────────────────────────────────
_SWEPT = ("Absent from all five sheets (Recalls, Pending, Weekly_Review, "
          "Weekly_Rejected, Rejected) on base b80a59a4 — searched by URL, "
          "by the notice's own reference number, by company and by a "
          "distinctive product word.")

MISSED = [
    {
        "Date": "2026-10-13",
        "Source": "RappelConso (FR)",
        "Company": "Nature's best harvest",
        "Brand": "Nature's best harvest",
        "Product": ("Chrysanthemum bud, lot 17/04/2028 "
                    "(minimum durability date 17/04/2028)"),
        "Pathogen": "Pesticide residues",
        "Reason": ("Pesticide levels above the authorised limits — presence "
                   "of pesticides"),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": FICHE % "23772",
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "%s, fiche ref 2026-10-0101, published \"13/10/2026\": product "
            "\"Chrysanthemum Bud\", marque \"Nature's best harvest\", lot "
            "\"17/04/2028\" with DDM 17/04/2028, risques encourus "
            "\"Dépassement des limites autorisées de pesticides\", motif du "
            "rappel \"Présence de pesticides\", distribution \"voir fichier "
            "distribution\" across ten French departments including Paris, "
            "Ain and Gard. %s] [the fiche names NO pesticide, so the hazard "
            "is the register's generic \"Pesticide residues\" and no "
            "substance is invented] [the fiche states no \"nom juridique du "
            "fabricant\"; Company mirrors the published marque, as on every "
            "other RappelConso row of this shape] [original Reason (fr): "
            "\"Présence de pesticides\"]"
            % (TODAY, FICHE % "23772", _SWEPT)),
    },
    {
        "Date": "2026-10-13",
        "Source": "NVWA (NL)",
        "Company": "Udea / Odin Groothandel",
        "Brand": "Cook",
        "Product": ("Cook ground coriander seed (korianderzaad gemalen), "
                    "best before 08-2028, barcode 3417960022619"),
        "Pathogen": "Salmonella",
        "Reason": "The product may contain Salmonella",
        "Class": "Recall",
        "Country": "Netherlands",
        "Outbreak": 0,
        "URL": ("https://www.nvwa.nl/actueel/nieuws/2026/10/13/"
                "veiligheidswaarschuwing-cook-korianderzaad-gemalen"),
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "the NVWA veiligheidswaarschuwing dated \"13-10-2026 | 09:36\": "
            "product \"Cook korianderzaad Gemalen\", hazard \"Het product "
            "bevat mogelijk salmonella\", advice \"Gebruik het korianderzaad "
            "niet\", \"Ten minste houdbaar tot (THT): 08-2028\", barcode "
            "3417960022619, article number 5005719, Odin article number "
            "10749, sold at Ekoplaza and other organic shops. %s] "
            "[the notice names NO producer — the warnings were issued by "
            "Udea and Odin Groothandel, so Company is those two firms and "
            "no manufacturer is invented] [original Reason (nl): \"Het "
            "product bevat mogelijk salmonella\"] [original product: Cook "
            "korianderzaad Gemalen]"
            % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-13",
        "Source": "NVWA (NL)",
        "Company": "Odin Groothandel",
        "Brand": "Cook",
        "Product": ("Cook mild curry powder (Kerrie mild), best before "
                    "08-2028, barcode 3417960022657"),
        "Pathogen": "Salmonella",
        "Reason": "The product may contain Salmonella",
        "Class": "Recall",
        "Country": "Netherlands",
        "Outbreak": 0,
        "URL": ("https://www.nvwa.nl/actueel/nieuws/2026/10/13/"
                "veiligheidswaarschuwing-cook-kerrie-mild"),
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "the NVWA veiligheidswaarschuwing dated \"13-10-2026\" (14:55): "
            "product \"Cook Kerrie mild\", hazard \"Het product bevat "
            "mogelijk salmonella\", advice \"Gebruik de kerrie niet\", THT "
            "\"08-2028\", barcode 3417960022657, Odin article number 10743. "
            "%s] [the notice names NO producer — the warning was issued by "
            "Odin Groothandel] [the notice prints no lot number and no "
            "point of sale] [original Reason (nl): \"Het product bevat "
            "mogelijk salmonella\"] [original product: Cook Kerrie mild]"
            % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-13",
        "Source": "CAA (JP)",
        "Company": "Sanrei Foods",
        "Brand": "Sanrei Foods",
        "Product": ("Minced-beef cutlet (menchi-katsu) made with "
                    "Shimane-produced beef"),
        "Pathogen": "Foreign material (metal)",
        "Reason": ("Possible contamination with foreign matter (metal "
                   "fragments)"),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php"
                "?rcl=00000035958&screenkbn=01"),
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "the Consumer Affairs Agency recall detail page, 管理番号 "
            "00000035958: company \"さんれいフーズ\", product "
            "\"島根県産牛肉入りメンチカツ\", 事由 "
            "\"異物（金属片）混入のおそれ\", action 返金／回収, 対応開始日 "
            "\"2026年09月18日\", 掲載日 \"2026/10/13\" on "
            "https://www.recall.caa.go.jp/result/index.php?screenkbn=01&"
            "category=1. 対象の特定情報 is blank, so the notice prints no "
            "lot or best-before. %s] [Date is the regulator's publication "
            "date 掲載日 2026/10/13; the 対応開始日 of 2026-09-18 is the "
            "firm's response start, not the notice date] [original company: "
            "さんれいフーズ — the CAA prints no English name for the firm; "
            "rendered as its standard romanization, per the operator rule "
            "of 2026-10-01] [original product: 島根県産牛肉入りメンチカツ] "
            "[original Reason (ja): \"異物（金属片）混入のおそれ\"] "
            "[recall.caa.go.jp has no module under scrapers/ — these rows "
            "reach the register only through the JP gap finder, which ran "
            "2026-10-13 and appended nothing]"
            % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-12",
        "Source": "RappelConso (FR)",
        "Company": "OUMATY",
        "Brand": "OUMATY",
        "Product": ("Spiced chicken tender strips (TENDERS POULET EPICES "
                    "STRIPS), 2.5 kg x 4 (10 kg), lots 1009260101, "
                    "1009260103, 1009260105, GTIN 3701479402261"),
        "Pathogen": "Veterinary drug residue",
        "Reason": ("Veterinary medicinal product residues: an antibiotic was "
                   "detected in the chicken meat used to make the product"),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": FICHE % "23745",
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "%s, fiche ref 2026-10-0074 version 1, published \"12/10/2026\": "
            "product \"TENDERS POULET EPICES STRIPS OUMATY (2,5KGx4) 10KG\", "
            "marque \"OUMATY\", lots \"1009260101, 1009260103, 1009260105\", "
            "GTIN \"3701479402261\", risques encourus \"Résidus de "
            "médicaments vétérinaires\", motif \"antibiotique détecté dans "
            "de la viande de poulet utilisée pour la fabrication du "
            "produit\", distributeurs \"METRO, KHADISPAL, LACORP LA "
            "REUNION\". No \"nom juridique du fabricant\" on the fiche. %s] "
            "[published 12/10, one day outside the 24-hour sweep window, but "
            "absent from all five sheets and inside W42 — the OPEN week, "
            "whose page this pass never ships and whose numbers no "
            "subscriber email has carried] [the fiche names no antibiotic, "
            "so the hazard is the register's generic \"Veterinary drug "
            "residue\"] [original Reason (fr): \"antibiotique détecté dans "
            "dans de la viande de poulet utilisée pour la fabrication du "
            "produit\" — the doubled \"dans\" is the regulator's own] "
            "[original product: TENDERS POULET EPICES STRIPS OUMATY "
            "(2,5KGx4) 10KG]"
            % (TODAY, FICHE % "23745", _SWEPT)),
    },
    {
        "Date": "2026-10-12",
        "Source": "RappelConso (FR)",
        "Company": "E.LECLERC SEBADIS",
        "Brand": "E.LECLERC SEBADIS",
        "Product": ("Home-made merguez sausages (MERGUEZ MAISON), internal "
                    "lot 46, use-by 04/10/2026"),
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Detection of Listeria monocytogenes",
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": FICHE % "23702",
        "Notes": (
            "[manual %s: AFTS morning fix pass, missed-recall sweep. Read on "
            "%s, fiche ref 2026-10-0031 version 1, published \"12/10/2026\": "
            "product \"MERGUEZ MAISON\", marque \"E.LECLERC SEBADIS\", "
            "\"Lot interne : 46\", \"Date limite de consommation "
            "04/10/2026\", risques encourus \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", motif \"Détection de Listéria "
            "Monocytogènes\", distributeur \"E.LECLERC SEBADIS\". The fiche "
            "names no manufacturer; its \"Origine de la fiche\" is "
            "\"SEBAZAC DISTRIBUTION E.Leclerc\". %s] [published 12/10, one "
            "day outside the 24-hour sweep window, but absent from all five "
            "sheets and inside W42 — the OPEN week] [a DIFFERENT recall from "
            "the two SEBAZAC DISTRIBUTION / Sebadis rows already published "
            "in July 2026, which were dry-cured charcuterie with Salmonella] "
            "[original Reason (fr): \"Détection de Listéria Monocytogènes\"] "
            "[original product: MERGUEZ MAISON]"
            % (TODAY, FICHE % "23702", _SWEPT)),
    },
]


def _fill_existing_pending(rows) -> list:
    """A missed recall may already be in Pending under the same URL with a
    collector's junk fields. Correct such a row IN PLACE from the authority
    page; only rows absent from every sheet go to add_manual_row (which
    would otherwise skip them as duplicate URLs and leave the junk held)."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    col = {h: i for i, h in enumerate(_hdr(ws))}
    left, changed = [], 0
    for rec in rows:
        hit = None
        for row in ws.iter_rows(min_row=2):
            if str(row[col["URL"]].value or "").strip().rstrip("/").lower() \
                    == rec["URL"].rstrip("/").lower():
                hit = row
                break
        if hit is None:
            left.append(rec)
            continue
        old = {k: hit[col[k]].value
               for k in ("Source", "Company", "Brand", "Product")
               if k in col}
        for k, v in rec.items():
            if k in col and k != "Notes":
                hit[col[k]].value = v
        hit[col["Notes"]].value = (
            rec["Notes"] + " [Pending row corrected in place; collector "
            "values were " + json.dumps(old, ensure_ascii=False) + "]").strip()
        changed += 1
        print(f"    Pending: {rec['Source']} {rec['URL']} corrected in place")
    if changed:
        wb.save(XLSX)
    wb.close()
    return left


def add_missed_recalls() -> int:
    rows = _fill_existing_pending(MISSED)
    done = len(MISSED) - len(rows)
    if not rows:
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
        print("    add_manual_row FAILED:", (out.stderr or "")[-900:])
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

    print("FAULT 0 — re-applying the 2026-10-13 pass "
          "(silently overwritten 3 min after upload)")
    n_0 = reapply_2026_10_13(wb)
    print(f"  {n_0} row(s) re-repaired\n")

    print("FAULT H — two more RappelConso rows published in French")
    n_h = translate_the_two_new_french_rows(wb)
    print(f"  {n_h} row(s) translated\n")

    print("FAULT H bis — a note that quotes the stamp it says is absent")
    n_hb = strip_quoted_cleanup_stamps(wb)
    print(f"  {n_hb} note(s) unquoted\n")

    print("FAULT I — four Pending rows blocked on a fact on their own notice")
    n_i = unblock_pending_rows(wb)
    print(f"  {n_i} row(s) unblocked\n")

    print("FAULT I bis — a recall published and rejected on one URL")
    n_ib = supersede_the_wrongly_rejected_mackerel(wb)
    print(f"  {n_ib} archive row(s) marked SUPERSEDED\n")

    if n_0 or n_h or n_hb or n_i or n_ib:
        wb.save(XLSX)
    wb.close()

    print("FAULT 0 bis — the two rows the overwrite removed")
    n_0b = reapply_2026_10_13_missed()
    print(f"  {n_0b} row(s) restored to Pending\n")

    print("FAULT J — six recalls the pipeline missed")
    n_j = add_missed_recalls()
    print(f"  {n_j} row(s) queued into Pending\n")

    total = n_0 + n_h + n_hb + n_i + n_ib + n_0b + n_j
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
