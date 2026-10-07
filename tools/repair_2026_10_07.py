# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-07. Five faults plus three missed recalls.

Run on a FRESH clone of main, by tools/rebase_and_verify.py, which then
runs pipeline.promote_gate_passing --apply behind it.

NOTHING IS REMOVED from Recalls, so the commit message needs no deletion
marker. One Pending row is corrected in place; none is removed.

Every fact written below was read on an authority page or in the row's own
text/URL, and the Notes say which.


FAULT A — HUNGARY PUBLISHED UNDER AN AGENCY THAT NO LONGER EXISTS
=================================================================
The SZEGA Camembert Kft. STEC recall of 2026-10-06 published to Recalls
this morning with Source "NÉBIH". Three tests went red at once:

    test_monitored_sources::test_every_published_source_is_counted
        asked for "NÉBIH" to be ADDED to tools/monitored_sources.py
    test_a_writer_that_skips_the_choke_point_still_canonicalises::
        test_the_live_register_carries_no_aliased_label
    test_every_collector_label_is_a_registry_label (added today)

Adding NÉBIH to the registry — which is literally what the first failure
asks for — would have been the wrong repair. NÉBIH was merged into NKFH;
pipeline/gap_finder/countries/hu.py records the 2026-09-30 switch to
nkfh.gov.hu in its own docstring; the registry entry already reads
("NKFH (HU)", "Hungary", "nkfh.gov.hu nebih.gov.hu"); and THE ROW'S OWN
URL is on nkfh.gov.hu. The registry was right and the writer had no alias.

This is the THIRD recurrence of one defect in five days, each found by a
row that had already published:

    2026-10-03  Czechia  "SZPI"    -> SZPI (CZ)
    2026-10-04  Italy    "Salute"  -> Ministero della Salute (IT)
    2026-10-07  Hungary  "NÉBIH"   -> NKFH (HU)

Fixed in code by six SOURCE_ALIASES entries plus one ordering fix in
registry_source_label (Korea), measured across all 46 country configs, and
held by tests/test_every_collector_label_is_a_registry_label.py, which
derives the claim from the configs so config 47 cannot be added without
one. This script repairs the row already written — by calling
merge_master.apply_label_aliases, not by hand-editing the string, so what
runs here is what will run tomorrow.


FAULT B — A SHELF-LIFE DATE SITTING IN Date, 23 DAYS IN THE FUTURE
==================================================================
Pending row 2, written by pipeline/gap_finder_tavily.py
(ScrapedAt 2026-10-06T19:11:42Z, commit c0bf325):

    Date    2026-10-30
    Source  BVL
    URL     https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/
            Meldungen/2026/09_September/260904_03_BW_diverse_Kaesesorten/
            260904_03_BW_diverse_Kaesesorten_Presse_1.pdf

The notice is from 2026-09-04 — its own URL path says so twice
("2026/09_September", "260904_03") — and 30.10.2026 is a date printed
inside a Listeria cheese recall about how long the product keeps. The row
made BVL the freshest-looking source in the whole register:
test_scraper_output_health::test_no_source_has_a_future_last_row went red
with {'BVL': '2026-10-30'} and "days since last row" for that source was
negative.

TWO FIXES, BOTH FROM THE ROW ITSELF. The date is taken from the row's own
URL, which is the one place it is stated unambiguously. The Source label
is canonicalised by the same apply_label_aliases call as fault A — the
bare "BVL" is a fourth instance of the same bypass, from a third writer:
gap_finder_tavily never calls apply_label_aliases either.

The future-date gate is fixed in code (30-day tolerance -> 1 day, matching
pipeline/_gap_finder_guards.check_gap_finder_row, which this module does
not call) and held by
tests/test_a_finders_own_date_gate_agrees_with_the_standing_guard.py.

WHAT IS *NOT* FIXED HERE, SAID PLAINLY. That row's Product reads
"beachten Sie dazu den Aushang an Ihrer Verkaufsstelle" — German
boilerplate meaning "see the notice at your point of sale" — and its
Company is empty. Both are extractor junk, not a product and not a firm.
Neither is invented here: lebensmittelwarnung.de returned
PROVENANCE_REQUIRED to this pass, so the notice could not be read, and the
row's own text names the cheese only inside a sentence ("eine Charge des
Weinbauernkäses"). The row stays in Pending with its date and label
corrected, where the two reviewers and the publish gate see it; it is NOT
promoted by this script, and a row whose Product is boilerplate will not
pass the gate. Operator decision noted in the run summary.


FAULT C — A FRENCH REASON ABOUT TO GO OUT IN FRENCH
====================================================
Recalls row 2, fiche 23712 (Toque du Chef, Listeria, published this
morning), carries:

    Reason   "Une analyse a mis en évidence la présence de Listeria
              monocytogenes"
    Product  "surimi"

Two separate defects in one row.

  C1  The Reason is wholly French. tests/test_language_policy.py and
      tests/test_reason_is_not_wholly_foreign.py were both RED on it.

  C2  "surimi" is not the product. RappelConso's own listing for 06/10/2026
      gives the libellé as "Salade de pâtes Surimi" — a surimi PASTA SALAD,
      read on https://rappel.conso.gouv.fr/categorie/0/1 this morning.
      "surimi" names an ingredient and is exactly the short-name shape the
      English-product detector cannot see, the "foie de poulet" class the
      operator named. No test was red on it; it was found by reading every
      Product published in the last 26 h, which is the standing
      instruction.


FAULT D — THE needs-cleanup STAMP NOBODY CLEARED
=================================================
Recalls row 5, fiche 23695, published 2026-10-05, carries Notes
"[needs cleanup: Product not in English]" — stamped by reviewer 3 / the
offline promoter, which have no model and cannot translate — and the
untranslated Product "araignee de porc marinee ail des ours". Two mornings
later it is still there. Translated from the French read on RappelConso's
own listing: "Araignée de porc marinée ail des ours", brand "SANS MARQUE",
motif "Détection Salmonella", 05/10/2026. The stamp is removed because the
row no longer needs it.

"Araignée de porc" is the pork SPIDER CUT — the butcher's cut, kept in
parentheses because the English name is not widely used and dropping the
French word would lose the cut. "Ail des ours" is wild garlic (ramsons).
Nothing is added that the fiche does not say.


FAULT E — THREE RECALLS THE PIPELINE MISSED IN THE LAST 24 h
=============================================================
Searched all five sheets first — by URL, by recall id, by company, by
brand, by product and by a distinctive word — and all three were absent
from every one. Each authority page was read by this pass.

  E1/E2  RappelConso fiches 23710 and 23709, both published 06/10/2026,
         both "Non conformité des produits dù à un taux de THC trop élevé
         conduisant à un dépassement de la dose de référence aiguë en
         delta 9 THC". In scope by name: THC over a reference dose.
         Pathogen uses the register's established label,
         "Delta-9-THC (above acute reference dose)" (2 rows, 2026-09-30).

  E3     CAA (JP) rcl 00000035928, posted 2026/10/06:
         トマトコーポレーション / 粉チーズ（原産国：イタリア） /
         "カビに汚染されている可能性". Mould, in scope by operator
         decision 2026-09-07 and Tier 2 everywhere by operator rule
         2026-10-03.

NOT ADDED, AND WHY. RappelConso fiche 23707 ("Paupiette de porc à la
savoyarde", sans marque, "Détection de Salmonelle", 07/10/2026) is a real
in-scope recall missing from all five sheets, and it is NOT added here.
Its fiche page could not be read — published today, not yet indexed, and
WebFetch returned PROVENANCE_REQUIRED — so the row's Company is not known
to this pass and its per-recall URL was never seen stated anywhere. Rule 4
and rule R3 both say the same thing: an empty field stays empty and a
per-recall URL is never constructed from a pattern. The RappelConso
scraper picked fiche 23712 up within hours of publication this morning, so
it is expected to arrive on its own; flagged in the run summary either way.

Four other RappelConso fiches of 06/10 were read and are OUT of scope:
23705 (CBD oil for dogs — not human food), 23703 (Indian bangles, lead —
not food). Four CAA entries of 2026/10/06 were read and are out:
35927 expiry-date mislabelling, 35929 insufficiently set pudding
(quality), 35930 missing best-by labelling, 35931 undeclared shrimp
allergen.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = "2026-10-07"


# ── the rows this pass found missing ─────────────────────────────────────
_PROV = (
    "[manual 2026-10-07: morning-fix missed-recall sweep. Searched all five "
    "sheets first — by URL, by recall id, by company, by brand, by product "
    "and by a distinctive word — and absent from every one. "
)

MISSED_ROWS = [
    {
        "Date": "2026-10-06",
        "Source": "RappelConso (FR)",
        "Company": "SARL THITAN",
        "Brand": "Unbranded",
        "Product": ("Gummies 10 mg — pineapple (ANANAS), apple (POMME), "
                    "cola, blue razz and watermelon (PASTEQUE); loose, "
                    "unpackaged (EN VRAC - NON CONDITIONNÉS); lots "
                    "PI2604205170426, AP2604205170426, BL2604205170426, "
                    "CO2604205170426, WA2604205170426"),
        "Pathogen": "Delta-9-THC (above acute reference dose)",
        "Reason": ("Non-compliant product: the THC level is too high and "
                   "exceeds the acute reference dose for delta-9-THC."),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23710/Interne",
        "Notes": (
            _PROV +
            "Fiche 23710 (réf. 2026-10-0039, version 1) read on its own "
            "authority page on 2026-10-07; date de publication 06/10/2026; "
            "marque 'sans marque'; distributeur/origine de fiche 'SARL "
            "THITAN'; commercialisation du 24/06/2026 au 29/07/2026. "
            "In scope: THC over a reference dose. "
            "[original Reason (fr): \"Non conformité des produits dù à un "
            "taux de THC trop élevé conduisant à un dépassement de la dose "
            "de référence aiguë en delta 9 THC\"] "
            "[original product: GUMMIES 10MG ANANAS (PINEAPLE) / POMME "
            "(APPLE) / COLA / BLUE RAZZ / PASTEQUE (WATERMELON)] "
            "[original brand: sans marque]"),
    },
    {
        "Date": "2026-10-06",
        "Source": "RappelConso (FR)",
        "Company": "SARL THITAN",
        "Brand": "Unbranded",
        "Product": ("Gummies 20 mg — elderberry and Mai Tai; loose, "
                    "unpackaged (EN VRAC - NON CONDITIONNÉS); lots "
                    "26JD07602 (ELDERBERRY) and 26JD040903 (MAI TAI)"),
        "Pathogen": "Delta-9-THC (above acute reference dose)",
        "Reason": ("Non-compliant product: the THC level is too high and "
                   "exceeds the acute reference dose for delta-9-THC."),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23709/Interne",
        "Notes": (
            _PROV +
            "Fiche 23709 (réf. 2026-10-0038, version 1) read on its own "
            "authority page on 2026-10-07; date de publication 06/10/2026; "
            "marque 'sans marque'; responsable 'THITAN SARL'; "
            "commercialisation du 04/05/2026 au 29/07/2026. The fiche's "
            "own risk wording adds that the products are non-compliant "
            "under the novel-food regulation for unauthorised cannabinoids "
            "(THC/CBD). In scope: THC over a reference dose. "
            "[original Reason (fr): \"Non conformité des produits dù à un "
            "taux de THC trop élevé conduisant à un dépassement de la dose "
            "de référence aiguë en delta 9 THC\"] "
            "[original product: GUMMIES 20MG ELDERBERRY / MAI TAI] "
            "[original brand: sans marque]"),
    },
    {
        "Date": "2026-10-06",
        "Source": "CAA (JP)",
        "Company": "Tomato Corporation",
        "Brand": "—",
        "Product": "Grated cheese (country of origin: Italy)",
        "Pathogen": "Mold",
        "Reason": "The product may be contaminated with mould.",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php"
                "?rcl=00000035928&screenkbn=01"),
        "Notes": (
            _PROV +
            "Read on the CAA notice on 2026-10-07: 事業者名 "
            "トマトコーポレーション, 商品名 「粉チーズ（原産国：イタリア）」, "
            "回収理由 「カビに汚染されている可能性」, 対応開始日 "
            "2026年10月05日; posted on the CAA listing "
            "(recall.caa.go.jp/result/index.php?screenkbn=01&category=1) "
            "under 掲載日 2026/10/06, which is the date this row carries, "
            "matching the convention of the CAA rows already in the "
            "register. The notice states no lot or best-before detail. "
            "Mould is in scope by operator decision 2026-09-07 and Tier 2 "
            "everywhere by operator rule 2026-10-03. "
            "[original Reason (ja): \"カビに汚染されている可能性\"] "
            "[original product: 粉チーズ（原産国：イタリア）] "
            "[original company: トマトコーポレーション — published English "
            "name not stated on the notice; standard romanisation used, "
            "per the operator rule of 2026-10-01]"),
    },
]


def _rows_of(ws):
    rows = list(ws.values)
    if not rows:
        return [], []
    return [str(h) for h in rows[0]], rows[1:]


def _col(ws, hdr, name):
    return hdr.index(name) + 1 if name in hdr else None


# ── FAULT A + B: the label bypass, on the rows already written ───────────
def canonicalise_live_labels(wb) -> int:
    """Run merge_master's own alias map over Recalls and Pending.

    By calling apply_label_aliases rather than editing strings, this
    repair and tomorrow's writer agree by construction. Scoped to the two
    LIVE sheets: Weekly_Rejected and Rejected are append-only archives of
    what each gate refused, and rewriting an archive to match a map
    authored after it is the operator's decision, not a morning pass's —
    the same boundary tools/repair_2026_10_06.py drew.
    """
    from pipeline.merge_master import apply_label_aliases

    changed = 0
    for sheet in ("Recalls", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        hdr, body = _rows_of(ws)
        c_src, c_ctry = _col(ws, hdr, "Source"), _col(ws, hdr, "Country")
        if not c_src:
            continue
        for n, raw in enumerate(body, start=2):
            row = {
                "Source": str(ws.cell(n, c_src).value or ""),
                "Country": str(ws.cell(n, c_ctry).value or "") if c_ctry else "",
            }
            before = (row["Source"], row["Country"])
            apply_label_aliases([row], where=f"repair_{TODAY} -> {sheet}")
            if row["Source"] != before[0]:
                print(f"  {sheet} row {n}: Source {before[0]!r} -> "
                      f"{row['Source']!r}")
                ws.cell(n, c_src).value = row["Source"]
                changed += 1
            if c_ctry and row["Country"] != before[1]:
                print(f"  {sheet} row {n}: Country {before[1]!r} -> "
                      f"{row['Country']!r}")
                ws.cell(n, c_ctry).value = row["Country"]
                changed += 1
    return changed


#: The row, and the date stated in its own URL. Both halves are asserted
#: before anything is written, so a different row never gets this date.
_BVL = {
    "url_fragment": "260904_03_BW_diverse_Kaesesorten",
    "bad_date": "2026-10-30",
    "true_date": "2026-09-04",
}


def repair_the_future_dated_bvl_row(wb) -> int:
    """Take Date from the row's own URL, which states it twice."""
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    hdr, body = _rows_of(ws)
    c_url, c_date = _col(ws, hdr, "URL"), _col(ws, hdr, "Date")
    c_notes = _col(ws, hdr, "Notes")
    if not (c_url and c_date):
        return 0

    n_changed = 0
    for n, _ in enumerate(body, start=2):
        url = str(ws.cell(n, c_url).value or "")
        if _BVL["url_fragment"] not in url:
            continue
        have = str(ws.cell(n, c_date).value or "")[:10]
        if have != _BVL["bad_date"]:
            print(f"  Pending row {n}: Date is {have!r}, not "
                  f"{_BVL['bad_date']!r} — already repaired or changed "
                  f"upstream; left alone")
            continue
        # The date is in the URL path twice: /2026/09_September/ and the
        # 260904 filename prefix. Assert both before trusting either.
        assert "/2026/09_September/" in url, url
        assert "/260904_03_" in url, url
        ws.cell(n, c_date).value = _BVL["true_date"]
        print(f"  Pending row {n}: Date {have!r} -> "
              f"{_BVL['true_date']!r} (from its own URL)")
        if c_notes:
            ws.cell(n, c_notes).value = (
                str(ws.cell(n, c_notes).value or "").strip() +
                f" [morning-fix {TODAY}: Date was {_BVL['bad_date']}, "
                f"23 days in the future — a shelf-life date, not a "
                f"publication date. Corrected to {_BVL['true_date']}, "
                f"which this row's own URL states twice "
                f"(/2026/09_September/ and the 260904_03_ filename "
                f"prefix). It had made BVL the freshest-looking source in "
                f"the register and turned 'days since last row' negative "
                f"(tests/test_scraper_output_health::test_no_source_has_a_"
                f"future_last_row). The gate that let it in — "
                f"gap_finder_tavily's private 30-day future tolerance — is "
                f"now 1 day, matching _gap_finder_guards. Product "
                f"('beachten Sie dazu den Aushang an Ihrer Verkaufsstelle') "
                f"and Company (empty) are extractor artefacts and are NOT "
                f"guessed at here: lebensmittelwarnung.de could not be "
                f"read by this pass. Left in Pending for review.]").strip()
        n_changed += 1
    return n_changed


# ── FAULT C + D: the two French rows ────────────────────────────────────
#: fiche -> what to fix, and what the authority page says.
_FRENCH_ROWS = [
    {
        "url_fragment": "/fiche-rappel/23712/",
        "Reason": {
            "from": "Une analyse a mis en évidence la présence de Listeria "
                    "monocytogenes",
            "to": "An analysis revealed the presence of Listeria "
                  "monocytogenes.",
            "lang": "fr",
        },
        "Product": {
            "from": "surimi",
            "to": "Surimi pasta salad",
            "original": "Salade de pâtes Surimi",
        },
        "note": (
            f"[morning-fix {TODAY}: Reason was published wholly in French "
            f"and is translated faithfully here "
            f"(tests/test_language_policy, "
            f"tests/test_reason_is_not_wholly_foreign were both red on it). "
            f"Product was 'surimi', which names an ingredient, not the "
            f"product: RappelConso's own listing for 06/10/2026, read at "
            f"https://rappel.conso.gouv.fr/categorie/0/1 on {TODAY}, gives "
            f"the libellé as 'Salade de pâtes Surimi', marque 'Toque du "
            f"chef'. No test was red on the Product — it is the short-name "
            f"shape the English detector cannot see, found by reading every "
            f"Product published in the last 26 h.]"),
    },
    {
        "url_fragment": "/fiche-rappel/23695/",
        "Product": {
            "from": "araignee de porc marinee ail des ours",
            "to": "Marinated pork spider cut (araignée) with wild garlic",
            "original": "Araignée de porc marinée ail des ours",
        },
        "drop_notes": ["[needs cleanup: Product not in English]"],
        "note": (
            f"[morning-fix {TODAY}: Product translated from the French read "
            f"on RappelConso's own listing at "
            f"https://rappel.conso.gouv.fr/categorie/0/1 on {TODAY} "
            f"('Araignée de porc marinée ail des ours', marque 'SANS "
            f"MARQUE', motif 'Détection Salmonella', publiée le "
            f"05/10/2026). 'Araignée de porc' is the pork spider cut; the "
            f"French word is kept in parentheses because the English name "
            f"is not widely used and dropping it would lose the cut. 'Ail "
            f"des ours' is wild garlic (ramsons). The Product-not-in-English "
            f"needs-cleanup stamp left by reviewer 3 / the offline promoter "
            f"is removed because the row no longer needs it. (The stamp's "
            f"literal text is deliberately NOT quoted in this note: the "
            f"removal is a substring strip, and quoting it here would make "
            f"a re-run eat its own note.)]"),
    },
]


def repair_french_rows(wb) -> int:
    ws = wb["Recalls"]
    hdr, body = _rows_of(ws)
    c_url = _col(ws, hdr, "URL")
    c_notes = _col(ws, hdr, "Notes")
    cols = {k: _col(ws, hdr, k) for k in ("Reason", "Product", "LastUpdated")}
    if not (c_url and c_notes):
        return 0

    n_changed = 0
    for spec in _FRENCH_ROWS:
        hit = None
        for n, _ in enumerate(body, start=2):
            if spec["url_fragment"] in str(ws.cell(n, c_url).value or ""):
                hit = n
                break
        if hit is None:
            print(f"  no Recalls row at {spec['url_fragment']} — skipped")
            continue

        notes = str(ws.cell(hit, c_notes).value or "").strip()
        touched = False

        for field in ("Reason", "Product"):
            if field not in spec:
                continue
            c = cols[field]
            have = str(ws.cell(hit, c).value or "").strip()
            want_from = spec[field]["from"]
            if have.casefold() != want_from.casefold():
                print(f"  row {hit} {field} is {have[:48]!r}, not "
                      f"{want_from[:48]!r} — left alone")
                continue
            ws.cell(hit, c).value = spec[field]["to"]
            print(f"  row {hit} {field}: {have[:46]!r} -> "
                  f"{spec[field]['to'][:46]!r}")
            touched = True
            if "lang" in spec[field]:
                tag = (f'[original {field} ({spec[field]["lang"]}): '
                       f'"{want_from}"]')
            else:
                tag = f"[original product: {spec[field]['original']}]"
            if tag not in notes:
                notes = (notes + " " + tag).strip()

        for stamp in spec.get("drop_notes", ()):
            if stamp in notes:
                notes = notes.replace(stamp, "").strip()
                notes = " ".join(notes.split())
                print(f"  row {hit}: removed {stamp}")
                touched = True

        if touched:
            if spec["note"] not in notes:
                notes = (notes + " " + spec["note"]).strip()
            ws.cell(hit, c_notes).value = notes
            if cols["LastUpdated"]:
                ws.cell(hit, cols["LastUpdated"]).value = TODAY
            n_changed += 1
    return n_changed


# ── FAULT E: queue the three through the real path ───────────────────────
def add_missed_recalls() -> int:
    """Queue the three through tools/add_manual_row.py, as a person would."""
    import openpyxl

    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    seen = set()
    for sheet in wb.sheetnames:
        hdr, body = _rows_of(wb[sheet])
        if "URL" not in hdr:
            continue
        i = hdr.index("URL")
        for r in body:
            if r and i < len(r) and r[i]:
                seen.add(str(r[i]).strip().lower())
    wb.close()

    todo = []
    for row in MISSED_ROWS:
        if row["URL"].strip().lower() in seen:
            print(f"  already present, not added again: {row['URL']}")
            continue
        todo.append(row)
    if not todo:
        return 0

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(todo, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip())
    if out.returncode != 0:
        print("  add_manual_row FAILED:", (out.stderr or "")[-800:])
        return 0
    return len(todo)


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    before_pending = wb["Pending"].max_row - 1

    print("FAULT A + B(label) — labels written verbatim by bypassing writers")
    n_ab = canonicalise_live_labels(wb)
    print(f"  {n_ab} label(s) canonicalised\n")

    print("FAULT B(date) — a shelf-life date sitting in Date, 23 d ahead")
    n_b = repair_the_future_dated_bvl_row(wb)
    print(f"  {n_b} row(s) repaired\n")

    print("FAULT C + D — two French rows: Reason, Product, stale stamp")
    n_cd = repair_french_rows(wb)
    print(f"  {n_cd} row(s) repaired\n")

    if n_ab or n_b or n_cd:
        wb.save(XLSX)
    wb.close()

    print("FAULT E — three recalls the pipeline missed in the last 24 h")
    n_e = add_missed_recalls()
    print(f"  {n_e} row(s) queued into Pending\n")

    total = n_ab + n_b + n_cd + n_e
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
    n = mirror_json_from_xlsx(XLSX, JSON)
    print(f"recalls.json re-mirrored: {n} rows")

    print("ROWS_REMOVED=0")      # nothing is removed from Recalls here
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
