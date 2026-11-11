# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-11.

Base: main at 09d789dc ("Italian gap finder: 2026-11-11 auto-update").

Every value set below was read this morning on the regulator's own page, or
is a faithful translation of text already on the row it is written to. No
row leaves Recalls, so the commit message carries no `remove-rows`.

THREE TESTS WERE RED ON MAIN AT THE BASE SHA. All three are data defects
and all three are fixed here.

    test_language_policy::test_every_description_field_is_english          A
    test_reason_is_not_wholly_foreign[Recalls]                             A
    test_a_recall_is_not_both_published_and_rejected[Weekly_Rejected-…]    B


A. SIX PUBLISHED RAPPELCONSO ROWS STILL IN FRENCH
=================================================
The six fiches published 10/11/2026 were promoted overnight carrying the
French Motif, and five of them the French product wording too. Reviewer 3
and the offline promoter stamp "[needs cleanup: … not in English]" but
have no model, so the stamp is all they can do. Each Product and Reason
below is a translation of the fiche's own wording, confirmed this morning
against the fiche itself; the French is kept in Notes, where the dashboard
shows it on hover and searches it. The stale stamps are dropped from the
rows that are now translated.

One of the six also carried the Pathogen "Listeria monocytogene" — the
French singular, one letter short of the canonical spelling 1,000-odd rows
in this register use. It is the only row in Recalls with that value.

B. TWO ARCHIVE ROWS CONTRADICTING THE REGISTER
==============================================
B.1 fiche 24050 (Maison Laborie, saucisson cuit à l'ail). The URL agent
    refused it with "Identity does not match: the official page does not
    contain the required details"; the offline promoter then published it
    anyway. The fiche was read this morning and it does contain them —
    Titre "Saucisson cuit à l'ail", Marque "Maison Laborie", Modèles
    "Saucisson cuit à l'ail entier ou en demi", Motif the Listeria text,
    Origine "SALAISONS LABORIE ET FILS Maison Laborie", 10/11/2026. The
    refusal is false; the published copy wins and the archive row says so.

B.2 the Ministero della Salute tuna recall of 2026-11-03, PUBLISHED as
    "Histamine / scombrotoxin", Tier 1, and archived the same morning by
    the Italian gap finder as "allergen: Undeclared allergen — out of
    scope (Rule B reject)" with Pathogen "mostarda". That is not a verdict
    about this recall: "mostarda" is a SECOND recall mentioned in the same
    news article. The rejection is a defect of the classifier, repaired in
    pipeline/gap_finder/rules.py in this same pass (see D) and held by
    tests/test_a_published_hazard_is_readable_in_every_language.py. The
    archive row is marked SUPERSEDED rather than deleted; archives are
    append-only.

C. TWO PENDING ROWS
===================
C.1 GIS (PL) 10.11.2026, mould in mineral water — held by the publish gate
    on an empty Company, which the row could not carry because the GIS
    listing parse leaves producer, lot and best-before on the detail page.
    The detail page was read this morning and names them. Mould has been
    in scope since 2026-09-07.

C.2 FDA "Ice Cream Recall Gets FDA's Highest Risk Level Over Listeria
    Concerns" (Google News, Date 2026-11-06). NO FIELD IS FILLED HERE. The
    fda.gov page the URL agent twice "confirmed" for this row was read
    this morning and it is the Ramar Foods Peekaboo mint-chocolate-chip
    recall whose Best Before is 10/08/2021 and which the page itself says
    "has been completed and FDA has terminated". A 2021 terminated recall
    is not the 2026 headline the row was discovered from. The row is held
    today only because Company is empty; the next pass that fills Company
    from that URL would publish a five-year-old recall as a 2026 one. So
    the mismatch is written onto the row instead, where the reviewers and
    the next fix pass will read it.

D. A VOCABULARY GAP AT THE GAP-FINDER GATE (code, not data)
===========================================================
B.2's rejection is written fresh by the Italian gap finder on every run,
because nothing in pipeline/gap_finder/rules.py could see the word
"istamina". Only the long phrase "intossicazione da istamina" was listed,
and a headline never says "intossicazione"; with the real hazard invisible
the allergen list was the only lexicon that matched anything in the
article. Sweeping every distinct Pathogen the Recalls sheet publishes
through classify() found three more of the same shape — the bare
"enterotoxin" (10 published rows), "Pseudomonas aeruginosa" (taken by the
publish gate and alert_vocab on 2026-10-29, never by this gate) and
hydrocyanic acid, whose French wording "acide cyanhydrique" is the motif
on the two fiches queued in E below. All four are repaired in rules.py
and held by the new test. tools/alert_vocab.py needed nothing: every
published hazard already reaches a subscriber alert.

E. THREE MISSED RECALLS (tools/rows_2026_11_11.json)
====================================================
RappelConso 24053 and 24054 (Biocoop brown flaxseed 250 g and 10 kg,
10/11/2026, hydrocyanic acid over the limit) and CAA (JP) 00000036115
(Kinpeido Honten sweet potato butter dorayaki, 掲載日 2026/11/11, mould).
Each was read on the authority's own page; the provenance is in each row's
Notes.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
ROWS_JSON = ROOT / "tools" / "rows_2026_11_11.json"

STAMP = "morning fix 2026-11-11"
NEEDS_PRODUCT = "[needs cleanup: Product not in English]"
NEEDS_REASON = "[needs cleanup: Reason not in English]"

FR = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"

LABORIE_REASON_FR = ("Présence faible de Listeria monocytogene; rappel de "
                     "précaution car développement possible dans le produits "
                     "avant la fin de dlc")
LABORIE_REASON_EN = ("Low level of Listeria monocytogenes found; precautionary "
                     "recall, because the organism may grow in the product "
                     "before the use-by date")
LABORIE_SRC = ("Read on the fiche itself today — Marque \"Maison Laborie\", "
               "Origine de la fiche \"SALAISONS LABORIE ET FILS Maison "
               "Laborie\", date 10/11/2026, Risques \"Listeria monocytogenes "
               "(agent responsable de la listériose)\", Motif "
               "\"Présence faible de listeria monocytogene Rappel de "
               "précaution car développement possible dans le produits avant "
               "la fin de DLC\" (the same Motif on all three Laborie fiches "
               "of that day).")


# ── A. the six translations ────────────────────────────────────────────────
RECALLS_FIXES = [
    {
        "url": FR % "24052",
        "set": {
            "Product": "Seasoned sheep sausage meat (chair à saucisse de "
                       "brebis assaisonnée)",
            "Reason": "Microbiological test result non-compliant",
        },
        "drop_stamps": (NEEDS_PRODUCT, NEEDS_REASON),
        "note": (
            f"[{STAMP}: Product and Reason translated from the fiche's own "
            "French. Read on fiche 24052 today — Fiche réf. 2026-11-0065, "
            "date 10/11/2026, Titre \"Chair à saucisse de brebis "
            "assaisonnée\", Marque \"LA FERME DES ROBINS\", Noms des modèles "
            "\"Chair à saucisse de brebis assaisonnée\", Motif \"Analyse "
            "microbiologique non conforme\", Risques \"Listeria "
            "monocytogenes (agent responsable de la listériose)\" AND "
            "\"Salmonella spp (agent responsable de la salmonellose)\", "
            "Origine de la fiche \"BIO A PRO BIO A PRO\". The published "
            "Pathogen names only Listeria monocytogenes; the fiche names "
            "both, and the second hazard is recorded here rather than "
            "written into a single-valued field. Nothing added that the "
            "fiche does not state.] "
            "[original product: chair à saucisse de brebis assaisonnée] "
            "[original Reason (fr): \"Analyse microbiologique non "
            "conforme\"]"),
    },
    {
        "url": FR % "24043",
        "set": {
            "Product": "Sliced chicken delicatessen loaf (tranche délice de "
                       "poulet), 120 g, reference AT002",
            "Reason": "Listeria monocytogenes detected",
        },
        "drop_stamps": (NEEDS_PRODUCT, NEEDS_REASON),
        "note": (
            f"[{STAMP}: Product and Reason translated from the fiche's own "
            "French. Read on fiche 24043 today — Fiche réf. 2026-11-0056, "
            "date 10/11/2026, Titre \"TRANCHE DELICE DE POULET 120g\", "
            "Marque \"ALJADID\", Noms des modèles \"AT002\", Motif \"Mise en "
            "évidence de la présence de Listeria Monocytogenes\", Risques "
            "\"Listeria monocytogenes (agent responsable de la "
            "listériose)\", Origine de la fiche \"NOUVELLE ATLAS NOUVELLE "
            "ATLAS\". The 120 g and the AT002 reference are the fiche's own.] "
            "[original product: tranche delice de poulet 120g] "
            "[original Reason (fr): \"Mise en évidence de la présence de "
            "Listeria monocytogenes\"]"),
    },
    {
        "url": FR % "24060",
        "set": {
            "Product": "Blood sausage (boudin viande; boudin à la viande)",
            "Reason": "Presence of Listeria",
        },
        "drop_stamps": (NEEDS_PRODUCT,),
        "note": (
            f"[{STAMP}: Product translated and the Reason recased from "
            "\"Presence of listéria\", which was half-translated French. "
            "Read on fiche 24060 today — Fiche réf. 2026-11-0073, date "
            "10/11/2026, Titre \"BOUDIN VIANDE\", Marque \"André CAZAUX\", "
            "Noms des modèles \"BOUDIN VIANDE BOUDIN A LA VIANDE\", Motif "
            "\"Présence de Listéria\", Risques \"Listeria monocytogenes "
            "(agent responsable de la listériose)\", Origine de la fiche "
            "\"SARL CHARCUTERIE ANDRE CAZAUX\". The fiche names the genus "
            "only in its Motif; the published Pathogen comes from the "
            "Risques line and is unchanged.] "
            "[original product: boudin viande; boudin a la viande] "
            "[original Reason (fr): \"Présence de Listéria\"]"),
    },
    {
        "url": FR % "24049",
        "set": {
            "Product": "Country pâté (pâté de campagne), turned out of the "
                       "mould, in the mould, and in slices or halves",
            "Reason": LABORIE_REASON_EN,
            "Pathogen": "Listeria monocytogenes",
        },
        "drop_stamps": (NEEDS_PRODUCT, NEEDS_REASON),
        "note": (
            f"[{STAMP}: Product and Reason translated from the fiche's own "
            "French, and the Pathogen corrected from \"Listeria "
            "monocytogene\" — the French singular, and the only row in "
            "Recalls carrying it — to the canonical spelling the rest of "
            "the register uses. Fiche réf. 2026-11-0062; Titre \"Pâté de "
            "campagne\"; Noms des modèles \"Pâté de campagne démoulé Pâté de "
            "campagne en moule Pâté de campagne en tranche ou en demi\". "
            + LABORIE_SRC + "] "
            "[original product: pâté de campagne démoulé; pâté de campagne "
            "en moule; pâté de campagne en tranche ou en demi] "
            f"[original Reason (fr): \"{LABORIE_REASON_FR}\"]"),
    },
    {
        "url": FR % "24050",
        "set": {
            "Product": "Cooked garlic sausage (saucisson cuit à l'ail), "
                       "whole or half",
            "Reason": LABORIE_REASON_EN,
        },
        "drop_stamps": (NEEDS_REASON, NEEDS_PRODUCT),
        "note": (
            f"[{STAMP}: Reason translated from the fiche's own French and "
            "the Product given its English name. Fiche réf. 2026-11-0063; "
            "Titre \"Saucisson cuit à l'ail\"; Noms des modèles \"Saucisson "
            "cuit à l'ail entier ou en demi\". " + LABORIE_SRC + " Reading "
            "the fiche is also what settles the URL agent's refusal of this "
            "row; see the SUPERSEDED note on the Weekly_Rejected copy.] "
            "[original product: saucisson cuit à l'ail entier ou en demi] "
            f"[original Reason (fr): \"{LABORIE_REASON_FR}\"]"),
    },
    {
        "url": FR % "24051",
        "set": {
            "Product": "Chestnut blood sausage (boudin aux châtaignes), in "
                       "packs of ten or of three",
            "Reason": LABORIE_REASON_EN,
        },
        "drop_stamps": (NEEDS_PRODUCT, NEEDS_REASON),
        "note": (
            f"[{STAMP}: Product and Reason translated from the fiche's own "
            "French. Fiche réf. 2026-11-0064; Titre \"Boudin aux "
            "chataignes\" (the title is printed without the circumflex); "
            "Noms des modèles \"Boudin aux châtaignes par dix ou par "
            "trois\". " + LABORIE_SRC + "] "
            "[original product: boudin aux châtaignes par dix ou par trois] "
            f"[original Reason (fr): \"{LABORIE_REASON_FR}\"]"),
    },
]


# ── C. Pending ─────────────────────────────────────────────────────────────
GIS_URL = ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-"
           "zywnosci-stwierdzenie-obecnosci-plesni-w-wodzie-mineralnej")
RAMAR_URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
             "alerts/ramar-foods-recalls-mint-chocolate-chip-hidden-spinach-"
             "ice-cream-because-possible-health-risk")

PENDING_FIXES = [
    {
        "url": GIS_URL,
        "set": {
            "Company": "Jurajska Spółka Akcyjna",
            "Product": "Jurajska Sport natural mineral water (Naturalna Woda "
                       "Mineralna Jurajska Sport), still, medium-mineral, "
                       "700 ml",
            "Pathogen": "Mold",
            "Reason": "Public food warning: mould found in mineral water",
        },
        "drop_stamps": (),
        "note": (
            f"[{STAMP}: the publish gate held this row on an empty Company, "
            "which the GIS listing parse cannot carry — the row's own Notes "
            "already said \"producer, lot and best-before are on the detail "
            f"page\". That page, {GIS_URL}, was read today and prints: "
            "published 10.11.2026; \"Naturalna Woda Mineralna Jurajska "
            "Sport, niegazowana, średniozmineralizowana 700 ml\"; Producent "
            "\"Jurajska Spółka Akcyjna ul. Myszkowska 40, Postęp, 42-350 "
            "Koziegłowy\"; one combined field \"Numer partii i data "
            "minimalnej trwałości: 29.07.2027\" — so there is no separate "
            "lot number on the page and none is written here; the hazard is "
            "mould (pleśń) confirmed by laboratory testing of one batch "
            "after a consumer complaint about taste and smell, with the "
            "producer withdrawing the batch under PIS/IW supervision. "
            "Pathogen is the register's canonical \"Mold\" (31 published "
            "rows); mould has been in scope since 2026-09-07. The Reason is "
            "a translation of the row's own Polish headline. Brand is left "
            "as it was: the page prints no separate brand field.] "
            "[original product: Stwierdzenie obecności pleśni w wodzie "
            "mineralnej] [original Reason (pl): \"Ostrzeżenie publiczne "
            "dotyczące żywności: Stwierdzenie obecności pleśni w wodzie "
            "mineralnej\"]"),
    },
    {
        "url": RAMAR_URL,
        "set": {},
        "drop_stamps": (),
        "note": (
            f"[{STAMP}: DO NOT PUBLISH THIS ROW AGAINST THIS URL. The page "
            "was read today and it is not the recall this row was "
            "discovered from. It is the Ramar Foods (Pittsburg, CA) "
            "\"Peekaboo\" Mint Chocolate Chip with Hidden Spinach Ice Cream "
            "recall, 14 ounce printed paper container, UPC 8685400001, Best "
            "Before 10/08/2021, sold at select Target stores in FL, GA, SC "
            "and NC, 33 containers reached consumers — and the page itself "
            "says \"This recall has been completed and FDA has terminated "
            "this recall.\" The row's own Date is 2026-11-06 and it was "
            "discovered from the Google News headline \"Ice Cream Recall "
            "Gets FDA's Highest Risk Level Over Listeria Concerns\", which "
            "is a different, current recall. The URL agent confirmed this "
            "URL for this row twice (pending_gap→v1→v2); it is wrong both "
            "times. The row is held today only because Company is empty, so "
            "filling Company from this page would publish a five-year-old "
            "terminated recall as a 2026 one. Either find the 2026 notice "
            "this headline is about and replace the URL, or reject the row "
            "— do not enrich it in place.]"),
    },
]


# ── B. archive annotations ─────────────────────────────────────────────────
TUNA_URL = ("https://www.salute.gov.it/new/sites/default/files/external_data/"
            "avvisi_sicurezza_alimentare/Cartello%20di%20Richiamo%20Filetti%20"
            "di%20Tonno%20Surgelato%20lotto%20n.%2076504_01_001%20_aggiornato%"
            "20_1__1793979220.pdf")

ARCHIVE_NOTES = [
    {
        "sheet": "Weekly_Rejected",
        "url": FR % "24050",
        "column": "RejectionReason",
        "reviewed": "SUPERSEDED",
        "prefix": (
            f"[SUPERSEDED {STAMP} — \"Identity does not match: the official "
            "page does not contain the required details\" is false. Fiche "
            "24050 was read today and contains every one of them: Fiche réf. "
            "2026-11-0063, date 10/11/2026, Titre \"Saucisson cuit à "
            "l'ail\", Marque \"Maison Laborie\", Noms des modèles "
            "\"Saucisson cuit à l'ail entier ou en demi\", Motif the low-"
            "Listeria precautionary text, Risques \"Listeria monocytogenes\", "
            "Origine de la fiche \"SALAISONS LABORIE ET FILS Maison "
            "Laborie\". The published row is the copy that wins; this row is "
            "kept as the audit trail of the refusal.] "),
    },
    {
        "sheet": "Weekly_Rejected",
        "url": TUNA_URL,
        "column": "RejectionReason",
        "reviewed": "SUPERSEDED",
        "prefix": (
            f"[SUPERSEDED {STAMP} — \"Undeclared allergen — out of scope "
            "(Rule B reject)\" is not a verdict about this recall. The "
            "recall is histamine in frozen tuna fillets, PUBLISHED in this "
            "register since 2026-11-03 as \"Histamine / scombrotoxin\", "
            "Tier 1, against this same salute.gov.it notice. The Pathogen on "
            "this archive row, \"mostarda\", is a SECOND recall mentioned in "
            "the news article the Italian gap finder read (Cascina Balcarino "
            "mostarde) and not the hazard of this one. The cause was a "
            "vocabulary gap: nothing in pipeline/gap_finder/rules.py could "
            "see the word \"istamina\" — only the phrase \"intossicazione da "
            "istamina\", which a headline never carries — so the allergen "
            "list was the only lexicon that matched anything in the article, "
            "and the allergen reject is deliberately last. The gap is "
            "repaired in this same pass and held by "
            "tests/test_a_published_hazard_is_readable_in_every_language.py. "
            "The published row wins; this row is kept as the audit trail.] "),
    },
]


# ── helpers (same shape as the previous passes) ────────────────────────────
def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _find_rows(ws, url):
    head = _head(ws)
    col = head.index("URL") + 1
    target = _key(url)
    return [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == target]


def _find_rows_exact(ws, url):
    """Archive rows are matched on the stored string.

    _normalize_url_for_dedup collapses the archive row and its published
    twin to one key, so annotating by key would also stamp the published
    row (the 2026-11-07 pass learned this).
    """
    head = _head(ws)
    col = head.index("URL") + 1
    target = str(url or "").strip()
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, col).value or "").strip() == target]


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    if notes.lower() in ("none", "nan"):
        notes = ""
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, " ")
    notes = re.sub(r"[ \t]{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs, required=True) -> int:
    if sheet not in wb.sheetnames:
        return 0
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        hit = False
        for r in _find_rows(ws, spec["url"]):
            row = _rowdict(ws, head, r)
            if STAMP in str(row.get("Notes") or ""):
                print(f"  {sheet}:{r}: already repaired, left alone")
                hit = True
                continue
            for field, value in spec["set"].items():
                if field in head:
                    _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"],
                         spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}:{r}: {str(row.get('Source'))[:18]!r} "
                  f"{spec['url'][-46:]}")
        if not hit and required:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return done


def annotate_archives(wb) -> int:
    done = 0
    for spec in ARCHIVE_NOTES:
        sheet = spec["sheet"]
        if sheet not in wb.sheetnames:                      # pragma: no cover
            continue
        ws = wb[sheet]
        head = _head(ws)
        rows = _find_rows_exact(ws, spec["url"])
        if not rows:
            raise SystemExit(
                f"{sheet}: no archive row stored at exactly {spec['url']} — "
                f"main has moved under this repair script")
        for r in rows:
            row = _rowdict(ws, head, r)
            col = spec["column"]
            if col not in head:                             # pragma: no cover
                continue
            cur = str(row.get(col) or "").strip()
            if cur.lower() in ("none", "nan"):
                cur = ""
            if f"SUPERSEDED {STAMP}" in cur:
                print(f"  {sheet}:{r}: already annotated, left alone")
                continue
            _set(ws, head, r, col, (spec["prefix"] + cur).strip())
            if "Reviewed" in head:
                _set(ws, head, r, "Reviewed", spec["reviewed"])
            done += 1
            print(f"  {sheet}:{r}: {spec['reviewed']} "
                  f"{str(row.get('Date'))[:10]} {str(row.get('Source'))[:16]}")
    return done


def queue_manual_rows() -> None:
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print("  NOTE: add_manual_row exited "
              f"{proc.returncode}. Fiches 24053 and 24054 are the 250 g and "
              "10 kg packs of one product filed as two fiches; a "
              "near-duplicate refusal of one of them is an accepted "
              "outcome. Read the verdict lines above.")


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    print("A. Recalls: six French rows translated")
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    print("A. Weekly_Review mirror (same specs; not every row is mirrored)")
    mirror_fixed = _fix_sheet(wb, "Weekly_Review", RECALLS_FIXES,
                              required=False)
    print("C. Pending: the GIS producer, and the Ramar URL mismatch")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("B. Archive annotations")
    annotated = annotate_archives(wb)
    wb.save(XLSX)
    wb.close()

    print("E. Queue the three missed recalls")
    queue_manual_rows()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"\nrepair_2026_11_11: Recalls {recalls_fixed} fixed, "
          f"Weekly_Review {mirror_fixed} mirrored, Pending {pending_fixed} "
          f"annotated, {annotated} archive row(s) SUPERSEDED")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
