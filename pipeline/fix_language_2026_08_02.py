#!/usr/bin/env python3
"""Bring every published row into line with the English-output policy.

    "everything in English except brand / or product name"   — operator, 2026-08-02

WHAT THIS FIXES
===============
A. REASON — 126 rows.
   Translated via pipeline/_language.py: a verified per-string table for the
   half-translated French and the whole-sentence German/Spanish/Italian, and a
   bilingual splitter for RASFF's native//English subjects. Nothing is machine-
   translated; a string the table does not know is left alone and reported.

B. PRODUCT — 30 rows that are not product NAMES at all.
   RASFF writes its notification subject into Product, bilingually:

       "Presencia de Salmonela spp en salchichón procedente de España //
        Presence of Salmonella spp. in cured sausage from Spain"

   That is a description, so the English half is kept. A genuine foreign
   product NAME — "brie a l'ail", "charcuterie seche", "Χούμους",
   "Freshona Bio Beerenmischung" — is exempt and untouched, which is why the
   test is "does this split into two languages", not "does this look French".

C. AESAN (ES) — five rows whose Company/Brand hold the alert TITLE or the
   AGENCY NAME rather than a company. This is what the dashboard was showing
   as the headline of the 31 July card:

       Company  "Alerta por presencia de Salmonella spp. en chips de fruta
                 procedentes de Alemania. (Ref. ES2026/473)"
       Product  "Aesan - Agencia Española de Seguridad Alimentaria y
                 Nutrición # Alerta por presencia de Salmonella spp"

   Neither field is a name; the scraper captured the page title and the site
   banner. For alert 56/2026 the real values were verified from the AESAN
   alert as reported in the Spanish press on 2026-07-31: brand Rossmann,
   product "Genuss Plus Kids Bio Chips de manzana" (apple chips, 35 g bags,
   EAN 4068134196903, best before 24/02/2027), origin Germany.

   The other four could not be re-read from aesan.gob.es from this
   environment, so their Company/Brand are set to the explicit
   "(not specified in AESAN alert N/2026)" disclosure the publish gate already
   recognises — never to a guess. Publishing the regulator's own name as the
   recalling company is worse than admitting the field was not captured.

D. ONE CONTAMINATED ROW, found while auditing the language.
   The FSA (UK) row of 2026-07-01 carries the Reason "Taux de t-2 toxine et
   ht-2 toxine supérieures à la valeur réglementaire" — French, and belonging
   to the six Terres de Moulin Madame flour recalls published the same day.
   Its own Product says the recall is O'Brien Fine Foods ham, Listeria
   monocytogenes. Company holds the FSA headline sentence rather than a
   company. Repaired from the row's own Product text and the FSA notice.

Run:  python -m pipeline.fix_language_2026_08_02 --dry-run
      python -m pipeline.fix_language_2026_08_02
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TODAY = "2026-08-02"

# ── C: AESAN rows whose Company/Brand are not names ─────────────────────────
AESAN = "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/"
AESAN_FIX = {
    AESAN + "2026_56.htm": dict(
        company="Rossmann",
        brand="Rossmann",
        product="Genuss Plus Kids Bio Chips de manzana (apple chips), 35 g bags "
                "— EAN 4068134196903, best before 24/02/2027",
        note="verified 2026-07-31 against the AESAN alert as reported by the "
             "Spanish press: brand Rossmann, apple chips for children, origin "
             "Germany, distributed through Rossmann stores in Spain. Company, "
             "Brand and Product had all captured the alert page title and the "
             "site banner instead."),
    AESAN + "2026_53.htm": dict(
        company="(not specified in AESAN alert 53/2026)",
        brand="Natura",
        product=None,
        note="Company and Brand held the alert page TITLE ('Alerta por "
             "presencia de Listeria monocytogenes en bacalao …'), not a name. "
             "Brand taken from the product designation already on the row "
             "('Bacalao Natura en aceite de girasol'); the recalling company "
             "is not recoverable from this environment and is NOT guessed."),
    AESAN + "2026_33_ampliacion_2.htm": dict(
        company="(not specified in AESAN alert 33/2026)",
        brand="(not specified in AESAN alert 33/2026)",
        product=None,
        note="Company and Brand held 'Agencia Española de Seguridad "
             "Alimentaria y Nutrición' — the regulator issuing the alert, not "
             "the recalling company."),
    AESAN + "2026_38_Ampliacion.htm": dict(
        company="(not specified in AESAN alert 38/2026)",
        brand="(not specified in AESAN alert 38/2026)",
        product="Frozen chicken product (extension of AESAN alert 38/2026)",
        note="Company and Brand held the regulator's own name; Product held "
             "the Spanish alert headline ('Ampliación de la alerta alimentaria "
             "relativa a Listeria monocytogenes en producto congelado de "
             "pollo') rather than a product name."),
    AESAN + "2026_22.htm": dict(
        company="(not specified in AESAN alert 22/2026)",
        brand="(not specified in AESAN alert 22/2026)",
        product=None,
        note="Company and Brand held the PRODUCT ('Longaniza payés'), which is "
             "a sausage type, not a company or a brand."),
}

# ── D: the contaminated FSA row ─────────────────────────────────────────────
FSA_URL = "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-32-2026"
FSA_FIX = dict(
    company="O'Brien Fine Foods",
    brand="O'Brien Fine Foods",
    product="Ham products",
    reason="Possible contamination with Listeria monocytogenes",
    note="[audit 2026-08-02: Reason held 'Taux de t-2 toxine et ht-2 toxine "
         "supérieures à la valeur réglementaire' — French, and the motif of "
         "the six Terres de Moulin Madame flour recalls published the same "
         "day (2026-07-01). Cross-row contamination from the url-gate batch "
         "mis-attribution. Company held the FSA headline sentence rather than "
         "a company name. Repaired from the row's own Product text and the "
         "FSA notice; Pathogen (Listeria monocytogenes) was already correct.]")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )
    from pipeline._language import (
        englishify_reason, split_bilingual, looks_non_english, detect_language,
    )

    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)

    n_reason = n_product = n_aesan = n_fsa = 0
    unresolved = []

    for row in approved:
        url = str(row.get("URL") or "")

        # D — the contaminated FSA row. Handled FIRST, because step A would
        # otherwise faithfully translate the WRONG French motif into English
        # and hide the contamination behind good grammar.
        if url == FSA_URL:
            row["Company"] = FSA_FIX["company"]
            row["Brand"] = FSA_FIX["brand"]
            row["Product"] = FSA_FIX["product"]
            row["Reason"] = FSA_FIX["reason"]
            if f"audit {TODAY}" not in str(row.get("Notes") or ""):
                row["Notes"] = (str(row.get("Notes") or "").strip() + " "
                                + FSA_FIX["note"]).strip()[:1800]
            row["LastUpdated"] = TODAY
            n_fsa += 1
            continue

        # A — Reason into English.
        before = str(row.get("Reason") or "")
        after, changed = englishify_reason(before)
        if changed:
            row["Reason"] = after
            n_reason += 1
            row["LastUpdated"] = TODAY
        elif looks_non_english(before):
            unresolved.append((detect_language(before), str(row.get("Source")),
                               before[:110]))

        # B — Product that is a bilingual notification subject, not a name.
        prod = str(row.get("Product") or "")
        if looks_non_english(prod):
            english = split_bilingual(prod)
            if english and english != prod:
                row["Product"] = english
                n_product += 1
                row["LastUpdated"] = TODAY

        # C — AESAN Company/Brand/Product that are not names.
        fix = AESAN_FIX.get(url)
        if fix:
            if fix.get("company"):
                row["Company"] = fix["company"]
            if fix.get("brand"):
                row["Brand"] = fix["brand"]
            if fix.get("product"):
                row["Product"] = fix["product"]
            if f"audit {TODAY} (language)" not in str(row.get("Notes") or ""):
                row["Notes"] = (
                    str(row.get("Notes") or "").strip()
                    + f" [audit {TODAY} (language): {fix['note']}]"
                ).strip()[:1800]
            row["LastUpdated"] = TODAY
            n_aesan += 1

    print(f"Reason  translated to English : {n_reason}")
    print(f"Product bilingual → English   : {n_product}")
    print(f"AESAN   name fields repaired  : {n_aesan}")
    print(f"FSA     contaminated row      : {n_fsa}")
    print(f"unresolved non-English Reason : {len(unresolved)}")
    for lang, src, text in unresolved:
        print(f"    [{lang}|{src}] {text}")

    if args.dry_run:
        print("\n(dry run — nothing written)")
        return 0

    save_xlsx_with_pending(sort_rows(approved), sort_rows(pending), xlsx)
    mirror_json_from_xlsx(xlsx, ROOT / "docs" / "data" / "recalls.json")
    print("\n✓ written + recalls.json mirrored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
