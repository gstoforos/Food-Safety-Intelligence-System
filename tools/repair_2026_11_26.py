#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-26 — every data edit of the run, in one script.

Run from the repo root against main's current workbook:

    python3 tools/repair_2026_11_26.py            # writes
    python3 tools/repair_2026_11_26.py --dry-run  # report only

tools/rebase_and_verify.py re-runs this against whatever main holds at
upload time rather than carrying a workbook built this morning, so every
edit here is keyed on the URL it finds and on the VALUE it expects, and is
a no-op when the value is already right. Nothing is positional and no row
is deleted from Recalls.


WHAT WAS WRONG ON MAIN AT 86bc7338
==================================

1. MAIN WAS RED. Five tests failed on the tip:
       tests/test_product_and_firm_names_are_english.py   (6 Products)
       tests/test_reason_is_not_wholly_foreign.py[Recalls] (4 Reasons)
       tests/test_language_policy.py::test_every_description_field_is_english
       tests/test_an_agency_headline_is_not_a_firm_and_a_food.py  (x2)
   Seven RappelConso rows published 2026-11-23/24/25 still carried the
   fiche's French Product and/or Motif. Six of the seven WERE stamped
   "[needs cleanup: …]", so the gates saw them; nothing had translated
   them yet, and a stamp only excuses a row for one day.

2. A ROW PUBLISHED WITH THE NOTIFIER'S WORD FOR "VARIOUS" IN THREE FIELDS.
   Fiche 24179 (the Étang de Thau shellfish harvesting ban, Tier 1,
   Marine biotoxin) published at 02:41 UTC today as:
       Company  'Diverses'      Brand 'Diverses'      Product 'diverses'
       Reason   'Biotoxines psp'
   and that is what the dashboard showed. DGCCRF's own marque and
   références fields read "Diverses" because the fiche bans a harvesting
   AREA, not a product. No gate held it and no language test saw it: the
   Product is one word and the Reason is two, so every detector that
   needs two hits had nothing to count.

   IT COST MORE THAN A BAD-LOOKING ROW. The same event is ALREADY
   published for fiche 24121 (row 55, 2026-11-19, 'Comité Régional de la
   Conchyliculture de Méditerranée' / 'Shellfish harvested in the Étang
   de Thau from 16 November 2026'), and merge_master's near-duplicate rule
   never saw the second one — a dedup key built on Company and Product
   cannot match 'Diverses'/'diverses' against the real names. A
   placeholder in a name field defeats deduplication. Held at the gate
   from now on by _PLACEHOLDER_VALUES in pipeline/_publish_gate.py.

   Both fiches are kept. They are two distinct DGCCRF notices six days
   apart, one "Imposé par arrêté préfectoral" and one "Volontaire", and
   this register's settled shape is one row per regulator notice — the
   same shape as the three red-potato fiches of lot 26128774 and the two
   emmental fiches 24152/24159. Row 5 is cross-referenced to row 55
   rather than removed, so the relationship is on the record and the
   archives stay append-only.

3. A WHOLE IN-SCOPE HAZARD CLASS BEING REJECTED BY REVIEWER 1.
   Measured on this workbook: five distinct rows rejected with the reason
   "Hazard is a foreign body (…)". Foreign material is in scope and the
   register publishes it — 20 rows carry 'Foreign material (glass)' and
   17 carry 'Foreign material (metal)'. The proof stands on its own:
   fiche 24087 was rejected on that reason (Rejected row 4096) AND is
   published at Recalls row 123.

   pipeline/_url_guard.reject_refusal is the one component meant to catch
   a wrong reviewer-1 rejection, and it had "foreign body" written into
   its own docstring as a content verdict reviewer 1 is entitled to make.
   Fixed today, pinned by tests/test_morning_fix_2026_11_26.py.

   Two real CFIA recalls were suppressed outright and reached no
   subscriber. Both are recovered: BSA Wiberg dry mixes (metal,
   2026-11-04) is queued through tools/manual_rows_2026_11_26.json, and
   the Yeshi dressing row (glass, 2026-11-24) is unblocked in Pending
   below.

4. ONE PENDING ROW BLOCKED ON A FIELD ITS OWN NOTICE PUBLISHES, and
   carrying Status 'rejected' while sitting in Pending — the CFIA Yeshi
   dressing row of finding 3.

5. A GOOD ROW REFUSED BECAUSE BAD COPIES REACHED THE ARCHIVE FIRST.
   The MPI (NZ) Tip Top Jelly Tip recall (Bacillus cereus, 2026-10-02) is
   in Pending as the right-hand side of a split headline — Company and
   Brand hold the headline's subject, Product holds the fragment "due to
   possible ..." — with Date empty. It is the one test still red on main.
   The register HAS the correct extraction, at Rejected row 1108, and
   archived it by 'repromotion-guard' rather than on merit, because three
   mangled copies of the same notice (Rejected 1093, 1094, 1312, all
   rejected by the Confirmer for "Date is empty") had already poisoned
   the URL. The Pending row is repaired from the MPI page below. THE
   GUARD ITSELF IS NOT TOUCHED: "bad copies first, good row refused" is a
   defect in its own right and is reported to the operator for a decision
   rather than patched around in a morning pass.


WHAT IS DELIBERATELY NOT CHANGED
================================

* Pending row for AGES "Schweinspasteten aus Tschechien" (Clostridium
  botulinum). The page's "In Verkehr gebracht von" came back masked and
  truncated from every read this session, so the Company cannot be read.
  Rule 4 is absolute: a Company not in the row's own text, its own URL or
  a page actually read stays EMPTY. Left in Pending.
* Pending row for the FDA uneviscerated dried HERRING alert. Its stored
  URL is truncated mid-slug ("…uneviscerated-dried-her") and returns a
  client error, so the notice could not be read. The croaker recall from
  the same firm IS published (row 28); the herring one may be a separate
  November recall and is reported to the operator rather than guessed.
* Pending row for RappelConso fiche 24089 (stones in hazelnut pieces).
  The fiche would not load for this session. "I could not read it" is not
  "it is gone"; left in Pending.
* The ten pre-existing structural URL problems pipeline.verify_urls
  reports (news hosts under a regulator Source, oldest 2026-01-07). Each
  needs the regulator's own notice found or the Source corrected, which
  is a judgement call per row, not a sweep.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"

NOTE_KEY = "[morning fix pass 2026-11-26"

#: The French original of every Product translated below, so the fiche's
#: own words stay on the row. The dashboard shows these on hover.
EDITS = [
    # ── fiche 24179 — the "Diverses" row (finding 2) ──────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24179/interne",
        "fields": {
            "Company": (
                "Diverses",
                "Comité Régional de la Conchyliculture de Méditerranée"),
            "Brand": ("Diverses", "Unbranded"),
            "Product": (
                "diverses",
                "Shellfish harvested in the Étang de Thau on or after "
                "16 November 2026 (all pack formats: wooden baskets, loose, "
                "trays)"),
            "Reason": (
                "Biotoxines psp",
                "PSP marine biotoxins (paralytic shellfish poisoning "
                "toxins)"),
        },
        "note": (
            "[morning fix pass 2026-11-26: Company, Brand AND Product all "
            "held the notifier's own word for \"various\" and the Reason was "
            "French, and the dashboard showed all four. No gate held it: the "
            "Product is one word and the Reason two, so every detector that "
            "needs two hits had nothing to count. Read on fiche 24179 today "
            "(Fiche réf. 2026-11-0185 • Version 1, date 25/11/2026): H1 "
            "\"Les coquillages récoltés dans l'étang de Thau à partir du "
            "16 novembre 2026\", \"Origine de la fiche: COMITE REGIONAL DE "
            "LA CONCHYLICULTURE DE LA MEDITERRANEE\", \"Nom de la marque du "
            "produit: Diverses\", \"Noms des modèles ou références: "
            "Diverses\", Catégorie Alimentation / Produits de la pêche et "
            "d'aquaculture, Conditionnements \"Tous conditionnements: "
            "bourriches bois, vrac, barquettes,....\", France entière, "
            "Motif \"Biotoxines PSP\", Risques \"Biotoxines marines PSP "
            "(toxines paralysantes)\", Nature juridique \"Volontaire (sans "
            "arrêté préfectoral)\", \"Ne plus consommer\", \"Produit à "
            "conserver à température ambiante\". Informations "
            "complémentaires: shellfish harvested BEFORE 16/11/2026 and held "
            "in protected storage or closed-circuit tanks under the "
            "29/11/2018 crisis protocol are not covered, and nor are "
            "shellfish from other zones. Company is \"Origine de la fiche\" "
            "as printed, which is this register's convention for a "
            "RappelConso area ban — the same value the already-published "
            "fiche 24121 row carries. Brand is \"Unbranded\" because the "
            "fiche's marque field says \"Diverses\": there is no brand, and "
            "\"Unbranded\" is this register's word for that. NO GTIN, lot or "
            "health mark is added: the fiche prints none (its Lot column "
            "holds the literal \"0\"), so the \"health marks beginning FR "
            "34.\" detail on the fiche 24121 row is NOT copied across. "
            "SAME EVENT AS ROW 55 / fiche 24121 (2026-11-19, same étang, "
            "same 16 November harvest start, same PSP hazard): the "
            "near-duplicate rule missed it because the dedup key read "
            "'Diverses'/'diverses' instead of the real names. Both notices "
            "are kept, one row per regulator notice, as with fiches "
            "24152/24159 and the three red-potato fiches of lot 26128774. "
            "[original company: Diverses (the fiche's own marque field)] "
            "[original brand: Diverses] [original product: diverses] "
            "[original Reason (fr): \"Biotoxines PSP\"]]"),
    },
    # ── fiche 24177 (row 2) ───────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24177/interne",
        "fields": {
            "Product": (
                "parmentier de bœuf et andouille de guémené; salade la "
                "rustique; pizza guémené; bruschetta raymond; bol breton",
                "Beef and andouille de Guémené parmentier; La Rustique "
                "salad; Pizza Guémené; Bruschetta Raymond; Bol Breton"),
            "Reason": (
                "Suspicion de Listéria",
                "Suspected presence of Listeria"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: Product and Reason were both "
            "French and the row was stamped for both. Read on fiche 24177 "
            "today (Fiche réf. 2026-11-0183 • Version 1, date 25/11/2026): "
            "H1 \"Produits élaborés à base d'andouille de Guémené\", "
            "\"Origine de la fiche: SA HENDIS CENTRE E. LECLERC HENNEBONT\", "
            "\"Nom de la marque du produit: E. Leclerc Hennebont\", "
            "Catégorie Alimentation / Plats préparés et snacks, \"Noms des "
            "modèles ou références: Parmentier de bœuf et andouille de "
            "Guémené / Salade La rustique / Pizza Guémené / Bruschetta "
            "Raymond / Bol breton\", no GTIN on the page, \"Date limite de "
            "consommation entre le 07/11/2026 et le 24/11/2026\", "
            "Conditionnements \"Produits vendus en boîte carton (pizza "
            "Guémené, bol et brushetta) et en barquette plastique avec "
            "couvercle (parmentier et salade)\", France entière, \"Du "
            "07/11/2026 au 23/11/2026\", Distributeurs \"E. Leclerc "
            "Hennebont\", Motif \"Suspicion de Listéria\", Risques "
            "\"Listeria monocytogenes (agent responsable de la "
            "listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". \"Andouille de Guémené\", \"La Rustique\", "
            "\"Pizza Guémené\", \"Bruschetta Raymond\" and \"Bol Breton\" "
            "are the products' own names on the fiche and are kept as "
            "written rather than explained. No lot, weight or date is "
            "added. [original product: parmentier de bœuf et andouille de "
            "guémené; salade la rustique; pizza guémené; bruschetta "
            "raymond; bol breton] [original Reason (fr): \"Suspicion de "
            "Listéria\"]]"),
    },
    # ── fiche 24176 (row 3) ───────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24176/interne",
        "fields": {
            "Product": (
                "andouille de guémené tranchée ou en morceaux, vendue au "
                "rayon traiteur traditionnel et en rayon libre service",
                "Andouille de Guémené, sliced or in pieces, sold at the "
                "traditional delicatessen counter and in self-service"),
            "Reason": (
                "Suspicion de présence de listéria",
                "Suspected presence of Listeria"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: Product and Reason were both "
            "French and the row was stamped for both. Read on fiche 24176 "
            "today (Fiche réf. 2026-11-0182 • Version 1, date 25/11/2026): "
            "H1 \"Andouille de Guémené\", \"Origine de la fiche: SA HENDIS "
            "CENTRE E. LECLERC HENNEBONT\", \"Nom de la marque du produit: "
            "E. Leclerc Hennebont\", Catégorie Alimentation / Plats préparés "
            "et snacks, \"Noms des modèles ou références: Andouille de "
            "Guémené tranchée ou en morceaux, vendue au rayon traiteur "
            "traditionnel et en rayon libre service\", GTIN 0200964000000, "
            "the Lot column holding \"Rayon traiteur libre service : "
            "andouille de Guémené tranchée\" and \"Rayon traiteur "
            "traditionnel : andouille de Guémené tranchée ou en morceaux\", "
            "\"Date limite de consommation entre le 17/11/2026 et le "
            "24/11/2026\" (libre service) and \"entre le 07/11/2026 et le "
            "24/11/2026\" (traditionnel), Conditionnements \"En rayon "
            "traditionnel : dans feuille de papier siglée E. Leclerc En "
            "rayon libre service : dans barquette en plastique "
            "transparent\", France entière, \"Du 07/11/2026 au 23/11/2026\", "
            "Distributeurs \"E. Leclerc Hennebont\", Motif \"Suspicion de "
            "présence de Listéria\", Risques \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", Nature juridique \"Volontaire "
            "(sans arrêté préfectoral)\". \"Andouille de Guémené\" is the "
            "product's own name and is kept as written. [original product: "
            "andouille de guémené tranchée ou en morceaux, vendue au rayon "
            "traiteur traditionnel et en rayon libre service] [original "
            "Reason (fr): \"Suspicion de présence de Listéria\"]]"),
    },
    # ── fiche 24175 (row 4) ───────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24175/interne",
        "fields": {
            "Product": (
                "pizza la léontine; bruschetta raymond; bol breton; "
                "croustillant andouille camembert *2; tarte tatin oignons "
                "confits *2; eventail gourmand (plateau de charcuterie)",
                "Pizza La Léontine; Bruschetta Raymond; Bol Breton; "
                "andouille and Camembert croustillant x2; confit onion tarte "
                "Tatin x2; Eventail Gourmand (charcuterie platter); "
                "Salade Rustique"),
            "Reason": (
                "Suspicion de présence de listéria",
                "Suspected presence of Listeria"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: Product and Reason were both "
            "French and the row was stamped for both. Read on fiche 24175 "
            "today (Fiche réf. 2026-11-0181 • Version 1, date 25/11/2026): "
            "H1 \"Produits élaborés à base d'andouille de Guémené\", "
            "\"Origine de la fiche: SA HENDIS CENTRE E. LECLERC HENNEBONT\", "
            "\"Nom de la marque du produit: E. Leclerc Hennebont\", "
            "Catégorie Alimentation / Plats préparés et snacks, \"Noms des "
            "modèles ou références: Pizza La Léontine Bruschetta Raymond Bol "
            "breton Croustillant andouille camembert *2 Tarte tatin oignons "
            "confits *2 Eventail gourmand (plateau de charcuterie)\", seven "
            "GTINs — 2000001133163 (Pizza La Léontine), 2000001134641 "
            "(Bruschetta Raymond), 2000001132333 (Bol breton), 2000001141731 "
            "(Tarte tatin oignons confits *2), 2000001131916 (Eventail "
            "gourmand), 0208958000000 (Salade rustique), 2000001140338 "
            "(Croustillant andouille camembert *2) — \"Date limite de "
            "consommation entre le 07/11/2026 et le 24/11/2026\", "
            "Conditionnements \"Produits vendus en rayon libre service dans "
            "des barquettes en plastique transparent.\", France entière, "
            "\"Du 07/11/2026 au 23/11/2026\", Distributeurs \"E. Leclerc "
            "Hennebont\", Motif \"Suspicion de présence de Listéria\", "
            "Risques \"Listeria monocytogenes (agent responsable de la "
            "listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". \"Salade Rustique\" is ADDED to the Product "
            "because the fiche's own GTIN table carries it (0208958000000) "
            "while its références line does not — the row had captured the "
            "références line only. Product names are kept as written on the "
            "fiche. No lot, weight or date is added. [original product: "
            "pizza la léontine; bruschetta raymond; bol breton; croustillant "
            "andouille camembert *2; tarte tatin oignons confits *2; "
            "eventail gourmand (plateau de charcuterie)] [original Reason "
            "(fr): \"Suspicion de présence de Listéria\"]]"),
    },
    # ── fiche 24157 (row 11) — the unstamped French Product ───────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24157/interne",
        "fields": {
            "Product": (
                "choux chantilly x2 (recette locale) fabrication",
                "Chantilly cream puffs x2 (local recipe, made in store)"),
            "Reason": (
                "Suite à la présence de Listeria monocytogenes (agent "
                "responsable de la listériose)",
                "Presence of Listeria monocytogenes (the agent of "
                "listeriosis)"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: the published Product was wholly "
            "French and the row carried NO \"needs cleanup: Product\" stamp "
            "— only the Reason stamp. product_needs_english() returned False "
            "because every word in it defeated a different rule: "
            "\"chantilly\" and \"fabrication\" are English words too, "
            "\"locale\" is an English word in its own right, and \"choux\" "
            "and \"recette\" were in no list at all, so there was no French "
            "food word, no French structure word and nothing for the two-hit "
            "detector to count. Fixed in pipeline/_language.py (two words "
            "added, measured over all 2,522 published Products as firing on "
            "this row and nothing else) and pinned by "
            "tests/test_morning_fix_2026_11_26.py. Read on fiche 24157 "
            "today (Fiche réf. 2026-11-0164 • Version 1, date 24/11/2026): "
            "H1 \"CHOUX CHANTILLY X2\", \"Origine de la fiche: AUCHAN RETAIL "
            "SERVICES AUCHAN\", \"Nom de la marque du produit: AUCHAN\", "
            "Catégorie Alimentation / Céréales et produits de boulangerie, "
            "\"Noms des modèles ou références: CHOUX CHANTILLY X2 (recette "
            "locale) FABRICATION\", GTIN 2030000020942, Lot \"Tous les lots "
            "vendus en rayon Pâtisserie libre service du 13/11/2026 au "
            "15/11/2026 inclus\", Départements HAUTES-ALPES (05), \"Du "
            "13/11/2026 au 15/11/2026\", Distributeurs \"Uniquement "
            "l'HYPERMARCHÉ AUCHAN GAP\", Motif \"Suite à la présence de "
            "Listeria monocytogenes (agent responsable de la listériose)\", "
            "Risques \"Listeria monocytogenes (agent responsable de la "
            "listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\", \"Produit à conserver au réfrigérateur\". "
            "\"FABRICATION\" is the fiche's own word for in-store "
            "production, which is what \"made in store\" renders; no lot, "
            "weight or date is added. [original product: CHOUX CHANTILLY X2 "
            "(recette locale) FABRICATION] [original Reason (fr): \"Suite à "
            "la présence de Listeria monocytogenes (agent responsable de la "
            "listériose)\"]]"),
    },
    # ── fiche 24158 (row 12) ──────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24158/interne",
        "fields": {
            "Product": (
                "paupiette de boeuf (stand boucherie)",
                "Beef paupiette (sold at the butcher's counter)"),
            "Reason": (
                "Presence of Salmonella spp. (agent responsable de la "
                "salmonellose)",
                "Presence of Salmonella spp. (the agent of salmonellosis)"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: the Product was French and the "
            "Reason was half-translated — an English opening with the "
            "fiche's French parenthesis left on the end, which is why "
            "tests/test_reason_is_not_wholly_foreign.py caught it while the "
            "two-English-word escape nearly did not. Read on fiche 24158 "
            "today (Fiche réf. 2026-11-0165 • Version 1, date 24/11/2026): "
            "H1 \"PAUPIETTE DE BOEUF (STAND BOUCHERIE)\", \"Origine de la "
            "fiche: AUCHAN RETAIL SERVICES AUCHAN\", \"Nom de la marque du "
            "produit: AUCHAN\", Catégorie Alimentation / Viandes, \"Noms des "
            "modèles ou références: PAUPIETTE DE BOEUF (STAND BOUCHERIE)\", "
            "no GTIN on the page, Lot \"Tous les los vendus entre le "
            "17/11/2026 et 19/11/2026 inclus.\" (the typo \"los\" is the "
            "fiche's own), Départements HAUT-RHIN (68), \"Du 17/11/2026 au "
            "19/11/2026\", Distributeurs \"Uniquement le SUPERMARCHÉ ST "
            "AMARIN\", Motif \"Présence de Salmonella spp (agent "
            "responsable de la salmonellose)\", Risques \"Salmonella spp "
            "(agent responsable de la salmonellose)\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\", \"Produit à conserver "
            "au réfrigérateur\". \"Paupiette\" is the cut's own name and is "
            "kept. No lot, weight or date is added. [original product: "
            "PAUPIETTE DE BOEUF (STAND BOUCHERIE)] [original Reason (fr): "
            "\"Présence de Salmonella spp (agent responsable de la "
            "salmonellose)\"]]"),
    },
    # ── fiche 24159 (row 13) ──────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24159/interne",
        "fields": {
            "Company": ("Top Budget", "Fromageries Terres d'Or"),
            "Product": (
                "emmental râpé 200g",
                "Grated Emmental cheese, 200 g"),
            "Reason": (
                "Risque de corps étranger de type verre",
                "Risk of glass foreign bodies"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-26: Product and Reason were both "
            "French and Company was the BRAND. Read on fiche 24159 today "
            "(Fiche réf. 2026-11-0166 • Version 1, date 24/11/2026): H1 "
            "\"Emmental Râpé Top Budget 200g\", \"Origine de la fiche: "
            "FROMAGERIES TERRES D'OR FROMAGERIES TERRES D'OR\", \"Nom de la "
            "marque du produit: Top Budget\", Catégorie Alimentation / Lait "
            "et produits laitiers, \"Noms des modèles ou références: Emmental "
            "Râpé 200g\", GTIN 3410280141671, \"Code lot commençant par 11 "
            "323\", \"Date de durabilité minimale 29/01/2027\", "
            "Conditionnements \"Sachet de 200g\", Marque de salubrité \"FR "
            "71 047 001 CE\", France entière, \"Du 21/11/2026 au "
            "23/11/2026\", Distributeurs \"INTERMARCHE\", Motif \"Risque de "
            "corps étranger de type verre\", Risques \"Inertes (verre, "
            "métal, plastique, papier, textile…)\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\". Company is now the "
            "firm named in \"Origine de la fiche\": \"Top Budget\" is a "
            "private label, and this is the same correction the 2026-11-24 "
            "pass made on fiche 24152 (register convention, see also fiches "
            "24094 and 24111). Brand keeps Top Budget. THIS IS NOT A "
            "DUPLICATE of the already-published fiche 24152 row: same GTIN "
            "and lot, but a different DGCCRF notice with a different "
            "distributor (INTERMARCHE, against \"ETS DOLE - ETS DONZERE - "
            "…\" on 24152) and a different sale window, so it is the same "
            "recall extended to another retailer and gets its own row, as "
            "with the three red-potato fiches of lot 26128774. [original "
            "product: Emmental Râpé 200g] [original company: Top Budget "
            "(brand; the fiche's Origine de la fiche is FROMAGERIES TERRES "
            "D'OR)] [original Reason (fr): \"Risque de corps étranger de "
            "type verre\"]]"),
    },
    # ── fiche 24153 (row 14) ──────────────────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24153/interne",
        "fields": {
            "Product": (
                "fromage de brebis au piment d'espelette",
                "Sheep's-milk cheese with Espelette pepper"),
        },
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-26: the published Product was French "
            "and the row was stamped for it. Read on fiche 24153 today "
            "(Fiche réf. 2026-11-0160 • Version 1, date 24/11/2026): H1 "
            "\"FROMAGE DE BREBIS AU PIMENT D'ESPELETTE\", \"Origine de la "
            "fiche: KERHADIS E. LECLERC\", \"Nom de la marque du produit: "
            "Sans marque\", Catégorie Alimentation / Lait et produits "
            "laitiers, \"Noms des modèles ou références: FROMAGE DE BREBIS "
            "AU PIMENT D'ESPELETTE\", GTIN 0207204000000, Lot column \"vendu "
            "23/10/2026 au 19/11/2026\", \"Date de durabilité minimale "
            "16/07/2027\", Conditionnements \"Rayon traditionnel\", Marque "
            "de salubrité \"ES/1505448/AB CE\", Départements FINISTERE (29), "
            "\"Du 23/10/2026 au 19/11/2026\", Distributeurs \"Halles de "
            "Kerharo (LECLERC)\", Motif \"Présence de Listeria "
            "monocytogenes\", Risques \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", Nature juridique \"Volontaire "
            "(sans arrêté préfectoral)\". The Reason was already English and "
            "matches the Motif; only the Product is changed. Company and "
            "Brand are left alone: the fiche's marque is \"Sans marque\", so "
            "Company is the distributor and Brand is \"Unbranded\", which is "
            "this register's convention for a RappelConso \"sans marque\" "
            "row. \"Espelette\" is a protected name (AOP) and is kept as "
            "written. [original product: FROMAGE DE BREBIS AU PIMENT "
            "D'ESPELETTE]]"),
    },
    # ── Pending: the MPI (NZ) Tip Top row (finding 5) ─────────────────────
    {
        "sheet": "Pending",
        "url": "https://www.mpi.govt.nz/news/media-releases/tip-top-6-pack-"
               "jelly-tip-ice-creams-recalled-due-to-possible-presence-of-"
               "bacteria",
        "fields": {
            "Date": ("", "2026-10-02"),
            "Company": ("Tip Top 6-pack Jelly Tip ice-creams",
                        "Tip Top Ice Cream Company"),
            "Brand": ("Tip Top 6-pack Jelly Tip ice-creams", "Tip Top"),
            "Product": (
                "due to possible ...",
                "Jelly Tip ice-cream, 6-pack cardboard box (6 x 75 ml), "
                "best before 30-SEP-28 03:00 L MO6252 to 30-SEP-28 "
                "05:00 L MO6252"),
            "Reason": ("Bacillus cereus — outbreak",
                       "Possible presence of Bacillus cereus"),
        },
        "note": (
            "[morning fix pass 2026-11-26: the row was the right-hand side "
            "of a split headline — Company and Brand held the headline's "
            "subject and Product held the fragment \"due to possible ...\", "
            "a search snippet cut mid-title — with Date empty. It is the one "
            "test still red on main "
            "(tests/test_an_agency_headline_is_not_a_firm_and_a_food.py::"
            "test_no_live_row_carries_a_split_headline) and the publish gate "
            "was right to hold it. THE RECALL IS REAL and the register has "
            "already extracted it correctly once: Rejected row 1108 carries "
            "Date 2026-10-02, Company 'Tip Top Ice Cream Company Limited', "
            "Brand 'Tip Top', Product 'Jelly Tip 6 Pack' — and was archived "
            "by 'repromotion-guard', not on merit, because three mangled "
            "copies of the same notice (Rejected 1093, 1094, 1312, all "
            "rejected by the Confirmer for 'Date is empty') had already "
            "poisoned the URL. A good row refused because bad rows reached "
            "the archive first is reported to the operator as its own "
            "defect; this edit does not touch the guard. Read on the MPI "
            "media release today: headline \"Tip Top 6-pack Jelly Tip "
            "ice-creams recalled due to possible presence of bacteria\", "
            "\"Date: 02 October 2026\", recalled by \"the Tip Top Ice Cream "
            "Company\", \"6-pack cardboard box (6 x 75ml)\", best before "
            "\"30-SEP-28 03:00 L MO6252 to 30-SEP-28 05:00 L MO6252\", "
            "hazard \"possible presence of Bacillus cereus\" (\"the bacteria "
            "Bacillus cereus\", which \"can cause severe gastrointestinal "
            "illness\"), \"sold at Woolworths supermarkets in the North "
            "Island\", products \"have been removed from store shelves and "
            "have not been exported\", and \"New Zealand Food Safety has not "
            "received any notifications of associated illness\". Company is "
            "\"Tip Top Ice Cream Company\" as the page prints it — NOT the "
            "\"Limited\" the archived copy carries, because that word is not "
            "on the page this pass read and no fact is added. The page "
            "labels MO6252 as part of the best-before text and does not give "
            "it as a batch number, so it is recorded that way. No illness is "
            "claimed and Outbreak stays 0. [original product: due to "
            "possible ...] [original company: Tip Top 6-pack Jelly Tip "
            "ice-creams (the headline's subject)]]"),
    },
    # ── Pending: the CFIA Yeshi row (finding 3 + finding 4) ───────────────
    {
        "sheet": "Pending",
        "url": "https://recalls-rappels.canada.ca/en/alert-recall/yeshi-"
               "brand-sesame-ginger-nutritional-yeast-dressing-dip-recalled-"
               "due-pieces-glass",
        "fields": {
            "Company": ("", "Apex Foodsource Ltd."),
            "Brand": ("", "Yeshi"),
            "Product": (
                "Yeshi brand Sesame Ginger Nutritional Yeast Dressing & Dip "
                "recalled due to pieces of glass",
                "Sesame Ginger Nutritional Yeast Dressing & Dip, 350 ml "
                "(UPC 6 27843 80892 3, lot 260615 402016, best before "
                "2028 JN 14)"),
            "Reason": (
                "Yeshi brand Sesame Ginger Nutritional Yeast Dressing & Dip "
                "recalled due to pieces of glass",
                "Pieces of glass (CFIA issue category: Food — Extraneous "
                "Material)"),
            # Class is NOT touched: the row already reads 'Recall', which
            # is the commonest Class on a CFIA row in this register (25
            # rows) and is not wrong. The notice's own "Class 2" is
            # recorded in the Notes below instead of overwriting a value
            # that is already valid.
            "Status": ("rejected", "pending"),
        },
        "note": (
            "[morning fix pass 2026-11-26: this row was BLOCKED TWICE OVER "
            "and is a real recall. (a) Reviewer 1 rejected it with \"URL "
            "agent: Hazard is a foreign body (glass)\" — Weekly_Rejected "
            "carries that copy — and foreign material is IN SCOPE for this "
            "register, which publishes 20 rows of 'Foreign material (glass)' "
            "including fiche 24087, the very fiche rejected on the same "
            "reason. pipeline/_url_guard.reject_refusal now refuses that "
            "class of rejection; see tests/test_morning_fix_2026_11_26.py. "
            "(b) It then sat in Pending carrying Status 'rejected', which is "
            "neither state, while the publish gate held it on an empty "
            "Company — and the Company is printed on its own notice. Read on "
            "the CFIA page today: headline \"Yeshi brand Sesame Ginger "
            "Nutritional Yeast Dressing & Dip recalled due to pieces of "
            "glass\", \"Recall date 2026-11-20\", \"Original published date: "
            "2026-11-24\", \"Last updated 2026-11-24\", \"Recalling firm: "
            "Apex Foodsource Ltd.\", brand \"Yeshi\", product \"Sesame "
            "Ginger Nutritional Yeast Dressing & Dip\", size \"350 ml\", UPC "
            "\"6 27843 80892 3\", codes \"LOT: 260615 402016\" and \"2028 JN "
            "14\", Issue \"Food - Extraneous Material\", \"Recall class: "
            "Class 2\", distribution \"British Columbia\" and \"Possibly "
            "other provinces and territories\", identification number "
            "RA-82994 (CFIA ID 17668), audience \"Retail\", category \"Food "
            "- Condiments\", \"Do not use, sell, serve or distribute the "
            "affected product.\" The page reports no illnesses and none is "
            "claimed. Company, Brand, Product, Reason and Class are all read "
            "from that page; Status goes to 'pending' so the normal gate and "
            "the normal reviewers decide, which is the only thing this edit "
            "asks for.]"),
    },
]

#: Reasons translated wherever they appear, keyed on the exact French.
REASON_EN = {
    "Suspicion de Listéria": "Suspected presence of Listeria",
    "Suspicion de présence de listéria": "Suspected presence of Listeria",
    "Suspicion de listéria": "Suspected presence of Listeria",
}

#: Sheets whose Reason must be English.
REASON_SHEETS = ("Recalls", "Pending", "Weekly_Review")

#: Sources whose Reason is a Commission notification subject, kept verbatim.
VERBATIM_SOURCES = ("RASFF (EU)",)


def _norm(v) -> str:
    return " ".join(str(v or "").split()).strip().lower()


def _append_note(row_notes, note: str) -> str:
    """Append `note` once, keyed on its own first 60 characters.

    The script has to be a no-op the second time it runs, because
    tools/rebase_and_verify.py re-runs it.
    """
    notes = str(row_notes or "")
    if notes.lower() == "nan":
        notes = ""
    if note[:60] in notes:
        return notes
    return (notes.strip() + " " + note).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(XLSX))
    # IT WRITES BY DEFAULT, AND THAT IS NOT A STYLE CHOICE.
    # tools/rebase_and_verify.py re-runs every tools/repair_*.py it finds in
    # the code zip with NO ARGUMENTS. A script that needed --apply would be a
    # silent no-op inside the gate, and the gate would zip main's unrepaired
    # workbook with a green suite and nothing in the report to say so.
    ap.add_argument("--dry-run", action="store_true",
                    help="report only, write nothing")
    ap.add_argument("--apply", action="store_true",
                    help="accepted and ignored — writing is the default")
    args = ap.parse_args()

    import openpyxl

    # ── 0. the three operator-found rows, through the real code path ──────
    #
    # tools/add_manual_row.py hands them to merge_master.append_to_pending,
    # so they get the same dedup, the same validation and the same review
    # stages as a scraped row. Called from HERE rather than by hand because
    # rebase_and_verify re-runs this script and nothing else.
    if not args.dry_run:
        import subprocess
        rows_json = ROOT / "tools" / "manual_rows_2026_11_26.json"
        if rows_json.exists():
            r = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
                 str(rows_json)],
                cwd=str(ROOT), capture_output=True, text=True)
            tail = [ln for ln in r.stdout.splitlines() if ln.strip()][-4:]
            print("add_manual_row: " + " | ".join(tail))
            if r.returncode != 0:
                print("add_manual_row FAILED:\n" + r.stdout[-3000:]
                      + "\n" + r.stderr[-2000:])
                return 1

    from pipeline import merge_master as mm

    path = Path(args.xlsx)
    # openpyxl in place, cell by cell, NOT a pandas round-trip: re-serialising
    # every sheet through pandas rewrites the Date column as timestamps and
    # drops the archive sheets' own types.
    wb = openpyxl.load_workbook(path)
    changed = 0
    report: list[str] = []

    def _sheet(name):
        if name not in wb.sheetnames:
            return None, {}
        ws = wb[name]
        hdr = {str(c.value): c.column for c in ws[1] if c.value is not None}
        return ws, hdr

    def _get(ws, hdr, row, col):
        if col not in hdr:
            return None
        v = ws.cell(row=row, column=hdr[col]).value
        return "" if v is None or str(v).lower() == "nan" else str(v)

    def _set(ws, hdr, row, col, value):
        ws.cell(row=row, column=hdr[col]).value = value

    # ── per-row edits ─────────────────────────────────────────────────────
    for spec in EDITS:
        name = spec["sheet"]
        ws, hdr = _sheet(name)
        if ws is None:
            report.append(f"  MISS  sheet {name} absent")
            continue
        target = mm._normalize_url_for_dedup(spec["url"])
        hits = [r for r in range(2, ws.max_row + 1)
                if mm._normalize_url_for_dedup(_get(ws, hdr, r, "URL") or "")
                == target]
        if not hits:
            report.append(f"  MISS  {name}: no row at {spec['url'][:70]}")
            continue
        for r in hits:
            touched = False
            for col, (was, now) in spec.get("fields", {}).items():
                if col not in hdr:
                    report.append(f"  MISS  {name} row {r}: no column {col}")
                    continue
                cur = _get(ws, hdr, r, col) or ""
                if _norm(cur) == _norm(now):
                    continue                       # already right; no-op
                if _norm(cur) != _norm(was):
                    report.append(
                        f"  SKIP  {name} row {r}.{col}: on main it reads "
                        f"{cur[:60]!r}, expected {was[:60]!r}")
                    continue
                _set(ws, hdr, r, col, now)
                touched = True
                report.append(f"  SET   {name} row {r}.{col} -> {now[:70]!r}")
            if "Notes" not in hdr:
                if touched:
                    changed += 1
                continue
            notes = _get(ws, hdr, r, "Notes") or ""
            new_notes = notes
            for stamp in spec.get("drop_stamps", ()):
                if stamp in new_notes:
                    new_notes = " ".join(new_notes.replace(stamp, "").split())
                    touched = True
                    report.append(f"  DROP  {name} row {r} stamp {stamp}")
            if spec.get("note"):
                out = _append_note(new_notes, spec["note"])
                if out != new_notes:
                    new_notes = out
                    touched = True
            if new_notes != notes:
                _set(ws, hdr, r, "Notes", new_notes)
            if touched:
                changed += 1

    # ── the same Reasons wherever else they appear ────────────────────────
    for name in REASON_SHEETS:
        ws, hdr = _sheet(name)
        if ws is None or "Reason" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            if (_get(ws, hdr, r, "Source") or "") in VERBATIM_SOURCES:
                continue
            cur = _get(ws, hdr, r, "Reason") or ""
            if not cur:
                continue
            en = REASON_EN.get(" ".join(cur.split()))
            if en is None or en == cur:
                continue
            _set(ws, hdr, r, "Reason", en)
            if "Notes" in hdr:
                _set(ws, hdr, r, "Notes", _append_note(
                    _get(ws, hdr, r, "Notes") or "",
                    "[morning fix pass 2026-11-26: Reason translated; the "
                    "regulator's own wording is kept here. "
                    f"[original Reason (fr): \"{cur}\"]]"))
            changed += 1
            report.append(f"  REASON {name} row {r} -> {en[:70]!r}")

    # ── stale cleanup stamps clear themselves ─────────────────────────────
    #
    # A "[needs cleanup: …]" stamp is a to-do, and
    # tests/test_product_and_firm_names_are_english.py excuses a stamped row
    # for one day — so a stamp nobody clears is a standing excuse. Step 0
    # calls add_manual_row, whose writer (merge_master._write_sheet)
    # re-stamps every row it writes whose field fails the predicate, and it
    # runs BEFORE the translations above.
    #
    # The stamps are swept against the SAME predicates the gates and the
    # tests use, so nothing is hidden: a field that still needs work keeps
    # its stamp, because the predicate still fires on it.
    from pipeline._language import (has_non_latin_script,
                                    product_needs_english,
                                    reason_needs_english)

    STAMPS = (
        ("[needs cleanup: Product not in English]",
         lambda row: not product_needs_english(row.get("Product"))),
        ("[needs cleanup: Company/Brand not in English]",
         lambda row: not (has_non_latin_script(row.get("Company"))
                          or has_non_latin_script(row.get("Brand")))),
        ("[needs cleanup: Reason not in English]",
         lambda row: not reason_needs_english(row.get("Reason"))),
    )
    swept = 0
    for name in ("Recalls", "Pending", "Weekly_Review"):
        ws, hdr = _sheet(name)
        if ws is None or "Notes" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            notes = _get(ws, hdr, r, "Notes") or ""
            if "[needs cleanup:" not in notes:
                continue
            row = {c: (_get(ws, hdr, r, c) or "")
                   for c in ("Product", "Company", "Brand", "Reason")}
            out = notes
            dropped = []
            for stamp, is_clean in STAMPS:
                if stamp in out and is_clean(row):
                    out = " ".join(out.replace(stamp, "").split())
                    dropped.append(stamp)
            long_stamp = ("[needs cleanup: Reason not in English and no "
                          "verified translation]")
            if long_stamp in out and not reason_needs_english(row["Reason"]):
                out = " ".join(out.replace(long_stamp, "").split())
                dropped.append(long_stamp)
            if dropped:
                out = _append_note(
                    out,
                    "[morning fix pass 2026-11-26: dropped "
                    f"{len(dropped)} stale cleanup stamp(s) — the field(s) "
                    "they name read in English under the same predicate the "
                    "publish gates and the language tests use. A stamp over "
                    "an English field is a standing excuse in "
                    "tests/test_product_and_firm_names_are_english.py, "
                    "which excuses a stamped row for one day.]")
                _set(ws, hdr, r, "Notes", out)
                swept += 1
                changed += 1
    if swept:
        report.append(f"  SWEEP {swept} row(s) lost a stale cleanup stamp")

    print(f"repair_2026_11_26: {changed} row edit(s)")
    for line in report:
        print(line)

    if args.dry_run:
        print("\n(--dry-run: nothing written)")
        return 0

    if changed:
        wb.save(path)
        print(f"wrote {path}")
        # recalls.json MUST mirror the Recalls sheet; mirror_json_from_xlsx
        # is the only sanctioned way to produce it. Without this the
        # dashboard and the language tests read the pre-repair values.
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"  recalls.json re-mirrored: "
              f"{mirror_json_from_xlsx(path, JSON_PATH)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
