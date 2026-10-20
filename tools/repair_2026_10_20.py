# -*- coding: utf-8 -*-
"""Morning fix 2026-10-20. ONE ROW IS REMOVED from Recalls — the commit
message carries "remove-rows".

A. TWELVE ROWS PUBLISHED IN FRENCH ON 2026-10-19.

   The whole RappelConso batch of 2026-10-19 — fiches 23820, 23827, 23842,
   23850, 23851, 23852, 23859, 23862, 23863, 23864, 23865, 23866 — went to
   the dashboard and to subscribers with Product still in French, and four of
   them with Reason in French too. Only FOUR carried
   "[needs cleanup: Product not in English]", so the Morning Fix, which reads
   what is stamped, would have seen a third of the batch.

   Why the other eight were invisible is in pipeline/_language.py in this
   same upload: _FRENCH_FOOD_WORDS held "haché" and "hachée" and not
   "hachés", and "steaks hachés 12% 2x125g charolais" contains no French
   FUNCTION word for the two-hit detector to find — it is a name, two numbers
   and a cattle breed. Three Tier-1 E. coli O157:H7 ground-beef recalls went
   out on that one letter of morphology.

   EVERY FICHE BELOW WAS READ ON 2026-10-20 and each row's own values are
   quoted in its note: the H1 ("Titre"), "Nom de la marque du produit",
   "Noms des modèles ou références", the GTIN/lot/DLC table, "Motif du
   rappel", "Risques encourus par le consommateur" and "Origine de la fiche".
   Nothing is inferred. The French is kept in Notes as [original product: …]
   so the row still matches the fiche, and the dashboard shows it on hover.

   THREE OF THE TWELVE HAD NO PRODUCT NAME AT ALL, only RappelConso's
   "Modèles ou références" field, which on those fiches holds carton codes:

       fiche 23842  Product '484565'           H1 "POULET MARINEE THYM CETRON"
       fiche 23850  Product 'mr10ae, mr41ck'   H1 "Crevette entière cuite
                                                   réfrigérée penaeus monodon"
       fiche 23862  (name present, French)

   and a fourth, published on 2026-06-26 and never caught:

       fiche 22635  Product 'x6 330g'          H1 "MERGUEZ BOEUF ET MOUTON"

   The gate half of that is _product_is_an_identifier_blockers_2026_10_20()
   in pipeline/_publish_gate.py, with
   tests/test_a_product_name_is_not_a_reference_code.py.

   THREE COMPANY FIELDS held the brand or the product where the fiche names
   a firm under "Origine de la fiche". Each replacement is that field,
   verbatim; the brand stays in Brand, where the fiche puts it:

       fiche 23842  Company 'Filet de Poulet'  -> 'Albi Boucherie'
                    ('Filet de Poulet' is the fiche's own Marque and stays
                     in Brand — the butcher entered the cut as the brand)
       fiche 23852  Company 'Sn Maison Vivant' -> 'Société Nouvelle Maison
                    Vivant'
       fiche 23859  Company 'Unbranded'        -> 'Corico'
                    (Marque is "NEUTRE", so Brand stays 'Unbranded')
       fiche 23820  Company 'GAEC Guinot'      -> 'Prestaviandes'
                    (Marque is "GAEC GUINOT" and stays in Brand)

B. "TUNA STEAK" IS NOT WHAT EUROSPIN RECALLED.

   Recalls row 66 (Ministero della Salute, 2026-10-13, Listeria
   monocytogenes, Tier 1, Eurospin Italia Spa / Portami Via) publishes
   Product "tuna steak". The Italian product is VITELLO TONNATO — veal in
   tuna sauce. It is named in the row's own URL, which is the ministry's own
   recall form: "Modulo Richiamo Vitello tonnato gr. 180 Portamivia
   L260_26_1791815682.pdf", and the row's own Reason already carries the lot,
   "Recall ID L260/26". Nothing is fetched and nothing is invented: the
   product name, the weight and the lot are all in the row's own URL.

   This is worse than an untranslated name. A subscriber who reads "tuna
   steak" avoids tuna and eats the recalled veal dish.

   The same error is in the Pending row for the sibling notice — Product
   "Tuna-flavored beef, roasted turkey, roast turkey" on
   "Modulo richiamo _ 5005 tacchino al limone 170g portamivia _ lotti
   multipli corretto" — and is corrected from that URL in the same way.

C. ONE RECALL PUBLISHED TWICE ON ONE URL.

   Recalls carries TWO rows for the FSAI Danone cereulide recall of
   2026-01-23, on the identical URL, both DateAdded 2026-05-14:

       row 2075  Company 'Danone' · Product 'Infant formula and follow-on
                 formula (specific batches)'          LastUpdated 2026-10-19
       row 2077  Company 'Danone infant formula and follow-on formula'
                 Product '| Food Safety Authority of Ireland'
                                                      LastUpdated 2026-07-25

   Row 2075 is the copy the 2026-10-19 pass repaired against the FSAI page.
   Row 2077 is the same notice with the site-name suffix still in Product and
   the headline still in Company, and it is why
   tests/test_publish_gate.py::TestWorkbookStaysClean is RED on main and why
   pipeline.verify_urls reports a duplicate URL. January is counted twice.

   Row 2077 is removed and archived to Rejected as a DUPLICATE, naming row
   2075 as the copy that wins. It is not deleted; archives are append-only.

   WHAT IS NOT ESTABLISHED, and it is left in the note rather than guessed:
   whether 2077 predates 2026-10-19 (so yesterday's URL-keyed fix should have
   repaired both copies and the loop only reached one) or was re-inserted
   afterwards (a dedup defect, since the two URLs are byte-identical). The
   register alone cannot say. Either way one URL carried two published rows.

D. ONE ARCHIVE ROW CONTRADICTING THE REGISTER IN SILENCE.

   Weekly_Rejected holds a row on the SAME vitello tonnato PDF URL as
   published row 66, rejected by gap_finder/it/rules.py as "unknown: No
   matching hazard category — defer to manual review", with nothing said
   about which copy wins — the RED half of
   tests/test_a_recall_is_not_both_published_and_rejected.py. The verdict is
   not in doubt: row 66 is published, Tier 1, with the ministry's own form as
   its URL. The archive row is marked SUPERSEDED and says so.

E. ONE PENDING ROW ENRICHED FROM ITS OWN FICHE.

   Fiche 23849 (Crusta C, whole cooked chilled shrimp, Listeria
   monocytogenes, 19/10/2026) has sat in Pending since 2026-10-19 blocked on
   "Company is empty", with Product holding twenty-two carton codes
   ("cr31ad, cr30ac, …"). Read on the fiche 2026-10-20: "Origine de la
   fiche: CRUSTA C", Marque "Sans marque", H1 "Crevette entière cuite
   réfrigérée", Motif "Présence de listeria monocytogenes", distributed to
   wholesalers (Méricq, Pomona, Provence Marée, CBM Homards, Scapp) and to
   Auchan and Carrefour, "France entière".

   This is a THIRD Crusta C notice, not a copy of the two already published
   (fiches 23785 and 23786, 2026-10-16, 20/30 and 30/40 count): its own
   fiche, its own reference list, its own date. If the near-duplicate rule
   refuses it against fiche 23850 — Auchan's retail recall of the same
   shrimp, published the same day — that refusal is correct and is reported
   rather than worked around.

F. THREE PENDING ROWS ARCHIVED. None of them can ever be published, and two
   of them hold a RED test open.

   * NAFDAC public alert 009/2023 — Intesa AFTERSHAVE, a cosmetic, recalled
     for Butyphenyl Methylpropional over the limit. Not food (R1), and the
     alert number in its own URL is from 2023 against a 2026 Date. Its own
     Notes already carry reviewer 2's rejection ("Pathogen not in approved
     vocabulary") and its Status is already 'rejected' — it was never moved.
   * NAFDAC public alert 021/2023 — counterfeit Dostinex (cabergoline)
     0.5 mg tablets, a MEDICINE. Not food (R1); 2023 number, 2026 Date.
     These two are the whole of
     tests/test_alert_number_year_matches_date.py's RED set, and they are the
     continuation of the four archived on 2026-10-19.
   * USDA FSIS 026-2026, Kayem Foods — a DUPLICATE of published row 13, which
     carries FSIS's own per-recall URL
     (fsis.usda.gov/recalls-alerts/kayem-foods-inc--recalls-meat-and-poultry-
     products-due-possible-temperature-abuse). The Pending copy's URL is a
     SEARCH QUERY, '?search=026-2026' (R3), which the publish gate already
     refuses and which holds
     tests/test_register_urls_pass_their_own_gate.py[us] RED — the test asks
     for the regex to be widened OR for the row to be established as one that
     should not be published. It is the latter: the recall is already in the
     register on the right URL.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-20"
OP = "[morning fix 2026-10-20]"
READ = "read on the fiche 2026-10-20"

CLEANUP_STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
)

# ── A — the RappelConso batch of 2026-10-19, plus one from June ────────────
# Keyed on the fiche number, which is in every row's own URL.
FICHES = {
    "fiche-rappel/23862": {
        "Product": ("Sliced parsleyed ham (jambon persillé tranché), EARL Un "
                    "Groin de Paradis — lot 4026, use-by 18/10/2026"),
        "Reason": ("Presence of Listeria. Consumer risk: Listeria "
                   "monocytogenes, the agent of listeriosis."),
        "_orig": {"product": "jambon persillé tranché earl un groin de paradis",
                  "reason": "Présence Listeria"},
        "_note": ("Fiche 23862 " + READ + ": Titre 'Jambon persillé tranché', "
                  "Marque 'Un groin de paradis', Modèles ou références "
                  "'Jambon persillé tranché EARL Un groin de paradis', lot "
                  "'4026', DLC '18/10/2026' (no GTIN on the fiche), Motif "
                  "'Présence Listeria', Risques 'Listeria monocytogenes "
                  "(agent responsable de la listériose)'. The firm is named "
                  "only as 'EARL Un groin de paradis' / 'UN GROIN DE PARADIS', "
                  "so Company and Brand are left as published."),
    },
    "fiche-rappel/23850": {
        "Product": ("Whole cooked chilled shrimp, Penaeus monodon (crevette "
                    "entière cuite réfrigérée penaeus monodon) — references "
                    "MR10AE and MR41CK; GTIN 3760082430320, lot 9262800100, "
                    "use-by 19/10/2026; GTIN 3574300034128, lot 9262800101, "
                    "use-by 16/10/2026"),
        "_orig": {"product": "mr10ae, mr41ck"},
        "_note": ("Fiche 23850 " + READ + ": Titre (H1) 'Crevette entière "
                  "cuite réfrigérée penaeus monodon', Marque 'Auchan le "
                  "poissonnier', Modèles ou références 'MR10AE, MR41CK' — "
                  "which is all the collector kept, so the row named no food "
                  "at all. GTIN 3760082430320 lot 9262800100 DLC 19/10/2026; "
                  "GTIN 3574300034128 lot 9262800101 DLC 16/10/2026; Motif "
                  "'Présence de listeria monocytogenes'; Distributeurs "
                  "'auchan'. The fiche gives no manufacturer, so Company "
                  "stays as published."),
    },
    "fiche-rappel/23866": {
        "Product": ("Beef burger patties, 5% fat, Charolais (steaks hachés 5% "
                    "charolais) — 2 x 125 g, GTIN 3700502700213, lot "
                    "26285-8749; 4 x 125 g, GTIN 3700502700220, lot "
                    "26285-8820; use-by 20/10/2026"),
        "Reason": "Detection of E. coli O157:H7",
        "_orig": {"product": ("steaks hachés 5% 2x125g charolais; steaks "
                              "hachés 5% 4x125g charolais")},
        "_note": ("Fiche 23866 (réf. 2026-10-0194) " + READ + ": Marque 'Les "
                  "Ateliers', Modèles ou références 'Steaks hachés 5% 2x125g "
                  "charolais' and 'Steaks hachés 5% 4x125g charolais', GTIN "
                  "3700502700213 lot 26285-8749 and GTIN 3700502700220 lot "
                  "26285-8820, both 'Date limite de consommation 20/10/2026', "
                  "Motif 'Détection E.coli O157H7', Risques 'Escherichia coli "
                  "shiga toxinogène (STEC)', Distributeurs 'ALPHAPRIM-"
                  "CARNIVOR-SUP VIANDES-NIKAIADIS', commercialised 12/10/2026 "
                  "to 20/10/2026. 'Charolais' is the cattle breed and is kept."),
    },
    "fiche-rappel/23865": {
        "Product": ("Beef burger patties, 12% fat, Charolais (steaks hachés "
                    "12% charolais) — 2 x 125 g, 4 x 125 g and 4 x 150 g; "
                    "GTIN 3700502701029, 3700502700244 and 3700502700251; "
                    "lots 22285-8812, 22285-8766 and 22285-8815; use-by "
                    "20/10/2026"),
        "Reason": "Detection of E. coli O157:H7",
        "_orig": {"product": ("steaks hachés 12% 2x125g charolais; steaks "
                              "hachés 12% 4x125g charolais; steaks hachés 12% "
                              "4x150g charolais")},
        "_note": ("Fiche 23865 " + READ + ": Marque 'Les Ateliers', Modèles ou "
                  "références 'Steaks hachés 12% 2x125g charolais', 'Steaks "
                  "hachés 12% 4x125g charolais', 'Steaks hachés 12% 4x150g "
                  "charolais'; GTIN 3700502701029, 3700502700244 and "
                  "3700502700251; lots 22285-8812, 22285-8766 and 22285-8815; "
                  "'Date limite de consommation 20/10/2026'; Motif 'Détection "
                  "E. coli O157H7'; Risques 'Escherichia coli shiga "
                  "toxinogène (STEC)'; Distributeurs 'SUP VIANDES-TSA "
                  "VIANDES-ALPHAPRIM-CARNIVOR-'."),
    },
    "fiche-rappel/23864": {
        "Product": ("Beef burger patties, 20% fat (steaks hachés 20%) — 6 x "
                    "100 g, GTIN 3700502701968, lot 26285-8772; 6 x 125 g, "
                    "GTIN 3700502710472, lot 26285-8740; use-by 20/10/2026"),
        "Reason": "Detection of E. coli O157:H7",
        "_orig": {"product": ("steaks hachés 20% 6*100g; steaks hachés 20% "
                              "6*125g")},
        "_note": ("Fiche 23864 " + READ + ": Marque 'Les Ateliers', Modèles ou "
                  "références 'steaks hachés 20% 6*100g' and 'steaks hachés "
                  "20% 6*125g'; GTIN 3700502701968 lot 26285-8772 and GTIN "
                  "3700502710472 lot 26285-8740, both 'Date limite de "
                  "consommation 20/10/2026'; Motif 'Détection E.coli O157H7'; "
                  "Risques 'Escherichia coli shiga toxinogène (STEC)'; "
                  "Distributeurs 'FRESH-CARNIVOR-JOUR DE MARCHE-SUP "
                  "VAINDES-OLIFRE'."),
    },
    "fiche-rappel/23863": {
        "Product": ("Minced beef, 20% fat (viande hachée 20%) — 350 g and "
                    "650 g; GTIN 3700502701074, lot 28285-8821; use-by "
                    "20/10/2026"),
        "Reason": "Detection of E. coli O157:H7",
        "_orig": {"product": ("viande hachée 20% 350g; viande hachée 20% "
                              "650g")},
        "_note": ("Fiche 23863 " + READ + ": Marque 'Les Ateliers', Modèles ou "
                  "références 'Viande hachée 20% 350g' and 'Viande hachée 20% "
                  "650g'; the identification table shows GTIN 3700502701074, "
                  "lot 28285-8821, 'Date limite de consommation 20/10/2026' "
                  "(the fiche lists further GTIN/lot pairs in the same table "
                  "which the page did not render in full — only what was read "
                  "is recorded); Motif 'détection E. coli O157H7'; Risques "
                  "'Escherichia coli shiga toxinogène (STEC)'; Distributeurs "
                  "'ALPHAPRIM-CARNIVAR-OLIFRE-SUP VIANDES-JOUR DE "
                  "MARCHE-NIKAIADIS-FRESH'."),
    },
    "fiche-rappel/23851": {
        "Product": ("Beef tartare \"Brasserie\", 5% fat, Charolais (tartare "
                    "brasserie 5% charolais) — 2 x 180 g, GTIN "
                    "3700502708417, lot 26285-8678, use-by 20/10/2026"),
        "Reason": "Detection of E. coli O157:H7",
        "_orig": {"product": "tartare brasserie 5% charolais 2x180g"},
        "_note": ("Fiche 23851 " + READ + ": Marque 'Les Ateliers', Modèle "
                  "'TARTARE BRASSERIE 5% CHAROLAIS 2X180G', GTIN "
                  "3700502708417, lot 26285-8678, 'Date limite de "
                  "consommation 20/10/2026', Motif 'Détection de E. coli "
                  "O157H7', Risques 'Escherichia coli shiga toxinogène "
                  "(STEC)', Distributeurs 'CARNIVOR-SUP VIANDES TSA "
                  "VIANDES-OLIFRE -NIKAIADIS'. Raw minced beef eaten raw — "
                  "the hazard class is unchanged."),
    },
    "fiche-rappel/23820": {
        "Company": "Prestaviandes",
        "Brand": "GAEC Guinot",
        "Product": ("Beef sausages, pack of 5 (saucisses de boeuf x 5) — "
                    "Basque-style beef sausages and herb beef sausages (sauc "
                    "basquaise boeuf x 5; sauc herbes boeuf x 5), lot "
                    "99090809, use-by 21/10/2026"),
        "Reason": ("Presence of Salmonella. Consumer risk: Salmonella spp., "
                   "the agent of salmonellosis."),
        "_orig": {"product": "sauc basquaise boeuf x 5; sauc herbes boeuf x 5",
                  "reason": "PRESENCE SALMONELLA",
                  "company": "GAEC Guinot"},
        "_note": ("Fiche 23820 " + READ + ": Titre (H1) 'SAUCISSES DE BOEUF X "
                  "5', Marque 'GAEC GUINOT', Modèles ou références 'SAUC "
                  "BASQUAISE BOEUF X 5' and 'SAUC HERBES BOEUF X 5', lot "
                  "'99090809', 'Date limite de consommation 21/10/2026' (no "
                  "GTIN), Motif 'PRESENCE SALMONELLA', Risques 'Salmonella spp "
                  "(agent responsable de la salmonellose)', Distributeurs "
                  "'AUCUN' (none). Company is set from 'Origine de la fiche: "
                  "PRESTAVIANDES' — the business that filed the notice; 'GAEC "
                  "GUINOT' is the fiche's Marque and stays in Brand."),
    },
    "fiche-rappel/23852": {
        "Company": "Société Nouvelle Maison Vivant",
        "Product": ("Cervelat sausage salad, 2 kg (salade de cervelas 2 kg) — "
                    "GTIN 3294070000672, lot 7082209, use-by between "
                    "07/10/2026 and 14/10/2026"),
        "Reason": ("Microbiological testing demonstrated the presence of "
                   "Listeria monocytogenes in this short-shelf-life (use-by "
                   "dated) product."),
        "_orig": {"product": "salade de cervelas 2kg",
                  "reason": ("Un contrôle microbiologique a mis en evidence, "
                             "dans ce produit à dlc, la présence de Listeria "
                             "monocytogenes"),
                  "company": "Sn Maison Vivant"},
        "_note": ("Fiche 23852 " + READ + ": Marque 'SN MAISON VIVANT', "
                  "Référence 'SALADE DE CERVELAS 2Kg', GTIN 3294070000672, "
                  "lot 7082209, 'Date limite de consommation entre le "
                  "07/10/2026 et le 14/10/2026', sous-catégorie 'Plats "
                  "préparés et snacks', Motif 'Un contrôle microbiologique a "
                  "mis en evidence, dans ce produit à DLC, la présence de "
                  "Listeria monocytogenes', Risques 'Listeria monocytogenes "
                  "(agent responsable de la listériose)', Distributeurs 'Voir "
                  "liste des clients concernés'. Company is set from 'Origine "
                  "de la fiche: SOCIETE NOUVELLE MAISON VIVANT'; the trading "
                  "name stays in Brand."),
    },
    "fiche-rappel/23827": {
        "Product": ("Farmhouse raw cow's milk Tomme cheese \"Le Salerac\" "
                    "(tomme de vache fermière au lait cru) — GTIN "
                    "377000044072, lot 046/26, best before between "
                    "28/08/2026 and 29/10/2026"),
        "Reason": ("Shiga toxin-producing Escherichia coli (STEC) "
                   "demonstrated in the product."),
        "_orig": {"product": "le salerac",
                  "reason": ("Mise en évidence de la présence d’E. coli "
                             "producteurs de shigatoxines (E. coli STEC) dans "
                             "le produit.")},
        "_note": ("Fiche 23827 " + READ + ": Titre (H1) 'Tomme de vache "
                  "fermière au lait cru', Marque 'Maison Marie SEVERAC', "
                  "Modèles ou références 'LE SALERAC' — which is all the "
                  "collector kept, a cheese's trade name with no food in it. "
                  "GTIN 377000044072, lot '046/26', 'Date de durabilité "
                  "minimale entre le 28/08/2026 et le 29/10/2026', "
                  "sous-catégorie 'Lait et produits laitiers', Motif 'Mise en "
                  "évidence de la présence d’Escherichia coli producteurs de "
                  "shigatoxines (E. coli STEC) dans le produit.', Risques "
                  "'Escherichia coli shiga toxinogène (STEC)', Distributeurs "
                  "'Cantal Direct' and 'Maison Marie SEVERAC', Origine de la "
                  "fiche 'MAISON MARIE SEVERAC' — Company already correct."),
    },
    "fiche-rappel/23842": {
        "Company": "Albi Boucherie",
        "Brand": "Filet de Poulet",
        "Product": ("Marinated chicken, thyme and lemon (poulet marinée thym "
                    "cetron) — reference 484565, lot \"POULET MARINE\", "
                    "use-by 10/10/2026"),
        "Reason": ("Salmonella found in a preparation of chicken marinated "
                   "with thyme and lemon."),
        "_orig": {"product": "484565",
                  "reason": ("SALMONELLE TROUVEE DANS UN PREPARATION DE POULET "
                             "MARINEE THYM CETRON"),
                  "company": "Filet de Poulet"},
        "_note": ("Fiche 23842 " + READ + ": Titre (H1) 'POULET MARINEE THYM "
                  "CETRON', Marque 'FILET DE POULET', Modèles ou références "
                  "'484565' — the bare reference number the row published as "
                  "its Product. Lot 'POULET MARINE', 'Date limite de "
                  "consommation 10/10/2026', Motif 'SALMONELLE TROUVEE DANS UN "
                  "PREPARATION DE POULET MARINEE THYM CETRON', Risques "
                  "'Risque microbiologique', Distributeurs 'CLIENT PRIVEE'. "
                  "Company is set from 'Origine de la fiche: ALBI BOUCHERIE'; "
                  "'FILET DE POULET' is the fiche's own Marque field — the "
                  "butcher entered the cut as the brand — and stays in Brand "
                  "rather than being invented away. 'CETRON' is the fiche's "
                  "spelling of 'citron' (lemon) and is kept in the original."),
    },
    "fiche-rappel/23859": {
        "Company": "Corico",
        "Product": ("Plain raw poultry sausages, 5 kg catering case, "
                    "modified-atmosphere packed (caissette saucisses de "
                    "volaille 5 kg s/ATM) — GTIN 3276440002219, lot "
                    "9776000285, use-by 26/10/2026"),
        "_orig": {"product": "caissette saucisses de volaille 5 kg s/atm",
                  "company": "Unbranded"},
        "_note": ("Fiche 23859 " + READ + ": Titre (H1) 'Saucisses de volaille "
                  "natures, crues', Marque 'NEUTRE', Modèles ou références "
                  "'CAISSETTE SAUCISSES DE VOLAILLE 5 KG S/ATM', GTIN "
                  "3276440002219, lot 9776000285, 'Date limite de "
                  "consommation 26/10/2026', Motif 'Présence de salmonelle', "
                  "Risques 'Salmonella spp (agent responsable de la "
                  "salmonellose)', Distributeurs 'RHD' (restauration hors "
                  "domicile — food service). Company is set from 'Origine de "
                  "la fiche: CORICO'; Marque is 'NEUTRE', so Brand stays "
                  "'Unbranded'. 's/ATM' is sous atmosphère modifiée."),
    },
    "fiche-rappel/22635": {
        "Product": ("Beef and mutton merguez sausages, pack of 6, 330 g "
                    "(merguez boeuf et mouton, X6 330G) — GTIN "
                    "3350030105587, lots 31335403, 31335653, 31336003, "
                    "31336285 and 31336621, use-by 29/06/2026 to 05/07/2026"),
        "_orig": {"product": "x6 330g"},
        "_note": ("Fiche 22635 " + READ + ": Titre (H1) 'MERGUEZ BOEUF ET "
                  "MOUTON', Marque 'MONOPRIX', Modèle 'X6 330G' — the pack "
                  "format, which is all the row published as its Product "
                  "since 2026-06-26. GTIN 3350030105587, lots 31335403, "
                  "31335653, 31336003, 31336285 and 31336621, 'Date limite de "
                  "consommation' 29/06/2026 to 05/07/2026, Motif 'Détection de "
                  "Salmonelle', Risques 'Salmonella spp (agent responsable de "
                  "la salmonellose)', Origine de la fiche 'MONOPRIX HOLDING'. "
                  "Found by the new identifier rule, not by the needs-cleanup "
                  "stamp — nothing had ever flagged it."),
    },
}

# ── B — vitello tonnato, from the row's own URL ────────────────────────────
VITELLO_URL = "modulo%20richiamo%20vitello%20tonnato%20gr.%20180%20portamivia"
VITELLO = {
    "Product": ("Vitello tonnato (veal in tuna sauce), 180 g, Portami Via — "
                "lot L260/26"),
}
VITELLO_NOTE = (
    " " + OP + " Product was 'tuna steak', which is not what was recalled: "
    "vitello tonnato is VEAL in a tuna-and-caper sauce. A subscriber who "
    "avoids tuna eats this dish. The product name, the weight and the lot are "
    "all in this row's own URL — the ministry's own recall form 'Modulo "
    "Richiamo Vitello tonnato gr. 180 Portamivia L260_26' — and the lot is "
    "already in this row's own Reason as 'Recall ID L260/26'. The brand "
    "'Portami Via' was already correct. Nothing fetched, nothing inferred.")

TACCHINO_URL = "5005%20tacchino%20al%20limone%20170g%20portamivia"
TACCHINO = {
    "Product": ("Tacchino al limone (turkey in lemon sauce), 170 g, Portami "
                "Via — multiple lots"),
}
TACCHINO_NOTE = (
    " " + OP + " Product was 'Tuna-flavored beef, roasted turkey, roast "
    "turkey' — the same vitello-tonnato mistranslation as published row 66, "
    "and it names neither the food nor the weight on this row's own URL. The "
    "ministry's own form in that URL is 'Modulo richiamo _ 5005 tacchino al "
    "limone 170g portamivia _ lotti multipli corretto': turkey in lemon "
    "sauce, 170 g, Portami Via, multiple lots. Read from the row's own URL; "
    "the PDF itself could not be opened from this sandbox (no search "
    "provenance), so only what the URL states is recorded.")

# ── C — the duplicate ──────────────────────────────────────────────────────
DANONE_URL = ("fsai.ie/news-and-alerts/latest-news/"
              "danone-infant-formula-and-follow-on-formula-recall")
DANONE_BAD_PRODUCT = "| food safety authority of ireland"
DANONE_DUP_REASON = (
    "DUPLICATE — this is a SECOND published row for the FSAI Danone cereulide "
    "recall of 2026-01-23 on the identical URL. The copy that wins is the one "
    "repaired against the FSAI page on 2026-10-19: Company 'Danone', Product "
    "'Infant formula and follow-on formula (specific batches)'. This copy "
    "still held the site-name suffix '| Food Safety Authority of Ireland' in "
    "Product and the notice headline in Company, which is why "
    "tests/test_publish_gate.py::TestWorkbookStaysClean was RED on main and "
    "why pipeline.verify_urls reported a duplicate URL; January was counted "
    "twice. Archived, not deleted. NOT ESTABLISHED: whether this row predates "
    "2026-10-19 (so the URL-keyed name fix should have repaired both copies "
    "and reached only one) or was re-inserted afterwards past a dedup key — "
    "the two URLs are byte-identical, so the register alone cannot say. "
    + OP)

# ── D — the silent archive row ─────────────────────────────────────────────
SUPERSEDED_REASON = (
    "SUPERSEDED — the published row on this URL is the correct one. "
    "gap_finder/it/rules.py filed this copy as 'unknown: No matching hazard "
    "category — defer to manual review', which is a refusal to classify and "
    "not a verdict on the recall. The register already publishes it: "
    "Ministero della Salute (IT), 2026-10-13, Eurospin Italia Spa / Portami "
    "Via, Listeria monocytogenes, Tier 1, on this same ministry recall form. "
    "The hazard is named on the row itself ('listeria'), so the vocabulary "
    "gap was in the gap-finder's category map, not in the notice. Kept as the "
    "audit trail of that gap. " + OP)

# ── E — the Crusta C fiche ─────────────────────────────────────────────────
CRUSTA_URL = "fiche-rappel/23849"
CRUSTA = {
    "Company": "Crusta C",
    "Brand": "Unbranded",
    "Product": ("Whole cooked chilled shrimp (crevette entière cuite "
                "réfrigérée) — references CR31AD, CR30AC, CR20AI, CR20CZ, "
                "CR20CL and seventeen further carton codes; lots per the "
                "annex to the fiche"),
    "Status": "pending",
}
CRUSTA_NOTE = (
    " " + OP + " Fiche 23849 " + READ + ": Titre (H1) 'Crevette entière cuite "
    "réfrigérée', Marque 'Sans marque', Modèles ou références the list "
    "beginning 'CR31AD, CR30AC, CR20AI, CR20CZ, CR20CL', Identification 'voir "
    "liste des produits en pièce jointe' (no GTIN or DLC on the fiche), Motif "
    "'Présence de listeria monocytogenes', Risques 'Listeria monocytogenes "
    "(agent responsable de la listériose)', Origine de la fiche 'CRUSTA C', "
    "Distributeurs 'GROSSISTE : Méricq, pomona, provence marée, CBM Homards, "
    "scapp' and 'GMS : Auchan, Carrefour', zone 'France entière'. Company was "
    "empty — the gate's only blocker — and Product held twenty-two carton "
    "codes and no food. A THIRD Crusta C notice, distinct from the two "
    "published on 2026-10-16 (fiches 23785 and 23786, 20/30 and 30/40 count): "
    "its own fiche, its own reference list, its own date. Marque is 'Sans "
    "marque', so Brand is 'Unbranded'.")

# ── F — three rows out of Pending ──────────────────────────────────────────
PENDING_OUT = {
    "public-alert-no-009-2023": (
        "not_food: NAFDAC public alert 009/2023 — Intesa AFTERSHAVE, a "
        "cosmetic, recalled for Butyphenyl Methylpropional (BMHCA) over the "
        "limit. Not food (R1), and the alert number in its own URL is from "
        "2023 against a 2026 Date. This row's own Notes already carry "
        "reviewer 2's refusal ('Pathogen not in approved vocabulary') and its "
        "Status was already 'rejected'; it was never moved out of the queue, "
        "so it kept tests/test_alert_number_year_matches_date.py RED. "
        "Continues the four archived on 2026-10-19. " + OP),
    "public-alert-no-021-2023": (
        "not_food: NAFDAC public alert 021/2023 — counterfeit Dostinex "
        "(cabergoline) 0.5 mg tablets, a MEDICINE, not food (R1). The alert "
        "number in its own URL is from 2023 against a 2026 Date. Continues "
        "the four archived on 2026-10-19. " + OP),
    "recalls-alerts?search=026-2026": (
        "duplicate + not a per-recall notice: USDA FSIS recall 026-2026, "
        "Kayem Foods Inc., is ALREADY PUBLISHED in Recalls on FSIS's own "
        "per-recall URL (fsis.usda.gov/recalls-alerts/kayem-foods-inc--"
        "recalls-meat-and-poultry-products-due-possible-temperature-abuse, "
        "Date 2026-10-17, operator ruling 2026-10-18). This copy's URL is a "
        "SEARCH QUERY, not the regulator's own per-recall page (R3), which "
        "the publish gate already refuses — so it could never be promoted, "
        "and it held tests/test_register_urls_pass_their_own_gate.py[us] RED "
        "by asking for the country's item regex to be widened to admit a "
        "search URL. Establishing the row as one that should not be published "
        "is the honest half of that test's own instruction. " + OP),
}


# ── helpers ────────────────────────────────────────────────────────────────

def _head(ws):
    return [c.value for c in ws[1]]


def _cols(ws):
    return {h: i for i, h in enumerate(_head(ws))}


def _archive(arch, row, reason):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append("operator review " + TODAY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(row.get(h, ""))
    arch.append(out)


def _strip_stamps(notes: str) -> str:
    for stamp in CLEANUP_STAMPS:
        notes = notes.replace(stamp, "")
    return " ".join(notes.split())


def _append_originals(notes: str, orig: dict) -> str:
    bits = []
    if orig.get("product"):
        bits.append(f'[original product: {orig["product"]}]')
    if orig.get("company"):
        bits.append(f'[original company: {orig["company"]}]')
    if orig.get("brand"):
        bits.append(f'[original brand: {orig["brand"]}]')
    if orig.get("reason"):
        bits.append(f'[original Reason (fr): "{orig["reason"]}"]')
    return (notes + " " + " ".join(bits)).strip() if bits else notes


# ── the fixes ──────────────────────────────────────────────────────────────

def translate_fiches(wb) -> int:
    """A — Product/Company/Reason read off each row's own fiche.

    Applied to every sheet that holds a copy of the row (Recalls and the
    Sunday review slice), keyed on the fiche number in the URL.
    """
    n = 0
    for sheet in ("Recalls", "Weekly_Review", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        col = _cols(ws)
        if "URL" not in col or "Product" not in col:
            continue
        for row in ws.iter_rows(min_row=2):
            url = str(row[col["URL"]].value or "").lower()
            key = next((k for k in FICHES if k in url), None)
            if key is None:
                continue
            fix = FICHES[key]
            changed = False
            for field, value in fix.items():
                if field.startswith("_") or field not in col:
                    continue
                if row[col[field]].value != value:
                    row[col[field]].value = value
                    changed = True
            notes = _strip_stamps(str(row[col["Notes"]].value or ""))
            if changed:
                notes = _append_originals(notes, fix.get("_orig", {}))
                notes = (notes + " " + OP + " " + fix["_note"]).strip()
            row[col["Notes"]].value = notes
            if "LastUpdated" in col and changed:
                row[col["LastUpdated"]].value = TODAY
            if changed:
                n += 1
                print(f"  {sheet}: {key} -> {str(fix['Product'])[:58]}")
    return n


def fix_vitello(wb) -> int:
    """B — veal is not tuna."""
    n = 0
    for sheet, url_key, fix, note in (
            ("Recalls", VITELLO_URL, VITELLO, VITELLO_NOTE),
            ("Weekly_Review", VITELLO_URL, VITELLO, VITELLO_NOTE),
            ("Pending", TACCHINO_URL, TACCHINO, TACCHINO_NOTE)):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        col = _cols(ws)
        for row in ws.iter_rows(min_row=2):
            if url_key not in str(row[col["URL"]].value or "").lower():
                continue
            old = str(row[col["Product"]].value or "")
            if old == fix["Product"]:
                continue
            row[col["Product"]].value = fix["Product"]
            row[col["Notes"]].value = (
                str(row[col["Notes"]].value or "")
                + f" [original product: {old}]" + note).strip()
            if "LastUpdated" in col:
                row[col["LastUpdated"]].value = TODAY
            n += 1
            print(f"  {sheet}: {old[:40]!r} -> {fix['Product'][:48]!r}")
    return n


def remove_danone_duplicate(wb) -> int:
    """C — one recall, one row."""
    ws, arch = wb["Recalls"], wb["Rejected"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    pcol = head.index("Product") + 1
    n = 0
    for r in range(ws.max_row, 1, -1):
        if DANONE_URL not in str(ws.cell(r, ucol).value or "").lower():
            continue
        if DANONE_BAD_PRODUCT not in str(ws.cell(r, pcol).value or "").lower():
            continue
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        _archive(arch, row, DANONE_DUP_REASON)
        ws.delete_rows(r, 1)
        n += 1
        print(f"  Recalls -> Rejected (duplicate): {row.get('Date')} "
              f"{row.get('Source')} {str(row.get('Product'))[:40]!r}")
    for sheet in ("Weekly_Review", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        wr = wb[sheet]
        col = _cols(wr)
        if "URL" not in col or "Product" not in col:
            continue
        for r in range(wr.max_row, 1, -1):
            if (DANONE_URL in str(wr.cell(r, col["URL"] + 1).value or "").lower()
                    and DANONE_BAD_PRODUCT in str(
                        wr.cell(r, col["Product"] + 1).value or "").lower()):
                wr.delete_rows(r, 1)
                print(f"  {sheet}: duplicate FSAI row deleted")
    return n


def annotate_vitello_archive(wb) -> int:
    """D — the archive must say which copy wins."""
    if "Weekly_Rejected" not in wb.sheetnames:
        return 0
    ws = wb["Weekly_Rejected"]
    col = _cols(ws)
    n = 0
    for row in ws.iter_rows(min_row=2):
        if VITELLO_URL not in str(row[col["URL"]].value or "").lower():
            continue
        current = str(row[col["RejectionReason"]].value or "")
        if "SUPERSEDED" in current:
            continue
        row[col["RejectionReason"]].value = (
            SUPERSEDED_REASON + " Original stamp, kept verbatim: " + current)
        n += 1
        print(f"  Weekly_Rejected SUPERSEDED: {row[col['Date']].value}")
    return n


def enrich_crusta(wb) -> int:
    """E — the fiche names the firm the gate was waiting for."""
    ws = wb["Pending"]
    col = _cols(ws)
    n = 0
    for row in ws.iter_rows(min_row=2):
        if CRUSTA_URL not in str(row[col["URL"]].value or "").lower():
            continue
        if str(row[col["Company"]].value or "") == CRUSTA["Company"]:
            continue
        old = str(row[col["Product"]].value or "")
        for field, value in CRUSTA.items():
            if field in col:
                row[col[field]].value = value
        row[col["Notes"]].value = (
            str(row[col["Notes"]].value or "")
            + f" [original product: {old}]" + CRUSTA_NOTE).strip()
        n += 1
        print("  Pending enriched: Crusta C whole cooked chilled shrimp "
              "(fiche 23849)")
    return n


def unify_crusta_spelling(wb) -> int:
    """One spelling per firm, decided by the pipeline's own rule.

    Enriching fiche 23849 with the firm named on it puts 'Crusta C' next to
    the four rows the register already spells 'CRUSTA C' (fiches 23785,
    23786, 22926, 22963), and
    tests/test_firm_names_are_uniform.py goes RED. The choice is not mine:
    pipeline._firm_names.choose() prefers the unshouted form, so 'Crusta C'
    is the canonical spelling and those four cells are the non-canonical
    ones. merge_master._write_sheet runs unify_firm_names() on every Recalls
    write and would converge them on the next merge; a repair script saving
    the workbook with openpyxl bypasses that choke point, so the same
    canonicaliser is called here rather than a spelling being hard-coded.
    Measured: 4 cells, all Company, all 'CRUSTA C' -> 'Crusta C'.
    """
    from pipeline._firm_names import FIELDS, unify_firm_names
    ws = wb["Recalls"]
    col = _cols(ws)
    head = _head(ws)
    # unify_firm_names() exempts RASFF by reading Source, so the row dicts
    # must carry it: an earlier draft of this function reimplemented
    # canonical_map() without Source and "unified" eight RASFF country names
    # ('Origin: IRELAND | Notifying: …' -> 'Ireland'), which is the RASFF
    # format this pass is forbidden to touch. Call the real one.
    rows = [{h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            for r in range(2, ws.max_row + 1)]
    before = [{f: str(r.get(f) or "") for f in FIELDS} for r in rows]
    # The row that creates the clash is still in Pending — the offline
    # promoter runs AFTER this script — so the Pending rows are shown to the
    # canonicaliser and only the Recalls rows are written back. Without this
    # the function sees one spelling, changes nothing, and the clash appears
    # after promotion with nothing left to fix it.
    pend = wb["Pending"]
    phead = _head(pend)
    queued = [{h: pend.cell(r, i + 1).value for i, h in enumerate(phead)}
              for r in range(2, pend.max_row + 1)]
    if not unify_firm_names(rows + queued):
        return 0
    n = 0
    seen = []
    for r, (old, new) in enumerate(zip(before, rows), start=2):
        for field in FIELDS:
            if field not in col:
                continue
            want = str(new.get(field) or "")
            if want != old[field]:
                ws.cell(r, col[field] + 1).value = want
                seen.append(f"{old[field]!r}->{want!r}")
                n += 1
    if n:
        print(f"  Recalls: {n} firm-name cell(s) unified "
              f"({', '.join(sorted(set(seen)))})")
    return n


def archive_pending(wb) -> int:
    """F — three rows that can never be published."""
    ws, arch = wb["Pending"], wb["Rejected"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    n = 0
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, ucol).value or "").lower()
        key = next((k for k in PENDING_OUT if k in url), None)
        if key is None:
            continue
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        _archive(arch, row, PENDING_OUT[key])
        ws.delete_rows(r, 1)
        n += 1
        print(f"  Pending -> Rejected: {row.get('Date')} {key}")
    return n


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    n_tr = translate_fiches(wb)
    n_vt = fix_vitello(wb)
    n_sp = annotate_vitello_archive(wb)
    n_cr = enrich_crusta(wb)
    n_rm = remove_danone_duplicate(wb)
    n_pa = archive_pending(wb)
    n_un = unify_crusta_spelling(wb)
    wb.save(XLSX)
    wb.close()
    print(f"fiche rows translated: {n_tr} · vitello/tacchino fixed: {n_vt} · "
          f"SUPERSEDED annotated: {n_sp} · Crusta C enriched: {n_cr} · "
          f"Pending archived: {n_pa} · firm-name cells unified: {n_un}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_REMOVED={n_rm}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
