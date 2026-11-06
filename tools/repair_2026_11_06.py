# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-06.

Base: main at bb10a75c ("Analytical schema sweep 2026-11-06").

Every value set below was read this morning on the regulator's own page, or
is a faithful translation of text already on the row it is written to.

ONE ROW LEAVES Recalls, so the commit message MUST carry `remove-rows`.
Four rows leave PENDING — Pending is a queue, not an archive; each is
copied into Rejected with its verdict and nothing is deleted.

TWO TESTS WERE RED ON MAIN AT THE BASE SHA. Both are data defects, and
both are fixed here; the code half of the second is in
pipeline/gap_finder/main.py and pipeline/gap_finder_gr/main.py.

    test_language_policy::test_every_description_field_is_english   -> B
    test_alert_number_year_matches_date[Pending]                    -> E


A. A NON-FOOD CONSUMER APPLIANCE WAS PUBLISHED THIS MORNING
===========================================================
Recalls carried, PUBLISHED, promoted at 04:27 UTC today by
pipeline.promote_gate_passing with the report line "passes every
publish-gate rule":

    Date      2026-11-03
    Source    NKFH (HU)
    Company   Adler Sp. z o. o. (Lengyelország)
    Brand     Camry
    Product   Electric shaver
    Pathogen  Mercury (heavy metal)
    Reason    Serious environmental hazard! The internal power sources of
              the electric shavers contain an excessive amount of mercury.
              Mercury is difficult to dissolve and can permanently damage
              the soil and groundwater when the discarded products reach
              the ground.
    Tier      2
    URL       nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-
              elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-
              kockazat-miatt-1
    Notes     Discovered via news: blikk.hu … [offline-enrich 2026-11-06:
              Pathogen '' -> 'Mercury (heavy metal)' …]

A cordless electric shaver, recalled because the batteries inside it hold
too much mercury to throw away. Not a food (scope rule R1), and the harm
the notice names is to soil and groundwater on disposal, not to anyone who
eats anything (scope rule R6). It was in Weekly_Review too, so the
operator's own Sunday review slice carried it as a verified promotion.

Both copies are removed and the row is archived to Rejected with a
terminal verdict. The gate half of the fix is
pipeline/_publish_gate._non_food_appliance_blockers, with the measurement
that justifies it; the test is
tests/test_non_food_appliance_is_never_published.py.

NKFH is Hungary's consumer-protection authority and its recall feed covers
every consumer product, not food alone, so this source will keep offering
non-food rows. That is a note for the collector, not a reason to drop the
source: it has published real food recalls.


B. TRANSLATION, AND A PLACEHOLDER-SHAPED PRODUCT ON A ROW PUBLISHED
   YESTERDAY
===================================================================
Four published rows carried a live "[needs cleanup: …]" stamp this
morning. Read on the fiches themselves today:

  24020  Titre "MOULES DE BOUCHOT"; Marque "MONBRUN"; Modèles ou
         références "MOULES DE BOUCHOT SAC DE 15 KG"; Conditionnements
         "SAC DE 15 KG"; Motif "PRESENCE E-COLI"; Risques "Escherichia
         coli"; Distributeurs "SCAPMAREE"; France entière; GTIN
         3370410150013, lot 018, DCR 31/10/2026; publiée le 05/11/2026;
         Fiche réf. 2026-11-0038. "Moules de bouchot" is the
         pole-cultivated mussel; the French term is kept in brackets
         because it names the growing method and has no English word.

  24013  Titre "Saucisse de Montbéliard"; Marque "sans marque"; Modèles
         ou références "Saucisse de Montbéliard vendu au rayon
         traditionnel"; Conditionnements "Vrac pour vente en rayon
         traditionnel."; Motif "Présence de Salmonelles"; Risques
         "Salmonella spp (agent responsable de la salmonellose)";
         Distributeurs "Carrefour Vimoutiers UNIQUEMENT"; ORNE (61);
         publiée le 05/11/2026; Fiche réf. 2026-11-0032. Saucisse de
         Montbéliard is an IGP protected name and is kept as written;
         "rayon traditionnel" is the served counter.

  24008  Titre "Tranches de truite fumée"; Marque "Leclerc Langon";
         Modèles ou références "Tranches de truite fumée";
         Conditionnements "Sous vide"; Motif "Contamination à la
         listeria"; Risques "Listeria monocytogenes (agent responsable de
         la listériose)"; Distributeurs "Leclerc Langon"; France entière;
         publiée le 04/11/2026; Fiche réf. 2026-11-0028. This is the row
         holding test_every_description_field_is_english red: its
         published Reason was still the French "Contamination à la
         Listeria".

  23981  Titre "Saucisse fraîche de canard"; Marque "Fontalbat Mazars";
         Modèles ou références "Saucisse fraîche de canard en poche sous
         vide"; Conditionnements "poche sous vide Produit vrac au marché
         / boucher"; Motif "Présence de Salmonelle"; Risques "Salmonella
         spp (agent responsable de la salmonellose)"; Distributeurs
         "Mangeons frais, provenc'halles, marché Figeac, Cahors, Cajarc";
         France entière; publiée le 04/11/2026; Fiche réf. 2026-11-0002.

A FIFTH row carried NO stamp and was worse than any of them. Fiche 23996,
published yesterday, had Product "baymar" — the brand name, in lower case,
in the food field. Read on the fiche today:

  23996  Titre "ESCALOPE DE POULET TRANCHE MARINE ANDALOUSE BAYMAR";
         Marque "BAYMAR"; Modèles ou références "BAYMAR";
         Conditionnements "VENDU EN VRAC AU SEIN DE LA BOUCHERIE"; Motif
         "DETECTION DE SALMONELLE"; Risques "Salmonella spp (agent
         responsable de la salmonellose)"; Distributeurs "BOUCHERIE
         BAYMAR 8 RUE DU TRAVAIL 67000 STRASBOURG"; BAS-RHIN (67); GTIN
         27102026, lot 27102026, DLC 30/10/2026; commercialisée du
         27/10/2026 au 30/10/2026; publiée le 04/11/2026; Fiche réf.
         2026-11-0016.

The RappelConso scraper takes Product from "Modèles ou références", and on
this fiche that field holds the brand, so the food name — which lives only
in the page heading — was never read. THIS IS THE THIRD TIME THIS EXACT
SHAPE HAS BEEN FOUND BY HAND: fiche 23552 published the single word
"Unbranded" from 18 September to 30 October, and fiche 23983 published a
barcode plus "non concerné" until 2026-11-04. Each was found by sweeping
every published Product for the SHAPE rather than for the stamp, because a
language detector cannot see a French WORD in a barcode or in a brand name
that is also a Latin-script string. Each time the note on the row said so.
The scraper is still reading one field and the heading is still the only
place the food name reliably is; that is the standing defect, recorded
here again because this pass fixes the row, not the scraper.


C. PENDING ROWS BLOCKED ON A FIELD THE REGULATOR'S OWN PAGE STATES
==================================================================
Four real, in-scope recalls from 04-05/11/2026 sat in Pending blocked by
the publish gate on one empty field each. Every value below was read on
the fiche today; nothing is inferred from another row.

  24019  Company empty. Titre "Merguez de boeuf origine France"; Marque
         "Sans marque"; Modèles ou références "Merguez de boeuf origine
         France"; Conditionnements "Vente quantité à la demande au rayon
         Boucherie traditionnelle"; Motif "Non conformité
         microbiologique"; Risques "Listeria monocytogenes (agent
         responsable de la listériose)"; Distributeurs "Magasins U";
         Régions Île-de-France, Normandie; GTIN 3467980036813, lots
         26302146008 / 26303146008 / 26306146008, DLC 10-14/11/2026;
         commercialisée du 02/11/2026 au 05/11/2026; publiée le
         05/11/2026; Fiche réf. 2026-11-0037.

  24005  Company empty. Same product and distributor, the earlier and
         narrower fiche: Modèles ou références "Merguez de boeuf origine
         France"; Motif "Non conformité microbiologique"; Description
         complémentaire du risque "Suspicion détection listeria";
         Distributeurs "Magasins U" + a "liste_magasins_livrés.pdf";
         Origine de la fiche "COOPERATIVE U ENSEIGNE"; GTIN
         3467980036813, lot 26302146008 only, DLC 10/11/2026;
         commercialisée du 02/11/2026 au 03/11/2026; publiée le
         04/11/2026; Fiche réf. 2026-11-0025.

  24015  Company empty. Titre "Merguez boeuf mouton"; Marque "SANS
         MARQUE"; Modèles ou références "Merguez boeuf mouton";
         Conditionnements "Vendus en rayon traditionnel (vrac)"; Motif
         "Suspicion de présence de Listéria monocytogenes"; Risques
         "Listeria monocytogenes (agent responsable de la listériose)";
         Origine de la fiche "VALLEGRAIN DISTRIBUTION"; Distributeurs
         "Détaillants, Carrefour, Sté Grosdoit, Intermarché, Leclerc,
         Métro, Super U"; eleven départements; lots 26303146008 /
         26306146008; commercialisée du 01/11/2026 au 03/11/2026;
         publiée le 04/11/2026; Fiche réf. 2026-11-0034.

         Company is the fiche's own "Origine de la fiche" here, not a
         distributor: the distributor field holds seven retailers and a
         list of seven retailers is not a firm. These three fiches share
         lot numbers (…302146008, …303146008, …306146008) — 24015 is the
         manufacturer-side notice for the lots the two Magasins U fiches
         recall at retail. If merge_master's near-duplicate rule refuses
         one of them for that reason, the refusal is right and the pass
         accepts it.

  24011  Pathogen empty. Titre "Viandes et préparations de viande
         ficelées vendues au rayon boucherie (paupiettes, rôtis,...)";
         Marque "sans marque"; Modèles ou références "Tout produit
         ficelé avec de la ficelle blanche vendu au rayon boucherie de
         votre magasin entre le 06/07 et le 19/10/2026 (paupiettes,
         rôtis,...)"; Motif "mise en évidence de bisphénol A dans
         certains lots de ficelle alimentaire de notre fournisseur
         Meyer-Sansboeuf"; Risques "Autres contaminants chimiques";
         Origine de la fiche "CARREFOUR FRANCE"; Distributeurs
         "CARREFOUR"; France entière; commercialisée du 06/07/2026 au
         19/10/2026; publiée le 04/11/2026; Fiche réf. 2026-11-0031.

         Bisphenol A is named in the AFTS scope rule by name and one
         published row already carries the canonical spelling written
         here (BLV (CH), 23.09.2026, Royal Orient bamboo shoots). The
         reason this row was stuck is a vocabulary gap in
         pipeline/enrich_pending_offline, fixed in this pass with
         tests/test_enricher_reads_bisphenol_and_pesticides.py. The
         Pathogen is set here as well so the rebase tool's promoter —
         which runs the gate, not the enricher — can publish the row.


D. ONE RECALL BOTH PUBLISHED AND WAITING IN THE QUEUE
=====================================================
Two Pending rows passed the FULL publish gate this morning and were
stopped only by merge_master's re-promotion guard, which is defence in
depth working exactly as designed — but a row that reaches the last brake
every single morning is a row the queue should not still be holding:

  thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/
      The founding incident of the publish gate itself: a September-2024
      NCC media statement re-dated 2026-11-03. Rejected EIGHT times —
      seven rows in Rejected (2026-08-09, 09-21, 09-28, 09-29, 10-05,
      10-09, and 10-18 by the repromotion-guard) and once in
      Weekly_Rejected yesterday by the review agent. Its own Status in
      Pending already reads 'rejected'; promote_gate_passing ignores
      Status and overwrites it.

  alerts.food.gov.uk/article/6837/Summit-Foods-is-recalling-various-…
      Company "Summit Foods is", Product "various products" — a sentence
      fragment and a placeholder. The recall itself is real and ALREADY
      PUBLISHED, at the FSA's own canonical URL
      alerts.food.gov.uk/news-alerts/alert/fsa-prin-40-2026 (2026-08-21,
      Summit Foods, Snacksters wraps, STEC), which is why the operator
      archived this copy on 2026-08-26 as "duplicate_already_approved".

Both are archived to Rejected with the verdict restated, so the queue stops
offering them. Nothing is deleted anywhere.


E. THREE STALE NAFDAC ALERTS, AND THE GATE THE GAP FINDERS DO NOT USE
=====================================================================
tests/test_alert_number_year_matches_date[Pending] was red on main with
three rows the Nigeria gap finder wrote to Pending at 18:44-18:46 last
night:

    2026-11-03  public-alert-no-014-2020  Liqui Fruit Red Grape Juice
    2026-11-01  public-alert-no-35-2025   Amoxicillin suspensions 125mg/5ml
    2026-10-28  public-alert-no-003-2020  Hand Sanitizers

Each URL carries the alert's own year and each contradicts its row's Date.
On the merits: the grape-juice alert is a 2020 notice re-dated to 2026, the
amoxicillin alert is a medicine and the hand-sanitiser alert is not a food
— so all three are terminal, not repairable.

merge_master.validate_pending_row has refused this shape since 2026-10-24,
and its comment there says it was added because "the Nigeria gap finder
re-queued that one May-2022 alert into Pending again and again". The rule
went into the wrong doorway: the gap finders reach Pending through their
OWN writers, which call nothing in merge_master. That is fixed in both
writers in this pass (pipeline/gap_finder/main.py and
pipeline/gap_finder_gr/main.py, with
tests/test_gap_finder_writers_refuse_stale_alert_numbers.py). These three
rows are the backlog that fix cannot reach.


F. SEVEN RECALLS THE PIPELINE MISSED, ADDED THROUGH THE REAL CODE PATH
======================================================================
Queued with tools/add_manual_row.py from tools/rows_2026_11_06.json, so
they take the same dedup, the same validation and the same gate as every
other row. The JSON holds, for each, every field as the authority page
prints it and a Notes entry saying where each fact was read:

    24022  La Cabane à jus, apple-cherry juice 1 L — patulin over limit
    24023  La Cabane à jus, apple-blackcurrant juice 1 L — patulin
    24024  La Cabane à jus, apple-raspberry juice 1 L — patulin
    24025  La Cabane à jus, apple-pear juice 1 L — patulin
    24026  Itafrut Tahiti limes from Brazil — pesticide residues over limit
    CAA 00000036085  Takase tea — aluminium fragments
    CAA 00000036086  Okara and six tofu products — stone-like foreign body

Deliberately NOT queued, from the same sweep:
    24021 Theragun G4PRO charger (fire risk) — not a food.
    AGES 05.11: myrug puzzle mats, two plush toys — not food.
    CAA 00000036090 high-school lactic-acid drink — a research-grade
        starter culture was used. No named pathogen, toxin, foreign
        material, heavy metal, chemical contaminant, additive over a
        limit, mould, pest or uninspected product: a process
        non-conformity, outside scope rule R6 as written.
    CAA 36091, 36093 Ise udon sauce best-before shorter than the box's,
        36084 Baba Chilli Powder best-before misprint — date marking.
    CAA 36092 canned bonito chilli oil / whale — allergen labelling.
    CFS (HK) 05.11 poultry import suspension (H5N1, Norway and the UK) —
        a trade measure, naming no product, firm or recall.
    Canada 05.11: nine items, every one a health or consumer product.
    FSS 04.11 AG Barr Rubicon — undeclared sulphites, allergen-only.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_06.json"

TODAY = "2026-11-06"
BY = "morning fix pass " + TODAY

PRODUCT_STAMP = "[needs cleanup: Product not in English]"
REASON_STAMP = "[needs cleanup: Reason not in English]"
REASON_STAMP_LONG = ("[needs cleanup: Reason not in English and no verified "
                     "translation on file]")

SHAVER = ("https://nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-"
          "elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-"
          "kockazat-miatt-1")
F23981 = "https://rappel.conso.gouv.fr/fiche-rappel/23981/interne"
F23996 = "https://rappel.conso.gouv.fr/fiche-rappel/23996/interne"
F24005 = "https://rappel.conso.gouv.fr/fiche-rappel/24005/interne"
F24008 = "https://rappel.conso.gouv.fr/fiche-rappel/24008/interne"
F24011 = "https://rappel.conso.gouv.fr/fiche-rappel/24011/interne"
F24013 = "https://rappel.conso.gouv.fr/fiche-rappel/24013/interne"
F24015 = "https://rappel.conso.gouv.fr/fiche-rappel/24015/interne"
F24019 = "https://rappel.conso.gouv.fr/fiche-rappel/24019/interne"
NCC_HUMMUS = ("https://thencc.org.za/media-statement-deli-hummus-range-"
              "product-safety-recall/")
FSA_6837 = ("https://alerts.food.gov.uk/article/6837/Summit-Foods-is-"
            "recalling-various-products-because-of-possible-contamination-"
            "with-Shiga-toxin-producing-E.-coli-STEC")
NAFDAC_014 = ("https://nafdac.gov.ng/public-alert-no-014-2020-recall-of-"
              "liquid-fruit-grape-juice-product-code-27327-by-south-africa-"
              "foods-company/")
NAFDAC_35 = ("https://nafdac.gov.ng/public-alert-no-35-2025-substandard-"
             "batches-of-annmox-amoxicillin-suspensions-125mg-5ml-and-"
             "jawamox-amoxicillin-suspension-125mg-5ml-manufactured-by-"
             "jawa-international-ltd-due-to-low-ac/")
NAFDAC_003 = ("https://nafdac.gov.ng/public-alert-no-003-2020-alert-on-"
              "illegal-distribution-and-sale-of-unregistered-hand-"
              "sanitizers/")


# ── B. Recalls + Weekly_Review: translate, then discharge the stamp ───────
#
# The same specs run against BOTH sheets. Weekly_Review is the slice the
# operator's Sunday review email reads, and three of these rows are
# mirrored there; a fix applied only to Recalls would leave the mirror
# disagreeing with the register. 24020 and 24013 were promoted by the
# offline promoter, which does not mirror, so they are Recalls-only — which
# is why the Weekly_Review pass is not `required`.
TRANSLATIONS = [
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24020/interne",
        "set": {"Product": "Bouchot mussels (moules de bouchot), 15 kg sack"},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (f"[{BY}: Product translated from fiche 24020's own French, "
                 f"read on the fiche today — Titre \"MOULES DE BOUCHOT\", "
                 f"Modèles ou références \"MOULES DE BOUCHOT SAC DE 15 KG\", "
                 f"Conditionnements \"SAC DE 15 KG\", Marque \"MONBRUN\", "
                 f"Motif \"PRESENCE E-COLI\", Risques \"Escherichia coli\", "
                 f"Distributeurs \"SCAPMAREE\", France entière, GTIN "
                 f"3370410150013, lot 018, date de consommation recommandée "
                 f"31/10/2026, publiée le 05/11/2026, Fiche réf. "
                 f"2026-11-0038. \"Moules de bouchot\" names the "
                 f"pole-cultivation method and has no single English word, "
                 f"so the French is kept in brackets. Reason was already "
                 f"English. No cut, weight or lot is added that the fiche "
                 f"does not give.] "
                 f"[original product: MOULES DE BOUCHOT SAC DE 15 KG]"),
    },
    {
        "url": F24013,
        "set": {"Product": ("Montbéliard sausage (saucisse de Montbéliard), "
                            "sold at the service counter")},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (f"[{BY}: Product translated from fiche 24013's own French, "
                 f"read on the fiche today — Titre \"Saucisse de "
                 f"Montbéliard\", Modèles ou références \"Saucisse de "
                 f"Montbéliard vendu au rayon traditionnel\", "
                 f"Conditionnements \"Vrac pour vente en rayon "
                 f"traditionnel.\", Marque \"sans marque\" (which is what "
                 f"Brand \"Unbranded\" records), Motif \"Présence de "
                 f"Salmonelles\", Risques \"Salmonella spp (agent "
                 f"responsable de la salmonellose)\", Distributeurs "
                 f"\"Carrefour Vimoutiers UNIQUEMENT\", ORNE (61), publiée "
                 f"le 05/11/2026, Fiche réf. 2026-11-0032. Saucisse de "
                 f"Montbéliard is an IGP protected name and is kept as "
                 f"written; \"rayon traditionnel\" is the served counter. "
                 f"Reason was already English.] "
                 f"[original product: Saucisse de Montbéliard vendu au "
                 f"rayon traditionnel]"),
    },
    {
        "url": F24008,
        "set": {"Product": "Smoked trout slices, vacuum-packed",
                "Reason": "Listeria contamination"},
        "drop_stamps": [PRODUCT_STAMP, REASON_STAMP, REASON_STAMP_LONG],
        "note": (f"[{BY}: Product and Reason translated from fiche 24008's "
                 f"own French, read on the fiche today — Titre \"Tranches "
                 f"de truite fumée\", Modèles ou références \"Tranches de "
                 f"truite fumée\", Conditionnements \"Sous vide\", Marque "
                 f"\"Leclerc Langon\", Motif \"Contamination à la "
                 f"listeria\", Risques \"Listeria monocytogenes (agent "
                 f"responsable de la listériose)\", Distributeurs \"Leclerc "
                 f"Langon\", France entière, GTIN 0208749000000, lot "
                 f"10610202607, DLC 28/10/2026, publiée le 04/11/2026, "
                 f"Fiche réf. 2026-11-0028. This row's French Reason is "
                 f"what held tests/test_language_policy::"
                 f"test_every_description_field_is_english red on main this "
                 f"morning; the row carried the Reason stamp twice, in both "
                 f"its short and its long wording, and both are dropped.] "
                 f"[original product: Tranches de truite fumée] "
                 f"[original Reason (fr): \"Contamination à la listeria\"]"),
    },
    {
        "url": F23981,
        "set": {"Product": "Fresh duck sausage in a vacuum pouch"},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (f"[{BY}: Product translated from fiche 23981's own French, "
                 f"read on the fiche today — Titre \"Saucisse fraîche de "
                 f"canard\", Modèles ou références \"Saucisse fraîche de "
                 f"canard en poche sous vide\", Conditionnements \"poche "
                 f"sous vide Produit vrac au marché / boucher\", Marque "
                 f"\"Fontalbat Mazars\", Motif \"Présence de Salmonelle\", "
                 f"Risques \"Salmonella spp (agent responsable de la "
                 f"salmonellose)\", Distributeurs \"Mangeons frais, "
                 f"provenc'halles, marché Figeac, Cahors, Cajarc\", France "
                 f"entière, GTIN 293134000002, lots 43304 / 43315 / 43324 / "
                 f"43342, DLC 29/10/2026 to 28/11/2026, publiée le "
                 f"04/11/2026, Fiche réf. 2026-11-0002. Reason was already "
                 f"English.] "
                 f"[original product: Saucisse fraîche de canard en poche "
                 f"sous vide]"),
    },
    {
        "url": F23996,
        "set": {"Product": ("Sliced marinated chicken escalope, Andalusian "
                            "marinade, sold loose at the butcher's counter")},
        "note": (f"[{BY}: the published Product was the single lower-case "
                 f"word \"baymar\", which is this fiche's BRAND, not a "
                 f"food. Read on fiche 23996 today: Titre \"ESCALOPE DE "
                 f"POULET TRANCHE MARINE ANDALOUSE BAYMAR\", Marque "
                 f"\"BAYMAR\", Modèles ou références \"BAYMAR\", "
                 f"Conditionnements \"VENDU EN VRAC AU SEIN DE LA "
                 f"BOUCHERIE\", Motif \"DETECTION DE SALMONELLE\", Risques "
                 f"\"Salmonella spp (agent responsable de la "
                 f"salmonellose)\", Distributeurs \"BOUCHERIE BAYMAR 8 RUE "
                 f"DU TRAVAIL 67000 STRASBOURG\", BAS-RHIN (67), GTIN "
                 f"27102026, lot 27102026, DLC 30/10/2026, commercialisée "
                 f"du 27/10/2026 au 30/10/2026, publiée le 04/11/2026, "
                 f"Fiche réf. 2026-11-0016. The scraper reads Product from "
                 f"\"Modèles ou références\" and on this fiche that field "
                 f"holds the brand, so the food name — which is only in the "
                 f"page heading — was never read. THIS ROW CARRIED NO "
                 f"\"needs cleanup\" STAMP: the detector looks for "
                 f"non-English WORDS and \"baymar\" is a Latin-script "
                 f"brand, exactly as a barcode is not a French word. Third "
                 f"instance of this shape found by hand — fiche 23552 "
                 f"published \"Unbranded\" from 18 September to 30 October "
                 f"and fiche 23983 published a barcode plus \"non "
                 f"concerné\" until 2026-11-04 — so this pass again swept "
                 f"every Product published in the last 26 h for the SHAPE "
                 f"rather than for the marker.] "
                 f"[original product: BAYMAR]"),
    },
]


# ── C. Pending: fill the one field each row is blocked on ─────────────────
PENDING_FIXES = [
    {
        "url": F24019,
        "set": {"Company": "Magasins U", "Brand": "Unbranded",
                "Product": ("Beef merguez sausages, origin France, sold by "
                            "weight at the service butcher's counter")},
        "note": (f"[{BY}: Company was empty and the publish gate held the "
                 f"row on it. Read on fiche 24019 today — Distributeurs "
                 f"\"Magasins U\", Marque \"Sans marque\", Modèles ou "
                 f"références \"Merguez de boeuf origine France\", "
                 f"Conditionnements \"Vente quantité à la demande au rayon "
                 f"Boucherie traditionnelle\", Motif \"Non conformité "
                 f"microbiologique\", Risques \"Listeria monocytogenes "
                 f"(agent responsable de la listériose)\", Régions "
                 f"Île-de-France et Normandie, GTIN 3467980036813, lots "
                 f"26302146008 / 26303146008 / 26306146008, DLC 10/11/2026, "
                 f"11/11/2026 and 14/11/2026, commercialisée du 02/11/2026 "
                 f"au 05/11/2026, publiée le 05/11/2026, Fiche réf. "
                 f"2026-11-0037. Company is the distributor because the "
                 f"product has no brand, which is this register's own "
                 f"convention for a RappelConso \"sans marque\" row.] "
                 f"[original product: Merguez de boeuf origine France]"),
    },
    {
        "url": F24005,
        "set": {"Company": "Magasins U", "Brand": "Unbranded",
                "Product": ("Beef merguez sausages, origin France, sold by "
                            "weight at the service butcher's counter")},
        "note": (f"[{BY}: Company was empty and the publish gate held the "
                 f"row on it. Read on fiche 24005 today — Distributeurs "
                 f"\"Magasins U\" with a \"liste_magasins_livrés.pdf\", "
                 f"Origine de la fiche \"COOPERATIVE U ENSEIGNE\", Marque "
                 f"\"Sans marque\", Modèles ou références \"Merguez de "
                 f"boeuf origine France\", Motif \"Non conformité "
                 f"microbiologique\", Description complémentaire du risque "
                 f"\"Suspicion détection listeria\", Risques \"Listeria "
                 f"monocytogenes (agent responsable de la listériose)\", "
                 f"Régions Île-de-France et Normandie, GTIN 3467980036813, "
                 f"lot 26302146008, DLC 10/11/2026, commercialisée du "
                 f"02/11/2026 au 03/11/2026, publiée le 04/11/2026, Fiche "
                 f"réf. 2026-11-0025. This fiche covers one lot; fiche "
                 f"24019 extends the same recall to three. If the "
                 f"near-duplicate rule refuses one of the two, that "
                 f"refusal is correct and this pass accepts it.] "
                 f"[original product: Merguez de boeuf origine France]"),
    },
    {
        "url": F24015,
        "set": {"Company": "Vallegrain Distribution", "Brand": "Unbranded",
                "Product": ("Beef and mutton merguez sausages, sold loose "
                            "at the service counter"),
                "Reason": "Suspected presence of Listeria monocytogenes"},
        "note": (f"[{BY}: Company was empty and the publish gate held the "
                 f"row on it. Read on fiche 24015 today — Origine de la "
                 f"fiche \"VALLEGRAIN DISTRIBUTION\", Marque \"SANS "
                 f"MARQUE\", Modèles ou références \"Merguez boeuf "
                 f"mouton\", Conditionnements \"Vendus en rayon "
                 f"traditionnel (vrac)\", Motif \"Suspicion de présence de "
                 f"Listéria monocytogenes\", Risques \"Listeria "
                 f"monocytogenes (agent responsable de la listériose)\", "
                 f"Distributeurs \"Détaillants, Carrefour, Sté Grosdoit, "
                 f"Intermarché, Leclerc, Métro, Super U\", eleven "
                 f"départements from CALVADOS (14) to ESSONNE (91), lots "
                 f"26303146008 and 26306146008, DLC 11/11/2026 and "
                 f"14/11/2026, commercialisée du 01/11/2026 au 03/11/2026, "
                 f"publiée le 04/11/2026, Fiche réf. 2026-11-0034. Company "
                 f"here is the fiche's own originator, not a distributor: "
                 f"the distributor field holds seven retailers and a list "
                 f"of seven retailers is not a firm. The two lot numbers "
                 f"are the same lots fiche 24019 recalls at Magasins U, so "
                 f"this is the manufacturer-side notice for the same "
                 f"product. The published Reason must be English, so the "
                 f"fiche's Motif is translated here too; the original is "
                 f"kept below.] "
                 f"[original product: Merguez boeuf mouton] "
                 f"[original Reason (fr): \"Suspicion de présence de "
                 f"Listéria monocytogenes\"]"),
    },
    {
        "url": F24011,
        "set": {"Pathogen": "Bisphenol A (chemical contaminant)",
                "Product": ("Meat and meat preparations tied with white "
                            "butcher's twine and sold at the butcher's "
                            "counter (paupiettes, roasting joints, etc.) "
                            "between 06/07/2026 and 19/10/2026"),
                "Reason": ("Bisphenol A detected in certain batches of the "
                           "food-grade twine supplied by Meyer-Sansboeuf")},
        "note": (f"[{BY}: Pathogen was empty and the publish gate held the "
                 f"row on it, for a recall whose Motif names the "
                 f"contaminant. Read on fiche 24011 today — Titre "
                 f"\"Viandes et préparations de viande ficelées vendues au "
                 f"rayon boucherie (paupiettes, rôtis,...)\", Marque \"sans "
                 f"marque\", Modèles ou références \"Tout produit ficelé "
                 f"avec de la ficelle blanche vendu au rayon boucherie de "
                 f"votre magasin entre le 06/07 et le 19/10/2026 "
                 f"(paupiettes, rôtis,...)\", Conditionnements \"produits "
                 f"ficelés, vendus au rayon traditionnel ou préparés par "
                 f"nos équipes et vendus en libre service (barquettes) au "
                 f"rayon attenant au rayon traditionnel\", Motif \"mise en "
                 f"évidence de bisphénol A dans certains lots de ficelle "
                 f"alimentaire de notre fournisseur Meyer-Sansboeuf\", "
                 f"Risques \"Autres contaminants chimiques\", Origine de la "
                 f"fiche \"CARREFOUR FRANCE\", Distributeurs \"CARREFOUR\", "
                 f"France entière, publiée le 04/11/2026, Fiche réf. "
                 f"2026-11-0031. Bisphenol A is named in the AFTS scope "
                 f"rule and the canonical spelling written here is the one "
                 f"a published row already carries (BLV (CH) 23.09.2026, "
                 f"Royal Orient bamboo shoots). The root cause was a "
                 f"vocabulary gap in pipeline/enrich_pending_offline, "
                 f"fixed in this pass; the value is written here as well "
                 f"because the rebase tool re-runs the gate and the "
                 f"promoter, not the enricher. No substance beyond the one "
                 f"the fiche names is added.] "
                 f"[original product: Tout produit ficelé avec de la "
                 f"ficelle blanche vendu au rayon boucherie de votre "
                 f"magasin entre le 06/07 et le 19/10/2026 (paupiettes, "
                 f"rôtis,...)] "
                 f"[original Reason (fr): \"mise en évidence de bisphénol A "
                 f"dans certains lots de ficelle alimentaire de notre "
                 f"fournisseur Meyer-Sansboeuf\"]"),
    },
]


# ── A. the row that must leave Recalls ───────────────────────────────────
SHAVER_REASON = (
    f"[REMOVED FROM Recalls {TODAY} — OUT OF SCOPE: NOT FOOD. A cordless "
    f"electric shaver (Camry, Adler Sp. z o. o.), recalled by Hungary's "
    f"NKFH because the batteries inside it contain an excessive amount of "
    f"mercury. The notice's own hazard sentence names the harm: mercury "
    f"\"can permanently damage the soil and groundwater when the discarded "
    f"products reach the ground\" — a disposal hazard to the environment, "
    f"not a contaminant in a food. It was promoted into Recalls at 04:27 "
    f"UTC on {TODAY} by pipeline.promote_gate_passing, which reported it as "
    f"passing every publish-gate rule, because the only non-food test that "
    f"could have seen it (rule 1, an empty Pathogen) was satisfied: the "
    f"offline enricher had correctly filled Pathogen 'Mercury (heavy "
    f"metal)' from the row's own text, and mercury IN a food is in scope. "
    f"The gate now carries _non_food_appliance_blockers for both halves of "
    f"the shape — a durable consumer good, and a harm named to the "
    f"environment rather than to the eater. Scope rules R1 and R6.]")

# ── D / E. Pending rows that must leave the queue ────────────────────────
PENDING_ARCHIVE = [
    {
        "url": NCC_HUMMUS,
        "reason": (
            f"[ARCHIVED FROM Pending {TODAY} — TERMINAL, UPHELD. The "
            f"September-2024 NCC media statement on the BM Foods Deli Hummus "
            f"range, carried in Pending with Date 2026-11-03. This is the "
            f"founding incident of pipeline/_publish_gate.py, whose docstring "
            f"records it: the recall is real and roughly 22 months stale. It "
            f"has been rejected EIGHT times — Rejected on 2026-08-09, 09-21, "
            f"09-28, 09-29, 10-05, 10-09 and 10-18 (the last by the "
            f"repromotion-guard), and Weekly_Rejected on 2026-11-05 by the "
            f"review agent — and its own Status in Pending already read "
            f"'rejected'. It passes the FULL publish gate today, so every "
            f"morning the only thing between it and publication is "
            f"merge_master's re-promotion guard, because "
            f"promote_gate_passing selects on publish_blockers() alone and "
            f"overwrites Status. Removing it from the QUEUE removes the "
            f"daily near-miss; the archive copies stay where they are.]"),
    },
    {
        "url": FSA_6837,
        "reason": (
            f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: DUPLICATE OF A "
            f"PUBLISHED ROW. Company \"Summit Foods is\" and Product "
            f"\"various products\" are a sentence fragment and a "
            f"placeholder, taken from the headline of FSA alert 6837. The "
            f"recall itself is real and ALREADY PUBLISHED in Recalls at the "
            f"FSA's own canonical URL, "
            f"alerts.food.gov.uk/news-alerts/alert/fsa-prin-40-2026 "
            f"(2026-08-21, Summit Foods, Snacksters Chicken Fajita Wrap and "
            f"others, Shiga toxin-producing E. coli (STEC)) — which is the "
            f"reason the operator archived this copy on 2026-08-26 as "
            f"\"duplicate_already_approved\". It passes the full publish "
            f"gate because the gate cannot see that a second URL is the same "
            f"recall; only the re-promotion guard stops it. Verified against "
            f"the published row this morning before archiving.]"),
    },
    {
        "url": NAFDAC_014,
        "reason": (
            f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: STALE ALERT. "
            f"NAFDAC public alert no. 014/2020 on Liqui Fruit Red Grape "
            f"Juice, written to Pending by the Nigeria gap finder at 18:44 "
            f"UTC on 2026-11-05 with Date 2026-11-03. The alert number in "
            f"the row's own URL is from 2020; the Date is the discovery day. "
            f"One of the three rows holding "
            f"tests/test_alert_number_year_matches_date[Pending] red on main "
            f"this morning. The gap-finder writers now apply "
            f"alert_number_year_mismatch themselves (see "
            f"pipeline/gap_finder/main.py), so this shape stops at the "
            f"Pending boundary from now on; this row is the backlog that "
            f"fix cannot reach.]"),
    },
    {
        "url": NAFDAC_35,
        "reason": (
            f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: STALE ALERT AND NOT "
            f"A FOOD. NAFDAC public alert no. 35/2025 on substandard "
            f"Annmox amoxicillin suspensions 125 mg/5 ml, written to Pending "
            f"by the Nigeria gap finder at 18:45 UTC on 2026-11-05 with Date "
            f"2026-11-01. The alert number is from 2025 and the product is a "
            f"prescription medicine, not a food. Held "
            f"tests/test_alert_number_year_matches_date[Pending] red on main "
            f"this morning.]"),
    },
    {
        "url": NAFDAC_003,
        "reason": (
            f"[ARCHIVED FROM Pending {TODAY} — TERMINAL: STALE ALERT AND NOT "
            f"A FOOD. NAFDAC public alert no. 003/2020 on the illegal "
            f"distribution and sale of unregistered hand sanitizers, written "
            f"to Pending by the Nigeria gap finder at 18:46 UTC on "
            f"2026-11-05 with Date 2026-10-28. The alert number is from 2020 "
            f"and a hand sanitizer is not a food. Held "
            f"tests/test_alert_number_year_matches_date[Pending] red on main "
            f"this morning.]"),
    },
]


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


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    if notes.lower() in ("none", "nan"):
        notes = ""
    for stamp in drop_stamps or ():
        # EVERY copy, not the first: fiche 24008 carries the Reason stamp
        # in two different wordings and one of them twice.
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
            for field, value in spec["set"].items():
                if field not in head:
                    continue
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


def _archive(wb, sheet, row, reason, by) -> bool:
    """Append `row` to an archive sheet with a terminal reason."""
    if sheet not in wb.sheetnames:                      # pragma: no cover
        print(f"  WARNING: no {sheet} sheet — the audit trail would be lost")
        return False
    arch = wb[sheet]
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        elif h == "Reviewed":
            out.append("Y")
        elif h == "Week_Added":
            out.append(TODAY)
        else:
            out.append(row.get(h, ""))
    arch.append(out)
    return True


def remove_shaver(wb) -> int:
    """A. The non-food appliance leaves Recalls and the review mirror."""
    removed = 0
    ws = wb["Recalls"]
    head = _head(ws)
    for r in reversed(_find_rows(ws, SHAVER)):
        row = _rowdict(ws, head, r)
        print(f"  Recalls row {r}: {row.get('Date')} {row.get('Source')} "
              f"{row.get('Product')!r} — removing (not food)")
        _archive(wb, "Rejected", row, SHAVER_REASON,
                 "operator review " + TODAY)
        ws.delete_rows(r, 1)
        removed += 1
    if not removed:
        raise SystemExit(
            "Recalls: the NKFH electric-shaver row is not there — main has "
            "moved under this repair script. Re-read the sheet: if another "
            "pass already removed it, delete section A before re-running.")
    # Weekly_Review is the live rolling review slice, not an archive.
    if "Weekly_Review" in wb.sheetnames:
        wv = wb["Weekly_Review"]
        for r in reversed(_find_rows(wv, SHAVER)):
            print(f"  Weekly_Review row {r}: removed (same row, the slice "
                  f"the Sunday review email reads)")
            wv.delete_rows(r, 1)
    return removed


def archive_pending(wb) -> int:
    """D + E. Rows leave the Pending QUEUE; nothing is deleted."""
    ws = wb["Pending"]
    head = _head(ws)
    done = 0
    for spec in PENDING_ARCHIVE:
        rows = _find_rows(ws, spec["url"])
        if not rows:
            raise SystemExit(
                f"Pending: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
        for r in reversed(rows):
            row = _rowdict(ws, head, r)
            _archive(wb, "Rejected", row, spec["reason"],
                     "operator review " + TODAY)
            ws.delete_rows(r, 1)
            done += 1
            print(f"  Pending row {r}: {str(row.get('Source'))[:16]!r} "
                  f"{spec['url'][-44:]} -> Rejected")
    return done


def queue_manual_rows() -> int:
    """F. Through the real code path, never straight into Recalls."""
    if not ROWS_JSON.exists():                          # pragma: no cover
        raise SystemExit(f"missing {ROWS_JSON}")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # add_manual_row exits non-zero when ANY requested row did not land.
        # A near-duplicate refusal is an acceptable outcome here (see the
        # note on fiches 24005/24015/24019 above) and the verdict lines
        # above say which row and why, so this is reported, not fatal.
        print(f"  NOTE: add_manual_row exited {proc.returncode} — read the "
              f"per-row verdict lines above; a near-duplicate refusal is "
              f"an accepted outcome for this pass.")
    return 0


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    print("A. Recalls: remove the non-food appliance")
    removed = remove_shaver(wb)
    print("B. Recalls translations and the placeholder Product")
    recalls_fixed = _fix_sheet(wb, "Recalls", TRANSLATIONS)
    print("B. Weekly_Review mirror (same specs; not every row is mirrored)")
    mirror_fixed = _fix_sheet(wb, "Weekly_Review", TRANSLATIONS,
                              required=False)
    print("C. Pending field fixes")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("D+E. Pending -> Rejected")
    archived = archive_pending(wb)
    wb.save(XLSX)
    wb.close()

    print("F. Queue the recalls the pipeline missed")
    queue_manual_rows()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={recalls_fixed}")
    print(f"WEEKLY_REVIEW_FIELDS_FIXED={mirror_fixed}")
    print(f"PENDING_FIELDS_FIXED={pending_fixed}")
    print(f"PENDING_ARCHIVED={archived}")
    print(f"ROWS_REMOVED={removed}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
