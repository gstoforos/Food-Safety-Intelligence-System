# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-28 — data repairs, scripted so the gate can re-run them.

Run with no arguments. tools/rebase_and_verify.py re-runs every
tools/repair_*.py it finds in the code zip WITH NO ARGUMENTS against main's
current workbook, so this script writes by default and is a no-op the second
time it runs.

WHAT WAS WRONG ON MAIN AT b4166012
──────────────────────────────────
THE SUITE WAS RED: 6 failures, in 3 groups.

FINDING 1 — FOUR PUBLISHED RappelConso ROWS STILL IN FRENCH (4 of the 6
failures). Promoted 2026-11-27 by the offline promoter, which has no model
and therefore stamps and leaves the translation to this pass — designed
behaviour, nothing broken upstream. Every fiche below was read on
rappel.conso.gouv.fr today and the French is kept on the row.

    fiche 24209  Product was the traceability line, not a food
    fiche 24205  Product and Reason both French
    fiche 24203  Product and Reason both French
    fiche 24197  Reason French — AND the Product too, UNSTAMPED (see below)

Fiche 24209's Product was 'n° de tracabilité s63130078 à consommer jusqu'au
28/11/2026'. That is the fiche's `Noms des modèles ou références` field,
which on this fiche holds a traceability note instead of a product name; the
food is in the H1, "merguez véritable". So this is not only a translation
defect — the published row named no food at all.

Fiche 24197's Product 'filet de hareng (conditionné sous-vide)' carried NO
"needs cleanup: Product" stamp, only the Reason stamp. A pass that reads
what is stamped would never have seen it; it was caught only by reading
every Product published in the last 26 h by hand. The detector hole is
fixed in pipeline/_language.py and pinned by
tests/test_morning_fix_2026_11_28.py — fourth measurement of that hole.

Fiche 24197's Reason was also a HALF-translation, which no stamp describes:
"Mise en évidence d'un taux d'histamine supérieur à la valeur cible during
an own-check." — the trailing clause englished and the rest left French.

FINDING 2 — A Clostridium botulinum RECALL HELD OUT OF THE REGISTER ON A
FIELD PRINTED ON ITS OWN SOURCE PAGE. The highest-cost item of the day.
Fiche 24210 (27/11/2026) sat in Pending behind publish-gate rule 4,
"Company is empty":

    Product   'soupe poisson traditionnelle pf 1c/6ux750ml'
    Pathogen  'Clostridium botulinum'
    Reason    "Risque d'un éventuel défaut de stérilisation"

A sterilisation failure in a glass jar of fish soup, which is the classic
botulism vehicle. The recalling firm is printed on the fiche as
"Origine de la fiche: MERICQ MERICQ SAS". The RappelConso scraper's company
fallback does not reach that field — the same cause as findings 3 of
2026-11-27 (fiches 24195, 24183, 24089) — so the row is filled from the page
here and publishes through the ordinary promoter.

FINDING 3 — A SEARCH-SNIPPET ROW IN Pending, AND THE RECALL IT MANGLED WAS
MISSING FROM THE REGISTER (1 of the 6 failures). Pending row 11 read

    Company   'Specialist toddler milk'
    Product   'due to the presence of ...'
    Date      (empty)
    URL       …/news/media-releases/specialist-toddler-milk-recalled-due-
              to-the-presence-of-cereulide

— the MPI media-release HEADLINE split at the word "recalled", with the
Product half truncated mid-title by the search snippet it came from. That is
the defect tests/test_an_agency_headline_is_not_a_firm_and_a_food.py exists
to catch, and it was RED.

Behind it is a real recall the register did not have: NESTLÉ ALFAMINO JUNIOR
1+ YEARS, a food for special medical purposes for toddlers with allergies,
recalled 27 November 2026 for CEREULIDE. Both MPI pages were read today and
MPI publishes a per-recall notice of its own, so the row gets that URL
rather than the media release (R3). Repaired in place rather than added
beside, because it IS this recall: adding would mint a duplicate.

FINDING 4 (fixed in code, no data edit) — BRAZIL'S DIÁRIO OFICIAL COLLAPSED
TO ONE DEDUP KEY (1 of the 6 failures). Two distinct ANVISA resolutions on
pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp shared one key because
the identity triple (jornal, pagina, data) was stripped. Both are in
Weekly_Rejected, so nothing published is wrong; the next ANVISA notice on
that shape would have been dropped in silence. Fixed in
merge_master._normalize_url_for_dedup with szpi.gov.cz's `docid`, the same
latent shape found by widening the sweep. Pinned by
tests/test_morning_fix_2026_11_28.py.

FINDING 5 — A SCOPE DECISION RECORDED, NOT ACTED ON. Fiche 24199 (Fuze Tea
Pêche, Coca-Cola European Partners France, 27/11/2026) sits in Pending with
Pathogen empty; pipeline.enrich_pending_offline reports "no hazard term in
the row's own text" although its Reason says "chlorate", a word the publish
gate's own chemical-contaminant list, pipeline/gap_finder/rules.py and
pipeline/daily_recall_search.py all know. That asymmetry is real. It is NOT
closed today, because closing it would PUBLISH this row, and the fiche says
the chlorate content is "non conforme aux standards de Qualité" with
"Risques encourus par le consommateur: Autre risque" — the regulator's own
catch-all class and its own word "Qualité". Quality is out of AFTS scope and
contaminants over a regulatory limit are in it, and this notice claims a
company standard, not a limit. Annotated so the row stops recurring as an
unexplained blocker, and left in Pending for George to decide.

NOT CHANGED, DELIBERATELY
─────────────────────────
* Company on fiches 24209, 24203 and 24197 stays the retailer
  ("Intermarché Pierre de Bresse", "Marque U", "E.Leclerc") even though each
  fiche's "Origine de la fiche" names a different operating company. Those
  three ARE the recalling retailer and the Distributeurs field confirms them,
  which is this register's convention for a RappelConso retailer fiche.
  Fiche 24205 IS changed, because "Bellarom" is a Lidl private LABEL and
  named no firm at all — exactly the "Top Budget" case of 2026-11-24 — and
  the fiche prints "Origine de la fiche: LIDL LIDL".
* pipeline.verify_urls reports the same 10 structural problems it has
  reported for months: published rows from 2026-01-07 to 2026-06-26 whose
  URL is a news host, not the regulator's. None is new, none is from the
  last 24 h, and re-sourcing them needs a regulator URL per row that may no
  longer exist — inventing one is forbidden and deleting an archive row is
  forbidden. A backlog of its own, not a morning-pass edit.
* The junk still in Pending — an NVWA search page ('Zoeken'/'NVWA'), a CDC
  page title beginning "5 days ago ·", three Livsmedelsverket news items with
  no product, a 2021 RASFF notification, and two FDA rows whose Product is
  the press-release headline — is left where it is. None is published, none
  fails a test, and each needs its own source read to retire honestly.
* The NAFDAC (NG) 'Counterfeit Brands of Proguanil … Tablet B.P 100mg' row
  is left in Pending too. It is an antimalarial DRUG, not food, so it is out
  of scope under R1 — but rejecting it is a scope call on a row the pipeline
  queued, and the register's convention for pharmaceuticals is not written
  down anywhere I can cite. Flagged for George instead.
* Pet food and animal feed are NOT added. The FDA's OC Raw freeze-dried
  chicken hearts (Salmonella) and Laub's hay cubes (Clostridium botulinum)
  both published 26-27 November; the register rejects pet food and feed
  consistently (Oma's Pride canine food, Elite Treats dog chips, Northwest
  Naturals, the RappelConso pet CBD oils — all in Rejected), so the
  convention is clear and was followed.
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

REASON_SHEETS = ("Recalls", "Pending", "Weekly_Review")

#: Per-row edits, keyed on the row's own normalised URL. `fields` is
#: {column: (expected value on main, value to write)} — the expected value is
#: checked so a row main has changed under us is SKIPPED and reported rather
#: than overwritten.
EDITS = [
    # ── FINDING 1 — fiche 24209, Intermarché Pierre de Bresse ─────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24209/interne",
        "fields": {
            "Product": (
                "n° de tracabilité s63130078 à consommer jusqu'au 28/11/2026",
                "Genuine merguez sausages (merguez véritable), traceability "
                "no. S63130078, use-by 28/11/2026, GTIN 45482056, sold "
                "pre-packed in self-service and at the traditional counter"),
        },
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-28: the published Product NAMED NO "
            "FOOD. It was the fiche's `Noms des modèles ou références` "
            "field, which on this fiche holds a traceability note rather "
            "than a product designation; the food is in the H1. Read on "
            "fiche 24209 today (Fiche réf. 2026-11-0214 • Version 1, date "
            "27/11/2026): H1 \"merguez véritable\" with the sub-line "
            "\"intermarché pierre de bresse\", \"Origine de la fiche: ALRINE "
            "INTERMARCHE\", \"Nom de la marque du produit: intermarché "
            "pierre de bresse\", \"Noms des modèles ou références: n° de "
            "tracabilité S63130078 à consommer jusqu'au 28/11/2026\", "
            "Catégorie Alimentation / Viandes, GTIN \"45482056\", Lot "
            "\"S63130078\", \"Date limite de consommation 28/11/2026\", "
            "Informations complémentaires \"vendu conditionne en libre "
            "service et au rayon traditionnel\", \"Date début/Fin de "
            "commercialisation: Du 16/11/2026 au 21/11/2026\", \"Produit à "
            "conserver au réfrigérateur\", Zone \"Départements: "
            "SAONE-ET-LOIRE (71)\", Distributeurs \"intermarché pierre de "
            "bresse 71270\", Motif \"salmonella\", Risques \"Salmonella spp "
            "(agent responsable de la salmonellose)\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\". No health mark is "
            "printed and none is added. \"merguez\" is the sausage's own "
            "name and is kept; \"véritable\" is the fiche's own qualifier. "
            "The Reason \"Salmonella\" is the Motif and already reads in "
            "English, so it is not touched. Company stays the published "
            "\"Intermarché Pierre de Bresse\", this register's convention "
            "for a RappelConso retailer fiche. [original product: n° de "
            "tracabilité S63130078 à consommer jusqu'au 28/11/2026] "
            "[original product heading: merguez véritable]]"),
    },
    # ── FINDING 1 — fiche 24205, Bellarom / Lidl ──────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24205/interne",
        "fields": {
            "Product": (
                "dosettes de café vanille et dosettes de café double "
                "chocolat",
                "Vanilla coffee pods and double-chocolate coffee pods, 126 g "
                "bag, GTINs 4056489736738 and 4056489736745, lot C28921, "
                "best-before 31/03/2027"),
            "Reason": (
                "Une analyse a mis en évidence la présence d'ochratoxine a "
                "supérieure au seuil règlementaire.",
                "An analysis revealed the presence of Ochratoxin A above the "
                "regulatory limit."),
            "Company": (
                "Bellarom",
                "Lidl"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-28: Product and Reason were both the "
            "fiche's French, and Company was the private LABEL rather than "
            "a firm — the \"Top Budget\" case of 2026-11-24 exactly. Read on "
            "fiche 24205 today (Fiche réf. 2026-11-0210 • Version 1, date "
            "27/11/2026): product heading \"Dosettes de café\" with the "
            "brand line \"Bellarom\", \"Origine de la fiche: LIDL LIDL\", "
            "\"Nom de la marque du produit: Bellarom\", \"Noms des modèles "
            "ou références: Dosettes de café Vanille et Dosettes de café "
            "double chocolat\", Catégorie Alimentation / Sous-catégorie "
            "\"Cacao, café et thé\", GTIN \"4056489736738\" and "
            "\"4056489736745\" both with Lot \"C28921\" and \"Date de "
            "durabilité minimale 31/03/2027\", Conditionnements \"Sachet de "
            "126g\", \"Date début/Fin de commercialisation: Du 13/03/2026 au "
            "05/05/2026\", \"Produit à conserver à température ambiante\", "
            "Zone \"France entière\", Distributeurs \"Voir liste des "
            "magasins concernés\", Motif \"Une analyse a mis en évidence la "
            "présence d'Ochratoxine A supérieure au seuil règlementaire.\", "
            "Risques \"Autres contaminants chimiques\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\". Company is set to the "
            "firm the fiche names in \"Origine de la fiche\"; Brand KEEPS "
            "\"Bellarom\", which is Lidl's own coffee label, so no "
            "information is lost. The Motif says \"supérieure au seuil "
            "règlementaire\" — above the REGULATORY limit — so the hazard is "
            "a chemical contaminant over a limit and squarely in AFTS scope; "
            "the existing Pathogen \"Ochratoxin A\" is right and is not "
            "touched. [original product: Dosettes de café Vanille et "
            "Dosettes de café double chocolat] [original company: Bellarom "
            "(brand; the fiche's Origine de la fiche is LIDL)] [original "
            "Reason (fr): \"Une analyse a mis en évidence la présence "
            "d'Ochratoxine A supérieure au seuil règlementaire.\"]]"),
    },
    # ── FINDING 1 — fiche 24203, Marque U ─────────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24203/interne",
        "fields": {
            "Product": (
                "moka cafe x2 et moka cafe x1",
                "Coffee moka (moka café), thawed, x2 and x1, 180 g plastic "
                "tray, GTIN 2000000286631, all lots, use-by 04/12/2026"),
            "Reason": (
                "Non conformité microbiologique, présence de Listeria "
                "monocytogénes",
                "Microbiological non-compliance, presence of Listeria "
                "monocytogenes"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-28: Product and Reason were both the "
            "fiche's French. Read on fiche 24203 today (Fiche réf. "
            "2026-11-0208 • Version 1, date 27/11/2026): product heading "
            "\"MOKA CAFE x 2 DECONGELE\" with the brand line \"MARQUE U\", "
            "\"Origine de la fiche: MONTARDIS U EXPRESS MONTARGIS\", \"Nom "
            "de la marque du produit: MARQUE U\", \"Noms des modèles ou "
            "références: MOKA CAFE x2 et MOKA CAFE x1\", Catégorie "
            "Alimentation / Sous-catégorie \"Produits sucrés\", GTIN "
            "\"2000000286631\", Lot \"Tous les lots\", \"Date limite de "
            "consommation 04/12/2026\", Conditionnements \"Barquette "
            "plastique QAUNTITE 180 grs\" (the misspelling is the fiche's), "
            "Zone \"Départements: LOIRET (45)\", Distributeurs \"U "
            "EXPRESS\", \"Depuis le 19/11/2026\" with no end date, \"Produit "
            "à conserver au réfrigérateur\", Motif \"Non conformité "
            "microbiologique, présence de listeria monocytogénes\", Risques "
            "\"Listeria monocytogenes (agent responsable de la "
            "listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". \"moka\" is the pastry's own name and is kept; "
            "\"café\" and \"DECONGELE\" are translated as \"coffee\" and "
            "\"thawed\". The fiche's sub-category is \"Produits sucrés\" and "
            "the word \"cake\" appears nowhere on it, so none is added. "
            "Company stays the published \"Marque U\", this register's "
            "convention for a RappelConso retailer fiche. [original product: "
            "MOKA CAFE x 2 DECONGELE / MOKA CAFE x2 et MOKA CAFE x1] "
            "[original Reason (fr): \"Non conformité microbiologique, "
            "présence de listeria monocytogénes\"]]"),
    },
    # ── FINDING 1 — fiche 24197, E.Leclerc — the UNSTAMPED Product ────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24197/interne",
        "fields": {
            "Product": (
                "filet de hareng (conditionné sous-vide)",
                "Herring fillet (filet de hareng), vacuum-packed, all lots, "
                "use-by between 06/11/2026 and 15/11/2026, GTIN "
                "0204644000000"),
            "Reason": (
                "Mise en évidence d’un taux d’histamine supérieur à la "
                "valeur cible during an own-check.",
                "An own-check found a histamine level above the target "
                "value."),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",),
        "note": (
            "[morning fix pass 2026-11-28: the Reason was a HALF-translation "
            "— the trailing clause englished, the rest left French — and the "
            "Product was wholly French while carrying NO \"needs cleanup: "
            "Product\" stamp. product_needs_english() returned False because "
            "\"hareng\" and \"conditionné\" were in no list at all, while "
            "\"filet\" and \"sous vide\" are English culinary words and "
            "\"de\" is name-safe, so neither the food-word rule nor the "
            "two-hit function-word detector had anything to count, and one "
            "accent is under looks_non_english()'s two-accent floor. That is "
            "the fourth measurement of this hole (\"pâté\" 2026-10-27, "
            "\"sandwich\" 2026-11-24, \"chantilly\" 2026-11-26). Fixed in "
            "pipeline/_language.py — measured over all 2,557 published "
            "Products as firing on this row and nothing else — and pinned by "
            "tests/test_morning_fix_2026_11_28.py. Read on fiche 24197 today "
            "(Fiche réf. 2026-11-0202 • Version 1, date 27/11/2026): product "
            "heading \"Filet de hareng (conditionné sous-vide)\" with the "
            "brand line \"E. Leclerc\", \"Origine de la fiche: SOCIETE "
            "LAVIDA E. Leclerc Les Portes d'Albi\", \"Nom de la marque du "
            "produit: E. Leclerc\", \"Noms des modèles ou références: Filet "
            "de hareng (conditionné sous-vide)\", Catégorie Alimentation / "
            "Sous-catégorie \"Produits de la pêche et d'aquaculture\", GTIN "
            "\"0204644000000\", Lot \"Tous les lots\", \"Date limite de "
            "consommation entre le 06/11/2026 et le 15/11/2026\", "
            "Conditionnements \"Produit conditionné sous vide\", "
            "Informations complémentaires \"Produit mis sous vide dans nos "
            "ateliers\", \"Date début/Fin de commercialisation: Du "
            "06/11/2026 au 14/11/2026\", \"Produit à conserver au "
            "réfrigérateur\", Zone \"Départements: TARN (81)\", "
            "Distributeurs \"E. Leclerc Les Portes d'Albi\", Motif \"Mise en "
            "évidence d'un taux d'histamine supérieur à la valeur cible lors "
            "d'un autocontrôle.\", Risques \"Toxines endogènes : "
            "histamine\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". NO NUMERIC HISTAMINE VALUE is printed and none "
            "is invented — the Reason says what the Motif says, that the "
            "level was above the target value. The existing Pathogen "
            "\"Histamine / scombrotoxin\" is a spelling the register already "
            "uses and is not touched. [original product: Filet de hareng "
            "(conditionné sous-vide)] [original Reason (fr): \"Mise en "
            "évidence d'un taux d'histamine supérieur à la valeur cible lors "
            "d'un autocontrôle.\"]]"),
    },
    # ── FINDING 2 — fiche 24210, the Clostridium botulinum row ────────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24210/interne",
        "fields": {
            "Company": ("", "Mericq SAS"),
            "Product": (
                "soupe poisson traditionnelle pf 1c/6ux750ml",
                "Traditional fish soup (soupe poisson traditionnelle), 750 "
                "mL glass jar, reference PF 1C/6Ux750ml, GTIN "
                "3770001302096, lot 60856138, best-before 24/09/2027"),
            "Reason": (
                "Risque d'un éventuel défaut de stérilisation",
                "Risk of a possible sterilisation defect"),
        },
        "note": (
            "[morning fix pass 2026-11-28: HELD BY THE PUBLISH GATE ON "
            "\"Company is empty\" FOR A CLOSTRIDIUM BOTULINUM RECALL, with "
            "the firm printed on its own source page. A sterilisation "
            "failure in a glass jar of fish soup is the classic botulism "
            "vehicle, and this row was the highest-cost item on main this "
            "morning. The RappelConso scraper's company fallback does not "
            "reach \"Origine de la fiche\" — the same cause as fiches 24195, "
            "24183 and 24089 on 2026-11-27. Read on fiche 24210 today (Fiche "
            "réf. 2026-11-0215 • Version 1, date 27/11/2026): title \"Soupe "
            "Poisson Traditionnelle PF 750ml\" with the brand line \"Sans "
            "marque\", \"Origine de la fiche: MERICQ MERICQ SAS\", \"Nom de "
            "la marque du produit: Sans marque\", \"Noms des modèles ou "
            "références: Soupe Poisson Traditionnelle PF 1C/6Ux750ml\", "
            "Catégorie Alimentation / Sous-catégorie \"Produits de la pêche "
            "et d'aquaculture\", GTIN \"3770001302096\", Lot \"60856138\", "
            "\"Date de durabilité minimale 24/09/2027\", Conditionnements "
            "\"Bocal en verre 750 mL\", \"Marque de salubrité: FR 40-150-002 "
            "UE\", \"Date début/Fin de commercialisation: Du 17/10/2026 au "
            "22/10/2026\", \"Produit à conserver à température ambiante\", "
            "Zone \"France entière\", Distributeurs \"Cf liste points de "
            "vente\", Motif \"Risque d'un éventuel défaut de "
            "stérilisation\", Risques \"Clostridium botulinum (agent "
            "responsable du botulisme)\", Nature juridique \"Volontaire "
            "(sans arrêté préfectoral)\". Company is the firm named in "
            "\"Origine de la fiche\", written once rather than twice as the "
            "fiche prints it; Brand is left as the register has it, because "
            "the fiche's own brand field says \"Sans marque\". The existing "
            "Pathogen \"Clostridium botulinum\" is confirmed by the Risques "
            "field above. No numeric or microbiological finding is printed "
            "and none is invented: the Motif states a RISK of a possible "
            "defect, and the Reason says exactly that. The row is not "
            "published by this script — it is unblocked, and "
            "pipeline.promote_gate_passing publishes it through the normal "
            "path. [original product: Soupe Poisson Traditionnelle PF "
            "1C/6Ux750ml] [original company: MERICQ MERICQ SAS (Origine de "
            "la fiche)] [original Reason (fr): \"Risque d'un éventuel défaut "
            "de stérilisation\"]]"),
    },
    # ── FINDING 3 — the mangled MPI headline, and the recall behind it ────
    {
        "sheet": "Pending",
        "url": ("https://www.mpi.govt.nz/news/media-releases/specialist-"
                "toddler-milk-recalled-due-to-the-presence-of-cereulide"),
        "fields": {
            "Date": ("", "2026-11-27"),
            "Company": ("Specialist toddler milk", "Nestlé New Zealand Limited"),
            "Brand": ("Specialist toddler milk", "Alfamino Junior"),
            "Product": (
                "due to the presence of ...",
                "Nestlé brand Alfamino Junior 1+ Years (Unflavoured), 400 g "
                "tin, batch 53440017Y1, use-by 10.12.2027 — a food for "
                "special medical purposes for ages 1+ years"),
            "Reason": (
                "Cereulide — outbreak",
                "One batch could contain cereulide, a heat-resistant toxin "
                "produced by Bacillus cereus; no associated illness has been "
                "reported"),
            "Country": ("", "New Zealand"),
            "URL": (
                "https://www.mpi.govt.nz/news/media-releases/specialist-"
                "toddler-milk-recalled-due-to-the-presence-of-cereulide",
                "https://www.mpi.govt.nz/food-safety-home/food-recalls-and-"
                "complaints/recalled-food-products/nestle-brand-alfamino-"
                "junior-1-years-recalled-due-to-the-presence-of-cereulide"),
        },
        "note": (
            "[morning fix pass 2026-11-28: THIS ROW WAS AN AGENCY HEADLINE "
            "SPLIT IN TWO, AND THE RECALL BEHIND IT WAS MISSING FROM THE "
            "REGISTER. The row read Company 'Specialist toddler milk' / "
            "Product 'due to the presence of ...' — the MPI media-release "
            "headline cut at the word \"recalled\", with the second half "
            "truncated mid-title by the search snippet it was scraped from, "
            "and no Date. tests/test_an_agency_headline_is_not_a_firm_and_a_"
            "food.py was RED on it. Repaired in place rather than added "
            "beside, because it IS this recall and a second row would be a "
            "duplicate. BOTH MPI pages were read today. The media release "
            "(https://www.mpi.govt.nz/news/media-releases/specialist-toddler-"
            "milk-recalled-due-to-the-presence-of-cereulide) prints \"Date: "
            "27 November 2026\", the headline \"Specialist toddler milk "
            "recalled due to the presence of cereulide\", \"Nestlé brand "
            "Alfamino Junior 1+ Years (400g)\", \"batch number 53440017Y1\", "
            "a \"use-by date of 10.12. 2027\", a quantity of \"103 cans\", "
            "that the affected products are \"mainly available through "
            "pharmacies\", and \"Fortunately, there have been no reports of "
            "associated illness.\" MPI ALSO PUBLISHES A PER-RECALL NOTICE, "
            "which the media release links, and the URL is moved to it "
            "because R3 wants the regulator's own per-recall page: that "
            "notice opens \"Recalled on 27 November 2026 by Nestlé.\", gives "
            "the product as \"Nestlé brand Alfamino Junior 1+ Years "
            "(Unflavoured)\", \"The product was sold in a 400 g tin.\", "
            "\"Batch number 53440017Y1\", \"Use by 10.12.2027\", "
            "\"Cereulide is produced by the bacterium Bacillus cereus.\", "
            "that the product \"is a food for special medical purposes for "
            "ages 1+ years\", that it \"was prescribed and available "
            "through the following pharmacies\" (22 named pharmacies from "
            "Orewa to Dunedin), that \"A small amount may have been "
            "purchased over the counter at pharmacies or online\", \"The "
            "product has not been re-exported.\", \"The product was "
            "imported. It was manufactured in Switzerland.\", that cereulide "
            "is \"extremely heat resistant\" so hot-water preparation would "
            "not destroy it, and \"This recall does not affect any other "
            "Nestlé brand products in New Zealand.\" The contact is "
            "\"Nestlé New Zealand Limited\", which is the Company written "
            "here; Brand is the product's own \"Alfamino Junior\". The "
            "existing Pathogen \"Cereulide (B. cereus toxin)\" is a spelling "
            "the register already carries (CFS HK 2026-06-01, RASFF "
            "841474/841471, SFA SG) and is kept. The old Reason said "
            "\"outbreak\"; BOTH pages say there have been no reports of "
            "associated illness, so the word is dropped and the notice's own "
            "statement recorded instead. Searched 'alfamino', 'cereulide', "
            "'toddler' and 'mpi.govt.nz' across all five sheets before "
            "repairing: no other row carries this recall. 103 cans and the "
            "pharmacy list are on the page but are not put in Product, which "
            "keeps the product, pack, batch and date only. [original "
            "company: Specialist toddler milk (page-headline split)] "
            "[original product: due to the presence of ... (page-headline "
            "split, truncated)] [original URL: the MPI media release, kept "
            "here; the row now cites MPI's own per-recall notice]]"),
    },
    # ── FINDING 5 — fiche 24199, the scope decision, recorded only ────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24199/interne",
        "note": (
            "[morning fix pass 2026-11-28: SCOPE DECISION — LEFT IN PENDING, "
            "NOT PUBLISHED, AND NOT ENRICHED. This row is held by the gate "
            "on \"Pathogen is empty\" and "
            "pipeline.enrich_pending_offline reports \"no hazard term in the "
            "row's own text\" although the Reason contains the word "
            "\"chlorate\" — which the publish gate's own "
            "chemical-contaminant list, pipeline/gap_finder/rules.py, "
            "pipeline/daily_recall_search.py and pipeline/gap_finder_claude."
            "py all know. The enricher's RULES table does not, and that "
            "asymmetry is a real defect of the same shape as the bisphenol "
            "A, pesticide and histamine gaps already recorded in that file. "
            "IT IS DELIBERATELY NOT CLOSED HERE, because adding a chlorate "
            "rule would fill the Pathogen and the gate would then PUBLISH "
            "this row, and this row should probably not be published. Read "
            "on fiche 24199 today (Fiche réf. 2026-11-0204 • Version 1, date "
            "27/11/2026): title \"Bouteille 1l25 Boisson rafraîchissante à "
            "l'extrait de thé noir, saveur pêche, avec sucre et "
            "édulcorant.\", brand line \"Fuze Tea Pêche\", \"Origine de la "
            "fiche: COCA-COLA EUROPEAN PARTNERS FRANCE\", Catégorie "
            "Alimentation / Sous-catégorie \"Boissons non alcoolisées\", "
            "GTIN \"05449000232250\" with lots \"207388\" and \"207389\" and "
            "\"Date de durabilité minimale 31/08/2027\", \"Date début/Fin de "
            "commercialisation: Du 18/08/2026 au 19/10/2026\", Zone \"France "
            "entière\", Motif \"Teneur en chlorate non conforme aux "
            "standards de Qualité\", \"Description complémentaire du "
            "risque\" the same sentence, and — decisively — \"Risques "
            "encourus par le consommateur: Autre risque\". The regulator "
            "files this under its own CATCH-ALL hazard class, not under a "
            "contaminant class, and its stated benchmark is a \"standards de "
            "Qualité\" — a quality standard — not a regulatory limit. AFTS "
            "scope takes chemical contaminants over a limit and excludes "
            "quality, and those two readings of this notice point opposite "
            "ways, so the call is George's and the row waits for it. For "
            "comparison, fiche 24205 of the same day says \"supérieure au "
            "seuil règlementaire\" and is published without hesitation. "
            "Nothing on this row is changed.]"),
    },
]

#: Rows whose Reason is translated above and may appear in another sheet too.
SUPERSEDE: list[dict] = []


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
    # IT WRITES BY DEFAULT. tools/rebase_and_verify.py re-runs every
    # tools/repair_*.py with NO ARGUMENTS; a script needing --apply would be a
    # silent no-op inside the gate and the gate would zip main's unrepaired
    # workbook with a green suite and nothing in the report to say so.
    ap.add_argument("--dry-run", action="store_true",
                    help="report only, write nothing")
    ap.add_argument("--apply", action="store_true",
                    help="accepted and ignored — writing is the default")
    args = ap.parse_args()

    import openpyxl

    # ── 0. the operator-found row, through the real code path ────────────
    #
    # tools/add_manual_row.py hands it to merge_master.append_to_pending, so
    # it gets the same dedup, the same validation and the same review stages
    # as a scraped row. Called from HERE rather than by hand because
    # rebase_and_verify re-runs this script and nothing else.
    if not args.dry_run:
        import subprocess
        rows_json = ROOT / "tools" / "manual_rows_2026_11_28.json"
        if rows_json.exists():
            r = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
                 str(rows_json)],
                cwd=str(ROOT), capture_output=True, text=True)
            tail = [ln for ln in r.stdout.splitlines() if ln.strip()][-7:]
            print("add_manual_row: " + " | ".join(tail))
            if r.returncode != 0:
                print("add_manual_row FAILED:\n" + r.stdout[-4000:]
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

    def _rows_at(ws, hdr, url):
        target = mm._normalize_url_for_dedup(url)
        return [r for r in range(2, ws.max_row + 1)
                if mm._normalize_url_for_dedup(_get(ws, hdr, r, "URL") or "")
                == target]

    # ── per-row edits ─────────────────────────────────────────────────────
    for spec in EDITS:
        name = spec["sheet"]
        ws, hdr = _sheet(name)
        if ws is None:
            report.append(f"  MISS  sheet {name} absent")
            continue
        hits = _rows_at(ws, hdr, spec["url"])
        if not hits:
            report.append(f"  MISS  {name}: no row at {spec['url'][:70]}")
            continue
        for r in hits:
            touched = False
            for col, (was, now) in spec.get("raw_fields", {}).items():
                if col not in hdr:
                    report.append(f"  MISS  {name} row {r}: no column {col}")
                    continue
                cur = _get(ws, hdr, r, col) or ""
                if cur == now:
                    continue                       # already right; no-op
                if cur != was:
                    report.append(
                        f"  SKIP  {name} row {r}.{col} (raw): on main it "
                        f"reads {cur[:60]!r}, expected {was[:60]!r}")
                    continue
                _set(ws, hdr, r, col, now)
                touched = True
                report.append(f"  SET   {name} row {r}.{col} (raw) -> "
                              f"{now[:70]!r}")
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

    # ── the same translated Reasons wherever else they appear ─────────────
    #
    # A fiche's row is often in Pending or Weekly_Review as well as Recalls.
    # Translating only the Recalls copy leaves the French one to be re-read by
    # the next reviewer pass and written back.
    # ONLY FOREIGN-LANGUAGE OLD VALUES PROPAGATE, and that guard is not
    # cosmetic — the --dry-run of this script caught it doing real damage.
    # The MPI row's old Reason was 'Cereulide — outbreak', a SHORT GENERIC
    # ENGLISH value that a completely unrelated published row also carries:
    # Recalls row 2530, FSANZ (AU) 2026-01-23, Sanulac Alula Gold infant
    # formula. Keying propagation on the value alone would have written
    # Nestlé New Zealand's Alfamino sentence onto an Australian recall from
    # ten months earlier — a fabricated Reason on a published row, which is
    # the one unforgivable error.
    #
    # The step exists to carry a TRANSLATION to a fiche's other copies, and a
    # French Motif is distinctive enough to key on safely. So the gate is the
    # same predicate the language tests use: propagate only where the OLD
    # value is itself not English. 'Cereulide — outbreak' is English and is
    # excluded automatically, by construction rather than by my remembering
    # to flag it.
    from pipeline._language import reason_needs_english as _needs_en
    reason_map = {}
    for spec in EDITS:
        f = spec.get("fields", {})
        if "Reason" in f:
            was, now = f["Reason"]
            if was and _needs_en(was):
                reason_map[_norm(was)] = now
            elif was:
                report.append(f"  HOLD  Reason {was[:40]!r} not propagated "
                              f"to other sheets: it reads as English, so the "
                              f"value is not distinctive enough to key on")
    for name in REASON_SHEETS:
        ws, hdr = _sheet(name)
        if ws is None or "Reason" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            cur = _get(ws, hdr, r, "Reason") or ""
            now = reason_map.get(_norm(cur))
            if now and _norm(cur) != _norm(now):
                _set(ws, hdr, r, "Reason", now)
                changed += 1
                report.append(f"  SET   {name} row {r}.Reason (same French "
                              f"value elsewhere) -> {now[:50]!r}")

    # ── stale cleanup stamps, swept against the gates' own predicates ─────
    #
    # add_manual_row's writer (merge_master._write_sheet) re-stamps every row
    # it writes whose field fails the predicate, and it runs BEFORE the
    # translations above. The sweep uses the SAME predicates the gates and the
    # tests use, so nothing is hidden: a field that still needs work keeps its
    # stamp, because the predicate still fires on it.
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
                    "[morning fix pass 2026-11-28: dropped "
                    f"{len(dropped)} stale cleanup stamp(s) — the field(s) "
                    "they name read in English under the same predicate the "
                    "publish gates and the language tests use.]")
                _set(ws, hdr, r, "Notes", out)
                swept += 1
                changed += 1
    if swept:
        report.append(f"  SWEEP {swept} row(s) lost a stale cleanup stamp")

    print(f"repair_2026_11_28: {changed} row edit(s)")
    for line in report:
        print(line)

    if args.dry_run:
        print("\n(--dry-run: nothing written)")
        return 0

    if changed:
        wb.save(path)
        print(f"wrote {path}")
        # recalls.json MUST mirror the Recalls sheet; mirror_json_from_xlsx is
        # the only sanctioned way to produce it. Without this the dashboard
        # and the language tests read the pre-repair values.
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"  recalls.json re-mirrored: "
              f"{mirror_json_from_xlsx(path, JSON_PATH)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
