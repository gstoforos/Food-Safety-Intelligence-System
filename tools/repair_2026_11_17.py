# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-17. Base: main at 671c418 (21:11 Athens).

Nothing from the eleven earlier nightly PRs (#35-#46) has reached main, so
yesterday's decisions were re-taken tonight against the regulator's own
pages where they resolve from this session (rappel.conso.gouv.fr,
recalls-rappels.canada.ca, fda.gov, thencc.org.za, aesan.gob.es WITHOUT the
"www." host) and re-applied here where they still hold. salute.gov.it,
foedevarestyrelsen.dk, recallswiss.admin.ch, blv.admin.ch, nafdac.gov.ng,
nvwa.nl and cdc.gov do not resolve from the session; webgate.ec.europa.eu
(RASFF) serves a JavaScript shell with no notification text.

Field corrections on live rows whose authority page resolves go through
pipeline/agents/curator.py with quoted evidence
(docs/data/agent-proposals/*-nightly-operator.json). This script does only
what the curator cannot:

A. TWENTY PENDING ROWS REFUSED — archived to Rejected with the reason, never
   deleted (append-only).
B. FIVE ARCHIVE VERDICTS THAT DO NOT HOLD — marked SUPERSEDED so the
   re-promotion guard releases fiches 24077, 24064, 24012, 24047 and 24097.
   Each was archived on a repairable field defect ("Pathogen is empty",
   Company "/"), a false reachability or URL-shape claim ("page is not
   found", "Official URL is a listing page" — both fiches read in full
   tonight), or no verdict at all. The three archive copies of fiche 24016
   are annotated to say the recall is published under its republication,
   fiche 24047.
C. THREE PUBLISHED Ministero della Salute ROWS DESCRIBE THE WRONG THING.
   salute.gov.it does not resolve from the session, so the PDFs themselves
   were not read; each correction is from the article the row was extracted
   from (ilfattoalimentare.it, named on the row) and each row is flagged
   NEEDS-HUMAN to confirm against the PDF.
     * Richiamo_salm scozz13112026: Company "NORVEL UAB" is a truncation of
       Norvelita UAB, Brand "Salmone Scozzese Affumicato" is the product
       name (the brand is La Nef), Pathogen carried the words "Possible
       presence", Reason carried the extractor's "(Recall ID AST-AN)".
     * Modulo richiamo Perfetto Moka e Macinato Universale nocciola: Product
       "Ground Coffee and Shelled Pistachios" merged two separate recalls —
       the Bialetti coffee (this PDF, ochratoxin A) and Ellegi Frutta Secca
       shelled pistachios (a different firm, a different PDF, aflatoxin B1).
       The pistachio recall is queued to Pending separately through
       tools/add_manual_row.py.
     * Modello_richiamo_Pomodorino: "Dried tomato" is fresh datterino
       tomatoes, 500 g, lot 4406, brand L'Ortofrutticola F (as in unmerged
       PR #46). The article was updated 17/11/2026 to say the producer is
       L'Ortofrutticola F S.r.l. and that the ministry notice named another
       firm "erroneously"; the register records the regulator's notice, so
       Company stays as the notice has it and the discrepancy is in Notes.
E. MAIN MOVED WHILE THIS REVIEW RAN. Commit 2b10982 (Tavily gap-finder,
   20:11 UTC) re-wrote the four NCC/NAFDAC Pending rows this review archived
   (same URLs, same content) and added one new Pending row: RASFF
   notification 470205 whose Product is the RASFF table's column headers
   ("Organisations · Reference · Subject · Product type · Notification
   basis …"), no Date, no firm. ``--phase merge --upstream <main's
   recalls.xlsx>`` copies every upstream Pending row this workbook lacks and
   has not decided tonight, value for value, so the branch can carry main's
   workbook forward without losing the pipeline's rows; the 470205 row is
   then archived with its reason (below).
F. THREE ARCHIVE ROWS SHARE A URL WITH A PUBLISHED RECALL AND SAY NOTHING
   (Eden Brothers duplicate; the two salute.gov.it listing-page rows). RED on
   main before this run; annotated in the reason column, nothing deleted.
D. RECALLS RE-SORTED NEWEST-FIRST. The promoter appends; eight RASFF rows
   published today sat at rows 2411-2418 below a 2026-01-02 row.

    python3 tools/repair_2026_11_17.py --dry-run
    python3 tools/repair_2026_11_17.py --phase a      # A + B + C, before the curator
    python3 tools/repair_2026_11_17.py --phase reject # A only;  --phase release = B;  --phase salute = C
    python3 tools/repair_2026_11_17.py --phase sort   # D, after the promoter
    python3 tools/repair_2026_11_17.py --phase merge --upstream /path/to/main-recalls.xlsx
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-17"
BY = "nightly operator review 2026-11-17"

# ── A. Pending rows refused ────────────────────────────────────────────────
PENDING_REJECTS = {
    "https://www.cdc.gov/ecoli/outbreaks/raw-milk-cheese-09-26/index.html":
        f"duplicate_of_published_recall — {BY}: CDC outbreak page for the E. coli "
        "O26:H11 raw-milk cheese outbreak. The recall is PUBLISHED as Sierra Nevada "
        "Cheese Company (FDA 2026-09-29, Outbreak=1); the FDA investigation page "
        "for the same outbreak was archived duplicate_of_published_recall "
        "2026-11-14, and the page read on 2026-11-16 says 'This outbreak is over' "
        "(investigation status Closed, Nov. 13, 2026). cdc.gov did not resolve "
        "tonight; the row's own Product is the single letter 'E'. Fourth archive "
        "copy of this URL.",
    "https://science.food.gov.uk/post/4364-launch-of-new-project-eu-harmonised-surveillance-of-antimicrobial-resistance-amr-e-coli-and-salmonella-found-in-fresh-beef-and-pork-meat-on-retail":
        f"not a recall — {BY}: an FSA science blog post announcing a surveillance "
        "project on antimicrobial-resistant E. coli and Salmonella in retail meat. "
        "No product, no firm, no lot; Product is the post title. The URL agent "
        "already marked the row rejected ('Not a food recall'); that verdict stands.",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/family-farmstead-dairy-announces-recall-of-all-cheddar-cheese-products-because-possible-health-risk":
        f"duplicate_of_published_recall — {BY}: CONSTRUCTED URL. This Google-News "
        "slug ('…recall-of-all-cheddar…') returns HTTP 404 tonight; the FDA "
        "notice's real address ('…recall-all-cheddar-cheese-products…') is "
        "PUBLISHED in Recalls as Family Farmstead Dairy, 'Raw Mild Cheddar; Raw "
        "Sharp Cheddar', Listeria monocytogenes, 2026-11-10. Company empty; "
        "Product and Reason are the WRNJ Radio headline.",
    "https://recalls-rappels.canada.ca/en/alert-recall/archibald-brand-nuit-blanche-white-ipa-and-desiree-session-ipa-recalled-due-pieces":
        f"duplicate_of_published_recall — {BY}: slug variant of CFIA notice "
        "RA-82940 (Archibald Inc., recall date 2026-11-06, original published "
        "2026-11-12, Class 3, Quebec, pieces of metal), PUBLISHED in Recalls under "
        "https://recalls-rappels.canada.ca/en/alert-recall/archibald-brand-nuit-"
        "blanche-white-ipa-recalled-due-pieces-metal. Both addresses were read "
        "tonight and show the same notice listing Nuit Blanche White IPA and "
        "Désirée Session IPA; the published row's Product was extended to both "
        "products through the curator. Company empty; Product and Reason are the "
        "headline.",
    "https://www.efsa.europa.eu/en/news/alternaria-toxins-food-and-feed-efsa-updates-its-risk-assessment":
        f"not a recall — {BY}: EFSA news item 'Alternaria toxins in food and feed: "
        "EFSA updates its risk assessment' (11 November 2026) is a risk-assessment "
        "update that names no product, firm or lot; Product is the page title and "
        "lead paragraph. Refused in its three AESAN language copies on 2026-11-14 "
        "and by the confirmer in Weekly_Rejected.",
    "https://rappel.conso.gouv.fr/affichettePDF/24016/Interne":
        f"duplicate — {BY}: the affichette (PDF poster) URL of RappelConso fiche "
        "24016 (Champibon dried morels 400 g, lot 262306, K 2 M Promocash, "
        "05/11/2026). That recall is PUBLISHED tonight under fiche 24047, the "
        "regulator's republication of 09/11/2026 (same product, brand, lot, GTIN "
        "3760201600023, distributor and Motif); fiche 24016 itself exists only in "
        "the archives. One recall, one row.",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/classic-delight-llc-issues-public-health-alert-due-possible-health-risk":
        f"stale — {BY}: the FDA notice read tonight is dated June 11, 2024 "
        "(company announcement June 10, 2024; Classic Delight LLC; individually "
        "wrapped sandwiches; Listeria monocytogenes). The row's Date 2026-11-09 is "
        "the date a news aggregator (WhatNow, via Google News) republished it. "
        "Product and Reason are the news headline. Outside the register's window.",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts?search_api_fulltext=H-0193-2027":
        f"duplicate_of_published_recall — {BY}: openFDA enforcement report "
        "H-0193-2027 (Lidl US Trading, Gelatelli Coffee & Vanilla Gelato with "
        "Coffee Sauce & Cookie Pieces, Listeria monocytogenes, recall initiated "
        "2026-10-16). The same recall is PUBLISHED as Lidl US, 'Coffee & Vanilla "
        "Gelato with Coffee Sauce & Cookie Pieces', Listeria monocytogenes, "
        "2026-10-17 under the FDA press-release URL. This row's URL is an FDA site "
        "search query, which the publish gate refuses, and the search page read "
        "tonight lists no results.",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/laubs-feeding-cubing-inc-voluntary-recalls-grade-mini-hay-cubes-because-potential-clostridium":
        f"not a food — {BY}: FDA page read tonight (publish date November 10, "
        "2026) lists the product type as 'Animal Feed' under Animal & Veterinary: "
        "Laub's Feeding & Cubing, Inc., Grade A Mini Hay Cubes, Clostridium "
        "botulinum, 'should not feed the product to their animals'. Human food only.",
    "https://alerts.food.gov.uk/article/3133/Prepworld-recalls-fruit-products-because-of-possible-contamination-with-Salmonella":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3133/ address is the same alert as the PUBLISHED FSA-PRIN-29-2026 "
        "(Prepworld RTE fruit products, Salmonella, 2026-06-23) and its CFS Hong "
        "Kong re-post of 2026-06-24. Already archived as duplicate_already_approved "
        "(operator review 2026-08-22); the Tavily fallback re-surfaced it with no "
        "Date. alerts.food.gov.uk refuses this session (HTTP 403).",
    "https://alerts.food.gov.uk/article/3176/OBrien-Fine-Foods-recalls-ham-products-because-of-possible-contamination-with-Listeria-monocytogenes":
        f"duplicate_of_published_recall — {BY}: the FSA double-URL trap. This "
        "/article/3176/ address is the same alert as the PUBLISHED FSA-PRIN-32-2026 "
        "(O'Brien Fine Foods ham products, Listeria monocytogenes, 2026-07-01). "
        "Already archived as duplicate_already_approved (operator review "
        "2026-08-22); re-surfaced by the Tavily fallback with no Date.",
    "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2025_06.htm":
        f"stale, fields fabricated — {BY}: read tonight at aesan.gob.es/en/alertas/"
        "2025_06 (the 'www.' host does not resolve from the session): alert "
        "ES2025/055 of 06/02/2025, 'Alerta por posible presencia de Escherichia "
        "coli productor de toxina Shiga (STEC) en quesos procedentes de Francia', "
        "Morbier DOP raw-milk cheese (Friesland Campina Iberia / Millan Vicente). "
        "A 2025 alert. The row's Product 'Alerta por presencia de Salmonella spp' "
        "and Pathogen 'Listeria monocytogenes' are the AESAN listing page's first "
        "item and a fabrication; no Date, no Company.",
    "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2026_24.htm":
        f"stale, fields fabricated — {BY}: read tonight at aesan.gob.es/en/alertas/"
        "2026_24: alert ES2026/161 of 26/03/2026, 'Alerta por presencia de "
        "Escherichia coli productora de toxinas Shiga (STEC) en queso Camembert "
        "procedente de Francia', Camembert de Normandie, brand La Réserve des "
        "Crémiers. Eight months old. The row's Product 'Alerta por presencia de "
        "Salmonella spp' and Pathogen 'Listeria monocytogenes' are the listing "
        "page's first item and a fabrication; no Date, no Company.",
    "https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/2026_12.htm":
        f"out of scope, stale, fields fabricated — {BY}: read tonight at "
        "aesan.gob.es/en/alertas/2026_12: alert ES2026/087 of 22/02/2026, 'Alerta "
        "por riesgo de asfixia en gominolas procedentes de Japón a través de Países "
        "Bajos', Sanrio Hello Kitty apple-konjac jelly, CHOKING hazard — not a "
        "pathogen, biotoxin, mycotoxin, foreign-material, pest or chemical hazard. "
        "The row's Product 'Alerta por presencia de Salmonella spp' and Pathogen "
        "'Listeria monocytogenes' are the listing page's first item and a "
        "fabrication; no Date, no Company.",
    "https://www.aesan.gob.es/actualidad/actualidad-noticias/Informaci-n-sobre-la-sospecha-de-casos-de-salmonelosis-asociados-a-huevos":
        f"not a recall notice — {BY}: an AESAN news note on suspected salmonellosis "
        "cases linked to eggs (Salmonella Enteritidis found in hen faeces on an "
        "Aragón farm). No product, firm, lot or recall; Product on the row is a "
        "sentence from the note. Already in Rejected under this URL (2026-10-16).",
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/babyvoeding-en-andere-speciale-voeding":
        f"not a recall — {BY}: the NVWA TOPIC page 'babyvoeding en andere speciale "
        "voeding' (a landing page). Product on the row is page furniture "
        "('24-03-2026 13:00 Beeld: © PureBudget Lees verder'); the cereulide item "
        "it skimmed is a March 2026 news entry saying infant-formula samples "
        "contained NO measurable cereulide. Already in Rejected under this URL.",
    "https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/":
        f"stale, terminal, upheld — {BY}: the NCC media statement read tonight is "
        "posted October 3, 2024 about a recall notified on 16 September 2024 (BM "
        "Foods / Shoprite Checkers Deli Hummus range, sell-by 10 September-08 "
        "October 2024, Listeria monocytogenes). The row's Date 2026-11-12 is a "
        "news-discovery date the statement does not carry. Operator review "
        "2026-11-06 ruled this URL TERMINAL; at least the thirteenth arrival. "
        "NOTE: tonight this row passed the full publish gate (promote_gate_passing "
        "dry run listed it as the only promotable row) — only the re-promotion "
        "guard's terminal verdict stood between a 2024 recall with a 2026 date and "
        "the public register.",
    "https://nafdac.gov.ng/public-alert-no-030-2026-alert-on-the-withdrawal-of-levamisole-containing-medicines-as-endorsed-by-the-italian-medicines-agency-aifa-over-serious-neurological-risks/":
        f"not a food — {BY}: NAFDAC Public Alert 030/2026 on the withdrawal of "
        "levamisole-containing MEDICINES as endorsed by the Italian Medicines "
        "Agency (AIFA) over neurological risks. A drug withdrawal; Pathogen empty. "
        "nafdac.gov.ng does not resolve from the session; the row's own content "
        "settles it. Archived five times before.",
    "https://nafdac.gov.ng/public-alert-no-034-2026-niger-republic-imposes-import-ban-on-bulmex-parboiled-rice-from-india-due-to-metallic-contamination/":
        f"not a recall — {BY}: NAFDAC Public Alert 034/2026 relays the Niger "
        "Republic's IMPORT BAN on Bulmex parboiled rice from India; it is not a "
        "Nigerian recall or withdrawal notice, names no firm (Company empty), and "
        "dates from August 2026 (archive copy Rejected 2026-08-13) while the row "
        "says 2026-11-11, the date a news site mentioned it. nafdac.gov.ng does not "
        "resolve from the session. Fourth arrival.",
    "https://nafdac.gov.ng/public-alert-no-043-2026-nafdac-places-products-marketed-by-mofus-nigeria-ltd-on-watchlist-pending-regulatory-investigation/":
        f"not a recall — {BY}: NAFDAC Public Alert 043/2026 places a marketer's "
        "products 'on watchlist pending regulatory investigation'. No product "
        "(Product empty), no hazard (Pathogen empty). nafdac.gov.ng does not "
        "resolve from the session; the row's own content settles it. Archived "
        "seven times before.",
}

# ── B. archive verdicts that do not hold ───────────────────────────────────
RELEASE = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24077/interne":
        "this archive row records no verdict on the recall, only the confirm "
        "agent's pass-through and the publish gate's repairable field defect "
        "(Company and Brand '/'). Fiche 24077 was read in full tonight: REBLOCHON "
        "DE SAVOIE au lait cru, origin GAEC LA LOUISA, 13/11/2026, Risques "
        "'Salmonella spp (agent responsable de la salmonellose)', Motif 'retrait "
        "par mesure de précaution dans un contexte suspicion salmonelle'. The "
        "curator-corrected Pending row passes the full publish gate and is the "
        "copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne":
        "this archive row records no verdict on the recall, only the confirm "
        "agent's pass-through and the publish gate's repairable field defect "
        "(Company and Brand '/'). Fiche 24064 was read in full tonight: REBLOCHON "
        "DE SAVOIE FERMIER 450g, GAEC LES DEUX SAVOIES, 11/11/2026, Risques "
        "'Escherichia coli shiga toxinogène (STEC)', Motif 'Présence de "
        "Escherichia Coli STEC'. The curator-corrected Pending row passes the "
        "full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24012/interne":
        "'Pathogen is empty' was a repairable field defect, now filled from the "
        "fiche's own Motif; 'the page is not found' is false — fiche 24012 was "
        "read in full tonight (CHAMPIGNONS MORILLES SECHEES 60G, CHAMPIBON, B&M "
        "France SAS, version 2, 04/11/2026, tous les lots, GTIN 3760201609743); "
        "'confirmer did not re-review' records that nobody looked. The "
        "curator-corrected Pending row passes the full publish gate and is the "
        "copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24047/interne":
        "'Pathogen is empty' was a repairable field defect, now filled from the "
        "fiche's own Motif; reviewer 1's 'No official regulator URL found' was "
        "refused twice by the url-guard as false. Fiche 24047 was read in full "
        "tonight (fiche réf. 2026-11-0060, version 1, 09/11/2026: CHAMPIGNONS "
        "MORILLES SECHEES, CHAMPIBON, 400g, lot 262306, GTIN 3760201600023, "
        "origine K 2 M PROMOCASH, Motif 'probable contamination d'un lot de "
        "morilles par une toxine susceptible d'être à l'origine d'intoxication "
        "alimentaire'). It is the regulator's republication of fiche 24016 and the "
        "only live copy of that recall. The curator-corrected Pending row passes "
        "the full publish gate and is the copy that wins",
    "https://rappel.conso.gouv.fr/fiche-rappel/24097/interne":
        "'Official URL is a listing page, not the specific recall page' is false — "
        "https://rappel.conso.gouv.fr/fiche-rappel/24097/interne is fiche réf. "
        "2026-11-0107, read in full tonight (Fromage blanc Belle France 0% and 3%, "
        "500 g and 1 kg, EVEN LAIT INDUSTRIE, 16/11/2026, Risques 'Autres "
        "contaminants chimiques', Motif 'Possible contamination par des produits "
        "de nettoyage à des doses ne présentant pas de risque pour la santé du "
        "consommateur'). The curator-corrected Pending row passes the full publish "
        "gate and is the copy that wins",
}

# The three archive copies of fiche 24016 are not contradicted by anything
# published under their URL; they are annotated so a reader knows where the
# recall went. Status is left alone.
ANNOTATE = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24016/interne":
        f"[annotated {TODAY} by the {BY}: this fiche (05/11/2026, version 1) is "
        "the first publication of the Champibon dried-morel recall (400 g, lot "
        "262306, GTIN 3760201600023, K 2 M Promocash). It was archived on the "
        "repairable defect 'Pathogen is empty' and never returned to Pending; the "
        "regulator's republication, fiche 24047 (09/11/2026), is the copy "
        "PUBLISHED tonight. Kept as the audit trail.] ",
}

# ── C. the three Ministero della Salute rows ───────────────────────────────
SALUTE_FIXES = [
    {
        "url": ("https://www.salute.gov.it/new/sites/default/files/external_data/"
                "avvisi_sicurezza_alimentare/Richiamo_salm%20scozz13112026_1794825190.pdf"),
        "expect": {"Company": "NORVEL UAB", "Product": "Smoked Scottish Salmon"},
        "set": {
            "Company": "Norvelita UAB",
            "Brand": "La Nef",
            "Product": ("Smoked Scottish salmon (salmone scozzese affumicato), 100 g, "
                        "lot 170794, use-by 18/11/2026, marketed by La Nef S.p.A."),
            "Pathogen": "Listeria monocytogenes",
            "Reason": "Possible presence of Listeria monocytogenes",
        },
        "note": (
            f"[{BY}: Company 'NORVEL UAB' was a truncation of the producer's name, "
            "Brand 'Salmone Scozzese Affumicato' was the product name, Pathogen "
            "carried 'Possible presence' and Reason the extractor's '(Recall ID "
            "AST-AN)'. The row's own discovery source, ilfattoalimentare.it "
            "'Salmone scozzese affumicato richiamato per possibile Listeria' "
            "(16/11/2026), quotes the ministry notice: marchio La Nef, prodotto da "
            "Norvelita Uab (Pramonės g. 1, Gabšių k., Raseinių r., Lituania, LT "
            "72-01 EB), commercializzato da La Nef Spa, 100 grammi, lotto 170794, "
            "scadenza 18/11/2026, 'possibile presenza di Listeria monocytogenes', "
            "notice dated 13 novembre, published by the ministry 16 novembre 2026. "
            "salute.gov.it does not resolve from this session, so the PDF itself "
            "was not read; NEEDS-HUMAN to confirm the PDF matches.]"),
    },
    {
        "url": ("https://www.salute.gov.it/new/sites/default/files/external_data/"
                "avvisi_sicurezza_alimentare/Modulo%20richiamo%20Perfetto%20Moka%20e"
                "%20Macinato%20Universale%20nocciola_1794593365.pdf"),
        "expect": {"Product": "Ground Coffee and Shelled Pistachios"},
        "set": {
            "Product": ("Perfetto Moka nocciola and Macinato Universale nocciola "
                        "hazelnut-flavoured ground coffee, 250 g, lots LC5 BD160B "
                        "(both) and LC5 BD161B (Perfetto Moka), best-before "
                        "09/06/2027 and 10/06/2027"),
            "Pathogen": "Ochratoxin A",
            "Reason": "Ochratoxin A above the legal limit",
        },
        "note": (
            f"[{BY}: Product 'Ground Coffee and Shelled Pistachios' merged two "
            "separate ministry recalls reported in one article. THIS PDF (its own "
            "filename: 'Modulo richiamo Perfetto Moka e Macinato Universale "
            "nocciola') is the Bialetti coffee recall; the shelled pistachios "
            "(pistacchi sgusciati, Ellegi Frutta Secca Sas, Rome, 50 g, lot 9.25.1, "
            "TMC 05/02/2027, aflatoxin B1) are a different firm's recall with its "
            "own ministry PDF and are queued to Pending separately tonight. The "
            "row's own discovery source, ilfattoalimentare.it 'Caffè Bialetti "
            "macinato e pistacchi sgusciati richiamati per micotossine' "
            "(16/11/2026), gives for the coffee: Perfetto Moka nocciola and "
            "Macinato Universale nocciola, 250 g, Bialetti Industrie Spa "
            "(Coccaglio, Brescia), lots LC5 BD160B (both) and LC5 BD161B (Perfetto "
            "Moka), TMC 09/06/2027 and 10/06/2027, 'ocratossina A' above the "
            "limits. Pathogen normalised to the agent; the lot codes the extractor "
            "wrote into Reason as 'Recall ID' moved to Product. salute.gov.it does "
            "not resolve from this session; NEEDS-HUMAN to confirm the PDF matches.]"),
    },
    {
        "url": ("https://www.salute.gov.it/new/sites/default/files/external_data/"
                "avvisi_sicurezza_alimentare/Modello_richiamo_Pomodorino_1794652742.pdf"),
        "expect": {"Product": "Dried tomato"},
        "set": {
            "Brand": "L'Ortofrutticola F",
            "Product": ("Fresh datterino tomatoes (pomodoro datterino, 'Pomodori "
                        "freschi siciliani - selezionati con amore'), 500 g, lot 4406, "
                        "barcode 8056590100820"),
        },
        "note": (
            f"[{BY}: Product was published as 'Dried tomato' with no Brand. The "
            "row's own discovery source, ilfattoalimentare.it 'Salmonella nel "
            "pomodoro datterino: richiamato un lotto prodotto in Sicilia' "
            "(15/11/2026, updated 17/11/2026), quotes the ministry notice: pomodoro "
            "datterino, 'pomodoro fresco di categoria I', marchio L'Ortofrutticola "
            "F, confezioni da 500 grammi, lotto 4406, codice a barre 8056590100820, "
            "'presenza di Salmonella spp.' — FRESH datterino tomatoes, not dried. "
            "The 17/11/2026 update says the tomatoes are 'confezionato e "
            "commercializzato da L'Ortofrutticola F S.r.l., di Niscemi "
            "(Caltanissetta)' and that the ministry notice named another producer "
            "'erroneamente'; the register records the regulator's notice, so "
            "Company (Mediterranea Snc di Milanese Rosanna & C.) is left as the "
            "notice has it. salute.gov.it does not resolve from this session, so "
            "the PDF itself was not read; NEEDS-HUMAN to confirm the PDF and "
            "decide whether the ministry has corrected the producer.]"),
    },
]


def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _rows_for(ws, url):
    hdr = _hdr(ws)
    col = hdr.index("URL") + 1
    want = _key(url)
    return [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == want]


def _row_dict(ws, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(_hdr(ws)) if h}


def _archive(wb, row: dict, reason: str) -> None:
    """Append to Rejected. Append-only; nothing in an archive is deleted."""
    arch = wb["Rejected"]
    out = []
    for h in _hdr(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            v = row.get(h, "")
            out.append("" if v is None else v)
    arch.append(out)


def reject_pending(wb, dry: bool) -> int:
    ws = wb["Pending"]
    n = 0
    for url, reason in PENDING_REJECTS.items():
        rows = _rows_for(ws, url)
        if not rows:
            print(f"    Pending row not present (nothing to archive): {url[-60:]}")
            continue
        for r in sorted(rows, reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} -> Rejected")
            if not dry:
                _archive(wb, row, reason)
                ws.delete_rows(r, 1)
            n += 1
    return n


def release_false_refusals(wb, dry: bool) -> int:
    n = 0
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        ws = wb[sheet]
        hdr = _hdr(ws)
        ci = hdr.index(col) + 1
        for url, why in RELEASE.items():
            for r in _rows_for(ws, url):
                cur = str(ws.cell(r, ci).value or "")
                if cur.strip().lower() in ("none", "nan"):
                    cur = ""
                if "SUPERSEDED" in cur:
                    continue
                stamp = (f"[SUPERSEDED {TODAY} by the {BY} — {why}. This row "
                         f"is kept as the audit trail of the original refusal.] ")
                print(f"    {sheet} row {r}: {url[-30:]} marked SUPERSEDED")
                if not dry:
                    ws.cell(r, ci).value = stamp + cur
                    if "Reviewed" in hdr:
                        ws.cell(r, hdr.index("Reviewed") + 1).value = "SUPERSEDED"
                    if "Status" in hdr and sheet == "Rejected":
                        ws.cell(r, hdr.index("Status") + 1).value = "SUPERSEDED"
                n += 1
        for url, text in ANNOTATE.items():
            for r in _rows_for(ws, url):
                cur = str(ws.cell(r, ci).value or "")
                if cur.strip().lower() in ("", "none", "nan"):
                    cur = ""
                if text[:40] in cur:
                    continue
                print(f"    {sheet} row {r}: {url[-30:]} annotated")
                if not dry:
                    ws.cell(r, ci).value = text + cur
                n += 1
    return n


def fix_salute_rows(wb, dry: bool) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    n = 0
    for fx in SALUTE_FIXES:
        rows = _rows_for(ws, fx["url"])
        if len(rows) != 1:
            raise SystemExit(f"expected exactly one Recalls row for "
                             f"{fx['url'][-50:]}, found {len(rows)}")
        r = rows[0]
        notes = str(ws.cell(r, hdr.index("Notes") + 1).value or "")
        if fx["note"][:60] in notes:
            print(f"    Recalls row {r}: already corrected")
            continue
        for k, want in fx["expect"].items():
            cur = str(ws.cell(r, hdr.index(k) + 1).value or "")
            if cur.strip().casefold() != want.casefold():
                raise SystemExit(f"Recalls row {r} {k} is {cur!r}, expected "
                                 f"{want!r} — not touching it")
        for k, v in fx["set"].items():
            cur = ws.cell(r, hdr.index(k) + 1).value
            print(f"    Recalls row {r}: {k} {str(cur)[:45]!r} -> {v[:60]!r}")
            if not dry:
                ws.cell(r, hdr.index(k) + 1).value = v
        if not dry:
            ws.cell(r, hdr.index("Notes") + 1).value = (notes + " " + fx["note"]).strip()
            ws.cell(r, hdr.index("LastUpdated") + 1).value = TODAY
        n += 1
    return n


# ── F. archive rows that share a URL with a published recall and say nothing ─
# tests/test_a_recall_is_not_both_published_and_rejected.py is RED on main (as
# it was on 2026-11-15 and 2026-11-16; those fixes sit in unmerged PRs #45/#46).
# The verdict is written where the contradiction check reads it, nothing is
# deleted. Texts as in PR #46, re-dated.
CONTRADICTIONS = {
    ("Rejected", "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/eden-brothers-recalls-calabrese-broccoli-seeds-due-possible-salmonella-contamination"):
        f"[duplicate — annotated {TODAY} by the {BY}: the published copy wins. "
        "The same FDA URL is published as Eden Brothers LLC, 'Broccoli Seeds - "
        "Calabrese', Salmonella, 2026-10-07. This archive row's own Notes already "
        "say 'duplicate: this URL is already published on 2026-10-07', but in "
        "Notes, which the contradiction check does not read; its Product is the "
        "notice's first paragraph. Kept as the audit trail.]",
    ("Rejected", "https://www.salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari/?tab=1&offset=0&pageElements=10"):
        f"[unresolved contradiction of provenance — annotated {TODAY} by the {BY}: "
        "this archived row is the Ministero della Salute LISTING page scraped as if "
        "it were a notice (Product is the page title 'Avvisi e richiami di prodotti "
        "alimentari', no firm, no hazard, discovered via ilmessaggero.it 2026-11-14). "
        "The same listing URL, without the query string, is carried by the published "
        "La Vacje Salumerie row of 2026-04-14, which cites an index page as its "
        "authority URL. This row is not a copy of that recall and is not a recall; "
        "the published row stands and its own URL remains to be resolved.]",
    ("Weekly_Rejected", "https://www.salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari/?tab=1&offset=0&pageElements=10"):
        f"[unresolved contradiction of provenance, not a copy of the published recall — annotated {TODAY} by the {BY}: this "
        "row is a news-only discovery (ilmessaggero.it, 2026-11-14, ground coffee "
        "recalled for a chemical risk) that was correctly refused for having no "
        "authority URL; the URL it carries is the ministry's LISTING page with a "
        "pagination query string. The published La Vacje Salumerie row of 2026-04-14 "
        "cites the same index page as its authority URL, which is why the two "
        "'share' a URL. Different recall, different product; the published row "
        "stands.] ",
}


def annotate_silent_contradictions(wb, dry: bool) -> int:
    n = 0
    for (sheet, url), text in CONTRADICTIONS.items():
        ws = wb[sheet]
        hdr = _hdr(ws)
        col = "RejectReason" if sheet == "Rejected" else "RejectionReason"
        ci = hdr.index(col) + 1
        for r in _rows_for(ws, url):
            cur = str(ws.cell(r, ci).value or "")
            if cur.strip().lower() in ("", "none", "nan"):
                cur = ""
            if text[:40] in cur:
                continue
            print(f"    {sheet} row {r}: {url[-40:]} annotated")
            if not dry:
                ws.cell(r, ci).value = text + cur
            n += 1
    return n


# ── E. upstream Pending rows that arrived during the review ────────────────
UPSTREAM_REJECTS = {
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/470205":
        f"not a usable row — {BY}: Product is the RASFF Window listing table's "
        "column headers ('Organisations · Reference · Subject · Product type · "
        "Notification basis …'), not a product; no Date, no firm. Notification id "
        "470205 belongs to the pre-2022 numbering (tonight's live RASFF rows are "
        "ids 882171-886913), so whatever it points at is years old. "
        "webgate.ec.europa.eu serves only a JavaScript shell to this session, so "
        "the page could not be read; the row's own content settles it. Arrived "
        "from the standalone Tavily gap-finder at 20:11 UTC while this review ran.",
}


def merge_upstream_pending(wb, upstream_xlsx: str, dry: bool) -> int:
    """Append every upstream Pending row this workbook lacks, value for value."""
    up = openpyxl.load_workbook(upstream_xlsx, read_only=True, data_only=True)
    uws = up["Pending"]
    urows = list(uws.iter_rows(values_only=True))
    uhdr = [str(h or "") for h in urows[0]]
    ws = wb["Pending"]
    hdr = _hdr(ws)
    if uhdr != hdr:
        raise SystemExit(f"Pending header differs upstream: {uhdr} vs {hdr}")
    have = {_key(ws.cell(r, hdr.index("URL") + 1).value)
            for r in range(2, ws.max_row + 1)}
    # Every URL this review decided tonight is also "had": it must not come
    # back from the upstream copy of the pre-review queue.
    arch = wb["Rejected"]
    ah = _hdr(arch)
    have |= {_key(arch.cell(r, ah.index("URL") + 1).value)
             for r in range(2, arch.max_row + 1)
             if str(arch.cell(r, ah.index("RejectedBy") + 1).value or "") == BY}
    rec = wb["Recalls"]
    rh = _hdr(rec)
    have |= {_key(rec.cell(r, rh.index("URL") + 1).value)
             for r in range(2, rec.max_row + 1)}
    n = 0
    for r in urows[1:]:
        d = dict(zip(uhdr, r))
        if _key(d.get("URL")) in have:
            continue
        print(f"    upstream Pending row copied: {d.get('Source')} "
              f"{str(d.get('Product'))[:40]!r} {str(d.get('URL'))[-50:]}")
        if not dry:
            ws.append([v for v in r])
        n += 1
    up.close()
    return n


def reject_upstream(wb, dry: bool) -> int:
    ws = wb["Pending"]
    n = 0
    for url, reason in UPSTREAM_REJECTS.items():
        for r in sorted(_rows_for(ws, url), reverse=True):
            row = _row_dict(ws, r)
            print(f"    Pending row {r}: {row['Source']} "
                  f"{str(row['Product'])[:50]!r} -> Rejected")
            if not dry:
                _archive(wb, row, reason)
                ws.delete_rows(r, 1)
            n += 1
    return n


def sort_recalls(wb, dry: bool) -> int:
    """Stable newest-first sort of Recalls by Date; explicit cell assignment."""
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    di = hdr.index("Date")
    rows = [list(r) for r in ws.iter_rows(min_row=2, values_only=True)]

    def _k(r):
        d = r[di]
        if d is None or d == "":
            return ""
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        return str(d)[:10]

    ordered = sorted(rows, key=_k, reverse=True)      # sorted() is stable
    moved = sum(1 for a, b in zip(rows, ordered) if a is not b)
    print(f"    Recalls: {len(rows)} rows, {moved} change position")
    if not dry and moved:
        for i, r in enumerate(ordered, start=2):
            for j, v in enumerate(r, start=1):
                cell = ws.cell(i, j)
                if cell.value != v:
                    cell.value = v
    return moved


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--phase", choices=("a", "reject", "release", "salute", "annotate", "sort", "merge"),
                    default="a", help="a = reject + release + salute")
    ap.add_argument("--upstream", help="main's recalls.xlsx for --phase merge")
    a = ap.parse_args(argv)
    wb = openpyxl.load_workbook(XLSX)
    if a.phase in ("a", "reject"):
        print("A  Pending rows refused:")
        print("   archived: %d" % reject_pending(wb, a.dry_run))
    if a.phase in ("a", "release"):
        print("B  earlier refusals that do not hold:")
        print("   archive rows marked SUPERSEDED / annotated: %d"
              % release_false_refusals(wb, a.dry_run))
    if a.phase in ("a", "salute"):
        print("C  the three Ministero della Salute rows:")
        print("   rows corrected: %d" % fix_salute_rows(wb, a.dry_run))
    if a.phase in ("a", "annotate"):
        print("F  archive rows sharing a URL with a published recall, silent:")
        print("   rows annotated: %d" % annotate_silent_contradictions(wb, a.dry_run))
    if a.phase == "sort":
        print("D  Recalls newest-first:")
        print("   rows moved: %d" % sort_recalls(wb, a.dry_run))
    if a.phase == "merge":
        if not a.upstream:
            raise SystemExit("--phase merge needs --upstream <xlsx>")
        print("E  upstream Pending rows added while the review ran:")
        print("   copied: %d" % merge_upstream_pending(wb, a.upstream, a.dry_run))
        print("   refused: %d" % reject_upstream(wb, a.dry_run))
    if not a.dry_run:
        wb.save(XLSX)
    wb.close()
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
