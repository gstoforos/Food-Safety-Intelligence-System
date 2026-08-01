#!/usr/bin/env python3
"""Data repair — audit 2026-08-02. Supersedes fix_auxico_duplicate.py.

Every change is justified against a source that was actually read. Nothing is
inferred from a pattern; nothing is deleted — removed rows go to
Weekly_Rejected carrying their reason.

════════════════════════════════════════════════════════════════════════════
PART A — SEVEN ALLERGEN-ONLY ROWS  →  Weekly_Rejected  (out of AFTS scope)
════════════════════════════════════════════════════════════════════════════
The scope rule is not new. It is printed at the foot of every daily brief:

    "Pathogens + biotoxins + mycotoxins + foreign material + pest + chemical
     hazards only. Allergen-only, labeling, quality issues excluded per AFTS
     scope."

and it was written down as policy on 2026-07-29 in
pipeline/fix_allergen_rows.py, which removed two FSANZ rows BY EXACT URL and
was never wired into any gate. One of those two — Auxico — came straight back
and, on 2026-08-01, went out to subscribers with an invented Listeria at
Tier 1. Seven such rows were in Recalls when this audit ran:

    2026-07-29  FSANZ  Auxico (Perth) Pty Ltd ......... undeclared peanuts
    2026-07-24  FSANZ  Viet Meatballs ................. undeclared gluten
    2026-07-17  FSANZ  Truong Ton Group ............... undeclared wheat
    2026-06-30  FSANZ  Kaisi Melbourne Pty Ltd ........ undeclared egg
    2026-05-13  FSANZ  Murray River Smokehouse ........ undeclared milk
    2026-04-21  FSANZ  Greenstorm Foods / Austral Herbs undeclared peanut
    2026-04-01  BLV    DAISY IMPORT-EXPORT ROSHANI .... undeclared sulfites

Two of the seven had already been flagged in their own Notes and left in
place:
    Greenstorm — "out-of-scope per locked Tier-1 rule (allergens excluded),
                  kept at Tier 3"
    BLV        — "SCOPE FLAG: SULFITES are allergens — explicitly EXCLUDED
                  from _pathogen_scope.py per locked rules; pipeline
                  scope-gate failure; RECOMMEND DROP from Recalls"

The rule now lives in pipeline/_publish_gate.py (rule 8), so it runs on every
promotion path instead of being a script somebody has to remember to run.

════════════════════════════════════════════════════════════════════════════
PART B — THE AUXICO DUPLICATE  →  Weekly_Rejected
════════════════════════════════════════════════════════════════════════════
FSANZ republishes an amended alert at a new slug ("updated-300726-<slug>")
and keeps both pages live, so one recall became two rows. The second carried
Pathogen "Listeria monocytogenes" — verified 2026-08-02, that word appears
nowhere on either FSANZ page — and Company held the page's own <h1> status
banner. Archived here; the dedup collapse is fixed in merge_master.

════════════════════════════════════════════════════════════════════════════
PART C — TEN CONTAMINATED RappelConso ROWS  →  repaired from the official
         French open-data record
════════════════════════════════════════════════════════════════════════════
rappel.conso.gouv.fr itself is unreachable from the audit environment (its
robots.txt fetch fails TLS verification), which is why these rows sat
unrepaired in the previous pass. They ARE reachable another way: the DGCCRF
publishes every fiche as open data, queryable per-record through the
data.gouv.fr tabular API:

    https://tabular-api.data.gouv.fr/api/resources/
        5a4e7174-657c-4920-af1f-3440a996837c/data/?id__exact=<fiche>

`id` in that dataset is exactly the fiche number in the rappel.conso URL.
Each record below was fetched individually on 2026-08-02 and is quoted
verbatim in FR_TRUTH so the repair can be re-checked without trusting this
script.

WHAT WAS WRONG. Each row's Reason had been overwritten with a NEIGHBOUR's —
the url-gate batch mis-attribution documented in tests/test_url_gate_identity.py
— and six of them also had RASFF's "Origin: X | Notifying: Y" convention
written into Company, on a French fiche, plus a category placeholder
("produits de la pêche et d'aquaculture (RappelConso fiche NNNNN)") in
Product. In every case the PATHOGEN was right and everything around it was
someone else's recall.

WHAT IS REPAIRED, and from which source field:
    Reason   <- motif_rappel        (translated)
    Brand    <- marque_produit
    Company  <- distributeurs       ONLY where Company currently holds the
                                    RASFF convention, which is provably wrong
                                    on a French fiche. Where Company already
                                    names a real French manufacturer (HERTA,
                                    Ferme Baracand, FOODMAKER, Salaisons
                                    Jouvin) it is left exactly as it is.
    Product  <- modeles_ou_references (translated) ONLY where Product is
                                    currently the category placeholder.
    Class    <- nature_juridique_rappel
    Pathogen <- NOT TOUCHED. It was already correct on all ten, and
                risques_encourus confirms each one.

FICHE 22205 IS NOT REPAIRED. It is absent from the open-data export (the ids
jump 22204 → 22206), so there is no authoritative record to repair it from.
It keeps its contradiction and stays pinned in the tests.

AN ELEVENTH ROW, FOUND BY ACCIDENT. Fiche 22184 (Salaisons Jouvin, merguez)
carries Reason 'Germina brand "Brocoli Calabrese" seeds recalled due to
possible contamination with pathogenic E. coli' while the official record
says "présence de salmonelle". The hazard-class guard cannot see this one —
Salmonella and E. coli are both biological, so the classes overlap and the
rule correctly stays silent. It was only visible by reading the source.
That is the whole argument for pipeline/verify_rappelconso.py.

════════════════════════════════════════════════════════════════════════════
PART D — RappelConso fiche 23067, Company/Brand = the literal string "0"
════════════════════════════════════════════════════════════════════════════
Still not repairable. Fiches 23060 and 23067 are 2026-07-31 recalls and the
open-data export currently ends at fiche 22990 (2026-07-24) — normal
publication lag, not an error. The honest placeholder stays and the row keeps
its OPERATOR ACTION flag until the export catches up, at which point
verify_rappelconso.py will fill it in automatically.

Run:  python -m pipeline.fix_audit_2026_08_02 --dry-run
      python -m pipeline.fix_audit_2026_08_02
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TODAY = "2026-08-02"

# ── PART A ──────────────────────────────────────────────────────────────────
FSANZ = "https://www.foodstandards.gov.au/food-recalls/recall-alert/"
BLV_SULFITES = ("https://www.blv.admin.ch/dam/blv/de/dokumente/rueckrufe/"
                "rr-sulfite.pdf.download.pdf/sulfites.pdf")

ALLERGEN_ONLY = {
    FSANZ + "auxico-perth-pty-ltd-lgm-hot-chilli-oil-275g":
        "undeclared peanuts (FSANZ Problem: 'The recall is due to the presence "
        "of an undeclared allergen (peanuts).')",
    FSANZ + "viet-meatballs-chinese-sausage-500g":
        "undeclared gluten",
    FSANZ + "truong-ton-group-vietnamese-sausage-500g":
        "undeclared wheat",
    FSANZ + "kaisi-melbourne-pty-ltd-wu-xian-zhai-soybean-snacks-five-spice-flavour":
        "undeclared egg",
    FSANZ + "murray-river-smokehouse-turkey-bacon-150g":
        "undeclared milk",
    FSANZ + "greenstorm-foods-ta-austral-herbs-certified-organic-garlic-powder-250g":
        "undeclared peanut — verified 2026-08-02 against the live FSANZ page, "
        "Problem reads 'The presence of an undeclared allergen (Peanut).' and "
        "no microbial pathogen is named; the row's own Notes already said "
        "'out-of-scope per locked Tier-1 rule (allergens excluded)'",
    BLV_SULFITES:
        "undeclared sulfites — the row's own Notes already said 'SCOPE FLAG: "
        "SULFITES are allergens ... pipeline scope-gate failure; RECOMMEND "
        "DROP from Recalls'",
}
ALLERGEN_REASON = (
    "Out of AFTS scope: allergen-only recall ({detail}). Policy 2026-07-29 — "
    "allergen-only, labelling and quality recalls are excluded; the database "
    "covers pathogens, biotoxins, mycotoxins, foreign material, pest and "
    "chemical hazards. Enforced from 2026-08-02 by rule 8 of "
    "pipeline/_publish_gate.py so it can no longer depend on a one-off script."
)

# ── PART B ──────────────────────────────────────────────────────────────────
FSANZ_DUP = FSANZ + "updated-300726-auxico-perth-pty-ltd-lgm-hot-chilli-oil-275g"
DUP_REASON = (
    "Duplicate of the same FSANZ recall (auxico-perth-pty-ltd-lgm-hot-chilli-"
    "oil-275g) — FSANZ republishes an amended alert at a new slug and keeps "
    "both pages live. Also carried a FABRICATED Pathogen: verified 2026-08-02 "
    "against both live FSANZ pages, the hazard is an undeclared peanut "
    "allergen and the word 'Listeria' appears nowhere on either page. Company "
    "held FSANZ's <h1> status banner 'UPDATED 30.07.26 | '. Rejected by "
    "claude-check on 2026-07-31 for 'pathogen mismatch', re-promoted by the "
    "Qwen review agent on 2026-08-01 at 14:16 UTC, and mailed to subscribers. "
    "The surviving row is itself out of scope (allergen-only) and is archived "
    "in the same pass."
)

# ── PART C ──────────────────────────────────────────────────────────────────
# Verbatim from the DGCCRF open-data record, fetched per fiche on 2026-08-02
# via the data.gouv.fr tabular API. Quoted so the repair is auditable without
# re-running the fetch.
FR_TRUTH = {
    "21975": dict(
        marque="auchan le poissonnier",
        modeles="filet de maquereau sous vide",
        motif="suite à la détection d'histamine",
        risque="toxines endogènes : histamine",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="uniquement auchan supermarché merignac - robinson",
        brand="Auchan Le Poissonnier",
        company="Auchan Supermarché Mérignac – Robinson",
        product="Vacuum-packed mackerel fillet (filet de maquereau sous vide)",
        reason="Histamine detected in the product",
        klass="Voluntary"),
    "21987": dict(
        marque="sans marque",
        modeles="longe de thon albacore vendu au rayon libre service attenant "
                "au rayon traditionnel",
        motif="présence d'histamine",
        risque="toxines endogènes : histamine",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="carrefour market louhans uniquement",
        brand="Unbranded",
        company="Carrefour Market Louhans",
        product="Albacore tuna loin sold at the self-service counter adjoining "
                "the traditional counter",
        reason="Presence of histamine",
        klass="Voluntary"),
    "22067": dict(
        marque="sans marque",
        modeles="tellines conditionnées le 15/04/2026 et 16/04/2026",
        motif="fermeture zone de pêche",
        risque="biotoxines marines dsp (toxines diarrhéiques)",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="grossistes–détaillants",
        brand="Unbranded",
        company="Grossistes–détaillants (wholesalers and retailers)",
        product="Tellines (wedge clams) packed on 15/04/2026 and 16/04/2026",
        reason="Fishing-zone closure — DSP (diarrhetic shellfish poisoning) "
               "marine biotoxins",
        klass="Voluntary"),
    "22082": dict(
        marque="sans marque",
        modeles="thon albacore steak ls",
        motif="présence d'histamine",
        risque="toxines endogènes : histamine",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="e.leclerc la réserve",
        brand="Unbranded",
        company="E.Leclerc La Réserve",
        product="Albacore tuna steak, self-service counter",
        reason="Presence of histamine",
        klass="Voluntary"),
    "22113": dict(
        marque="sans marque",
        modeles="sardines entieres en rayon traditionnel",
        motif="teneur eleve en histamine",
        risque="toxines endogènes : histamine",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="intermarche le passage",
        brand="Unbranded",
        company="Intermarché Le Passage",
        product="Whole sardines sold at the traditional counter",
        reason="High histamine level",
        klass="Voluntary"),
    "22157": dict(
        marque="sans marque (vrac étale poissonnerie) et l'estran à pénéstin",
        modeles="moules de filière de l'ile d'houat en filets et sachets",
        motif="suspicion de toxines diarrhéiques (dsp)",
        risque="biotoxines marines dsp (toxines diarrhéiques)",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="e.leclerc l'immaculee saint nazaire, e.leclerc herbignac, "
                      "intermarche sene, intermarche elven",
        brand="L'Estran à Pénestin / unbranded (bulk, fishmonger's display)",
        company="E.Leclerc L'Immaculée Saint-Nazaire; E.Leclerc Herbignac; "
                "Intermarché Séné; Intermarché Elven",
        product="Rope-grown mussels from Île d'Houat, in nets and bags",
        reason="Suspected DSP (diarrhetic shellfish poisoning) toxins",
        klass="Voluntary"),
    "22186": dict(
        marque="foodmaker",
        modeles="300g",
        motif="possible présence de listeria monocytogenes",
        risque="listeria monocytogenes",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="franprix",
        brand="FOODMAKER",
        company=None,          # already correct — FOODMAKER
        product=None,          # workbook carries the fuller name; keep it
        reason="Possible presence of Listeria monocytogenes",
        klass="Voluntary"),
    "22206": dict(
        marque="ferme baracand",
        modeles="picodon",
        motif="contamination e.coli stec",
        risque="escherichia coli shiga toxinogène (stec)",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="au primeur du chantre, saint-marcel-lès-valence",
        brand="Ferme Baracand",
        company=None,
        product=None,
        reason="Contamination with Shiga toxin-producing E. coli (STEC)",
        klass="Voluntary"),
    "22208": dict(
        marque="herta",
        modeles="lardons fumés 200g et 200g+25% (promotion 250g)",
        motif="en raison d'une détection de présence isolée de salmonella sur "
              "un lot de lardons fumés 200g+25%",
        risque="salmonella spp",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="carrefour, provera, auchan, intermarché, leclerc, "
                      "casino, monoprix",
        brand="HERTA",
        company=None,
        product=None,
        reason="Isolated detection of Salmonella on a batch of Lardons Fumés "
               "200g+25%",
        klass="Voluntary"),
    # Found by reading neighbouring records — same contamination, invisible to
    # the hazard-class rule because both fields are biological.
    "22184": dict(
        marque="salaisons jouvin",
        modeles="merguez bœuf volaille halal",
        motif="présence de salmonelle",
        risque="salmonella spp",
        nature="volontaire (sans arrêté préfectoral)",
        distributeurs="smch",
        brand="Salaisons Jouvin",
        company=None,
        product=None,
        reason="Presence of Salmonella",
        klass="Voluntary"),
}

FR_NOTE = ("[audit 2026-08-02: Reason had been overwritten with a neighbouring "
           "recall's text by the url-gate batch mis-attribution. Repaired from "
           "the official DGCCRF open-data record for fiche {fiche} "
           "(motif_rappel: \"{motif}\"; risques_encourus: \"{risque}\"; "
           "marque_produit: \"{marque}\"), fetched via the data.gouv.fr "
           "tabular API. Pathogen was already correct and is unchanged.]")

# ── PART C2 — the sweep that PART C's detector was too narrow to find ───────
#
# PART C started from the ten rows the hazard-class rule flagged. Reading the
# neighbouring open-data records showed that rule can only ever see CROSS-class
# contamination: fiche 22184 (Salmonella row wearing an E. coli reason) was
# invisible to it because both fields are biological.
#
# So the whole French corpus — 547 rows — was re-swept for the three physical
# signatures of the url-gate mis-attribution instead:
#     • Company holding RASFF's "Origin: X | Notifying: Y" convention
#     • Reason carrying RASFF's "; risk: …; category: …" tail
#     • Product left as a bare category placeholder
# That found eleven more. Each was checked individually against its official
# DGCCRF record; the results split into two very different groups.
#
# GROUP 1 — FIVE ROWS WHERE THE PATHOGEN IS ALSO WRONG.
# These are not "a row with a bad Reason". Every field except Date and URL
# belongs to a different recall, and the published hazard is fabricated:
#
#   fiche 21811  published: Salmonella Agona, turkey breast from Poland
#                reality:   lead in a raw material of a Dieti Natura
#                           food supplement (60 capsules)
#   fiche 22005  published: Listeria monocytogenes, "DROME ARDECHE TRADITION"
#                reality:   non-compliant mercury level in ling fillet,
#                           Carrefour Market Vimoutiers
#   fiche 22039  published: Salmonella spp., poultry meat from Brazil
#                reality:   cadmium above the maximum limit in avocados, Lidl
#   fiche 22055  published: Salmonella — "outbreak", pistachio products
#                reality:   cadmium in Simpl avocados, Carrefour
#   fiche 22056  published: Rossmann, "foreign bodies posing injury risk"
#                reality:   high histamine in sardine fillets,
#                           Intermarché Agen  (Pathogen was right; nothing else)
#
# PART C's rule was "never overwrite Pathogen". That rule was correct for the
# ten rows it was written for — there the Pathogen was the one field that
# survived. It is wrong here, and following it would leave a fabricated
# Tier-1 Listeria sitting on a mercury recall. The official record names the
# hazard unambiguously, so Pathogen and Tier are corrected too, and each row
# says in its Notes exactly which field changed and why.
#
# Tier follows the workbook's existing convention for these hazards:
# 'Cadmium (heavy metal)' already sits at Tier 3 on two other rows.
#
# GROUP 2 — FIVE ROWS THAT ARE SIMPLY MISSING A PRODUCT NAME.
# Company, Pathogen and Reason all check out against the official record;
# Product was never extracted and holds the RappelConso category instead
# ("viandes (RappelConso fiche 22190)"). Filled in from
# modeles_ou_references. Nothing else on these rows is touched.
FR_TRUTH2 = {
    # Group 1 — hazard itself was wrong.
    "21811": dict(
        motif="détection d'une quantité anormale de plomb dans une matière "
              "première contenue dans ce produit",
        risque="éléments traces métalliques (métaux lourds : plomb, mercure, "
               "cadmium...)",
        marque="dieti natura",
        company="Dieti Natura", brand="Dieti Natura",
        product="Food supplement — pot of 60 capsules",
        pathogen="Lead (heavy metal)", tier=3,
        reason="Abnormal level of lead detected in a raw material used in "
               "this product",
        klass="Voluntary"),
    "22005": dict(
        motif="taux de mercure non conforme",
        risque="éléments traces métalliques (métaux lourds : plomb, mercure, "
               "cadmium...)",
        marque="sans marque",
        company="Carrefour Market Vimoutiers", brand="Unbranded",
        product="Ling fillet (filet de julienne) sold in vacuum-packed trays",
        pathogen="Mercury (heavy metal)", tier=3,
        reason="Non-compliant mercury level",
        klass="Voluntary"),
    "22039": dict(
        motif="dépassement de la teneur maximale autorisée sur un contaminant "
              "(cadmium)",
        risque="éléments traces métalliques (métaux lourds : plomb, mercure, "
               "cadmium...)",
        marque="-",
        company="Lidl (selected stores — see the attached list)",
        brand="Unbranded",
        product="Avocados — net of 3 fruits, and loose",
        pathogen="Cadmium (heavy metal)", tier=3,
        reason="Cadmium above the maximum permitted level",
        klass="Voluntary"),
    "22055": dict(
        motif="présence de cadmium",
        risque="éléments traces métalliques (métaux lourds : plomb, mercure, "
               "cadmium...)",
        marque="simpl",
        company="Carrefour", brand="Simpl",
        product="Avocados — net of 3 fruits",
        pathogen="Cadmium (heavy metal)", tier=3,
        reason="Presence of cadmium",
        klass="Voluntary"),
    "22056": dict(
        motif="taux élevé histamine",
        risque="toxines endogènes : histamine",
        marque="sans",
        company="Intermarché Agen", brand="Unbranded",
        product="Sardine fillets",
        pathogen=None, tier=None,          # already correct
        reason="High histamine level",
        klass="Voluntary"),
    # Group 2 — only Product was missing.
    "22173": dict(
        motif="détection de listéria monocytogènes",
        risque="listeria monocytogenes",
        marque="fournisseur fabriquant : charcuterie l.fassier (sarthe)",
        company=None, brand=None,
        product="Charcuterie from manufacturer Charcuterie L. Fassier (Sarthe)",
        pathogen=None, tier=None,
        reason="Detection of Listeria monocytogenes", klass="Voluntary"),
    "22177": dict(
        motif="présence de listeria",
        risque="listeria monocytogenes",
        marque="sans marque",
        company=None, brand=None,
        product="Beech-smoked lardons, packed 27/04/2026",
        pathogen=None, tier=None,
        reason="Presence of Listeria", klass="Voluntary"),
    "22178": dict(
        motif="teneurs élevées en aflatoxine b1 et en aflatoxines totales",
        risque="aflatoxines",
        marque="vracbio",
        company=None, brand=None,
        product="Dried figs (figue séchée)",
        pathogen=None, tier=None,
        reason="High levels of aflatoxin B1 and total aflatoxins",
        klass="Voluntary"),
    "22190": dict(
        motif="présence de listéria",
        risque="listeria monocytogenes",
        marque="mont charvin salaisons",
        company=None, brand=None,
        product="Dry-cured sausage with walnuts (saucisson sec aux noix)",
        pathogen=None, tier=None,
        reason="Presence of Listeria", klass="Voluntary"),
    "22211": dict(
        motif="présence salmonelle",
        risque="salmonella spp",
        marque="les ateliers",
        company=None, brand=None,
        product="Tartare Brasserie 5% Charolais — 2 x 180 g",
        pathogen=None, tier=None,
        reason="Presence of Salmonella", klass="Voluntary"),
}

FR2_NOTE = ("[audit 2026-08-02 (sweep 2): reconciled against the official "
            "DGCCRF open-data record for fiche {fiche} — motif_rappel: "
            "\"{motif}\"; risques_encourus: \"{risque}\"; marque_produit: "
            "\"{marque}\". Fields corrected: {fields}.{hazard}]")
FR2_HAZARD = (" THE PUBLISHED HAZARD WAS WRONG: this row had been overwritten "
              "with a different recall's content by the url-gate batch "
              "mis-attribution, so Pathogen and Tier are corrected too.")

# ── PART D ──────────────────────────────────────────────────────────────────
PLACEHOLDER_23067 = "(not specified in RappelConso fiche 23067)"
NOTE_23067 = (
    "[audit 2026-08-02: Company and Brand both arrived as the literal string "
    "'0' — an extractor sentinel, not a name. Fiche 23067 is dated 2026-07-31 "
    "and the DGCCRF open-data export currently ends at fiche 22990 "
    "(2026-07-24), so there is no authoritative record to repair from yet; "
    "normal publication lag. The names are NOT guessed. 'e.leclerc sebadis' "
    "in this row's Notes is the RappelConso distributeurs field, not the "
    "company. pipeline/verify_rappelconso.py will fill this in automatically "
    "once the export catches up; until then, OPERATOR ACTION: open fiche "
    "23067 and set Company/Brand.]"
)

_RASFF_COMPANY = re.compile(r"^\s*Origin:\s.*\|\s*Notifying:", re.IGNORECASE)
_CATEGORY_PLACEHOLDER = re.compile(
    r"^\s*(?:produits de la p[êe]che[^()]*|viandes|lait et produits laitiers|"
    r"plats pr[ée]par[ée]s[^()]*|fruits et l[ée]gumes)\s*"
    r"\(RappelConso fiche \d+\)\s*$", re.IGNORECASE)


def _fiche(url: str):
    m = re.search(r"fiche-rappel/(\d+)", str(url or ""))
    return m.group(1) if m else None


def _norm(u) -> str:
    return str(u or "").strip().lower().rstrip("/")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )
    JSON_PATH = ROOT / "docs" / "data" / "recalls.json"

    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    allergen_norm = {_norm(u): d for u, d in ALLERGEN_ONLY.items()}

    kept, archived = [], []
    n_fr = n_case = 0
    fr_changes = []

    for row in approved:
        url = _norm(row.get("URL"))

        # A — allergen-only, out of scope.
        if url in allergen_norm:
            reason = ALLERGEN_REASON.format(detail=allergen_norm[url])
            row["Notes"] = (str(row.get("Notes") or "").strip()
                            + f" [REJECTED {TODAY}: {reason}]").strip()
            row["RejectedBy"] = "scope-gate-2026-08-02"
            row["RejectedAt"] = stamp
            row["Status"] = "rejected"
            archived.append(row)
            print(f"ARCHIVE(scope)  {row.get('Date')}  "
                  f"{str(row.get('Company'))[:44]}")
            continue

        # B — the FSANZ duplicate.
        if url == _norm(FSANZ_DUP):
            row["Notes"] = (str(row.get("Notes") or "").strip()
                            + f" [REJECTED {TODAY}: {DUP_REASON}]").strip()
            row["RejectedBy"] = "audit-2026-08-02"
            row["RejectedAt"] = stamp
            row["Status"] = "rejected"
            archived.append(row)
            print(f"ARCHIVE(dup)    {row.get('Date')}  "
                  f"{str(row.get('Company'))[:44]}")
            continue

        # C — RappelConso repairs from the official record.
        fiche = _fiche(url)
        if fiche in FR_TRUTH:
            t = FR_TRUTH[fiche]
            before = {k: row.get(k) for k in
                      ("Company", "Brand", "Product", "Reason", "Class")}
            row["Reason"] = t["reason"]
            row["Brand"] = t["brand"]
            row["Class"] = t["klass"]
            if t["company"] and _RASFF_COMPANY.match(str(row.get("Company") or "")):
                row["Company"] = t["company"]
            if t["product"] and _CATEGORY_PLACEHOLDER.match(
                    str(row.get("Product") or "")):
                row["Product"] = t["product"]
            if f"audit {TODAY}" not in str(row.get("Notes") or ""):
                row["Notes"] = (
                    str(row.get("Notes") or "").strip() + " "
                    + FR_NOTE.format(fiche=fiche, motif=t["motif"],
                                     risque=t["risque"], marque=t["marque"])
                ).strip()[:1600]
            row["LastUpdated"] = TODAY
            row["LastChecked"] = TODAY
            n_fr += 1
            fr_changes.append((fiche, before,
                               {k: row.get(k) for k in before}))

        # C2 — the wider sweep.
        if fiche in FR_TRUTH2:
            t = FR_TRUTH2[fiche]
            changed = []
            for field, key in (("Company", "company"), ("Brand", "brand"),
                               ("Product", "product"), ("Reason", "reason"),
                               ("Pathogen", "pathogen"), ("Class", "klass")):
                val = t.get(key)
                if val and str(row.get(field) or "") != str(val):
                    row[field] = val
                    changed.append(field)
            if t.get("tier") and str(row.get("Tier")) != str(t["tier"]):
                row["Tier"] = t["tier"]
                changed.append("Tier")
            if changed and f"audit {TODAY} (sweep 2)" not in str(row.get("Notes") or ""):
                row["Notes"] = (
                    str(row.get("Notes") or "").strip() + " "
                    + FR2_NOTE.format(
                        fiche=fiche, motif=t["motif"], risque=t["risque"],
                        marque=t["marque"], fields=", ".join(changed),
                        hazard=FR2_HAZARD if t.get("pathogen") else "")
                ).strip()[:1800]
            if changed:
                row["LastUpdated"] = TODAY
                row["LastChecked"] = TODAY
                n_fr += 1
                fr_changes.append((fiche, {}, {}))
                print(f"REPAIR(sweep2)  fiche {fiche}  {row.get('Date')}  "
                      f"{', '.join(changed)}")

        # D — fiche 23067: Company and Brand both arrived as the literal
        #     string "0", an extractor sentinel rather than a name. Not yet
        #     repairable — see the module docstring — so it gets an honest
        #     placeholder and an operator flag rather than an invented name.
        if fiche == "23067":
            for field in ("Company", "Brand"):
                if str(row.get(field) or "").strip() in ("0", ""):
                    row[field] = PLACEHOLDER_23067
            if "audit 2026-08-02" not in str(row.get("Notes") or ""):
                row["Notes"] = (str(row.get("Notes") or "").strip()
                                + " " + NOTE_23067).strip()[:1600]
            row["LastUpdated"] = TODAY

        # Canonical /Interne casing.
        raw = str(row.get("URL") or "")
        if raw.endswith("/interne"):
            row["URL"] = raw[:-len("/interne")] + "/Interne"
            n_case += 1

        kept.append(row)

    print(f"\nRecalls {len(approved)} -> {len(kept)}  "
          f"({len(archived)} archived) | {n_fr} French rows repaired | "
          f"{n_case} URLs recased")
    for fiche, b, a in fr_changes:
        if not b:
            continue                      # sweep-2 rows print inline above
        print(f"\n  fiche {fiche}")
        for k in ("Company", "Brand", "Product", "Reason", "Class"):
            if str(b[k]) != str(a[k]):
                print(f"    {k:8} {str(b[k])[:62]!r}")
                print(f"    {'':8} -> {str(a[k])[:62]!r}")

    if args.dry_run:
        print("\n(dry run — nothing written)")
        return 0

    save_xlsx_with_pending(sort_rows(kept), sort_rows(pending), xlsx,
                           newly_rejected_rows=archived)
    try:
        from pipeline.weekly_rejected_capture import record_rejections
        record_rejections(archived, xlsx_path=xlsx)
    except Exception as exc:
        print(f"  (Weekly_Rejected capture skipped: {exc})")
    mirror_json_from_xlsx(xlsx, JSON_PATH)
    print("\n✓ written + recalls.json mirrored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
