# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-23 — curator proposals.

Writes docs/data/agent-proposals/20261123T203000Z-nightly-operator.json.
Every quote below was read on the RappelConso fiche itself on 2026-11-23
(evening). Apply with:
    python -m pipeline.agents.curator --proposals <file> --apply
"""
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/data/agent-proposals/20261123T203000Z-nightly-operator.json"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
T = "2026-11-23T20:15:00Z"
S = "[nightly operator 2026-11-23: fiche %s re-read tonight; fields read on the fiche, French translated]"


def ev(fid, *quotes):
    return [{"kind": "regulator_page", "url": RC % fid, "quote": q,
             "retrieved_utc": T} for q in quotes]


P = [
    # ── Pending: correct, then promote via promote_gate_passing ──────────
    dict(action="enrich", target={"url": RC % "24149"}, changes={
        "Company": "Etablissements Massonniere", "Brand": "Unbranded",
        "Product": "Beef round roast 2 kg (ROTI BF TT ROND TG 2KG), lot M62940018, use-by 15/11/2026, sold by Boucherie SARL Benoist Fils et Fille, Libourne",
        "Pathogen": "Salmonella",
        "Reason": "Detection of Salmonella (Salmonella spp.)",
        "Date": "2026-11-23",
        "Notes": "boucherie sarl benoist fils et fille 33500 libourne " + S % "24149" + " [Product was the firm name 'massonniere'; the fiche names a beef roast]"},
        reason="fiche 24149 (23/11/2026): Product carried the firm name, Company empty.",
        evidence=ev("24149", "ROTI BF TT ROND TG 2KG", "M62940018", "DETECTION SALMONELLA",
                    "ETABLISSEMENTS MASSONNIERE ETS MASSONNIERE")),
    dict(action="enrich", target={"url": RC % "24127"}, changes={
        "Company": "Cooperative U Enseigne U", "Brand": "Unbranded",
        "Product": "Brioche bread x32, 400 g, GTIN 3251517091006, lot 6260",
        "Pathogen": "Foreign material",
        "Reason": "Physical risk (suspected foreign body)",
        "Date": "2026-11-20",
        "Notes": "magasins u [offline-enrich 2026-11-23: Pathogen '' -> 'Foreign material', read from this row's own hazard text; no fetch, no model] " + S % "24127"},
        reason="fiche 24127 (20/11/2026): Company empty, Product and Reason French. Same decision as unmerged PRs #50-#52.",
        evidence=ev("24127", "Pain brioché x32 400g", "3251517091006", "6260",
                    "Risque physique (suspicion de corps étranger)",
                    "COOPERATIVE U ENSEIGNE U ENSEIGNE")),
    dict(action="enrich", target={"url": RC % "24089"}, changes={
        "Company": "Cooperative U Enseigne U", "Brand": "Unbranded",
        "Product": "Vanilla praline rolled Yule log cake (GTIN 2000000324418, lots 1/288/26 - 1/289/26); chocolate praline Rocher log cake (GTIN 2000000278902, lot 1/286/26)",
        "Pathogen": "Foreign material (stones)",
        "Reason": "Physical risk: possible presence of small stone-type foreign bodies in 2/4 mm hazelnut pieces",
        "Date": "2026-11-16",
        "Notes": "magasins u [offline-enrich 2026-11-16: Pathogen '' -> 'Foreign material', read from this row's own hazard text; no fetch, no model] " + S % "24089"},
        reason="fiche 24089 (16/11/2026): Company empty (confirm-agent hold), Product and Reason French. Same decision as unmerged PRs #51/#52.",
        evidence=ev("24089", "Bûchette Rocher chocolat praliné", "2000000324418",
                    "2000000278902", "cailloux dans des éclats de noisette 2/4mm",
                    "COOPERATIVE U ENSEIGNE U ENSEIGNE")),
    dict(action="enrich", target={"url": RC % "24109"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Paysan Breton",
        "Product": "Paysan Breton fromage blanc (fresh white cheese) 0%, 5 kg pail, GTIN 3412290026917",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-18",
        "Notes": "lcm 62 billy, lcm 33 cestas " + S % "24109"},
        reason="fiche 24109 (18/11/2026): firm EVEN LAIT INDUSTRIE; chemical contaminant in scope; Product and Reason French. Same decision as unmerged PR #52.",
        evidence=ev("24109", "EVEN LAIT INDUSTRIE", "3412290026917",
                    "Autres contaminants chimiques",
                    "Fromage blanc Paysan Breton 0% Seau 5kg")),
    dict(action="enrich", target={"url": RC % "24097"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Belle France",
        "Product": "Belle France fromage blanc (fresh white cheese) 0% and 3%, 500 g and 1 kg (GTIN 3258561419695, 3258561419701, 3258561419718, 3258561419725)",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-16",
        "Notes": "diapar, codifrance, segurel&fils " + S % "24097"},
        reason="fiche 24097 (16/11/2026): firm EVEN LAIT INDUSTRIE; chemical contaminant in scope; Product and Reason French. Same decision as unmerged PR #52.",
        evidence=ev("24097", "EVEN LAIT INDUSTRIE", "3258561419695",
                    "Autres contaminants chimiques",
                    "Fromage blanc Belle France 0% 500g")),
    # 24125 (sterilised milk, unnamed "microorganisme potentiellement pathogène")
    # was proposed and REFUSED by the curator: no alert vocabulary term can
    # reach an unnamed agent. Left in Pending for a human policy call.
    # ── Published rows (Recalls): English-only wording fixes ─────────────
    dict(action="enrich", target={"url": RC % "24150"}, changes={
        "Product": "Superior cooked ham, rindless and defatted, cloth-cooked (jambon au torchon), sold at the traditional counter 17-19/11/2026, and products made with it: ham cones, ham-mushroom bruschetta, pizza reine, pizza fermière, ham sandwiches, croque monsieur, ham croissant (use-by 20-22/11/2026 and other dates on the fiche)"},
        reason="published row 24150: Product was the fiche's French list verbatim, ending in a stray ';'.",
        evidence=ev("24150", "JAMBON CUIT SUPERIEUR DECOUENNE DEGRAISSE AU TORCHON",
                    "Présence de Listeria monocytogenes", "E.LECLERC ATOUT SUD")),
    dict(action="enrich", target={"url": RC % "24152"}, changes={
        "Company": "Fromageries Terres d'Or",
        "Product": "Top Budget grated Emmental cheese 200 g, GTIN 3410280141671, lot codes beginning 11 323",
        "Reason": "Risk of glass-type foreign body"},
        reason="published row 24152: Product and Reason French; Company was the brand, the fiche names Fromageries Terres d'Or.",
        evidence=ev("24152", "Emmental Râpé Top Budget 200g", "3410280141671",
                    "Risque de corps étranger de type verre", "FROMAGERIES TERRES D'OR")),
    dict(action="enrich", target={"url": RC % "24140"}, changes={
        "Product": "Basque-style tuna with rice (thon basquaise/riz), sold in protective-atmosphere packs (self-service) or at the traditional counter, GTIN 0204053011871, use-by 03/11/2026 to 22/11/2026",
        "Reason": "Presence of Salmonella detected; product withdrawn from sale and recalled"},
        reason="published row 24140: Reason was the French scraper template ('procède au retrait…; mise en évidence…'), Product French.",
        evidence=ev("24140", "Thon Basquaise/Riz", "0204053011871",
                    "la présence de salmonelles",
                    "Salmonella spp (agent responsable de la salmonellose)")),
    dict(action="enrich", target={"url": RC % "24119"}, changes={
        "Pathogen": "Hydrocyanic acid"},
        reason="published row 24119: Pathogen carried French '(acide cyanhydrique)' and a '— chemical hazard' suffix. Same fix as unmerged PR #52.",
        evidence=ev("24119", "Acide cyanhydrique supérieur à la teneur maximale",
                    "GRAINE DE LIN 650G METRO CHEF")),
]

for p in P:
    p["confidence"] = 0.95
    p["agent"] = "nightly-operator"
    p["reason"] = "nightly operator 2026-11-23: " + p["reason"]
OUT.write_text(json.dumps({"schema": 1, "agent": "nightly-operator",
                           "generated_utc": "20261123T203000Z",
                           "proposals": P}, indent=2, ensure_ascii=False),
               encoding="utf-8")
print(OUT)
