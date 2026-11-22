# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-22 — curator proposals.

Writes docs/data/agent-proposals/20261122T203000Z-nightly-operator.json.
Every quote below was read on the RappelConso fiche itself on 2026-11-22
(evening). Apply with:
    python -m pipeline.agents.curator --proposals <file> --apply
"""
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/data/agent-proposals/20261122T203000Z-nightly-operator.json"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
T = "2026-11-22T20:15:00Z"
S = "[nightly operator 2026-11-22: fiche %s re-read tonight; fields read on the fiche, French translated, needs-cleanup stamps removed because the defects are repaired]"


def ev(fid, *quotes):
    return [{"kind": "regulator_page", "url": RC % fid, "quote": q,
             "retrieved_utc": T} for q in quotes]


P = [
    dict(action="enrich", target={"url": RC % "24127"}, changes={
        "Company": "Cooperative U Enseigne U", "Brand": "Unbranded",
        "Product": "Brioche bread x32, 400 g, GTIN 3251517091006, lot 6260",
        "Pathogen": "Foreign material",
        "Reason": "Physical risk (suspected foreign body)",
        "Date": "2026-11-20",
        "Notes": "magasins u [offline-enrich 2026-11-22: Pathogen '' -> 'Foreign material', read from this row's own hazard text; no fetch, no model] " + S % "24127"},
        reason="fiche 24127 (20/11/2026): Company empty, Product and Reason French. Same decision as unmerged PRs #50/#51.",
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
        reason="fiche 24089 (16/11/2026): Company empty (confirm-agent hold), Product and Reason French. Same decision as unmerged PR #51.",
        evidence=ev("24089", "Bûchette Rocher chocolat praliné", "2000000324418",
                    "2000000278902", "cailloux dans des éclats de noisette 2/4mm",
                    "COOPERATIVE U ENSEIGNE U ENSEIGNE")),
    dict(action="enrich", target={"url": RC % "24109"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Paysan Breton",
        "Product": "Paysan Breton fromage blanc (fresh white cheese) 0%, 5 kg pail, GTIN 3412290026917, use-by 01/12/2026",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-18",
        "Notes": "lcm 62 billy, lcm 33 cestas " + S % "24109"},
        reason="fiche 24109 (18/11/2026): firm EVEN LAIT INDUSTRIE; chemical contaminant in scope; Product and Reason French.",
        evidence=ev("24109", "EVEN LAIT INDUSTRIE", "3412290026917",
                    "Autres contaminants chimiques",
                    "Fromage blanc Paysan Breton 0% Seau 5kg")),
    dict(action="enrich", target={"url": RC % "24097"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Belle France",
        "Product": "Belle France fromage blanc (fresh white cheese) 0% and 3%, 500 g and 1 kg (GTIN 3258561419695, 3258561419701, 3258561419718, 3258561419725), use-by 01/12/2026",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-16",
        "Notes": "diapar, codifrance, segurel&fils " + S % "24097"},
        reason="fiche 24097 (16/11/2026): firm EVEN LAIT INDUSTRIE; chemical contaminant in scope; Product and Reason French.",
        evidence=ev("24097", "EVEN LAIT INDUSTRIE", "3258561419695",
                    "Autres contaminants chimiques",
                    "Fromage blanc Belle France 0% 500g")),
    dict(action="enrich", target={"url": RC % "24119"}, changes={
        "Pathogen": "Hydrocyanic acid"},
        reason="published row 24119: Pathogen carried the French term '(acide cyanhydrique)' and a '— chemical hazard' suffix in an English field.",
        evidence=ev("24119", "Acide cyanhydrique supérieur à la teneur maximale",
                    "Autres contaminants chimiques", "GRAINE DE LIN 650G METRO CHEF")),
]

for p in P:
    p["confidence"] = 0.95
    p["agent"] = "nightly-operator"
    p["reason"] = "nightly operator 2026-11-22: " + p["reason"]
OUT.write_text(json.dumps({"schema": 1, "agent": "nightly-operator",
                           "generated_utc": "20261122T203000Z",
                           "proposals": P}, indent=2, ensure_ascii=False),
               encoding="utf-8")
print(OUT)
