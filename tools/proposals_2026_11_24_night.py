# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-24 — curator proposals.

Writes docs/data/agent-proposals/20261124T203000Z-nightly-operator.json.
Every quote below was read on the regulator's own page on the evening of
2026-11-24 (RappelConso fiches; fda.gov). Apply with:
    python -m pipeline.agents.curator --proposals <file> --apply
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/data/agent-proposals/20261124T203000Z-nightly-operator.json"
XLSX = ROOT / "docs/data/recalls.xlsx"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
T = "2026-11-24T20:00:00Z"
S = "[nightly operator 2026-11-24: %s re-read tonight; fields read on the page, French translated]"


def ev(url, *quotes):
    return [{"kind": "regulator_page", "url": url, "quote": q,
             "retrieved_utc": T} for q in quotes]


def notes(url, extra):
    """Current Notes of the row, minus 'needs cleanup' stamps the fix clears."""
    import openpyxl
    from pipeline.merge_master import _normalize_url_for_dedup as k
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    for sh in ("Pending", "Recalls"):
        rows = wb[sh].iter_rows(values_only=True)
        h = list(next(rows))
        for r in rows:
            if k(str(r[h.index("URL")] or "")) == k(url):
                n = str(r[h.index("Notes")] or "")
                n = re.sub(r"\s*\[needs cleanup: [^\]]*\]", "", n)
                n = re.sub(r"^REJECTED: [^|]*\|\s*", "", n)
                return (n.strip() + " " + extra).strip()
    raise SystemExit(f"row not found: {url}")


P = [
    # ── Published today with French Product/Reason (main is RED on these) ──
    dict(action="enrich", target={"url": RC % "24157"}, changes={
        "Company": "Auchan Retail Services", "Brand": "Auchan",
        "Product": "Cream puffs with whipped cream x2 (choux chantilly, in-store local recipe), GTIN 2030000020942, all lots sold at the self-service pastry counter 13/11/2026 to 15/11/2026, Auchan hypermarket Gap only",
        "Reason": "Presence of Listeria monocytogenes",
        "_notes": S % "fiche 24157"},
        reason="published row 24157: Product and Reason French.",
        evidence=ev(RC % "24157", "CHOUX CHANTILLY X2 (recette locale) FABRICATION",
                    "2030000020942",
                    "Suite à la présence de Listeria monocytogenes (agent responsable de la listériose)",
                    "AUCHAN RETAIL SERVICES AUCHAN")),
    dict(action="enrich", target={"url": RC % "24158"}, changes={
        "Company": "Auchan Retail Services", "Brand": "Auchan",
        "Product": "Beef paupiettes (stuffed rolled beef), butcher counter, all lots sold 17/11/2026 to 19/11/2026, Auchan supermarket Saint-Amarin only",
        "Reason": "Presence of Salmonella spp.",
        "_notes": S % "fiche 24158"},
        reason="published row 24158: Product French; Reason carried French parenthetical.",
        evidence=ev(RC % "24158", "PAUPIETTE DE BOEUF (STAND BOUCHERIE)",
                    "Présence de Salmonella spp (agent responsable de la salmonellose)",
                    "AUCHAN RETAIL SERVICES AUCHAN")),
    dict(action="enrich", target={"url": RC % "24153"}, changes={
        "Company": "Kerhadis E. Leclerc", "Brand": "Unbranded",
        "Product": "Sheep's milk cheese with Espelette chilli pepper, GTIN 0207204000000, sold 23/10/2026 to 19/11/2026, best before 16/07/2027, Halles de Kerharo (Leclerc)",
        "Reason": "Presence of Listeria monocytogenes",
        "_notes": S % "fiche 24153"},
        reason="published row 24153: Product French; Company was the store, the fiche origin is KERHADIS E. LECLERC.",
        evidence=ev(RC % "24153", "FROMAGE DE BREBIS AU PIMENT D'ESPELETTE",
                    "0207204000000", "Présence de Listeria monocytogenes",
                    "KERHADIS E. LECLERC")),
    dict(action="enrich", target={"url": RC % "24159"}, changes={
        "Company": "Fromageries Terres d'Or", "Brand": "Top Budget",
        "Product": "Top Budget grated Emmental cheese 200 g, GTIN 3410280141671, lot codes beginning 11 323, best before 29/01/2027, sold 21/11/2026 to 23/11/2026 at Intermarché",
        "Reason": "Risk of glass-type foreign body",
        "_notes": S % "fiche 24159"},
        reason="published row 24159: Product and Reason French; Company was the brand, the fiche names Fromageries Terres d'Or.",
        evidence=ev(RC % "24159", "Emmental Râpé 200g", "3410280141671",
                    "Risque de corps étranger de type verre", "FROMAGERIES TERRES D'OR")),
    # ── Pending: correct, then promote via promote_gate_passing ──────────
    dict(action="enrich", target={"url": FDA + "prime-food-processing-llc-issues-safety-warning-regarding-uneviscerated-dried-herring-fish-0"}, changes={
        "Company": "Prime Food Processing LLC", "Brand": "No Brand",
        "Product": "Uneviscerated Dried Herring Fish, Item #AF4110, lot code 25180, expiration 12.28.27",
        "Pathogen": "Clostridium botulinum",
        "Reason": "Potential contamination with Clostridium botulinum (uneviscerated dried fish)",
        "Date": "2026-11-23",
        "_notes": S % "FDA notice"},
        reason="Pending FDA row: Product was the FDA boilerplate, Company empty, Reason claimed an outbreak the notice does not report. Distinct from the published 2026-06-04 herring notice (lot 26020, URL without '-0').",
        evidence=ev(FDA + "prime-food-processing-llc-issues-safety-warning-regarding-uneviscerated-dried-herring-fish-0",
                    "Prime Food Processing LLC Issues Safety Warning Regarding Uneviscerated “Dried Herring Fish”",
                    "Item #AF4110", "25180",
                    "Potential contamination with Clostridium Botulinum")),
    dict(action="enrich", target={"url": RC % "24109"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Paysan Breton",
        "Product": "Paysan Breton fromage blanc (fresh white cheese) 0%, 5 kg pail, GTIN 3412290026917, use-by 01/12/2026",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-18",
        "_notes": S % "fiche 24109"},
        reason="fiche 24109: firm EVEN LAIT INDUSTRIE; Pathogen empty; Product and Reason French. Same decision as unmerged PRs #52/#53.",
        evidence=ev(RC % "24109", "EVEN LAIT INDUSTRIE", "3412290026917",
                    "Autres contaminants chimiques",
                    "Fromage blanc Paysan Breton 0% Seau 5kg")),
    dict(action="enrich", target={"url": RC % "24097"}, changes={
        "Company": "Even Lait Industrie", "Brand": "Belle France",
        "Product": "Belle France fromage blanc (fresh white cheese) 0% and 3%, 500 g and 1 kg (GTIN 3258561419695, 3258561419701, 3258561419718, 3258561419725), use-by 01/12/2026",
        "Pathogen": "Chemical contaminant (cleaning product residue)",
        "Reason": "Possible contamination by cleaning products at doses presenting no risk to consumer health (RappelConso: other chemical contaminants)",
        "Date": "2026-11-16",
        "_notes": S % "fiche 24097"},
        reason="fiche 24097: the 'listing page' verdict is false (fiche re-read tonight); firm EVEN LAIT INDUSTRIE; Product and Reason French. Same decision as unmerged PRs #52/#53.",
        evidence=ev(RC % "24097", "EVEN LAIT INDUSTRIE", "3258561419695",
                    "Autres contaminants chimiques",
                    "Fromage blanc Belle France 0% 500g")),
    dict(action="enrich", target={"url": RC % "24089"}, changes={
        "Company": "Cooperative U Enseigne U", "Brand": "Unbranded",
        "Product": "Vanilla praline rolled Yule log cake (GTIN 2000000324418, lots 1/288/26 - 1/289/26); chocolate praline Rocher log cake (GTIN 2000000278902, lot 1/286/26), sold 21/10/2026 to 09/11/2026 at Magasins U",
        "Pathogen": "Foreign material (stones)",
        "Reason": "Physical risk: possible presence of small stone-type foreign bodies in 2/4 mm hazelnut pieces",
        "Date": "2026-11-16",
        "_notes": S % "fiche 24089"},
        reason="fiche 24089: Company empty (confirm-agent hold), Product and Reason French. Same decision as unmerged PRs #51-#53.",
        evidence=ev(RC % "24089", "Bûchette Rocher chocolat praliné", "2000000324418",
                    "2000000278902", "cailloux dans des éclats de noisette 2/4mm",
                    "COOPERATIVE U ENSEIGNE U ENSEIGNE")),
]

if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    for p in P:
        extra = p["changes"].pop("_notes")
        p["changes"]["Notes"] = notes(p["target"]["url"], extra)
        p["confidence"] = 0.95
        p["agent"] = "nightly-operator"
        p["reason"] = "nightly operator 2026-11-24: " + p["reason"]
    OUT.write_text(json.dumps({"schema": 1, "agent": "nightly-operator",
                               "generated_utc": "20261124T203000Z",
                               "proposals": P}, indent=2, ensure_ascii=False),
                   encoding="utf-8")
    print(OUT)
