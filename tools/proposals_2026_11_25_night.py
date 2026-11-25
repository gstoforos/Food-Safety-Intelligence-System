# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-25 — curator proposals.

Writes docs/data/agent-proposals/20261125T203000Z-nightly-operator.json.
Quotes for fiches 24175/24176/24177 and the CFIA Yeshi notice were read on
2026-11-25. The four published-row translations, the FDA herring notice and
fiche 24089 re-apply unmerged PR #54 (quotes read 2026-11-24); the curator
re-fetches every URL and refuses any row the page no longer supports. Apply with:
    python -m pipeline.agents.curator --proposals <file> --apply
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/data/agent-proposals/20261125T203000Z-nightly-operator.json"
XLSX = ROOT / "docs/data/recalls.xlsx"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
FDA = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
CFIA_YESHI = "https://recalls-rappels.canada.ca/en/alert-recall/yeshi-brand-sesame-ginger-nutritional-yeast-dressing-dip-recalled-due-pieces-glass"
T = "2026-11-25T20:00:00Z"
S = "[nightly operator 2026-11-25: %s re-read tonight; fields read on the page, French translated]"


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
    # ── new tonight: Pending rows verified on the regulator's page ──
    dict(action="enrich", target={"url": RC % "24176"}, changes={
        "Company": "SA Hendis Centre E. Leclerc Hennebont", "Brand": "E. Leclerc Hennebont",
        "Product": "Andouille de Guémené (smoked chitterling sausage), sliced or in pieces, self-service and traditional delicatessen counters, GTIN 0200964000000, use-by 07/11/2026 to 24/11/2026, sold 07/11/2026 to 23/11/2026, E. Leclerc Hennebont",
        "Reason": "Suspected presence of Listeria monocytogenes",
        "Date": "2026-11-25",
        "_notes": S % "fiche 24176 (réf. 2026-11-0182)"},
        reason="fiche 24176: Product and Reason French; Company was the store brand, the fiche names SA HENDIS CENTRE E. LECLERC HENNEBONT.",
        evidence=ev(RC % "24176", "Andouille de Guémené", "0200964000000",
                    "Suspicion de présence de Listéria",
                    "SA HENDIS CENTRE E. LECLERC HENNEBONT")),
    dict(action="enrich", target={"url": RC % "24175"}, changes={
        "Company": "SA Hendis Centre E. Leclerc Hennebont", "Brand": "E. Leclerc Hennebont",
        "Product": "Prepared dishes made with andouille de Guémené: La Léontine pizza, Raymond bruschetta, Breton bowl, andouille-camembert crisp, caramelised onion tarte tatin, charcuterie platter, rustic salad (GTIN 2000001133163, 2000001134641, 2000001132333, 2000001141731, 2000001131916, 0208958000000, 2000001140338), use-by 07/11/2026 to 24/11/2026, E. Leclerc Hennebont",
        "Reason": "Suspected presence of Listeria monocytogenes",
        "Date": "2026-11-25",
        "_notes": S % "fiche 24175 (réf. 2026-11-0181)"},
        reason="fiche 24175: Product and Reason French; Company was the store brand.",
        evidence=ev(RC % "24175", "Produits élaborés à base d'andouille de Guémené",
                    "2000001133163", "Suspicion de présence de Listéria",
                    "SA HENDIS CENTRE E. LECLERC HENNEBONT")),
    dict(action="enrich", target={"url": RC % "24177"}, changes={
        "Company": "SA Hendis Centre E. Leclerc Hennebont", "Brand": "E. Leclerc Hennebont",
        "Product": "Prepared dishes made with andouille de Guémené: beef and andouille parmentier, La Rustique salad, Guémené pizza, Raymond bruschetta, Breton bowl, sold 07/11/2026 to 23/11/2026, E. Leclerc Hennebont",
        "Reason": "Suspected Listeria monocytogenes",
        "Date": "2026-11-25",
        "_notes": S % "fiche 24177 (réf. 2026-11-0183)"},
        reason="fiche 24177: Product and Reason French; Company was the store brand. Separate fiche from 24175 (different reference and model list).",
        evidence=ev(RC % "24177", "Parmentier de bœuf et andouille de Guémené",
                    "Suspicion de Listéria",
                    "SA HENDIS CENTRE E. LECLERC HENNEBONT")),
    dict(action="enrich", target={"url": CFIA_YESHI}, changes={
        "Company": "Apex Foodsource Ltd.", "Brand": "Yeshi",
        "Product": "Yeshi Sesame Ginger Nutritional Yeast Dressing & Dip, 350 ml, UPC 6 27843 80892 3, lot 260615 402016, 2028 JN 14",
        "Reason": "Pieces of glass (extraneous material)",
        "Pathogen": "Foreign material (glass)",
        "Date": "2026-11-20",
        "_notes": "[nightly operator 2026-11-25: CFIA notice re-read tonight; distribution British Columbia, possibly other provinces]"},
        reason="CFIA row: Company empty, Product and Reason were the headline; Date was the scrape day (CFIA recall date 2026-11-20).",
        evidence=ev(CFIA_YESHI, "Sesame Ginger Nutritional Yeast Dressing & Dip",
                    "6 27843 80892 3", "Apex Foodsource Ltd.", "pieces of glass")),
    dict(action="enrich", target={"url": RC % "24089"}, changes={
        "Company": "Cooperative U Enseigne U", "Brand": "Unbranded",
        "Product": "Vanilla praline rolled Yule log cake (GTIN 2000000324418, lots 1/288/26 - 1/289/26); chocolate praline Rocher log cake (GTIN 2000000278902, lot 1/286/26), sold 21/10/2026 to 09/11/2026 at Magasins U",
        "Pathogen": "Foreign material (stones)",
        "Reason": "Physical risk: possible presence of small stone-type foreign bodies in 2/4 mm hazelnut pieces",
        "Date": "2026-11-16",
        "_notes": S % "fiche 24089"},
        reason="fiche 24089: Company empty (confirm-agent hold), Product and Reason French. Same decision as unmerged PRs #51-#54.",
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
        p["reason"] = "nightly operator 2026-11-25: " + p["reason"]
    OUT.write_text(json.dumps({"schema": 1, "agent": "nightly-operator",
                               "generated_utc": "20261125T203000Z",
                               "proposals": P}, indent=2, ensure_ascii=False),
                   encoding="utf-8")
    print(OUT)
