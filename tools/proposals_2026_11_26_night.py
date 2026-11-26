# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-26 — curator proposals.

Every quote below was read on the regulator's page on 2026-11-26 (~20:10 UTC).
Writes docs/data/agent-proposals/20261126T201500Z-nightly-operator.json; apply:
    python -m pipeline.agents.curator --proposals <file> --apply
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/data/agent-proposals/20261126T201500Z-nightly-operator.json"
XLSX = ROOT / "docs/data/recalls.xlsx"
RC = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
FDA_HERRING = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
               "prime-food-processing-llc-issues-safety-warning-regarding-uneviscerated-dried-herring-fish-0")
CFIA_YESHI = ("https://recalls-rappels.canada.ca/en/alert-recall/"
              "yeshi-brand-sesame-ginger-nutritional-yeast-dressing-dip-recalled-due-pieces-glass")
T = "2026-11-26T20:10:00Z"
S = "[nightly operator 2026-11-26: %s read tonight; fields taken from the page, French translated]"


def ev(url, *quotes):
    return [{"kind": "regulator_page", "url": url, "quote": q,
             "retrieved_utc": T} for q in quotes]


def notes(url, extra):
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
    # ── published today, Product/Reason still French ──
    dict(action="enrich", target={"url": RC % "24190"}, changes={
        "Company": "Ares Distribution Leclerc", "Brand": "Unbranded",
        "Product": "Meat boudin (pork sausage), GTIN 3505870000343, lot 630915, use-by 27/11/2026, sold at E. Leclerc Arès",
        "Reason": "Listeria monocytogenes detected by product analysis",
        "_notes": S % "fiche 24190 (réf. 2026-11-0196) [original product: boudin a la viande]"},
        reason="published row 24190: Product and Reason French; Company was the store, the fiche origin is ARES DISTRIBUTION LECLERC.",
        evidence=ev(RC % "24190", "boudin a la viande", "3505870000343", "630915",
                    "Détection de la listéria monocytogène suite a une analyse produit",
                    "ARES DISTRIBUTION LECLERC")),
    dict(action="enrich", target={"url": RC % "24191"}, changes={
        "Company": "Société Castelorienne de Distribution E.Leclerc",
        "Brand": "Distralis - Bergers du Haut Béarn",
        "Product": "Farmhouse raw-milk cheese with wild garlic (pavé fermière ail des ours au lait cru), GTIN 2609898025201, lot 217, best before 08/12/2026, sold 03/11/2026 to 12/11/2026 at Leclerc Montval-sur-Loir",
        "Reason": "Presence of Listeria monocytogenes",
        "_notes": S % "fiche 24191 (réf. 2026-11-0197) [original product: pavé fermière ail des ours au lait cru]"},
        reason="published row 24191: Product French; Company was the brand, the fiche origin is SOCIETE CASTELORIENNE DE DISTRIBUTION E.LECLERC.",
        evidence=ev(RC % "24191", "Pavé fermière ail des ours au lait cru", "2609898025201",
                    "Présence de listeria monocytogène",
                    "SOCIETE CASTELORIENNE DE DISTRIBUTION E.LECLERC")),
    dict(action="enrich", target={"url": RC % "24181"}, changes={
        "Company": "Maison Montauzer", "Brand": "Montauzer",
        "Product": "Dried flat pork belly (ventrèche plate séchée): whole, half vacuum-packed and sliced vacuum-packed, lots 240726A (best before 17/12/2026) and 240726B (18/12/2026), sold 20/08/2026 to 19/10/2026",
        "Reason": "Presence of Listeria monocytogenes",
        "_notes": S % "fiche 24181 (réf. 2026-11-0187)"},
        reason="published row 24181: Product French.",
        evidence=ev(RC % "24181", "Ventrèche plate séchée", "240726A", "Présence de Listéria",
                    "MAISON MONTAUZER")),
    dict(action="enrich", target={"url": RC % "24182"}, changes={
        "Company": "Maison Montauzer", "Brand": "Montauzer",
        "Product": "Dried rolled pork belly (ventrèche roulée séchée): whole and sliced vacuum-packed, lot 130726, best before 18/12/2026, sold 04/09/2026 to 19/10/2026",
        "Reason": "Presence of Listeria monocytogenes",
        "_notes": S % "fiche 24182 (réf. 2026-11-0188)"},
        reason="published row 24182: Product French.",
        evidence=ev(RC % "24182", "Ventrèche roulée séchée", "130726", "Présence de Listéria",
                    "MAISON MONTAUZER")),
    # ── published rows with wrong fields ──
    dict(action="enrich", target={"url": CFIA_YESHI}, changes={
        "Date": "2026-11-20",
        "_notes": "[nightly operator 2026-11-26: CFIA notice read tonight — recall date November 20, 2026 (row carried 2026-11-24, the scrape day); Class 2; distribution British Columbia, possibly other provinces]"},
        reason="published CFIA Yeshi row: Date was the scrape day; CFIA recall date is 2026-11-20.",
        evidence=ev(CFIA_YESHI, "November 20, 2026", "Apex Foodsource Ltd.",
                    "Sesame Ginger Nutritional Yeast Dressing & Dip", "6 27843 80892 3")),
    dict(action="enrich", target={"url": FDA_HERRING}, changes={
        "Brand": "No Brand",
        "Product": "Uneviscerated Dried Herring Fish, Item #AF4110, lot code 25180, expiration 12.28.27",
        "Reason": "Potential contamination with Clostridium botulinum (uneviscerated dried fish)",
        "_notes": "[nightly operator 2026-11-26: FDA notice read tonight — announced November 23, 2026; 'no illnesses or adverse reactions ... have been reported'; Brand held the scraper label 'Brand Name(s)' + newline. Distinct from the published 2026-06-04 notice (lot 26020).]"},
        reason="published FDA herring row: Brand carried the scraper label 'Brand Name(s)\\n'; Product lacked item/lot.",
        evidence=ev(FDA_HERRING, "Item #AF4110", "25180", "12.28.27", "No Brand",
                    "Potential contamination with Clostridium Botulinum")),
    # ── Pending: correct, then promote ──
    dict(action="enrich", target={"url": RC % "24195"}, changes={
        "Company": "Spécialités Antillaises Saveurs Créoles", "Brand": "Unbranded",
        "Product": "Three-cheese tart (tarte 3 fromages), lots 26315 (use-by 20/11/2026), 26317 (22/11/2026) and 26327 (02/12/2026), sold at Saveurs et Vie",
        "Reason": "Listeria monocytogenes detected",
        "_notes": S % "fiche 24195 (réf. 2026-11-0201)"},
        reason="Pending 24195: Company empty, Product and Reason French.",
        evidence=ev(RC % "24195", "Tarte 3 fromages", "26327",
                    "Détection de présence de listeria monocytogenes",
                    "SPECIALITES ANTILLAISES SAVEURS CREOLES")),
    dict(action="enrich", target={"url": RC % "24183"}, changes={
        "Company": "Vanni Franck Charcuterie Tavagna", "Brand": "Unbranded",
        "Product": "Figatellu (Corsican pork liver sausage), lot S45021126, sold 03/11/2026 to 06/11/2026 in Haute-Corse",
        "Reason": "Presence of Salmonella spp.",
        "_notes": S % "fiche 24183 (réf. 2026-11-0189) [Product held scraper junk '$lot s45021126$non concerné$$']"},
        reason="Pending 24183: Product was a doubled scraper string of lot codes, Company empty.",
        evidence=ev(RC % "24183", "figatellu", "s45021126", "SALMONELLA SPP",
                    "VANNI FRANCK CHARCUTERIE TAVAGNA")),
    dict(action="enrich", target={"url": RC % "24089"}, changes={
        "Company": "Coopérative U Enseigne", "Brand": "Unbranded",
        "Product": "Vanilla praline rolled Yule log (GTIN 2000000324418, lots 1/288/26 - 1/289/26); chocolate praline Rocher Yule log (GTIN 2000000278902, lot 1/286/26), sold 21/10/2026 to 09/11/2026 at Magasins U",
        "Pathogen": "Foreign material (stones)",
        "Reason": "Physical risk: possible presence of small stone-type foreign bodies in 2/4 mm hazelnut pieces",
        "_notes": S % "fiche 24089 (réf. 2026-11-0101)"},
        reason="Pending 24089: Company empty, Reason French. Same reading as unmerged PRs #51-#55; the fiche loaded tonight.",
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
        p["reason"] = "nightly operator 2026-11-26: " + p["reason"]
    OUT.write_text(json.dumps({"schema": 1, "agent": "nightly-operator",
                               "generated_utc": "20261126T201500Z",
                               "proposals": P}, indent=2, ensure_ascii=False),
                   encoding="utf-8")
    print(OUT)
