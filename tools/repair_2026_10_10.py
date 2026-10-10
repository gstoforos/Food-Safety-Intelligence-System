# -*- coding: utf-8 -*-
"""Operator pass, 2026-10-10: five weeks of Swiss notices the BLV collector
missed, and one FSA recall blocked in Pending on an empty Reason.

Run on a FRESH clone of main, by tools/rebase_and_verify.py, which then
runs pipeline.promote_gate_passing --apply behind it.

NOTHING IS REMOVED from Recalls. Seven rows are queued into Pending and
five Pending rows are filled from their own authority pages (section C,
in PENDING_FILLS below). Section D translates thirteen published French/Polish
Reason/Product values in place. Section E runs the register-wide
SUPERSEDED sweep (an EFET archive row re-discovered via news on 10-05
and mis-filed as "allergen" while its URL is the published Listeria
recall of 2026-06-17).

Every fact below was read on an authority page, and each row's Notes say
which page.


A. SWITZERLAND — SIX IN-SCOPE NOTICES, NONE IN ANY SHEET
=========================================================
Prompted by Food Safety News, "Botulism sickens two in Switzerland"
(2026-10-08), which sat in the NEWS sheet only. The official notices were
read on the FSVO's own listing, 2026-10-10:

  https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/
  rueckrufe-und-oeffentliche-warnungen.html

The last Swiss row in Recalls is dated 2026-09-04. The collector
(scrapers/europe_non_eu/blv_ch.py) returned 0 rows every day since
2026-09-06 — fixed in code in the same upload; this script recovers what
it missed. Out of scope and NOT added: RecallSwiss 1149 (Lidl, broken cold
chain), the 21.09 Ezogelin warning (undeclared gluten — allergen only),
the 11.09 Migros sashimi warning (wrong use-by date — labelling).

RecallSwiss detail pages are a JavaScript application with no readable
content, so recall facts come from the FSVO listing sentence, and for the
terrine also from the Canton of Valais communiqué of 07.10.2026, an
official cantonal public-health release:

  https://www.vs.ch/web/communication/w/deux-cas-de-botulisme-en-valais-
  rappel-de-produit-diffus%C3%A9-par-l-office-f%C3%A9d%C3%A9ral-de-la-
  s%C3%A9curit%C3%A9-alimentaire-et-des-affaires-v%C3%A9t%C3%A9rinaires-1

Lot numbers and dates that appeared only in the press (manufacture
03.09.2026, shelf life 02.06.2027) are NOT written: the official texts
read here do not state them.


B. FSA (UK) FSA-PRIN-48-2026 — GREENCORE, SALMONELLA, BLOCKED ON REASON
======================================================================
Pending from 2026-10-08 with Company, Product, Pathogen and URL filled
and Reason empty; the publish gate held it on "Reason is empty", and the
confirm agent archived it twice to Weekly_Rejected, the second reason
being "Product looks like a headline" for a five-product list. The FSA
scraper copies Reason from the API "description" field, which this alert
did not carry. The FSA's own alert title, as published on its alerts site
(alerts.food.gov.uk/article/6920 — robots-disallowed to automated reads,
title seen in the search result for that page on 2026-10-10), is:

  "Greencore recalls several products due to the presence of Salmonella"

The row is re-queued with Reason set from that sentence and nothing else;
the archived copies stay as they are (append-only).
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

LISTING = ("https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/"
           "rueckrufe-und-oeffentliche-warnungen.html")
VALAIS = ("https://www.vs.ch/web/communication/w/deux-cas-de-botulisme-en-valais-"
          "rappel-de-produit-diffus%C3%A9-par-l-office-f%C3%A9d%C3%A9ral-de-la-"
          "s%C3%A9curit%C3%A9-alimentaire-et-des-affaires-v%C3%A9t%C3%A9rinaires-1")
RS = "https://www.recallswiss.admin.ch/customer-access/#Recalls/"
OP = "[operator pass 2026-10-10]"

MISSED_ROWS = [
    {
        "Date": "2026-10-07",
        "Source": "BLV (CH)",
        "Company": "Le Grand'Joie (Eddy Gaspoz)",
        "Brand": "Le Grand'Joie",
        "Product": "Pork shank terrine (Terrine de jarret de porc), 360 g glass jar",
        "Pathogen": "Clostridium botulinum",
        "Reason": ("Suspected botulism; two cases of foodborne botulism identified in "
                   "Valais. Do not eat or open the product; discard it."),
        "Class": "Recall",
        "Country": "Switzerland",
        "Outbreak": 1,
        "URL": RS + "1148",
        "Notes": (f"{OP} FSVO listing ({LISTING}), RecallSwiss 1148: \"Le Grand'Joie ruft "
                  "das Produkt «Terrine de jarret de porc (360g), en bocal en verre» wegen "
                  "Verdacht auf Botulismus zurück — 07.10.2026\". Cases, producer (Eddy "
                  f"Gaspoz – Le Grand'Joie, VD) and sale points (markets in Sierre and "
                  f"Martigny, certain butcher shops) from the Canton of Valais communiqué "
                  f"of 07.10.2026: {VALAIS}. [original product: Terrine de jarret de porc "
                  "(360g), en bocal en verre] Missed by the BLV collector; reached AFTS "
                  "only via Food Safety News (NEWS sheet, 2026-10-08)."),
    },
    {
        "Date": "2026-10-07",
        "Source": "BLV (CH)",
        "Company": "Rapelli Orior Food AG",
        "Brand": "RAP",
        "Product": "Beef tartare (RAP Tartare Manzo), 1400 g (20 × 70 g)",
        "Pathogen": "Escherichia coli (generic)",
        "Reason": "Escherichia coli detected.",
        "Class": "Recall",
        "Country": "Switzerland",
        "Outbreak": 0,
        "URL": RS + "1147",
        "Notes": (f"{OP} FSVO listing ({LISTING}), RecallSwiss 1147: \"Rapelli Orior Food "
                  "AG ruft das Produkt «RAP Tartare Manzo 1400g 20x70g Cg» wegen Nachweis "
                  "von Escherichia coli zurück — 07.10.2026\". [original product: RAP "
                  "Tartare Manzo 1400g 20x70g Cg] Missed by the BLV collector."),
    },
    {
        "Date": "2026-10-02",
        "Source": "BLV (CH)",
        "Company": "Kägi Söhne AG",
        "Brand": "Kägi",
        "Product": "Kägi Butterbiscuits (butter biscuits)",
        "Pathogen": "Foreign material (metal)",
        "Reason": "Foreign body: metal fragments.",
        "Class": "Recall",
        "Country": "Switzerland",
        "Outbreak": 0,
        "URL": RS + "1146",
        "Notes": (f"{OP} FSVO listing ({LISTING}), RecallSwiss 1146: \"Kägi Söhne AG ruft "
                  "das Produkt «Kägi Butterbiscuits» wegen Fremdkörper (Metallfragmente) "
                  "zurück — 02.10.2026\". Missed by the BLV collector."),
    },
    {
        "Date": "2026-10-09",
        "Source": "BLV (CH)",
        "Company": "Action Switzerland",
        "Brand": "Cokoc",
        "Product": "Cokoc fried-egg fruit gummies (Fruchtgummi Spiegeleier), 80 g",
        "Pathogen": "Mold",
        "Reason": "Mould.",
        "Class": "Recall",
        "Country": "Switzerland",
        "Outbreak": 0,
        "URL": RS + "1150",
        "Notes": (f"{OP} FSVO listing ({LISTING}), RecallSwiss 1150: \"Action Switzerland "
                  "ruft das Produkt «Cokoc Fruchtgummi Spiegeleier 80 g» aufgrund von "
                  "Schimmel zurück — 09.10.2026\". [original product: Cokoc Fruchtgummi "
                  "Spiegeleier 80 g] The same product's French recall is RappelConso 23674 "
                  "(2026-10-01); this is the Swiss regulator's own recall. Missed by the "
                  "BLV collector."),
    },
    {
        "Date": "2026-09-11",
        "Source": "BLV (CH)",
        "Company": "Coop",
        "Brand": "Coop Naturaplan",
        "Product": ("Naturaplan organic Jumbo shrimp 120 g (art. 4.084.735, lots 14127, "
                    "14120, 14124) and Naturaplan organic Cocktail shrimp 150 g (art. "
                    "4.484.998, lots 14120, 14132)"),
        "Pathogen": "Salmonella",
        "Reason": ("Salmonella detected; a health risk cannot be ruled out. Do not eat "
                   "the affected products."),
        "Class": "Public Health Alert",
        "Country": "Switzerland",
        "Outbreak": 0,
        "URL": "https://www.blv.admin.ch/de/newnsb/QQe-MhuPbath",
        "Notes": (f"{OP} FSVO public warning of 11.09.2026, read on its own page: "
                  "\"Öffentliche Warnung: Salmonellen in Bio Crevetten Jumbo und Bio "
                  "Crevetten Cocktail von Coop Naturaplan\". Recalled by Coop; sold at "
                  "Coop supermarkets, Coop City, Coop Pronto and coop.ch. [original "
                  "product: Naturaplan Bio Crevetten Jumbo 120 g; Naturaplan Bio Crevetten "
                  "Cocktail 150 g] Missed by the BLV collector."),
    },
    {
        "Date": "2026-09-23",
        "Source": "BLV (CH)",
        "Company": "Asia Markt; Beta Markt GmbH; Thai Haus Potongkham",
        "Brand": "ROYAL ORIENT",   # the register's existing spelling (RappelConso 23655)
        "Product": ("Bamboo shoot strips, 567 g can, lots LM00021616 and LM00022060, "
                    "best before 29.09.2028"),
        "Pathogen": "Bisphenol A (chemical contaminant)",
        "Reason": ("Bisphenol A content too high; a health risk cannot be ruled out. "
                   "Do not consume."),
        "Class": "Public Health Alert",
        "Country": "Switzerland",
        "Outbreak": 0,
        "URL": "https://www.blv.admin.ch/de/newnsb/Oo7g5dWsbEox",
        "Notes": (f"{OP} FSVO public warning of 23.09.2026, read on its own page: "
                  "\"Öffentliche Warnung: Bisphenol A in Bambussprossen-Konserven\". "
                  "Company = the three businesses the FSVO names as having withdrawn the "
                  "product and started the recall (Asia Markt, Reinach BL; Beta Markt GmbH, "
                  "Langenthal; Thai Haus Potongkham, Dübendorf); no importer is named. "
                  "Measured level not stated. [original product: BAMBOO SHOOT (Strips)] "
                  "Missed by the BLV collector."),
    },
]

GREENCORE_URL = "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026"
GREENCORE_ROW = {
    "Date": "2026-10-08",
    "Source": "FSA (UK)",
    "Company": "Greencore",
    "Brand": "—",
    # Exactly as the FSA scraper read it from the FSA API productDetails.
    "Product": ("Pinch Pan Poppin’ Chicken Pasanda & Nutty Pilau Rice; Pinch Bangin’ "
                "Butter Chicken & Nutty Pilau Rice; M&S Super Nutty Wholefood with a Soy "
                "& Ginger Dressing; M&S Nutrient Dense Nutty Super Wholefood; M&S Fresh "
                "Collection Pistachio Pesto"),
    "Pathogen": "Salmonella",
    "Reason": "Greencore recalls several products due to the presence of Salmonella.",
    "Class": "Alert",
    "Country": "United Kingdom",
    "Outbreak": 0,
    "URL": GREENCORE_URL,
    "Notes": (f"PRIN-48-2026 {OP} Re-queued for re-validation. Archived twice by the "
              "confirm agent (2026-10-08/09): Reason was empty (the FSA API record had "
              "no description) and the 5-product list was judged a 'headline' by "
              "length alone — both fixed in code in the same upload. Reason is the "
              "FSA's own alert title (alerts.food.gov.uk/article/6920, as listed in "
              "search results 2026-10-10; the page refuses automated reads). Company, "
              "Product, Pathogen and Date are as the FSA scraper read them from the FSA "
              "API."),
}


def _urls_in(sheets) -> set:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    seen = set()
    for sheet in sheets:
        if sheet not in wb.sheetnames:
            continue
        rows = wb[sheet].iter_rows(values_only=True)
        try:
            hdr = list(next(rows))
        except StopIteration:
            continue
        if "URL" not in hdr:
            continue
        i = hdr.index("URL")
        for r in rows:
            if r and i < len(r) and r[i]:
                seen.add(str(r[i]).strip().lower())
    wb.close()
    return seen


def _queue(rows) -> int:
    if not rows:
        return 0
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run([sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
                         cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip()[-3000:])
    if out.returncode != 0:
        print("  add_manual_row FAILED:", (out.stderr or "")[-800:])
        return 0
    return len(rows)


def requeue_greencore() -> int:
    """Only Recalls/Pending count as present: the archived copies are the
    refusals being corrected, and add_manual_row replaces a rejected key."""
    if GREENCORE_URL in _urls_in(("Recalls", "Pending")):
        print("  already in Recalls or Pending — left alone")
        return 0
    return _queue([GREENCORE_ROW])


def add_missed_recalls() -> int:
    """Queue through tools/add_manual_row.py — the same path a scraper uses."""
    seen = _urls_in(("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected", "Rejected"))
    todo = [r for r in MISSED_ROWS if r["URL"].strip().lower() not in seen]
    for r in MISSED_ROWS:
        if r not in todo:
            print(f"  already present, not added again: {r['URL']}")
    return _queue(todo)


#: C. Two Pending rows blocked on fields their own authority page states.
#: Keyed by URL; each value is written only where the row's field is EMPTY,
#: or where noted as a correction of scrape debris. Notes get the source.
PENDING_FILLS = {
    # GIS (PL) 06.10.2026 — read on its own page 2026-10-10. Product held a
    # fragment of the Polish headline; Company was empty.
    ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-"
     "obecnosci-bakterii-escherichia-coli-produkujacej-toksyne-shiga-stec-w-dwoch-"
     "rodzajach-sera-z-surowego-mleka-krowiego"): {
        "Company": "Gillot SAS",
        "Brand": "Le Petit Cru; Bertrand Crémier",
        "Product!": ("Raw-milk Camembert 250 g — Le Petit Cru (lot 232511, best before "
                     "17/10/26) and Bertrand Crémier (lot 232511, best before 21/10/26)"),
        "Reason!": ("Shiga toxin-producing Escherichia coli (STEC) detected; risk of food "
                    "poisoning. Do not eat the listed products."),
        "note": ("GIS warning of 06.10.2026, read on its own page: producer Gillot SAS, "
                 "Le Moulin, 61220 Saint-Hilaire-de-Briouze, France; Camembert au lait "
                 "cru 250 g (Le Petit Cru) and Camembert Bertrand Crémier 250 g, both lot "
                 "232511. [original product: wykrycie obecności bakterii Escherichia coli "
                 "produkującej toksynę Shiga (STEC) w dwóch rodzajach sera z surowego "
                 "mleka krowiego]"),
    },
    # FSAI (IE) 2026.62, 08.10.2026 — read on its own page 2026-10-10. The
    # Irish alert for the same Greencore incident as FSA-PRIN-48-2026.
    "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-various-ready-to-eat-products": {
        "Date": "2026-10-08",
        "Company!": "Greencore",          # held "several ready-to-eat Marks and Spencer products"
        "Brand!": "Marks and Spencer",
        "Pathogen": "Salmonella",
        "Product!": ("M&S Food Super Nutty Wholefood with a Soy & Ginger Dressing 200 g; "
                     "M&S Food Nutrient Dense Nutty Super Wholefood 285 g; M&S Collection "
                     "Pistachio and Parmigiano Reggiano Cheese Pesto 115 g"),
        "Reason!": "Possible presence of Salmonella in pistachios.",
        "note": ("FSAI alert 2026.62 of 08.10.2026, read on its own page: \"Recall of "
                 "several ready-to-eat Marks and Spencer products due to the possible "
                 "presence of Salmonella in pistachios\"; Greencore is recalling; use-by "
                 "dates up to and including 10/10, 11/10 and 13/10/2026. Same incident as "
                 "FSA (UK) FSA-PRIN-48-2026."),
    },
    # RappelConso 23707, 07.10.2026 — read on its own page 2026-10-10. The
    # fiche names no recalling firm (voluntary recall, details "transmises
    # par le professionnel"); distributors are Fresh / Mon Marché / Grand
    # Frais. Operator 2026-10-10: publish it and leave the firm unnamed —
    # Company says exactly that rather than promoting a distributor.
    "https://rappel.conso.gouv.fr/fiche-rappel/23707/interne": {
        "Company!": "Not named on the notice (sold at Fresh / Mon Marché and Grand Frais)",
        "Brand!": "(unbranded — Sans Marque)",        # the register's spelling
        "Product!": ("Pork paupiette, Savoy style (Paupiette de porc à la savoyarde), "
                     "lot 162680008, use-by 09/10/2026"),
        "Reason!": "Detection of Salmonella (Salmonella spp.).",
        "note": ("RappelConso fiche 23707 (ref. 2026-10-0036), published 07/10/2026: motif "
                 "\"Détection de Salmonelle\", risk \"Salmonella spp (agent responsable de "
                 "la salmonellose)\", sold 30/09–05/10/2026 at Fresh / Mon Marché and Grand "
                 "Frais; no recalling firm named. Operator ruling 2026-10-10: publish with "
                 "the firm left unnamed. [original product: paupiette de porc à la "
                 "savoyarde]"),
    },
    # RappelConso 23698, 05.10.2026 — read on its own page 2026-10-10.
    # Operator 2026-10-10: "add it with verbatim microbial risk, the info in
    # the description, Tier 1."
    "https://rappel.conso.gouv.fr/fiche-rappel/23698/interne": {
        "Date!": "2026-10-05",
        "Company!": "Auchan Retail Services",          # the register's spelling
        "Brand!": "CHATKA",
        "Product!": ("Antarctic king crab, 100% meat, 170 g (CHATKA), GTIN 5410238917325, "
                     "lot 6266, best before 24/09/2030"),
        "Pathogen!": "Suspected microbiological risk (container swelling)",
        "Reason!": ("Suspected microbiological risk; risk of swelling. Stated risks: "
                    "manufacturing defect, seal failure (e.g. micro-leaks, faulty heat "
                    "sealing) or packaging anomalies. Sold 29/09–02/10/2026 throughout "
                    "France. Do not eat; return to the store."),
        "Tier!": 1,
        "note": ("RappelConso fiche 23698, published 05/10/2026: motif \"Suite à une "
                 "Suspicion de risque microbiologique, risque de gonflement\"; risques "
                 "\"Défaut de fabrication, défaut d'étanchéité (ex: micro fuites, "
                 "thermoscellage défectueux) ou anomalies de conditionnement\". Tier 1 is "
                 "the operator's ruling of 2026-10-10 (a swelling, possibly leaking sealed "
                 "shelf-stable seafood product), not the computed tier. [original product: "
                 "crabe royal de l'antarctique 100 % chair 170g]"),
    },
    # CFS (HK) press release of 09.10.2026 — read on its own page 2026-10-10.
    # Arrived from the daily search with Company = Brand = "Press Release"
    # and the headline as Product; Date was the day before the release.
    "https://www.cfs.gov.hk/english/press/20261009_12660.html": {
        "Date!": "2026-10-09",
        "Company!": "ALF Retail Hong Kong Limited (importer)",
        "Brand!": "Marks & Spencer",
        "Product!": ("Marks & Spencer Nutrient Dense Nutty Super Wholefood Salad 285 g "
                     "(use-by 8–11 Oct 2026); Marks & Spencer Collection Pistachio Pesto "
                     "115 g (use-by 8–13 Oct 2026)"),
        "Reason!": ("Possibly contaminated with Salmonella; recalled in the UK and Ireland. "
                    "The importer has stopped sales and is recalling under CFS instructions."),
        "note": ("CFS press release of 09.10.2026, read on its own page: products imported "
                 "from the UK by ALF Retail Hong Kong Limited; the CFS learned of the "
                 "incident from FSA (UK) and FSAI notifications. Same incident as "
                 "FSA-PRIN-48-2026 and FSAI 2026.62. [original company/brand: Press Release] "
                 "[original product: CFS urges public not to consume two kinds of imported "
                 "prepackaged salad and pesto products suspected to be contaminated with "
                 "Salmonella]"),
    },
}


def fill_pending(wb) -> int:
    ws = wb["Pending"]
    hdr = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    col = {name: i for i, name in enumerate(hdr)}
    n = 0
    for row in ws.iter_rows(min_row=2):
        url = str(row[col["URL"]].value or "").strip()
        spec = PENDING_FILLS.get(url)
        if not spec:
            continue
        changed = []
        for key, val in spec.items():
            if key in ("note", "Tier!"):
                continue
            force = key.endswith("!")
            field = key.rstrip("!")
            cell = row[col[field]]
            if force or not str(cell.value or "").strip() or str(cell.value).strip() == "None":
                if str(cell.value or "") != val:
                    cell.value = val
                    changed.append(field)
        if changed:
            # Tier is derived, never hand-set: recompute it from the filled
            # Pathogen exactly as tools/add_manual_row.py does.
            from scrapers._models import assign_tier
            try:
                outbreak = 1 if int(row[col["Outbreak"]].value or 0) else 0
            except (TypeError, ValueError):
                outbreak = 0
            tier = spec.get("Tier!") or assign_tier(str(row[col["Pathogen"]].value or ""),
                                                   outbreak)
            if str(row[col["Tier"]].value) != str(tier):
                row[col["Tier"]].value = tier
                changed.append("Tier")
            row[col["Notes"]].value = (str(row[col["Notes"]].value or "")
                                       + f" {OP} filled {', '.join(changed)} — "
                                       + spec["note"]).strip()
            n += 1
            print(f"  {url[:70]}…: {', '.join(changed)}")
    return n


#: D. Published RappelConso rows whose Reason or Product is still French —
#: the standing "published Reason/Product must be English" rule, and the
#: three language tests that went red on main while no morning pass was
#: uploaded (2026-10-08..10). Each value is a faithful translation of the
#: row's own text; brand names, lot and weight details kept; the original
#: goes to Notes. Fiche 23733 was re-read on its own page 2026-10-10
#: (motif "Erreur de DLC étiquetée (allongement de la DLC)", risk
#: "Listeria monocytogenes"). Keyed by URL, lower-cased.
_RC = "https://rappel.conso.gouv.fr/fiche-rappel/"
RECALLS_ENGLISH = {
    _RC + "23743/interne": {
        "Product": ("Cooked chilled whole farmed shrimp 30/40, Signature du Poissonnier, "
                    "300 g (Penaeus monodon)"),
        "Reason": "Presence of Listeria monocytogenes.",
    },
    _RC + "23733/interne": {
        "Product": "Garlic pâté 180 g",
        "Reason": "Labeled use-by date error (use-by date extended).",
    },
    _RC + "23734/interne": {"Reason": "Labeled use-by date error (use-by date extended)."},
    _RC + "23737/interne": {"Reason": "Labeled use-by date error (use-by date extended)."},
    _RC + "23740/interne": {"Reason": "Labeled use-by date error (use-by date extended)."},
    _RC + "23731/interne": {
        "Product": "Organic pure apple juice, 6 × 20 cl and 20 cl",
        "Reason": "Presence of mycotoxins (patulin).",
    },
    _RC + "23701/interne": {
        "Product": "Mexican-style chicken platter 1 kg",
        "Reason": "Suspected Salmonella spp.",
    },
    _RC + "23716/interne": {
        "Product": "Bouchot mussels",
        "Reason": "Presence of E. coli.",
    },
    _RC + "23748/interne": {"Reason": "Presence of Listeria monocytogenes."},
    _RC + "23746/interne": {"Reason": "Presence of Listeria monocytogenes."},
    _RC + "23616/interne": {"Reason": "Suspected Listeria."},
    _RC + "23538/interne": {"Reason": "Presence of Listeria."},
    # GIS (PL) 2026-10-08, Bell Polska — Reason and Product were the Polish
    # listing title; translated from the row's own text.
    ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-"
     "obecnosci-bakterii-listeria-monocytogenes-w-jednej-partii-boczku-wedzonego"): {
        "Product": "Smoked pork belly bacon, sliced (Boczek wieprzowy wędzony, plastry)",
        "Reason": "Listeria monocytogenes detected in one batch of smoked bacon.",
    },
    # CFS (HK) press release of 07.10.2026 — read on its own page 2026-10-10.
    # PUBLISHED with Company = Brand = "Press Release" (the page's section
    # label), which the publish gate now reads as empty. Same Gillot SAS
    # camembert lot 232511 as the GIS (PL) warning of 06.10.2026.
    "https://www.cfs.gov.hk/english/press/20261007_12650.html": {
        "Company": "Repertoire Culinaire Hong Kong Limited (importer)",
        "Brand": "Bertrand Crémier",
        "Product": "Camembert Bertrand Crémier 250 g, batch 232511, use-by 17 Oct 2026 (France)",
        "Reason": ("Possibly contaminated with Shiga toxin-producing E. coli (STEC); under "
                   "recall in the EU (RASFF). The importer has stopped sales and is "
                   "recalling under CFS instructions."),
    },
}


def english_recalls(wb) -> int:
    ws = wb["Recalls"]
    hdr = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    col = {name: i for i, name in enumerate(hdr)}
    n = 0
    for row in ws.iter_rows(min_row=2):
        spec = RECALLS_ENGLISH.get(str(row[col["URL"]].value or "").strip().lower())
        if not spec:
            continue
        notes = []
        for field, new in spec.items():
            old = str(row[col[field]].value or "")
            if old == new:
                continue
            row[col[field]].value = new
            url_now = str(row[col["URL"]].value)
            if "cfs.gov.hk" in url_now:
                notes.append(f"[corrected {field}: was {old!r}; read on the CFS page]")
                continue
            lang = "pl" if "gov.pl" in url_now else "fr"
            tag = f"original Reason ({lang})" if field == "Reason" else "original product"
            notes.append(f'[{tag}: "{old}"]' if field == "Reason" else f"[{tag}: {old}]")
        if notes:
            row[col["Notes"]].value = (str(row[col["Notes"]].value or "") + f" {OP} translated/corrected "
                                       + " ".join(notes)).strip()
            n += 1
    return n


EFET_5398 = ("https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/"
             "item/5398-deltio-typou-epektasi-anaklisis-proionton-kapnistis-pestrofas")
EFET_NOTE = (f" {OP} DUPLICATE — this URL is the published EFET press release 5398 "
             "(2026-06-17, Listeria monocytogenes, expansion of the Karpenisi smoked "
             "trout recall). Re-discovered via news (protothema.gr) on 2026-10-05 and "
             "filed here as \"allergen\" by the gap-finder rules; the published copy "
             "wins.")


def mark_efet_duplicate() -> int:
    """A content verdict (\"allergen\") the SUPERSEDED sweep rightly does not
    touch — so it is resolved by hand, saying which copy wins."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Weekly_Rejected"]
    hdr = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    iu, ir = hdr.index("URL"), hdr.index("RejectionReason")
    n = 0
    for row in ws.iter_rows(min_row=2):
        if str(row[iu].value or "").strip().lower() != EFET_5398:
            continue
        reason = str(row[ir].value or "")
        if "duplicate" in reason.lower():
            continue
        row[ir].value = (reason + EFET_NOTE).strip()
        n += 1
    if n:
        wb.save(XLSX)
    wb.close()
    return n


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    print("C — five Pending rows filled from their own authority pages")
    n_c = fill_pending(wb)
    print(f"  {n_c} row(s) filled\n")
    print("D — published French Reason/Product values put into English")
    n_d = english_recalls(wb)
    print(f"  {n_d} row(s) translated\n")
    if n_c or n_d:
        wb.save(XLSX)
    wb.close()
    print("B — FSA-PRIN-48-2026 Greencore: re-queued with the FSA's own Reason")
    n_b = requeue_greencore()
    print(f"  {n_b} row(s) queued\n")

    print("A — six Swiss notices the BLV collector missed")
    n_a = add_missed_recalls()
    print(f"  {n_a} row(s) queued into Pending\n")

    print("E — archive rows that contradict a published recall on the same URL")
    n_dup = mark_efet_duplicate()
    print(f"  {n_dup} EFET archive row(s) marked DUPLICATE")
    from pipeline.promote_gate_passing import supersede_every_published_url
    n_e = supersede_every_published_url(XLSX)
    print(f"  {n_e} archive row(s) stamped SUPERSEDED\n")

    if n_a or n_b or n_c or n_d or n_e or n_dup:
        from pipeline.merge_master import mirror_json_from_xlsx
        wb2 = openpyxl.load_workbook(XLSX, read_only=True)
        print(f"Recalls {before} -> {wb2['Recalls'].max_row - 1}")
        wb2.close()
        print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
