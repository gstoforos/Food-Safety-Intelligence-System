# -*- coding: utf-8 -*-
"""Morning fix pass 2026-10-17. Base b2450ecc.

SIX faults, all found by reading the register and the regulators' own
notices this morning. Six tests were RED on plain main before this pass;
every one of them is a symptom of a fault below.

A. TWO PUBLISHED ROWS CARRY A SCRAPE DATE FOR AN OLD, OUT-OF-SCOPE ALERT
   (removed from Recalls — archived, never deleted)

   A1. Recalls / NAFDAC (NG) / Date 2026-10-16 / Pathogen "Risk of Serious
       Injury or Death From Poisoning" / Company "CIN" / "Multivitamin
       Supplements".
       nafdac.gov.ng Public Alert No. 023/2025 read this pass: "Released
       July 5, 2025". The row's Date is the discovery date, 15 months
       later. The alert's hazard is the ABSENCE OF CHILD-RESISTANT
       PACKAGING on iron-containing supplements ("Iron supplements, even a
       few tablets, can be highly toxic to young children") — a packaging
       defect, out of scope under the hazard rule (allergen-only,
       labelling, packaging and quality are OUT). "CIN" is a truncation of
       the brand cell the extractor read, "CINCINNATI (WKRC)", a US
       television station the alert quotes; the firm the alert names is
       iHerb / California Gold Nutrition. Four field defects on one row.
       It is also the row the curator refuses
       (test_curator_scope_2026_09_09) and one of the two no alert can
       reach (test_alert_vocab).

   A2. Recalls / NAFDAC (NG) / Date 2026-10-13 / Pathogen "Adulterated" /
       Chigodson International Company Limited / "Alcoholic Bitters".
       nafdac.gov.ng Public Alert No. 027/2022 read this pass: "May 10,
       2022" — four and a half years before the row's Date. The alert
       names no adulterant: laboratory analysis found the products
       "adulterated, unwholesome and not fit for human consumption". A
       hazard database row whose hazard is the word "adulterated" names no
       hazard; no pathogen alert can reach it (test_alert_vocab).

   Both rows are the same bug shape: the NAFDAC discovery channel surfaces
   old public alerts (the register's archives hold NAFDAC alerts from
   2018, 2019, 2021, 2022, 2023, 2024 and 2025, all stamped with their
   2026 discovery date) and the alert number carries the year — 023/2025,
   027/2022 — so the contradiction is on the row's own URL. Every other
   NAFDAC row was refused upstream; these two are the only ones that
   reached Recalls. tests/test_alert_number_year_matches_date.py, added in
   this pass, holds the shape for every source that numbers its notices
   by year, not just NAFDAC.

   Not re-added with the correct Date: a July-2025 packaging alert and a
   May-2022 "unwholesome" finding are both out of scope on their own
   merits, so a corrected Date would publish two out-of-scope rows into
   closed months. Operator decision if he wants them.

B. NINE PUBLISHED RAPPELCONSO ROWS READ IN FRENCH
   Five Reasons wholly French (test_language_policy,
   test_reason_is_not_wholly_foreign) and one Product the detector caught
   (test_product_and_firm_names_are_english), plus four more Products the
   detector cannot see because they are three words long ("barquette coppa
   100g", "melange tandoori", "u saveurs", "méli-mélo de poissons") — the
   morning read of every Product published in the last 26 h is what caught
   those. Two rows carry reviewer 3's "[needs cleanup: Product not in
   English]" stamp; seven do not.

   Row 23811's Product was not merely French: it read "u saveurs", which
   is the fiche's "Noms des modèles ou références" field — the brand
   range, not a food. The fiche's own category listing on
   rappel.conso.gouv.fr/categorie/0/3 titles it "Jambon à l'ancienne cuit
   au torchon U Saveurs BBC"; both pages were read this pass.

C. A FABRICATED RECALL IDENTIFIER IN A PENDING ROW ABOUT TO PUBLISH
   Pending / AGES (AT) / Cocktail tomatoes / Salmonella Strathcona carries
   Reason "Salmonella Strathcona (Recall ID AGES-2026-07-10)". The AGES
   notice was read this pass: it prints NO reference number of any kind.
   "AGES-2026-07-10" is the extractor's own identifier template filled
   from nothing — the same defect class as the "Recall ID 842632" prompt
   example named in publish-gate rule 2, and the same one the Germinal
   hummus row's Notes record being stripped on 2026-10-16. It is stripped
   here. The row is otherwise a real 16.10.2026 recall and is unblocked
   (see E1).

D. A PENDING ROW PASSING THE PUBLISH GATE WITH A MISTRANSLATED HAZARD
   Pending / Mattilsynet / Norex Market AS / "Dried chrysanthemum buds"
   reads Pathogen "Plant extracts exceeding limit", Reason "Found plant
   extracts above the allowed limit." The row's OWN URL ends
   ...-funn-av-plantevernmidler-over-grenseverdi. Norwegian
   "plantevernmidler" is plant PROTECTION products — pesticides — not
   plant extracts; Mattilsynet writes "plantetoksiner" when it means
   plant toxins. The row passes the publish gate, so the next promoter run
   would have published a pesticide-residue recall under a hazard label
   that no pesticide alert reaches. Corrected from the row's own URL; no
   fetch (mattilsynet.no will not open from this sandbox).

E. FOUR PENDING ROWS WRONGLY BLOCKED OR WRONGLY PASSING
   E1. AGES cocktail tomatoes — blocked on "Company is empty". The AGES
       notice names no company (read this pass). Filled with the
       register's existing wording for an unnamed firm, "Not named on the
       notice (...)", as on the 2026-10-07, 2026-10-13 and 2026-10-16
       rows.
   E2. FSAI (IE) SuperValu / Inspired By Centra smoked salmon — blocked on
       "Pathogen is empty" since discovery, Date empty. FSAI alert 2026.66
       read this pass: Wednesday 14 October 2026, Category 1, "due to the
       presence of Listeria monocytogenes", five products with use-by
       dates, no manufacturer named. A separate alert from the SuperValu
       Smoked Salmon 40 g/150 g recall published at 2026-10-11 and from
       Skellig Bay at 2026-10-12 — verified by URL against all five
       sheets.
   E3. AGES Milbona High Protein Pudding Chocolate / Bacillus cereus —
       PASSES the publish gate with Date 2026-10-16. The AGES page was
       read this pass: the recall date is 19.08.2026. The same recall is
       already PUBLISHED at Recalls 2026-08-19 (NVWA, Lidl Nederland,
       same product, same 200 g pack, same Fude + Serrahn supplier) and
       again at 2026-08-21 (GIS, Pilos) and 2026-06-26 (EFET). The
       promoter would have published a fourth copy, dated two months
       late, into October's totals — the near-duplicate rule does not
       catch it because the Source and the URL differ. Archived as a
       duplicate. If the Austrian national notice is wanted as a row in
       its own right it belongs at Date 2026-08-19, which reopens a
       closed month: operator decision.
   E4. Not touched: the six "Défaut organoleptique" / fishy-smell growth-
       milk rows and the two Pawell CBD rows stay in Pending. A sensory
       defect is a quality matter, out of scope, and the two Pawell fiches
       name no hazard at all. They are correctly stuck, not broken.

F. AN ARCHIVE ROW CONTRADICTING THE REGISTER IN SILENCE
   Weekly_Rejected 2026-10-12, gov.pl Listeria warning, RejectionReason
   "llm-extraction-failed: Llama timeout or circuit-open" — a transport
   failure, not a verdict, on a URL that is PUBLISHED in Recalls. A second
   copy of the same URL was already superseded on 2026-10-13; the capture
   fleet made another. Annotated (test_a_recall_is_not_both_published_and_rejected).
   The two NAFDAC archive rows carrying the URLs removed in A are
   re-annotated in the same pass: their "the same URL is PUBLISHED in
   Recalls" stamp stops being true the moment A lands.

ADDED — three recalls the pipeline discovered and correctly refused
Germany's three 16.10.2026 recalls reached Weekly_Rejected three to four
times each with "no-authority-url: no official BVL press-release URL in
the source article (news-only discovery)". That refusal is right: the
discovery was produktwarnung.eu, a news aggregator. Nothing then went and
found the regulator's own page. lebensmittelwarnung.de was read this pass
and each notice's own URL is on the row. See MISSING_ROWS below.

NOT ADDED, with the reason
  * CAA (JP) rcl 00000035978 (16.10.2026, 大仁産業 「おいし杉バウム（ソフト）」):
    detail page read, 理由 "密閉できていない可能性" — possible failure to
    seal. A packaging defect, out of scope.
  * CAA (JP) rcl 00000035980 (16.10.2026, 京急ストア久里浜店, cookie cream
    tart and three others): detail page read, 理由 "保存温度10℃以下の要冷蔵
    商品を常温で販売" — chilled product requiring ≤10 °C sold at ambient
    temperature. A cold-chain breach with no hazard named. The scope list
    covers "uninspected product (hazard not assessed)" only for FSIS
    import-eligibility cases, so this falls outside it as written.
    Flagged for the operator, not filed on a guess.
  * RappelConso 23805 (16.10.2026, Menzil Pazari sultanas): undeclared
    sulphites — allergen/labelling, out of scope.
  * recalls-rappels.canada.ca (16.10.2026, mooncake): undeclared peanut.
  * NVWA (16.10.2026, ALDI curly parsley): allergens. HAMAX child seat:
    not food.
  * FSA/FSS (13-14.10.2026, Halo Foods peanut butter bars): undeclared
    peanut.
  * lebensmittelwarnung.de 15.10.2026, Thüringer Landstolz Rindergulasch
    im Glas: "Sonstige Gründe", the product "was not heated as intended".
    Outside the 24 h window and no hazard named.
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
JSON_OUT = ROOT / "docs" / "data" / "recalls.json"

TODAY = "2026-10-17"
BY = "morning fix pass " + TODAY
FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
)


# ── helpers (same shapes as the 2026-10-13/14/16 passes) ───────────────────

def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _urls(ws):
    hdr = _hdr(ws)
    if "URL" not in hdr:
        return []
    col = hdr.index("URL") + 1
    return [(r, str(ws.cell(row=r, column=col).value or "").strip())
            for r in range(2, ws.max_row + 1)]


def _find(ws, url):
    """Rows whose URL is EXACTLY `url`, trailing slash and case ignored."""
    want = url.rstrip("/").lower()
    return [r for r, u in _urls(ws) if u.rstrip("/").lower() == want]


def _find_containing(ws, needle):
    n = needle.lower()
    return [r for r, u in _urls(ws) if n in u.lower()]


def _get(ws, row, field):
    hdr = _hdr(ws)
    if field not in hdr:
        return ""
    v = ws.cell(row=row, column=hdr.index(field) + 1).value
    return "" if v is None else str(v)


def _set(ws, row, field, value):
    hdr = _hdr(ws)
    if field not in hdr:
        return False
    cell = ws.cell(row=row, column=hdr.index(field) + 1)
    if str(cell.value or "") == str(value):
        return False
    cell.value = value
    return True


def _note(ws, row, text, *, drop=()):
    prev = _get(ws, row, "Notes").strip()
    if prev.lower() == "nan":
        prev = ""
    elif prev.lower().startswith("nan "):
        prev = prev[4:].strip()
    for d in drop:
        prev = prev.replace(d, "").strip()
    while "  " in prev:
        prev = prev.replace("  ", " ")
    _set(ws, row, "Notes", (prev + " " + text).strip())


def _archive(wb, row: dict, reason: str) -> None:
    """Append to Rejected. The archive is append-only; nothing is deleted."""
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


# ── FAULT A — two published rows out ───────────────────────────────────────

NAFDAC_OUT = {
    "public-alert-no-023-2025": (
        "out_of_scope + scrape-date-in-Date: nafdac.gov.ng Public Alert No. 023/2025, "
        "read %s, is dated \"Released July 5, 2025\" — the row carried the 2026-10-16 "
        "discovery date, 15 months later. The alert's hazard is the absence of "
        "child-resistant packaging on iron-containing supplements (\"Iron supplements, "
        "even a few tablets, can be highly toxic to young children\"), a packaging "
        "defect and out of scope. The alert names iHerb / California Gold Nutrition as "
        "the firm and three products (Daily Prenatal Multi; Ultamins Women's "
        "Multivitamin; Ultamins Women's 50+ Multivitamin); the row's Company \"CIN\" is "
        "a truncation of the brand cell the extractor read, \"CINCINNATI (WKRC)\", a US "
        "television station the alert quotes. Pathogen \"Risk of Serious Injury or Death "
        "From Poisoning\" is an outcome, not a hazard, and no pathogen alert reaches it. "
        "Removed from Recalls by %s. Re-add only at Date 2025-07-05 and only on an "
        "operator ruling that packaging defects are in scope." % (TODAY, BY)),
    "public-alert-no-027-2022": (
        "out_of_scope + scrape-date-in-Date: nafdac.gov.ng Public Alert No. 027/2022, "
        "read %s, is dated \"May 10, 2022\" — the row carried the 2026-10-13 discovery "
        "date, four and a half years later. The alert names no adulterant: laboratory "
        "analysis found the products \"adulterated, unwholesome and not fit for human "
        "consumption\". Pathogen \"Adulterated\" names no hazard and no pathogen alert "
        "reaches it. Removed from Recalls by %s. Re-add only at Date 2022-05-10 and only "
        "with a named substance." % (TODAY, BY)),
}

# The stamp those URLs' archive copies carry becomes false once A lands.
STALE_STAMP = (
    " [RE-ANNOTATED %s — the Recalls copy this row was superseded against was itself "
    "removed from Recalls on %s (see tools/repair_%s.py fault A: the alert predates the "
    "row's Date by years and its hazard is out of scope). This URL is no longer "
    "published anywhere in the register, so there is no longer a published copy for this "
    "archive row to contradict. The refusal recorded below stands.]"
    % (TODAY, TODAY, TODAY.replace("-", "_")))


def remove_the_two_nafdac_rows(wb) -> int:
    ws = wb["Recalls"]
    hdr = _hdr(ws)
    removed = 0
    for key, reason in NAFDAC_OUT.items():
        for r in sorted(_find_containing(ws, key), reverse=True):
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(hdr)}
            print("    Recalls row %d: %s %s %r — removing (%s)"
                  % (r, str(row.get("Date"))[:10], row.get("Source"),
                     str(row.get("Product"))[:46], reason.split(":", 1)[0]))
            _archive(wb, row, reason)
            ws.delete_rows(r, 1)
            removed += 1
    # a Weekly_Review copy would put the row back on the Sunday slice
    if "Weekly_Review" in wb.sheetnames:
        wr = wb["Weekly_Review"]
        for key in NAFDAC_OUT:
            for r in sorted(_find_containing(wr, key), reverse=True):
                print("    Weekly_Review row %d: removing the review copy too" % r)
                wr.delete_rows(r, 1)
    return removed


def reannotate_the_nafdac_archive_rows(wb) -> int:
    n = 0
    for sheet, col in (("Rejected", "RejectReason"),
                       ("Weekly_Rejected", "RejectionReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        if col not in _hdr(ws):
            continue
        for key in NAFDAC_OUT:
            for r in _find_containing(ws, key):
                cur = _get(ws, r, col)
                if "RE-ANNOTATED %s" % TODAY in cur:
                    continue
                if "superseded" not in cur.lower():
                    continue          # never said it was superseded; leave it
                _set(ws, r, col, (cur + STALE_STAMP).strip())
                print("    %s row %d: superseded-against-a-published-copy stamp "
                      "re-annotated" % (sheet, r))
                n += 1
    return n


# ── FAULT B — nine French rows ─────────────────────────────────────────────
#
# Every value below is a translation of text already on the row or read on
# the fiche named in `read`. Brand names, protected names and lot/weight/
# date details are kept as written; nothing is added.

FRENCH_ROWS = {
    "23830": dict(
        product="Coppa, 100 g tray",
        reason="Suspected Listeria monocytogenes",
        read='Product read on the row as "barquette coppa 100g"; motif "Suspicion de Listeria monocytogène"',
        orig_reason="fr",
    ),
    "23790": dict(
        product="Tex-Mex chicken",
        reason=("E.Leclerc Cholet Ribou is recalling the following products after "
                "Salmonella was detected."),
        read='Product read on the row as "poulet tex mex"',
        orig_reason="fr",
    ),
    "23760": dict(
        product="Mixed fish selection (méli-mélo de poissons)",
        reason="Histamine contamination",
        read='Product read on the row as "méli-mélo de poissons"; motif "Contamination à l\'histamine"',
        orig_reason="fr",
    ),
    "23825": dict(
        product=("Picodons, apérichèvre, cœur, brique, bûche, pyramide, tommes "
                 "\"affinage entre deux\" (retail sale, unlabelled)"),
        reason=("Product contaminated with Escherichia coli — the notice writes "
                "\"echiachicoli\"; its stated risk is Shiga toxin-producing "
                "Escherichia coli (STEC)"),
        read=('fiche read %s: marque "vente au détail/sans étiquette", références '
              '"Picodons, apérichèvre, cœur, brique, buche, pyramide, Tommes affinage '
              'entre deux", motif "produit contaminé par echiachicoli", risques '
              '"Escherichia coli shiga toxinogène (STEC)", "Origine de la fiche : HEVIN '
              'CHARLOTTE CABRIOU", zone "ARDECHE (07)", publication 16/10/2026' % TODAY),
        orig_reason="fr",
        note=("[cheese names kept as written: Picodon is a protected designation "
              "(AOP) and apérichèvre, cœur, brique, bûche, pyramide and tomme are "
              "the notice's own product names. Only \"vente au détail/sans "
              "étiquette\" is translated. The word \"cheese\" is NOT added: the "
              "fiche never prints it.]"),
    ),
    "23819": dict(
        product="Tandoori spice blend",
        read='Product read on the row as "melange tandoori"; its Reason was already English',
    ),
    "23811": dict(
        product=("Traditional cloth-cooked ham, U Saveurs BBC (jambon à l'ancienne "
                 "cuit au torchon), lot 02625631, use-by 04/11/2026"),
        reason=("As a precaution the store is recalling the product because of the "
                "presence of Listeria monocytogenes"),
        read=('fiche read %s: marque "U", références "U saveurs", GTIN "2887938000000", '
              'lot "02625631", "Date limite de consommation 04/11/2026", motif "Par '
              'mesure de précaution, le magasin procède à un rappel du produit en raison '
              'd\'une présence Listéria Monocytogènes", risques "Listeria monocytogenes '
              '(agent responsable de la listériose)", "Origine de la fiche : SOCAPDIS '
              'HYPER-U", distributeur "Hyper U Agde", région Occitanie, catégorie '
              '"Viandes", publication 16/10/2026; the fiche\'s own entry on '
              'https://rappel.conso.gouv.fr/categorie/0/3, read the same pass, titles it '
              '"Jambon à l\'ancienne cuit au torchon U Saveurs BBC"' % TODAY),
        orig_reason="fr",
        note=("[the Product was not merely French: \"u saveurs\" is the fiche's "
              "\"Noms des modèles ou références\" field, a brand range rather than "
              "a food. The food is named on the regulator's own category listing "
              "for this fiche, read this pass, and that is where the English "
              "Product comes from. Company/Brand left as published: the fiche "
              "prints no \"nom juridique du fabricant\" and its only firm field is "
              "\"Origine de la fiche : SOCAPDIS HYPER-U\", the submitter.]"),
    ),
    "23810": dict(
        product="All products from the Izakaya sushi counter in Saint-Dié",
        reason=("On 13/10/2026 Hana Group withdrew from sale all products on the "
                "Izakaya counter in Saint-Dié after Listeria monocytogenes was "
                "detected."),
        read='Product read on the row as "l\'ensemble des produits du stand sushi izakaya de saint-dié"',
        orig_reason="fr",
    ),
    "23786": dict(
        product=("Whole cooked chilled shrimp, 30/40 count, wild Penaeus monoceros "
                 "caught in the Western Indian Ocean (FAO 51)"),
        read=('Product read on the row as "Crevette entière cuite réfrigérée 30/40 '
              'sauvage penaeus monocéros pêchés en océan indien ouest (FAO51)"; its '
              'Reason was already English'),
    ),
    "23844": dict(
        product="Coppa, 60 slices, 2 x 250 g, Metro Chef",
        reason="Listeria monocytogenes contamination",
        read='Product read on the row as "coppa 60 tranches 2x250g metro chef"; motif "Contamination à la Listeria monocytogenes"',
        orig_reason="fr",
    ),
}


def translate_the_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, spec in FRENCH_ROWS.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            print("    fiche %s: no row on main — skipped" % fiche)
            continue
        for r in rows:
            if _get(ws, r, "Product") == spec["product"]:
                continue
            was_p = _get(ws, r, "Product")
            was_r = _get(ws, r, "Reason")
            stamped = any(s in _get(ws, r, "Notes") for s in STAMPS)
            bits = ["[translated %s: %s -> English. %s.]"
                    % (TODAY,
                       "Product and Reason" if spec.get("reason") else "Product",
                       spec["read"]),
                    "[original product: %s]" % was_p]
            _set(ws, r, "Product", spec["product"])
            if spec.get("reason"):
                _set(ws, r, "Reason", spec["reason"])
                bits.append('[original Reason (%s): "%s"]'
                            % (spec["orig_reason"], was_r))
            if not stamped:
                bits.append(
                    "[detector-miss %s: reviewer 3 did not stamp this row "
                    "Product-not-in-English, and the language test's two-hit "
                    "threshold did not see this value either — it is too short. "
                    "The threshold is deliberately not loosened; the morning read "
                    "of every Product published in the last 26 h is what caught "
                    "it. The stamp is described here, never quoted: a note that "
                    "quotes it makes a translated row look stamped to the next "
                    "pass.]" % TODAY)
            if spec.get("note"):
                bits.append(spec["note"])
            _note(ws, r, " ".join(bits), drop=STAMPS)
            print("    Recalls row %d (fiche %s): %r -> %r"
                  % (r, fiche, was_p[:40], spec["product"][:40]))
            n += 1
    return n


# ── FAULTS C, D, E1, E2 — four Pending rows ────────────────────────────────

AGES_TOMATOES = "https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/produkt/produktwarnung-salmonellen-in-tomaten-aus-zwei-regionen-in-sizilien"
MATTILSYNET_CHRYS = "https://www.mattilsynet.no/tilbakekallinger/torkede-krystantemum-knopper-chrystantemum-bud-50-gram-tilbakekalles-pa-grunn-av-funn-av-plantevernmidler-over-grenseverdi"
FSAI_SALMON = "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-centra-inspired-by-supervalu-smoked-salm"
AGES_MILBONA = "https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/produkt/milbona-high-protein-pudding-schoko-200g"

PENDING_FIXES = {
    AGES_TOMATOES: dict(
        fields={
            "Company!": "Not named on the notice (origin Ragusa and Caltanissetta, Sicily)",
            "Reason!": "Salmonella Strathcona",
            "Product!": "Cocktail tomatoes",
        },
        note=("[fabricated identifier stripped %s: the Reason read \"Salmonella "
              "Strathcona (Recall ID AGES-2026-07-10)\". The AGES notice was read this "
              "pass and prints NO reference number of any kind — not an AGES number, "
              "not a RASFF number; its only outbound link is to the AGES "
              "Radar für Infektionskrankheiten. \"AGES-2026-07-10\" is the extractor's "
              "own identifier template filled from nothing, the same defect class as "
              "the \"Recall ID 842632\" prompt example named in publish-gate rule 2. "
              "READ ON THE NOTICE: \"Lebensmittel-Warnung\" 16.10.2026; "
              "\"Rückrufgrund: Salmonellen\"; serovar \"Salmonella Strathcona\"; "
              "\"Cocktailtomaten\" from \"den italienischen Regionen Ragusa und "
              "Caltanissetta in Sizilien\"; no company and no brand named. Company "
              "follows the register's existing wording for an unnamed firm.]" % TODAY),
    ),
    MATTILSYNET_CHRYS: dict(
        fields={
            "Pathogen!": "Pesticide residues exceeding limit",
            "Reason!": ("Pesticide residues found above the maximum residue level. "
                        "Mattilsynet states this can be harmful if consumed."),
        },
        note=("[hazard corrected %s: the row read Pathogen \"Plant extracts exceeding "
              "limit\" and Reason \"Found plant extracts above the allowed limit.\" The "
              "correct value is on the row's OWN URL, which ends "
              "\"...-funn-av-plantevernmidler-over-grenseverdi\": Norwegian "
              "\"plantevernmidler\" is plant PROTECTION products — pesticides — not "
              "plant extracts. Mattilsynet writes \"plantetoksiner\" when it means "
              "plant toxins (cf. the register's 2026-09-21 Herbapol row). No fetch: "
              "mattilsynet.no does not open from this sandbox, so nothing beyond the "
              "row's own URL was used and no lot, limit or substance name was added. "
              "The row passes the publish gate, so without this the next promoter run "
              "would have published a pesticide-residue recall under a label no "
              "pesticide alert reaches.]" % TODAY),
    ),
    FSAI_SALMON: dict(
        fields={
            "Date!": "2026-10-14",
            "Pathogen!": "Listeria monocytogenes",
            "Reason!": "Presence of Listeria monocytogenes",
            "Company!": ("Not named on the notice (FSAI alert 2026.66; brands SuperValu "
                         "and Inspired By Centra)"),
            "Brand!": "SuperValu; Inspired By Centra",
            "Product!": ("SuperValu Signature Tastes Dingle Gin Infused Smoked Salmon "
                         "100 g (use-by 31/10/2026); SuperValu Signature Tastes Handcut "
                         "Smoked Salmon Ribbons 80 g (04/11/2026); SuperValu Signature "
                         "Tastes Applewood Smoked Salmon 100 g (31/10/2026); SuperValu "
                         "Signature Tastes Dill and Black Pepper Smoked Salmon 100 g "
                         "(04/11/2026); Inspired By Centra Applewood Smoked Salmon 80 g "
                         "(31/10/2026)"),
            "Class!": "Recall",
            "Country!": "Ireland",
        },
        note=("[enriched %s from the authority page: FSAI food alert 2026.66 read this "
              "pass — \"Wednesday, 14 October 2026\", \"Category 1: requiring action\", "
              "\"due to the presence of Listeria monocytogenes\", country of origin "
              "Ireland, five products with the use-by dates above, no batch codes "
              "listed and no manufacturer named. The row had sat in Pending at "
              "'pending_enrichment' with Date, Pathogen and Reason empty and the alert "
              "title copied into Company, Product and Reason alike. Verified by URL "
              "against all five sheets before enriching: this is a DIFFERENT alert from "
              "the SuperValu Smoked Salmon 40 g/150 g recall published at 2026-10-11 "
              "and from Skellig Bay at 2026-10-12. Tier is recomputed by the pipeline, "
              "never hand-set.]" % TODAY),
    ),
}

MILBONA_REASON = (
    "duplicate: the same recall is already PUBLISHED in Recalls at 2026-08-19 (NVWA, "
    "Lidl Nederland, \"Milbona High Protein Pudding Chocolate Flavour, 200 g\", supplier "
    "Fude + Serrahn Milchprodukte GmbH & Co. KG) and again at 2026-08-21 (GIS, Pilos) "
    "and 2026-06-26 (EFET). The AGES page was READ %s: its recall date is 19.08.2026, "
    "not the 2026-10-16 this row carried — the row's Date is the produktwarnung.eu "
    "discovery date. The row PASSED the publish gate, so the next promoter run would "
    "have published a fourth copy of an August recall into October's totals; the "
    "near-duplicate rule does not catch it because the Source and the URL differ. "
    "Archived by %s. If the Austrian national notice is wanted as a row of its own it "
    "belongs at Date 2026-08-19, which reopens a closed month — operator decision, not "
    "taken here." % (TODAY, BY))


def fix_pending_rows(wb) -> int:
    ws = wb["Pending"]
    hdr = _hdr(ws)
    col = {name: i for i, name in enumerate(hdr)}
    n = 0
    for url, spec in PENDING_FIXES.items():
        rows = _find(ws, url)
        if not rows:
            print("    %s...: no Pending row on main — skipped" % url[:58])
            continue
        for r in rows:
            changed = []
            for key, val in spec["fields"].items():
                force = key.endswith("!")
                field = key.rstrip("!")
                if field not in col:
                    continue
                cur = _get(ws, r, field).strip()
                if force or not cur or cur in ("None", "nan", "—"):
                    if _set(ws, r, field, val):
                        changed.append(field)
            if not changed:
                continue
            _note(ws, r, spec["note"])
            print("    Pending row %d: %s -> %s"
                  % (r, _get(ws, r, "Source"), ", ".join(changed)))
            n += 1
    return n


def archive_the_milbona_duplicate(wb) -> int:
    ws = wb["Pending"]
    hdr = _hdr(ws)
    n = 0
    for r in sorted(_find(ws, AGES_MILBONA), reverse=True):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(hdr)}
        print("    Pending row %d: %s %r -> Rejected (duplicate of the 2026-08-19 "
              "NVWA row)" % (r, row.get("Source"), str(row.get("Product"))[:40]))
        _archive(wb, row, MILBONA_REASON)
        ws.delete_rows(r, 1)
        n += 1
    return n


# ── FAULT F — a silent archive contradiction ───────────────────────────────

#: Words the test accepts as "this row says which copy wins".
RESOLVED = ("superseded", "duplicate", "unresolved contradiction",
            "broken provenance", "already_approved")

#: A refusal that is NOT a content verdict: the box fell over, so the row
#: says nothing at all about the recall.
TRANSPORT = ("llm-extraction-failed", "llama timeout", "circuit-open",
             "circuit open", "timeout", "fetch error", "http 5")

STAMP_TRANSPORT = (
    "[SUPERSEDED %s — the recall at this URL is PUBLISHED in Recalls and the refusal "
    "recorded below is not a content verdict: it records a transport failure (the "
    "extraction box timed out or its circuit was open), which says nothing about the "
    "recall itself. The published copy wins. This row is kept as the audit trail of the "
    "failed extraction. If the same URL keeps coming back this way, the fix belongs in "
    "the capture path, not in another annotation here.]" % TODAY)
STAMP_VERDICT = (
    "[SUPERSEDED %s — the defect named below was repaired and this recall is PUBLISHED "
    "in Recalls. This row is kept only as the audit trail of the original refusal; the "
    "Recalls copy is what the register means.]" % TODAY)


def supersede_the_silent_archive_rows(wb) -> int:
    """Every archive row that shares a URL with a PUBLISHED recall and says
    nothing about which copy wins — swept, not named one URL at a time.

    The 2026-10-17 instance was Weekly_Rejected 2026-10-12, the gov.pl
    Listeria warning on one batch of rennet cheese, refused with
    "llm-extraction-failed: Llama timeout or circuit-open". A second copy
    of a DIFFERENT gov.pl Listeria slug was superseded on 2026-10-13 and
    the slugs differ only in their tail, so naming a URL would have missed
    it. This function is run AFTER rows leave Recalls, so a row whose
    published twin this pass removed is correctly left alone.
    """
    from pipeline.merge_master import _normalize_url_for_dedup as key
    published = {key(u) for _, u in _urls(wb["Recalls"])} - {"", "nan"}
    n = 0
    for sheet, rc in (("Weekly_Rejected", "RejectionReason"),
                      ("Rejected", "RejectReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        if rc not in _hdr(ws):
            continue
        for r, url in _urls(ws):
            if key(url) not in published:
                continue
            cur = _get(ws, r, rc)
            low = cur.lower()
            if any(w in low for w in RESOLVED):
                continue
            stamp = (STAMP_TRANSPORT if any(w in low for w in TRANSPORT)
                     else STAMP_VERDICT)
            _set(ws, r, rc, (stamp + " " + cur).strip())
            print("    %s row %d: %s — silent contradiction annotated (%s)"
                  % (sheet, r, _get(ws, r, "Date")[:10],
                     "transport failure" if stamp is STAMP_TRANSPORT
                     else "content verdict"))
            n += 1
    return n


# ── ADDED — three German recalls the pipeline refused for want of a URL ─────

LMW = "https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/2026/10_Oktober/"
PW = "https://www.produktwarnung.eu/"

MISSING_ROWS = [
    {
        "Date": "2026-10-16",
        "Source": "BVL (DE)",
        "Company": "Dinkelhof",
        "Brand": "Dinkelhof",
        "Product": ("Spelt Sombrelli pasta with wild herbs (Dinkel Sombrelli mit "
                    "Wildkräutern), 500 g, best before 09.06.2028"),
        "Pathogen": "Salmonella",
        "Reason": ("Salmonella was detected in one batch of the wild herbs used in the "
                   "product, supplied by an external supplier. Sold in Bavaria."),
        "Class": "Recall",
        "Country": "Germany",
        "Outbreak": 0,
        "URL": LMW + "261016_18_BY_Dinkel_Nudeln/261016_18_BY_Dinkel_Nudeln.html",
        "Notes": (
            "[manual %s: morning fix pass MISSING #1. Discovered by the gap-finder fleet "
            "via produktwarnung.eu and correctly refused three times into Weekly_Rejected "
            "with \"no-authority-url: no official BVL press-release URL in the source "
            "article (news-only discovery)\". The regulator's own notice was then found "
            "and read this pass on lebensmittelwarnung.de and that is the URL on this "
            "row. READ ON THE NOTICE: Produktbezeichnung \"Dinkel Sombrelli mit "
            "Wildkräutern, 500 Gramm\"; Inverkehrbringer \"Dinkelhof\", \"Dorfstr. 3, "
            "82216 Maisach\", Ansprechpartner \"Michael Dinkel\"; Grund der Warnung "
            "\"Krankheitserreger\" with \"In einer Charge der im Produkt verwendeten "
            "Wildkräuter von einem externen Zulieferer wurden Salmonellen festgestellt.\"; "
            "\"Mindesthaltbarkeitsdatum: 09.06.2028\"; betroffenes Bundesland \"Bayern\"; "
            "Veröffentlichung 16.10.2026. No Losnummer is printed, so none is recorded. "
            "Discovery page: %s2026/10/16/rueckruf-salmonellen-dinkelhof-ruft-dinkel-"
            "sombrelli-mit-wildkraeutern-zurueck/39146 (news, not cited as the authority). "
            "Verified absent from Recalls, Pending, Weekly_Review, Weekly_Rejected and "
            "Rejected by URL, company, product and the word \"Sombrelli\". "
            "[original product: Dinkel Sombrelli mit Wildkräutern, 500 Gramm] "
            "[original Reason (de): \"In einer Charge der im Produkt verwendeten "
            "Wildkräuter von einem externen Zulieferer wurden Salmonellen festgestellt.\"]"
            % (TODAY, PW)),
    },
    {
        "Date": "2026-10-16",
        "Source": "BVL (DE)",
        "Company": "Ulrich Walter Gesellschaft mit beschränkter Haftung",
        "Brand": "Lebensbaum",
        "Product": ("Savory, rubbed and dried (Bohnenkraut, gerebelt und getrocknet), "
                    "25 g — batches 103150 (best before 31.08.2028), 104062 "
                    "(31.12.2028) and 104731 (31.03.2029); article 1546, EAN "
                    "4012346154695"),
        "Pathogen": "Pyrrolizidine alkaloids",
        "Reason": ("Contamination with pyrrolizidine alkaloids and their N-oxides was "
                   "detected. The notice states these can harm health, in particular "
                   "through their damaging effect on the liver, and that some are "
                   "regarded as genotoxic and so can damage DNA and raise cancer risk. "
                   "Buyers are told not to eat the product and to return it to the "
                   "point of sale."),
        "Class": "Recall",
        "Country": "Germany",
        "Outbreak": 0,
        "URL": (LMW + "261016_17_NI_Bohnenkraut,_gerebelt_und_getrocknet,_25_Gramm/"
                "261016_17_NI_Bohnenkraut,_gerebelt_und_getrocknet,_25_Gramm.html"),
        "Notes": (
            "[manual %s: morning fix pass MISSING #2. Discovered via produktwarnung.eu "
            "and correctly refused four times into Weekly_Rejected for want of an "
            "official BVL URL; the regulator's own notice was found and read this pass on "
            "lebensmittelwarnung.de. READ ON THE NOTICE: Produktbezeichnung "
            "\"Bohnenkraut, gerebelt und getrocknet, 25 Gramm\"; Inverkehrbringer "
            "\"Ulrich Walter Gesellschaft mit beschränkter Haftung\", \"Doktor "
            "Jürgen-Ulderup-Straße 12, 49356 Diepholz\"; Grund der Meldung \"Rückstände "
            "und Kontaminanten\" with \"Kontamination mit Pyrrolizidinalkaloiden und "
            "deren N-Oxiden wurde nachgewiesen.\"; mögliche Folgen \"Pyrrolizidinalkaloide "
            "können bei Aufnahme gesundheitsschädlich sein, insbesondere aufgrund ihrer "
            "leberschädigenden Wirkung.\"; Chargen 103150 / MHD 31.08.2028, 104062 / "
            "31.12.2028, 104731 / 31.03.2029; Artikelnummer 1546; EAN 4012346154695; "
            "betroffene Bundesländer Baden-Württemberg, Bayern, Berlin, Brandenburg, "
            "Hamburg, Hessen, Mecklenburg-Vorpommern, Niedersachsen, Nordrhein-Westfalen, "
            "Saarland, Schleswig-Holstein, Thüringen; Meldung erstellt von Niedersachsen; "
            "Veröffentlichung 16.10.2026. The brand Lebensbaum is Ulrich Walter's own and "
            "is on the discovery page; the notice itself prints no Marke field. "
            "Verified absent from all five sheets by URL, company, product and the words "
            "\"Bohnenkraut\" and \"Lebensbaum\". "
            "[original product: Bohnenkraut, gerebelt und getrocknet, 25 Gramm] "
            "[original company: Ulrich Walter Gesellschaft mit beschränkter Haftung] "
            "[original Reason (de): \"Kontamination mit Pyrrolizidinalkaloiden und deren "
            "N-Oxiden wurde nachgewiesen.\"]" % TODAY),
    },
    {
        "Date": "2026-10-16",
        "Source": "BVL (DE)",
        "Company": "Fitmart GmbH & Co. KG",
        "Brand": "ESN",
        "Product": ("ESN Designer Protein Bar Dark Chocolate Salted Almond, 45 g — "
                    "batch L3226505-11, best before 13.04.2027"),
        "Pathogen": "Foreign material (plastic)",
        "Reason": ("A hard, transparent plastic foreign body, about the size of the "
                   "almond pieces, was found in individual bars from this batch. Entry "
                   "during production is considered unlikely; entry in the supply chain "
                   "is not excluded. Because the affected bars cannot be narrowed down "
                   "within the batch, the whole batch is recalled. The notice states a "
                   "risk of injury, for example from swallowing the foreign body, and "
                   "tells buyers not to eat the bars and not to pass them on."),
        "Class": "Recall",
        "Country": "Germany",
        "Outbreak": 0,
        "URL": (LMW + "261015_16_SH_ESN_Designer_Protein_Bar/"
                "261015_16_SH_ESN_Designer_Protein_Bar_Meldung.html"),
        "Notes": (
            "[manual %s: morning fix pass MISSING #3. Not captured by any sheet in any "
            "form — unlike MISSING #1 and #2 it is not even in Weekly_Rejected. READ ON "
            "THE NOTICE (lebensmittelwarnung.de, read this pass): Bildunterschrift \"ESN "
            "Designer Protein Bar Dark Chocolate Salted Almond\", Seitentitel \"ESN "
            "Designer Protein Bar Dark Chocolate, 45 Gramm\"; Inverkehrbringer \"Fitmart "
            "GmbH & Co. KG\", \"Werner-von-Siemens-Straße 8\", \"25337 Elmshorn\"; Grund "
            "der Meldung \"Fremdkörper\" with \"In einzelnen Riegeln dieser Charge wurde "
            "ein harter, transparenter Fremdkörper aus Kunststoff\" of about the size of "
            "the almond pieces; \"Ein Eintrag in der Produktion gilt als unwahrscheinlich. "
            "Ein Eintrag in der Lieferkette ist nicht ausgeschlossen.\"; Los-Kennzeichnung "
            "\"L3226505-11\"; MHD \"13.04.2027\"; Verpackungseinheit \"45 Gramm\"; 15 "
            "Bundesländer; Veröffentlichung 16.10.2026. The notice's own file name "
            "carries 261015 while the published date on the page is 16.10.2026; the "
            "page's date is what is recorded. The discovery page %s2026/10/16/"
            "rueckruf-fremdkoerper-esn-ruft-designer-protein-bar-dark-chocolate-salted-"
            "almond-zurueck/39135 titles the product \"Designer Vegan Protein, Milky "
            "Chocolate\"; the regulator's wording is used instead. Verified absent from "
            "all five sheets by URL, company, brand, product and the words \"Fitmart\" "
            "and \"Designer Protein\". "
            "[original product: ESN Designer Protein Bar Dark Chocolate Salted Almond, "
            "45 Gramm] [original Reason (de): \"In einzelnen Riegeln dieser Charge wurde "
            "ein harter, transparenter Fremdkörper aus Kunststoff.\"]" % (TODAY, PW)),
    },
]


def add_missing_recalls() -> int:
    """Queue through tools/add_manual_row.py — the real Pending code path."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    present = set()
    for sheet in ("Recalls", "Pending"):
        rows = wb[sheet].iter_rows(values_only=True)
        hdr = list(next(rows))
        i = hdr.index("URL")
        for r in rows:
            if r and r[i]:
                present.add(str(r[i]).strip().rstrip("/").lower())
    wb.close()
    todo = [row for row in MISSING_ROWS
            if row["URL"].strip().rstrip("/").lower() not in present]
    for row in MISSING_ROWS:
        if row not in todo:
            print("    already in Recalls or Pending — left alone: %s"
                  % row["Product"][:52])
    if not todo:
        return 0
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(todo, fh, ensure_ascii=False)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip()[-2500:])
    if out.returncode != 0:
        print("    add_manual_row FAILED:", (out.stderr or "")[-1200:])
        return 0
    return len(todo)


# ── main ───────────────────────────────────────────────────────────────────

def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)

    print("A  two published rows carrying a scrape date for an old, out-of-scope alert:")
    removed = remove_the_two_nafdac_rows(wb)
    print("   removed from Recalls: %d" % removed)
    print("B  published RappelConso rows reading in French:")
    n_fr = translate_the_french_rows(wb)
    print("   translated: %d" % n_fr)
    print("C+D+E1+E2  Pending rows with a fabricated id, a mistranslated hazard "
          "or a field the authority page settles:")
    n_pend = fix_pending_rows(wb)
    print("   repaired: %d" % n_pend)
    print("E3  a Pending duplicate that passed the publish gate:")
    n_dup = archive_the_milbona_duplicate(wb)
    print("   archived: %d" % n_dup)
    print("F  archive rows contradicting the register:")
    n_sil = supersede_the_silent_archive_rows(wb)
    n_re = reannotate_the_nafdac_archive_rows(wb)
    print("   annotated: %d silent, %d re-annotated" % (n_sil, n_re))

    wb.save(XLSX)
    wb.close()

    print("ADDED  recalls the pipeline discovered and refused for want of an "
          "authority URL:")
    n_add = add_missing_recalls()
    print("   queued into Pending: %d" % n_add)

    from pipeline.merge_master import mirror_json_from_xlsx
    print("recalls.json re-mirrored: %d rows"
          % mirror_json_from_xlsx(XLSX, JSON_OUT))
    print("ROWS_REMOVED=%d" % removed)
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
