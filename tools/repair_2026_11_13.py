# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-13. Base c10742a6.

Four faults and seven missing recalls, all found by reading the register
and the regulators' own notices this morning. Two tests were RED on plain
main before this pass; both are symptoms of a fault below.

A. FIVE PUBLISHED RAPPELCONSO ROWS READ IN FRENCH
   Every row RappelConso published on 12/11/2026 reached Recalls with its
   Product field in French, and two of them with the Reason in French too.
   Five carry the reviewers' "[needs cleanup: Product not in English]"
   stamp; one carries "[needs cleanup: Reason not in English]" as well.
   tests/test_language_policy.py::TestPublishedWorkbook::
   test_every_description_field_is_english was RED on plain main, naming
   fiche 24065's Reason.

   Note what the stamp did NOT catch: fiche 24073's Reason, "Contamination
   en Salmonella", is French and carries no stamp — the detector needs two
   non-English word hits and "Salmonella" is a Latin binomial it counts as
   English. That is the fifth consecutive morning on which the by-hand
   sweep of every Product and Reason published in the last 26 h has found
   something the detector missed (2026-11-07 fiche 24027 "saucisson à
   l'ail"; 2026-10-30 fiche 23552 "Unbranded"; 2026-11-04 fiche 23983 a
   barcode; 2026-11-06 fiche 23996 a brand).

B. TWO OF THOSE ROWS CARRY A BRAND IN THE COMPANY FIELD
   Fiche 24062 published Company "B&S et Sans Marque" — that is the
   fiche's Marque field, and it is half French ("et sans marque" = "and
   unbranded"); the firm on the fiche is ALLONNEDIS. Fiche 24065
   published Company "Bonne Maman", which is the Marque; the fiche's
   "Origine de la fiche" is NOVANDIE. Same shape as the 2026-11-06 fiche
   23996 repair (Company was the brand "baymar"). This register's
   convention, set on 2026-11-06, is Company = the firm on "Origine de la
   fiche", Brand = the Marque.

C. PENDING HELD ONE AESAN ALERT TWICE, ONE "www." APART
   https://www.aesan.gob.es/en/alertas/2026_81   (pending_gap_v2)
   https://aesan.gob.es/en/alertas/2026_81       (pending_gap)
   One notice, two rows. Reviewer 2 indexes Pending as url_to_idx[url] —
   one slot per URL, last write wins — so the first copy was invisible to
   it: it could never be corrected, promoted or refused, while reviewer 3
   held it and the offline promoter reported it every morning.
   tests/test_pending_holds_one_row_per_url.py was RED on plain main.

   The CAUSE is fixed in code in this pass, not here:
   merge_master.validate_pending_row keyed this gate with a bare
   url.rstrip("/").lower() on both sides while _dedup_key used
   _normalize_url_for_dedup, so every fold the normaliser performs —
   protocol, "www.", fragment, query canonicalisation, and the admin.ch /
   AESAN / FSA language variants added for exactly this reason — was lost
   at this one gate. pipeline/agents/curator.py and
   pipeline/recall_confirm_agent.py carried the same shape and are fixed
   with it. tests/test_pending_gate_keys_urls_like_the_dedup_key.py holds
   it (RED on plain main, 7 of 15 cases).

   Here the surviving copy is the more advanced one (pending_gap_v2); the
   pending_gap copy is archived to Rejected, never deleted.

D. THE SURVIVING AESAN ROW IS BLOCKED ON FIELDS ITS OWN ALERT SETTLES
   Its Product is "calendar_today 10/11/2026" — a scraped UI artifact,
   the page's date widget, not a food — and Company is empty, so the
   publish gate holds it. Both were read this morning on the alert
   itself, https://www.aesan.gob.es/en/alertas/2026_81. Same shape as the
   2026-11-08 repair of AESAN alert 2026_80, whose Product was a
   search-engine snippet.

MISSING (seven, queued through tools/add_manual_row.py — the real Pending
code path, so each is subject to the same dedup, gate and review stages
as every other row):
   1 AGES (AT)  mould in wheat germ, published 12.11.2026
   4 CAA (JP)   mould x2, glass, a sweetener over its usage standard
   2 CFIA (CA)  metal in beer, Sudan I in turmeric, both 2026-11-12

NOT ADDED, deliberately: produktwarnung.eu carried two German food
recalls on 12.11.2026 — ahead GmbH Marshmallows Rainbow Swirls
(Salmonellen) and Stockhausen Gastro Service Putenbrustfilet
(Salmonellen, found in the firm's own Eigenkontrolle). Both are already
in Weekly_Rejected, six rows between them, attributed to BVL (DE), AFSCA
(BE) and NVWA (NL) with URL empty. Neither appears on
lebensmittelwarnung.de, whose newest entries this morning are dated
11.11.2026, so neither has a regulator's own per-recall URL. R3 forbids a
news or aggregator URL and forbids a guessed slug, so they stay out until
BVL publishes them. The six Weekly_Rejected rows attributing a
produktwarnung.eu article to three different national agencies are a
separate defect in the discovery channel, reported but not repaired here.

Also not added: CFS (HK) published one item on 12.11.2026, "FEHD and Hong
Kong Customs continue to inspect premises selling hairy crabs in various
districts" — an enforcement inspection, not a recall and naming no
product, firm or hazard. R2 refuses it.
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

TODAY = "2026-11-13"
BY = "morning fix pass " + TODAY
FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
    "[needs cleanup: Reason not in English]",
)


# ── sheet helpers (same shape as every previous repair script) ─────────────

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


def _note(ws, row, text):
    old = _get(ws, row, "Notes")
    if text in old:
        return False
    return _set(ws, row, "Notes", (old + " " + text).strip())


def _drop_stamps(ws, row):
    notes = _get(ws, row, "Notes")
    new = notes
    for s in STAMPS:
        new = new.replace(s, "")
    new = " ".join(new.split())
    if new != notes:
        _set(ws, row, "Notes", new)
        return True
    return False


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


# ── FAULTS A + B — the five French rows of 12/11/2026 ──────────────────────
#
# Every value below was read this morning on the fiche itself, on
# rappel.conso.gouv.fr. Each entry gives the fiche's own French, so the
# translation can be checked against the source without opening it.

FRENCH_ROWS = {
    "24062": dict(
        product="Pumpkin seeds, and mixes containing pumpkin seeds: "
                "sports mix, special summer salad mix",
        original_product="Mélanges contenant Graines de courge : Mélange "
                         "sportif, Mélange spécial salade estivale Graines "
                         "de courge",
        company="Allonnedis",
        original_company="B&S et Sans Marque",
        brand="B&S / unbranded",
        original_brand="B&S et sans marque",
        note=(
            'Read on fiche 24062 today (Fiche réf. 2026-11-0075, Version 1), '
            'publiée le 12/11/2026 — Dénomination "Graines de courge et '
            'mélanges contenant des graines de courge", sous-catégorie '
            '"Noix et graines", Marque "B&S et sans marque", Modèles ou '
            'références "Mélanges contenant Graines de courge : Mélange '
            'sportif, Mélange spécial salade estivale Graines de courge", '
            'Conditionnements "Barquettes à poids variables entre 100g et '
            '600G, barquette 60g, barquette 70g", Motif "Détection '
            'Salmonelle", Risques "Salmonella spp (agent responsable de la '
            'salmonellose)", Distributeurs "SNC ALLONNEDIS, ZAC de la '
            'Pointe, 72190 Sargé-lès-le-Mans", Départements SARTHE (72), '
            'commercialisée du 01/03/2026 au 23/09/2026, GTIN 3580281282205 '
            '/ 3580281232231 / 3580281232224. Company held the fiche\'s '
            'Marque, which is a brand field and half French ("et sans '
            'marque" = "and unbranded"); the firm is "Origine de la fiche : '
            'ALLONNEDIS". Company/Brand corrected on the 2026-11-06 '
            'convention (Company = Origine de la fiche, Brand = Marque).'),
    ),
    "24075": dict(
        product="Chicken legs, 3 kg (Rhônes Alpes)",
        original_product="Cuisse de poulet 3 Kgs Rhônes Alpes",
        note=(
            'Read on fiche 24075 today (Fiche réf. 2026-11-0088, Version 1), '
            'publiée le 12/11/2026 — Marque "L\'Atelier de Gustave", Modèles '
            'ou références "Cuisse de poulet 3 Kgs Rhônes Alpes", '
            'sous-catégorie "Viandes", Motif "Présence de Salmonella", '
            'Risques "Salmonella spp (agent responsable de la '
            'salmonellose)", Distributeurs "Carrefour Bassens (73) - '
            'Carrefour Market Seyssel (01) - Carrefour Grésy Sur Aix (73) - '
            'Carrefour Market St Jean de Maurienne (73)", région '
            'Auvergne-Rhône-Alpes, commercialisée depuis le 04/11/2026, '
            'lots 315831 / 316435 / 317089, DLC 08/11, 09/11 and '
            '10/11/2026, "Origine de la fiche : L\'ATELIER DE GUSTAVE". '
            '"Rhônes Alpes" is kept as the fiche spells it; it is part of '
            'the product reference, not a translated word.'),
    ),
    "24074": dict(
        product="Chicken legs x2 (Rhônes Alpes), S/AT",
        original_product="Cuisse de poulet X2 Rhônes Alpes S/AT",
        note=(
            'Read on fiche 24074 today (Fiche réf. 2026-11-0087, Version 1), '
            'publiée le 12/11/2026 — Marque "L\'Atelier de Gustave", Modèles '
            'ou références "Cuisse de poulet X2 Rhônes Alpes S/AT", Motif '
            '"Présence de Salmonella", Risques "Salmonella spp (agent '
            'responsable de la salmonellose)", région Auvergne-Rhône-Alpes, '
            'commercialisée depuis le 05/11/2026, GTIN 3661632021800, lots '
            '313700 and 317379, DLC 11/11 and 13/11/2026, "Origine de la '
            'fiche : L\'ATELIER DE GUSTAVE". "S/AT" is an abbreviation the '
            'fiche does not expand, so it is carried over as printed rather '
            'than guessed at.'),
    ),
    "24073": dict(
        product="Plain chicken legs, 3 kg",
        original_product="Cuisses de poulet Nature x 3Kgs",
        reason="Salmonella contamination",
        original_reason="Contamination en Salmonella",
        note=(
            'Read on fiche 24073 today (Fiche réf. 2026-11-0086, Version 1), '
            'publiée le 12/11/2026 — Marque "L\'Atelier de Gustave", Modèles '
            'ou références "Cuisses de poulet Nature x 3Kgs" (the fiche\'s '
            'title differs: "Cuisse de poulet X12 Rhônes Alpes"; the '
            'register publishes the Modèles field, as the scraper reads it), '
            'Motif "Contamination en Salmonella", Risques "Salmonella spp '
            '(agent responsable de la salmonellose)", région '
            'Auvergne-Rhône-Alpes, commercialisée depuis le 04/11/2026, '
            'GTIN 3661632012860, lots 315829 / 316434 / 317091, DLC 08/11, '
            '09/11 and 10/11/2026, "Origine de la fiche : L\'ATELIER DE '
            'GUSTAVE". The Reason was French and carried NO "needs cleanup" '
            'stamp — the detector counts "Salmonella" as English and needs '
            'two non-English hits, so "Contamination en Salmonella" reads as '
            'English to it. Found by the by-hand sweep, not by the stamp.'),
    ),
    "24065": dict(
        product="Creamy yoghurt on a bed of raspberries, 2 x 125 g",
        original_product="5013230301 : CREMEUX SUR LIT FRAMBOISES 125X2 BM",
        reason="Potential risk of small glass fragments in the yoghurt",
        original_reason="RISQUE POTENTIEL de bris de verre de petite taille "
                        "dans le Yaourt",
        company="Novandie",
        original_company="Bonne Maman",
        brand="Bonne Maman",
        note=(
            'Read on fiche 24065 today (Fiche réf. 2026-11-0078, Version 1), '
            'publiée le 12/11/2026 — Dénomination "Yaourt Cremeux sur lit de '
            'Framboise", sous-catégorie "Lait et produits laitiers", Marque '
            '"Bonne Maman", Modèles ou références "5013230301 : CREMEUX SUR '
            'LIT FRAMBOISES 125X2 BM", Motif "RISQUE POTENTIEL de bris de '
            'verre de petite taille dans le Yaourt", Risques "Inertes '
            '(verre, métal, plastique, papier, textile…)", Distributeurs '
            '"AUCHAN, CARREFOUR, LECLERC, SYSTEME U, INTERMARCHE", France '
            'entière, GTIN 3608580183297, lot 23/11, DLC 23/11/2026, '
            'commercialisée du 24/10/2026 au 11/11/2026, "Origine de la '
            'fiche : NOVANDIE". The published Product was the Modèles field, '
            'which on this fiche opens with the internal reference 5013230301 '
            'and ends in the initials "BM"; the food is the Dénomination. '
            'Same shape as the 2026-11-04 fiche 23983 repair (a barcode) and '
            'the 2026-11-06 fiche 23996 repair (a brand). Company held the '
            'Marque; the firm is NOVANDIE.'),
    ),
}


def translate_the_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, spec in FRENCH_ROWS.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            print("    fiche %s: no Recalls row at that URL — left alone" % fiche)
            continue
        for r in rows:
            changed = False
            if _get(ws, r, "Product") == spec["original_product"]:
                changed |= _set(ws, r, "Product", spec["product"])
                changed |= _note(ws, r, "[original product: %s]"
                                 % spec["original_product"])
            # Case-insensitive: fiche 24065 prints its Motif in capitals
            # ("RISQUE POTENTIEL de bris de verre...") and the scraper
            # title-cases it, so an exact compare here matched nothing and
            # left the French Reason published.
            if "reason" in spec and (_get(ws, r, "Reason").strip().lower()
                                     == spec["original_reason"].strip().lower()):
                changed |= _set(ws, r, "Reason", spec["reason"])
                changed |= _note(ws, r, '[original Reason (fr): "%s"]'
                                 % spec["original_reason"])
            if "company" in spec and _get(ws, r, "Company") == spec["original_company"]:
                changed |= _set(ws, r, "Company", spec["company"])
                changed |= _note(ws, r, "[original company: %s]"
                                 % spec["original_company"])
            if "brand" in spec and _get(ws, r, "Brand") == spec.get(
                    "original_brand", spec.get("original_company")):
                changed |= _set(ws, r, "Brand", spec["brand"])
                changed |= _note(ws, r, "[original brand: %s]"
                                 % spec.get("original_brand",
                                            spec.get("original_company")))
            changed |= _note(ws, r, "[%s: %s]" % (BY, spec["note"]))
            changed |= _drop_stamps(ws, r)
            if changed:
                _set(ws, r, "LastUpdated", TODAY)
                n += 1
                print("    fiche %s row %d -> %r" % (fiche, r, spec["product"][:56]))
    return n


# ── FAULT C — the AESAN 2026_81 duplicate in Pending ───────────────────────

AESAN_KEEP = "https://www.aesan.gob.es/en/alertas/2026_81"
AESAN_DROP = "https://aesan.gob.es/en/alertas/2026_81"

AESAN_DUP_REASON = (
    "duplicate: one AESAN notice, two Pending rows, one \"www.\" apart — this "
    "row at %s and the surviving row at %s. Both are alert 2026_81 (Ref. "
    "ES2026/678, posible presencia de Listeria monocytogenes en productos "
    "lácteos Priégola), both scraped at 2026-11-11T08:12:44Z, identical in "
    "every other field. Reviewer 2 indexes Pending as url_to_idx[url] (one "
    "slot per URL, last write wins), so this copy was invisible to it: never "
    "correctable, never promotable, never refusable, while reviewer 3 held it "
    "and the offline promoter reported it every morning. The surviving copy "
    "is the more advanced one (pending_gap_v2 against this row's "
    "pending_gap) and carries the field repairs of %s. Archived, not "
    "deleted. CAUSE, fixed in code in the same pass: "
    "merge_master.validate_pending_row keyed its duplicate check with a bare "
    "url.rstrip('/').lower() while _dedup_key used _normalize_url_for_dedup, "
    "so the \"www.\" fold the normaliser has always performed was lost at "
    "that one gate; pipeline/agents/curator.py and "
    "pipeline/recall_confirm_agent.py carried the same shape and were fixed "
    "with it. Held by "
    "tests/test_pending_gate_keys_urls_like_the_dedup_key.py."
    % (AESAN_DROP, AESAN_KEEP, TODAY))


def archive_the_aesan_duplicate(wb) -> int:
    ws = wb["Pending"]
    hdr = _hdr(ws)
    n = 0
    for r in sorted(_find(ws, AESAN_DROP), reverse=True):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(hdr)}
        print("    Pending row %d: %s %r -> Rejected (duplicate of %s)"
              % (r, row.get("Source"), str(row.get("Product"))[:40], AESAN_KEEP))
        _archive(wb, row, AESAN_DUP_REASON)
        ws.delete_rows(r, 1)
        n += 1
    return n


# ── FAULT D — the surviving AESAN row's Product and Company ────────────────

AESAN_PRODUCT = (
    "Priégola fresh milk and cream: Leche Fresca Entera 1 L; Leche Fresca "
    "Semidesnatada 1 L; Leche Fresca Desnatada 1 L; Leche Fresca "
    "Semidesnatada SIN LACTOSA 1 L; Lában Cremoso (fermented milk) 1 L; "
    "Simbiotic Drink (fermented milk) 500 ml; Nata Fresca 1 L; Nata Fresca "
    "250 ml — carton bricks, refrigerated"
)
AESAN_COMPANY = "(not specified in AESAN alert 2026_81)"
AESAN_NOTE = (
    "[%s: Product was \"calendar_today 10/11/2026\" — the page's date widget, "
    "a scraped UI artifact and not a food — and Company was empty, so the "
    "publish gate held the row. Both read today on the alert itself, %s: "
    "title \"Alerta por posible presencia de Listeria monocytogenes en "
    "productos lácteos procedentes de España. (Ref.ES2026/678)\", fecha "
    "10/11/2026, \"Marca: Priégola\", the eight products listed in Product "
    "above, \"Lotes: 299, 300, 302, 303, 307, 308, 309, 310\", \"envasado en "
    "brick de cartón\", \"Temperatura: refrigeración\", distribución inicial "
    "a Madrid y Comunidad Valenciana with redistribution not ruled out, the "
    "finding \"resultante del autocontrol de la propia empresa\". The alert "
    "names NO firm, so Company carries the same explicit placeholder this "
    "register used for AESAN alert 2026_80 on 2026-11-08 rather than a "
    "guess; reviewer 2 to name the firm if AESAN publishes it. Expiry dates "
    "are in two attached images and were not read, so no date is claimed. "
    "Same shape as the 2026-11-08 repair of alert 2026_80, whose Product was "
    "a search-engine snippet.]" % (BY, AESAN_KEEP)
)


def repair_the_aesan_row(wb) -> int:
    ws = wb["Pending"]
    n = 0
    for r in _find(ws, AESAN_KEEP):
        changed = False
        if _get(ws, r, "Product").strip().startswith("calendar_today"):
            changed |= _set(ws, r, "Product", AESAN_PRODUCT)
        if not _get(ws, r, "Company").strip():
            changed |= _set(ws, r, "Company", AESAN_COMPANY)
        if _get(ws, r, "Brand").strip() in ("", "—"):
            changed |= _set(ws, r, "Brand", "Priégola")
        changed |= _note(ws, r, AESAN_NOTE)
        if changed:
            n += 1
            print("    Pending row %d: AESAN 2026_81 Product and Company "
                  "read from the alert" % r)
    return n


# ── MISSING — seven recalls the sweep found and the register did not hold ──
#
# Every field is read from the regulator's own notice, named in Notes.
# Nothing is inferred: where a notice prints no firm, no lot or no date,
# the field is left empty rather than filled.

_CAA = "https://www.recall.caa.go.jp/result/detail.php?rcl=%s&screenkbn=01"
_CAA_LISTING = ("https://www.recall.caa.go.jp/result/index.php"
                "?screenkbn=01&category=1")
_CAA_SWEEP = (
    "Found by the %s missed-recall sweep of the CAA food recall listing (%s), "
    "which this morning carried six notices with 掲載日 2026/11/12 or "
    "2026/11/13. Two are out of scope and were not added: rcl=00000036129 "
    "(ミートサプライ「牛プルコギ味付焼肉」, 回収理由 「アレルゲン「りんご」の表示欠落、"
    "原材料名の誤表示」 — undeclared allergen and a mislabelled ingredient "
    "name, labelling only) and rcl=00000036118 (World Import「緑豆-ドリアン"
    "ピアケーキ」, 回収理由 「原材料と添加物が明確に区分されていない、記載の保存方法と"
    "消費期限の表示が不適切で著しい品質劣化のおそれ、アレルゲン「卵」の表示欠落」 — "
    "labelling and quality). Date is the CAA 掲載日, as every CAA row in this "
    "register uses. The detail page prints no lot, date mark or region, and "
    "its 対象の特定情報 and 連絡先 headings are empty in the served HTML, so no "
    "such field is claimed here." % (BY, _CAA_LISTING))

MISSING_ROWS = [
    {
        "Date": "2026-11-12",
        "Source": "AGES (AT)",
        "Company": "GoodMills Österreich GmbH",
        "Brand": "Fini's Feinstes",
        "Product": "Fini's Feinstes Bio-Weizenkeime (organic wheat germ), 150 g",
        "Pathogen": "Mold",
        "Reason": "Microbial contamination in the form of mould",
        "Class": "Recall",
        "Country": "Austria",
        "URL": ("https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/"
                "produkt/finis-feinstes-bio-weizenkeime-150g"),
        "Notes": (
            "Found by the %s missed-recall sweep of the AGES product-warning "
            "listing (https://www.ages.at/mensch/produktwarnungen-"
            "produktrueckrufe), where it is the newest entry, dated "
            "12.11.2026. Read on the AGES product page itself today: "
            "Rückrufgrund \"Schimmel\"; \"eine mikrobielle Kontamination in "
            "Form von Schimmel auftreten kann\"; \"In Verkehr gebracht von\" "
            "and Hersteller both GoodMills Österreich GmbH; Chargen GM3051 "
            "(MHD 03.12.2026) and GM3053 (MHD 31.12.2026); \"Das Unternehmen "
            "hat am 12.11.2026 einen Rückruf des Produkts ... veranlasst\"; "
            "\"Andere Produkte der Marke Fini's Feinstes sind vom Rückruf "
            "nicht betroffen.\" The page names no sales area beyond the "
            "retail chains where the product was bought, and no species, so "
            "neither is claimed. Mould is in AFTS scope. The register's "
            "newest AGES row before this one was 2026-11-05. "
            "[original product: Fini's Feinstes Bio-Weizenkeime (150g)] "
            "[original Reason (de): \"Schimmel\" / \"eine mikrobielle "
            "Kontamination in Form von Schimmel auftreten kann\"]" % BY),
    },
    {
        "Date": "2026-11-13",
        "Source": "CAA (JP)",
        "Company": "Daizen",
        "Brand": "—",
        "Product": "Whole-pickled takuan daikon radish (takuan ippon-zuke)",
        "Pathogen": "Food additive above legal limit (saccharin sodium)",
        "Reason": ("The sweetener saccharin sodium was found above its "
                   "permitted usage standard, at 0.20 g/kg (standard: below "
                   "0.20 g/kg)"),
        "Class": "Recall",
        "Country": "Japan",
        "URL": _CAA % "00000036128",
        "Notes": (
            "%s Read on the CAA detail page today: 件名 「河鶴「たくあん一本漬」"
            "（北海道のダイゼン、イオン北海道で販売） - 回収」, 回収理由 "
            "「甘味料サッカリンナトリウムの使用基準超過0.20g/kg（基準値：0.20g/kg未満）"
            "が判明」, 対応開始日 2026年10月31日; CAA listed it 2026/11/13. "
            "Company is ダイゼン (katakana, romanised Daizen), the Hokkaido "
            "firm the title names; イオン北海道 (AEON Hokkaido) is the retailer. "
            "Brand is left \"—\": the title's 河鶴 is a kanji brand for which "
            "the page publishes neither a reading nor an English name, and "
            "this register does not invent a field value — the kanji is kept "
            "verbatim here instead. Unapproved additives over a limit are in "
            "AFTS scope; same shape as the 2026-11-09 Hotei Nyugyo "
            "acesulfame-potassium row and the 2026-10-29 SBK Toyama "
            "calcium-pantothenate row. "
            "[original brand: 河鶴] [original company: ダイゼン] "
            "[original product: たくあん一本漬] "
            "[original Reason (ja): \"甘味料サッカリンナトリウムの使用基準超過"
            "0.20g/kg（基準値：0.20g/kg未満）が判明\"]" % _CAA_SWEEP),
    },
    {
        "Date": "2026-11-12",
        "Source": "CAA (JP)",
        "Company": "Kobe Bussan",
        "Brand": "—",
        "Product": "Sugar-free date bars (oats & cocoa)",
        "Pathogen": "Mold",
        "Reason": "Mold growth",
        "Class": "Recall",
        "Country": "Japan",
        "URL": _CAA % "00000036119",
        "Notes": (
            "%s Read on the CAA detail page today: 件名 「神戸物産「砂糖不使用"
            "デーツバー（オーツ麦&ココア）」 - 返金/回収」, 回収理由 「かびの発生」, "
            "対応開始日 2026年11月11日; CAA listed it 2026/11/12. Company is "
            "神戸物産, whose own published English name is Kobe Bussan. "
            "[original company: 神戸物産] "
            "[original product: 砂糖不使用デーツバー（オーツ麦&ココア）] "
            "[original Reason (ja): \"かびの発生\"]" % _CAA_SWEEP),
    },
    {
        "Date": "2026-11-12",
        "Source": "CAA (JP)",
        "Company": "Hiroshima Prefectural Saijo Agricultural High School "
                   "(school festival)",
        "Brand": "—",
        "Product": "Tomato ketchup",
        "Pathogen": "Foreign material (glass)",
        "Reason": "Glass fragments found in the product",
        "Class": "Recall",
        "Country": "Japan",
        "URL": _CAA % "00000036120",
        "Notes": (
            "%s Read on the CAA detail page today: 件名 「広島県立西条農業高等学校"
            "（文化祭）「トマトケチャップ」 - 回収」, 回収理由 「ガラス片の混入」, "
            "対応開始日 2026年11月10日; CAA listed it 2026/11/12. The seller is "
            "a prefectural high school selling at its school festival, not a "
            "firm; its own English name is Hiroshima Prefectural Saijo "
            "Agricultural High School. "
            "[original company: 広島県立西条農業高等学校（文化祭）] "
            "[original product: トマトケチャップ] "
            "[original Reason (ja): \"ガラス片の混入\"]" % _CAA_SWEEP),
    },
    {
        "Date": "2026-11-12",
        "Source": "CAA (JP)",
        "Company": "Asahi Trust Foods",
        "Brand": "Trust",
        "Product": "Trust coarse-ground fresh breadcrumbs, 120 g",
        "Pathogen": "Mold",
        "Reason": "Risk of mold growth",
        "Class": "Recall",
        "Country": "Japan",
        "URL": _CAA % "00000036125",
        "Notes": (
            "%s Read on the CAA detail page today: 件名 「旭トラストフーズ"
            "「トラスト 粗挽き生パン粉120g」 - 交換/回収」, 回収理由 "
            "「カビが発生するおそれ」, 対応開始日 2026年11月10日; CAA listed it "
            "2026/11/12. 回収理由 states a RISK of mould, not a finding, and "
            "the Reason says so. "
            "[original company: 旭トラストフーズ] [original brand: トラスト] "
            "[original product: トラスト 粗挽き生パン粉120g] "
            "[original Reason (ja): \"カビが発生するおそれ\"]" % _CAA_SWEEP),
    },
    {
        "Date": "2026-11-12",
        "Source": "CFIA",
        "Company": "Archibald Inc.",
        "Brand": "Archibald",
        "Product": ("Archibald brand Nuit Blanche White IPA, 473 ml (UPC "
                    "8 76976 00075 3) and 4 x 473 ml (UPC 8 76976 00078 4), "
                    "all codes starting with J28Q61"),
        "Pathogen": "Foreign material (metal)",
        "Reason": "Pieces of metal; CFIA issue type \"Food - Extraneous Material\"",
        "Class": "Class 3",
        "Country": "Canada",
        "URL": ("https://recalls-rappels.canada.ca/en/alert-recall/archibald-"
                "brand-nuit-blanche-white-ipa-recalled-due-pieces-metal"),
        "Notes": (
            "Found by the %s missed-recall sweep of the Canadian recall "
            "database (https://recalls-rappels.canada.ca/en), which carried "
            "two food items dated 2026-11-12. Read on the CFIA notice itself "
            "today: \"Archibald brand Nuit Blanche White IPA recalled due to "
            "pieces of metal\"; recalling firm Archibald Inc.; issue \"Food - "
            "Extraneous Material\"; class 3; distribution Quebec; "
            "identification number RA-82940; original published date "
            "2026-11-12; recall date 2026-11-06. No best-before is printed. "
            "The register's newest CFIA row before this one was 2026-10-29. "
            "Related but distinct: Pending already holds the CFIA notice "
            "\"Various brands Alcoholic and non-alcoholic beer recalled due "
            "to pieces of metal\" of 2026-11-09; this is its own notice with "
            "its own RA number and its own URL, naming one firm. If the "
            "near-duplicate rule refuses it as an extension of that recall, "
            "that refusal is correct and should stand." % BY),
    },
    {
        "Date": "2026-11-12",
        "Source": "CFIA",
        "Company": "(not named in CFIA alert RA-78452)",
        "Brand": "Cold Grind",
        "Product": "Cold Grind brand Organic Turmeric Powder, 50 g (UPC "
                   "6 28110 34220 1), all packages",
        "Pathogen": "Sudan I (unauthorised dye)",
        "Reason": ("Non-permitted colour Sudan I; CFIA issue type \"Food - "
                   "Chemical\""),
        "Class": "Food safety warning",
        "Country": "Canada",
        "URL": ("https://recalls-rappels.canada.ca/en/alert-recall/cold-grind-"
                "brand-organic-turmeric-powder-may-be-unsafe-due-non-"
                "permitted-colour"),
        "Notes": (
            "Found by the %s missed-recall sweep of the Canadian recall "
            "database (https://recalls-rappels.canada.ca/en). Read on the "
            "CFIA notice itself today: \"Cold Grind brand Organic Turmeric "
            "Powder may be unsafe due to non-permitted colour\", \"due to "
            "non-permitted colour Sudan I\"; issue \"Food - Chemical\"; alert "
            "type food safety warning, not a recall, so no recall class is "
            "printed; 50 g, UPC 6 28110 34220 1, codes \"All packages\"; "
            "identification number RA-78452; original published date "
            "2026-11-12; \"no illnesses have been reported\". The notice's "
            "Companies section is empty, so Company carries the same explicit "
            "placeholder this register used for AESAN alert 2026_80 on "
            "2026-11-08 rather than a guess. No distribution provinces and no "
            "best-before are printed, so neither is claimed. An unauthorised "
            "dye is in AFTS scope and is reachable by the \"Unauthorised food "
            "additive\" alert term (token \"unauthorised dye\")." % BY),
    },
]


def add_missing_recalls() -> int:
    """Queue through tools/add_manual_row.py — the real Pending code path."""
    import openpyxl
    from pipeline.merge_master import _normalize_url_for_dedup as _norm
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    present = set()
    for sheet in ("Recalls", "Pending"):
        rows = wb[sheet].iter_rows(values_only=True)
        hdr = list(next(rows))
        i = hdr.index("URL")
        for r in rows:
            if r and r[i]:
                present.add(_norm(str(r[i])))
    wb.close()
    todo = [row for row in MISSING_ROWS if _norm(row["URL"]) not in present]
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
    print((out.stdout or "").rstrip()[-3000:])
    if out.returncode != 0:
        print("    add_manual_row reported a row that did not land:",
              (out.stderr or "")[-1200:])
    return len(todo)


# ── main ───────────────────────────────────────────────────────────────────

def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)

    print("A+B  published RappelConso rows of 12/11/2026 reading in French:")
    n_fr = translate_the_french_rows(wb)
    print("   repaired: %d" % n_fr)
    print("C  one AESAN notice held twice in Pending, one \"www.\" apart:")
    n_dup = archive_the_aesan_duplicate(wb)
    print("   archived: %d" % n_dup)
    print("D  the surviving AESAN row, blocked on fields its own alert settles:")
    n_aesan = repair_the_aesan_row(wb)
    print("   repaired: %d" % n_aesan)

    wb.save(XLSX)
    wb.close()

    print("MISSING  recalls the sweep found and the register did not hold:")
    n_add = add_missing_recalls()
    print("   queued into Pending: %d" % n_add)

    from pipeline.merge_master import mirror_json_from_xlsx
    print("recalls.json re-mirrored: %d rows"
          % mirror_json_from_xlsx(XLSX, JSON_OUT))
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
