# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-14. Base de5ac1c3.

Four tests were RED on plain main before this pass. Every one of them is a
symptom of a fault below.

A. YESTERDAY'S REPAIR REPORTED FIVE TRANSLATIONS IT NEVER MADE
   tools/repair_2026_11_13.py guards each field edit on an EXACT compare
   against the fiche's own capitalisation::

       if _get(ws, r, "Product") == spec["original_product"]:

   The RappelConso scraper lower-cases the "Modèles ou références" field,
   so the register held

       cuisse de poulet 3 kgs rhônes alpes

   while the spec held

       Cuisse de poulet 3 Kgs Rhônes Alpes

   The compare never matched, the Product was never written, and
   `_note()` — which is NOT guarded — recorded the full verbatim reading of
   the fiche anyway, so the row now carries a note describing a repair that
   did not happen. The same script had ALREADY fixed this exact bug for the
   Reason field one block lower ("Case-insensitive: fiche 24065 prints its
   Motif in capitals ... an exact compare here matched nothing and left the
   French Reason published") and did not carry the fix across to Product,
   Company or Brand. Company and Brand matched by luck of casing; Product
   did not, on all five rows.

   Result: five RappelConso rows published on 12/11/2026 are still in
   French this morning, two days after publication, and the reviewers'
   "[needs cleanup: Product not in English]" stamp is still on all five —
   the deterministic promoter re-stamps them every run, because the
   condition it stamps for is still true.

   Every field compare in THIS script is case-insensitive and every one of
   them is REQUIRED to match: an expected original that is not found raises,
   so a silent no-op cannot be reported as a repair again.

B. THE REGISTER'S OWN TEST LICENSED THAT MISS INDEFINITELY
   tests/test_product_and_firm_names_are_english.py excuses any row that
   carries the stamp — "tracked work, not a silent miss" — with no bound on
   how long the work may stay untracked. Five French Products sat in the
   published register for two days, through one morning pass, with the
   suite green. The stamp is a to-do, not a licence. Bounded in code in
   this pass: the excuse now expires, so a row still stamped more than one
   day after it was added is an offender again. RED on plain main plus
   this bound, on exactly the five rows of fault A.

C. ONE RECALL IS BOTH PUBLISHED AND REJECTED, AND ONE PAIR ONLY LOOKS IT
   tests/test_a_recall_is_not_both_published_and_rejected.py named two
   Weekly_Rejected rows.

   1. Fiche 24087 (Mamie Nova, glass in yoghurt, 13/11/2026) was refused by
      reviewer 1 with "Hazard is a foreign body (glass) not a microbial
      pathogen" and is PUBLISHED in Recalls. The publication is correct:
      foreign material has been in AFTS scope since 2026-10-01 and
      merge_master._microbial_only_refusal_excused exists for precisely
      this refusal. The archive row is annotated SUPERSEDED. Reviewer 1
      writing a scope verdict for an in-scope non-microbial hazard is
      reported, not repaired here — the excuse mechanism already catches
      it and changing reviewer 1's prompt is not a data repair.

   2. The salute.gov.it pair is NOT one recall twice. Recalls row 2071
      (2026-04-14, La Vacje Salumerie, Salame Nostrano) publishes the
      ministry's LISTING INDEX as its authority URL, and today's
      Weekly_Rejected row — a news-only discovery correctly refused for
      having no authority URL — carries the same index page with a
      pagination query string. They "share a URL" only because a published
      row cites an index page. Both rows are annotated: the archive row
      says it is not a copy of the published recall, and the published row
      is flagged UNRESOLVED on its own URL. Nothing is deleted.

D. PENDING HELD ONE EFSA NEWS ITEM THREE TIMES
   tests/test_pending_holds_one_row_per_url.py was RED on three Pending
   rows that normalise to one key::

       https://www.aesan.gob.es/actualidad/actualidad-noticias/La-EFSA-actualizacion
       https://www.aesan.gob.es/ca/actualidad/actualidad-noticias/La-EFSA-actualizacion
       https://aesan.gob.es/eu/actualidad/actualidad-noticias/La-EFSA-actualizacion

   Spanish, Catalan and Basque copies of one AESAN page. The page is not a
   recall at all — "La EFSA actualiza la evaluación de riesgos de las
   toxinas de Alternaria" is an EFSA risk-assessment update — and all three
   are already refused in Weekly_Rejected on exactly that ground. All three
   are archived to Rejected, never deleted.

E. A PUBLISHED ROW POINTS AT A FICHE THE REGULATOR HAS WITHDRAWN
   Recalls row 69 cites https://rappel.conso.gouv.fr/fiche-rappel/24039/interne
   for the PINTON Sardinella pâté recall (Clostridium botulinum, published
   06/11/2026 and read on this register's 2026-11-07 pass). That fiche is
   gone: the URL now serves RappelConso's generic "Rappel Consommateur -
   Accueil" listing, and the same recall is on the site this morning under
   fiche 24086, dated 13/11/2026, with the identical Motif ("Gonflement de
   certaines boites lié à un problème au sertissage. Présence de flore
   anaérobie élevée.") and the identical Risques. The URL is repointed at
   the live fiche; the Date stays 2026-11-06, which is when the recall was
   published and when this register recorded it. Fiche 24086 is NOT added
   as a new row — it is the same firm, product, hazard and Motif, and
   adding it would double-count one recall.

   The URL guardian did not see this because RappelConso answers HTTP 200
   with its listing page for a fiche that does not exist, so a status check
   reads "alive". Fixed in code in this pass (review/url_validator.py), not
   here.

F. TWO PENDING ROWS ARE BLOCKED OR WRONG ON FIELDS THEIR OWN NOTICE SETTLES
   1. CFIA "Producers brand Brown Rice recalled due to presence of insects"
      (2026-11-13) is held by the publish gate on an empty Company. The
      recalling firm is printed on the Canadian recall page: Tree of Life
      Canada.
   2. FDA "Outbreak Investigation of E. coli O26:H11: Raw Milk Cheese
      (September 2026)" — a row the offline promoter has reported as
      gate-passing every morning and the re-promotion guard has vetoed every
      morning, because an operator ruled it
      "duplicate_of_published_recall" on 2026-10-04 and was right: the
      recall is PUBLISHED at 2026-09-29 under the FDA's own per-recall
      notice. Three of its fields were wrong, so they are corrected from the
      notice first and the row is then archived, ending the loop. It
      carries Date 2026-09-01 — a month-start taken from
      the URL slug, not a date printed anywhere on the notice. The FDA page
      says FDA contacted the firm on September 24, 2026 and the firm "that
      same day agreed to and initiated a recall"; the Current Update is
      dated September 25, 2026. Date is corrected to the recall date,
      2026-09-24. Company held "Sierra Nevada Graziers", which is the
      BRAND; the firm on the page is Sierra Nevada Cheese Company.
      Outbreak was 0 for a page reporting 13 illnesses, 8 hospitalisations,
      3 HUS cases and 9 states; it is set to 1.

MISSING, queued through tools/add_manual_row.py (the real Pending code
path, so each is subject to the same dedup, gate and review stages as
every other row) — tools/manual_rows_2026_11_14.json:
   BVL (DE)   Knackwurst, Fleischerei Lars Methe, Listeria monocytogenes,
              published 13.11.2026
   NVWA (NL)  Milbona UHT volle melk, Lidl Nederland, published 13.11.2026

NOT ADDED, deliberately:
   RappelConso fiche 24086 — the same recall as the published fiche 24039
   row; see fault E. Repointed, not duplicated.
   RappelConso 24076 (Envia yoghurt), 24078 (MILSANI yoghurt), 24083
   (AUCHAN yoghurt) — all three are "Défaut organoleptique ... dus à une
   flore" with Risques "Modifications organoleptiques", and 24078 and
   24083 say in terms that there is no health risk. Quality/spoilage, out
   of scope.
   RappelConso 24079 — a DLC printing error, Risques "Anomalie
   d'étiquetage". Labelling, out of scope.
   RappelConso 24080 — Hawaiian Tropic sun lotion, Catégorie
   "Hygiène-Beauté". Not food (R1).
   CAA (JP) rcl=00000036129 — apple allergen not declared. Allergen
   labelling, out of scope. (rcl=00000036128, the saccharin-over-limit
   takuan of the same day, IS already published as Recalls row 2.)
   CFS (HK) 13.11.2026 — a poultry import suspension and a hairy-crab
   surveillance result. Neither is a recall (R2).
   BVL IsoSport Vital Zitrone-Kirsche — a real recall with a live
   lebensmittelwarnung.de Meldung, refused four times on 2026-11-11 for
   having no authority URL. Its first publication date is 11.11.2026,
   outside this sweep's 24-hour window (R4), so it is reported, not added.
   The Ministero della Salute "MOSTARDE" row in Pending (Cascina
   Balcarino, 2026-11-04) is left exactly as it is. The offline promoter's
   re-promotion guard blocks it anyway, on an existing archived verdict
   from gap_finder/it/rules.py ("Undeclared allergen — out of scope (Rule B
   reject)"), and a published Cascina Balcarino row of 2026-11-10 already
   sits in Recalls. Neither of those is a verification of THIS row, which
   is why it is only annotated. Its authority PDF on
   salute.gov.it could not be read this morning — the host served a Gcore
   "Browser Validation Page" interstitial, and so did the ministry's own
   listing — so nothing on that row was verified and nothing on it is
   changed. A note records the block. If the offline promoter publishes it
   on its own gate, that is the gate's decision and not a verification by
   this pass.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
MANUAL_ROWS = ROOT / "tools" / "manual_rows_2026_11_14.json"

TODAY = "2026-11-14"
BY = "morning fix pass " + TODAY
FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
    "[needs cleanup: Reason not in English]",
)


class RepairNoOp(RuntimeError):
    """An expected original was not found, so the repair would do nothing.

    Raised rather than skipped. See fault A: a repair that silently does
    nothing and still writes its note is worse than a repair that stops.
    """


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
    """Rows whose URL is `url`, trailing slash and case ignored."""
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


def _same(a, b) -> bool:
    """Field compare, case- and whitespace-insensitive.

    THE WHOLE OF FAULT A IS THIS FUNCTION NOT EXISTING YESTERDAY.
    """
    return " ".join(str(a or "").split()).casefold() == \
           " ".join(str(b or "").split()).casefold()


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


# ── FAULT A — the five French Products of 12/11, and fiche 24087 ───────────
#
# Every French string below was read on the fiche itself this morning,
# 2026-11-14, on rappel.conso.gouv.fr. The English is a translation of the
# fiche's own words: the food description is translated, brand names, the
# regional reference "Rhônes Alpes" as the fiche spells it, the unexpanded
# abbreviation "S/AT", the internal reference 5013230301 and the pack sizes
# are kept as printed, and nothing is added.

FRENCH_ROWS = {
    "24062": dict(
        original_product="mélanges contenant graines de courge : mélange "
                         "sportif, mélange spécial salade estivale; graines "
                         "de courge",
        product="Mixes containing pumpkin seeds: sports mix, special summer "
                "salad mix; pumpkin seeds",
        note='Re-read on fiche 24062 today (Fiche réf. 2026-11-0075 • '
             'Version 1, 12/11/2026): h1 "Graines de courge et mélanges '
             'contenant des graines de courge", sous-catégorie "Noix et '
             'graines", Marque "B&S et sans marque", Modèles ou références '
             '"Mélanges contenant Graines de courge : Mélange sportif, '
             'Mélange spécial salade estivale Graines de courge", Motif '
             '"Détection Salmonelle", Risques "Salmonella spp (agent '
             'responsable de la salmonellose)", Origine de la fiche '
             '"ALLONNEDIS ALLONNEDIS". The 2026-11-13 pass recorded this '
             'reading and left the Product in French (see fault A of this '
             'script); the translation is made here.',
    ),
    "24073": dict(
        original_product="cuisses de poulet nature x 3kgs",
        product="Plain chicken legs, x 3 kg",
        note='Re-read on fiche 24073 today (Fiche réf. 2026-11-0086 • '
             'Version 1, 12/11/2026): Modèles ou références "Cuisses de '
             'poulet Nature x 3Kgs", h1 "Cuisse de poulet X12 Rhônes Alpes", '
             'Marque "L\'Atelier de Gustave", Motif "Contamination en '
             'Salmonella", Risques "Salmonella spp (agent responsable de la '
             'salmonellose)". The register publishes the Modèles field, as '
             'the scraper reads it. The Reason was translated on 2026-11-13; '
             'only the Product was left in French.',
    ),
    "24074": dict(
        original_product="cuisse de poulet x2 rhônes alpes s/at",
        product="Chicken legs x2, Rhônes Alpes, S/AT",
        note='Re-read on fiche 24074 today (Fiche réf. 2026-11-0087 • '
             'Version 1, 12/11/2026): Modèles ou références "Cuisse de '
             'poulet X2 Rhônes Alpes S/AT", Marque "L\'Atelier de Gustave", '
             'Motif "Présence de Salmonella", Risques "Salmonella spp (agent '
             'responsable de la salmonellose)". "Rhônes Alpes" is kept as '
             'the fiche spells it and "S/AT" is an abbreviation the fiche '
             'does not expand, so both are carried over as printed rather '
             'than guessed at.',
    ),
    "24075": dict(
        original_product="cuisse de poulet 3 kgs rhônes alpes",
        product="Chicken legs, 3 kg, Rhônes Alpes",
        note='Re-read on fiche 24075 today (Fiche réf. 2026-11-0088 • '
             'Version 1, 12/11/2026): Modèles ou références "Cuisse de '
             'poulet 3 Kgs Rhônes Alpes", Marque "L\'Atelier de Gustave", '
             'Motif "Présence de Salmonella", Risques "Salmonella spp (agent '
             'responsable de la salmonellose)".',
    ),
    "24065": dict(
        original_product="5013230301 : cremeux sur lit framboises 125x2 bm",
        product="Creamy yoghurt on a bed of raspberries, 2 x 125 g "
                "(ref. 5013230301, BM)",
        note='The Product is a translation of THIS ROW\'S OWN French, not of '
             'a page re-read today: fiche 24065 was not among the five '
             'fiches this pass re-opened. The 2026-11-13 reading recorded on '
             'this row gives Dénomination "Yaourt Cremeux sur lit de '
             'Framboise" and Modèles ou références "5013230301 : CREMEUX SUR '
             'LIT FRAMBOISES 125X2 BM"; the internal reference and the '
             'initials "BM" are kept as printed. The Reason and Company were '
             'repaired on 2026-11-13; only the Product was left in French.',
    ),
    "24087": dict(
        original_product="gourmand à la grecque citron yuzu 2x140g",
        product="Gourmand Greek-style yoghurt, lemon-yuzu, 2 x 140 g",
        original_reason="Risque potentiel de bris de verre de petite taille "
                        "dans le yaourt",
        reason="Potential risk of small glass fragments in the yoghurt",
        note='Re-read on fiche 24087 today (Fiche réf. 2026-11-0099 • '
             'Version 1, 13/11/2026): h1 "Gourmand à La Grecque Citron Yuzu '
             '2x140g", sous-catégorie "Lait et produits laitiers", Marque '
             '"Mamie Nova", Modèles ou références "Gourmand à La Grecque '
             'Citron Yuzu 2x140g", Motif "RISQUE POTENTIEL de Bris de verre '
             'de petite taille dans le Yaourt", Risques "Inertes (verre, '
             'métal, plastique, papier, textile…)", Distributeurs "LECLERC, '
             'AUCHAN, SYSTEME U et CARREFOUR", Origine de la fiche "NOVANDIE '
             'NOVANDIE", Conditionnements "2x140g". "Gourmand à la Grecque" '
             '"Gourmand" is Mamie Nova\'s own product-line name and is kept '
             'as printed; "à la Grecque" is a description of the food, not '
             'part of the name, and is translated ("Greek-style"), which is '
             'also what the English detector requires — a kept French '
             'phrase long enough to read as French leaves the row flagged. The Reason is the same '
             'sentence as fiche 24065\'s and takes the same English. Same '
             'firm, NOVANDIE, as fiche 24065.',
    ),
}


def translate_the_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, spec in FRENCH_ROWS.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            raise RepairNoOp("fiche %s: no Recalls row at %s" % (fiche, FICHE % fiche))
        for r in rows:
            if not _same(_get(ws, r, "Product"), spec["original_product"]):
                raise RepairNoOp(
                    "fiche %s row %d: Product is %r, expected %r — the repair "
                    "would have done nothing and said it had"
                    % (fiche, r, _get(ws, r, "Product"), spec["original_product"]))
            _set(ws, r, "Product", spec["product"])
            _note(ws, r, "[original product: %s]" % spec["original_product"])
            if "reason" in spec:
                if not _same(_get(ws, r, "Reason"), spec["original_reason"]):
                    raise RepairNoOp(
                        "fiche %s row %d: Reason is %r, expected %r"
                        % (fiche, r, _get(ws, r, "Reason"), spec["original_reason"]))
                _set(ws, r, "Reason", spec["reason"])
                _note(ws, r, '[original Reason (fr): "%s"]' % spec["original_reason"])
            _note(ws, r, "[%s: %s]" % (BY, spec["note"]))
            _drop_stamps(ws, r)
            _set(ws, r, "LastUpdated", TODAY)
            n += 1
            print("    fiche %s row %d -> %r" % (fiche, r, spec["product"][:56]))
    return n


# ── FAULT C.1 — the fiche 24087 archive row ────────────────────────────────

SUPERSEDED_24087 = (
    "[SUPERSEDED %s by the morning fix pass — the published copy wins. "
    "Recalls carries this recall (RappelConso fiche 24087, Mamie Nova "
    "Gourmand à la Grecque lemon-yuzu yoghurt, 13/11/2026), and the refusal "
    "recorded here, \"URL agent: Hazard is a foreign body (glass) not a "
    "microbial pathogen\", is a SCOPE VERDICT THAT IS WRONG ON ITS OWN "
    "TERMS: foreign material has been in AFTS scope since 2026-10-01 and "
    "the fiche's Risques field reads \"Inertes (verre, métal, plastique, "
    "papier, textile…)\". merge_master._microbial_only_refusal_excused "
    "exists for exactly this refusal — a \"not a microbial pathogen\" "
    "verdict that also names an in-scope non-microbial hazard — and it is "
    "what let the row be re-promoted and published. This archive row is "
    "annotated, not deleted; the archives are append-only.] "
) % TODAY


def annotate_the_24087_refusal(wb) -> int:
    n = 0
    for sheet in ("Weekly_Rejected", "Rejected"):
        ws = wb[sheet]
        hdr = _hdr(ws)
        field = "RejectionReason" if "RejectionReason" in hdr else "RejectReason"
        for r in _find(ws, FICHE % "24087"):
            cur = _get(ws, r, field)
            if "SUPERSEDED" in cur:
                continue
            _set(ws, r, field, (SUPERSEDED_24087 + cur).strip())
            _note(ws, r, "[%s: annotated SUPERSEDED; see the %s field]" % (BY, field))
            n += 1
            print("    %s row %d: fiche 24087 refusal annotated SUPERSEDED" % (sheet, r))
    if not n:
        raise RepairNoOp("no archive row found for fiche 24087")
    return n


# ── FAULT C.2 — the salute.gov.it listing-index collision ──────────────────

SALUTE_INDEX = ("https://www.salute.gov.it/new/it/avvisi/"
                "avvisi-e-richiami-di-prodotti-alimentari")
SALUTE_INDEX_Q = SALUTE_INDEX + "/?tab=1&offset=0&pageElements=10"

SALUTE_ARCHIVE_NOTE = (
    "[NOT A DUPLICATE OF THE PUBLISHED ROW — annotated by the %s. This row "
    "shares a URL with Recalls row for 2026-04-14 (La Vacje Salumerie Snc, "
    "Salame Nostrano, Listeria monocytogenes) ONLY because that published "
    "row cites the ministry's LISTING INDEX, %s, as its authority URL "
    "instead of a per-recall notice. This row is a news-only discovery of "
    "the index page itself, correctly refused for having no authority URL, "
    "and it is NOT a copy of that recall: different date, different product, "
    "different firm. The contradiction to resolve is on the published row, "
    "which is flagged UNRESOLVED there. Nothing is deleted.] "
) % (BY, SALUTE_INDEX)

SALUTE_PUBLISHED_NOTE = (
    "[UNRESOLVED %s (morning fix pass): this row's URL is the Ministero "
    "della Salute's LISTING INDEX, %s, not a per-recall notice — an R3 "
    "defect. A second published row for the same 2026-04-14 Salame Nostrano "
    "recall cites ilfattoalimentare.it, a news site, which "
    "pipeline.verify_urls reports as a host that does not belong to Source "
    "'Ministero della Salute (IT)'. The ministry's per-recall PDF for an "
    "April 2026 notice could not be found this morning: salute.gov.it served "
    "a Gcore 'Browser Validation Page' interstitial to every request, "
    "including the pre-authorized listing page, so neither the index nor the "
    "PDF could be read. R3 forbids a news URL and forbids a guessed slug, so "
    "the URL is left exactly as published and the defect is recorded here "
    "rather than papered over. The matching archive row in Weekly_Rejected "
    "is annotated to say it is NOT a copy of this recall.] "
) % (TODAY, SALUTE_INDEX)


def annotate_the_salute_index_collision(wb) -> int:
    n = 0
    ws = wb["Weekly_Rejected"]
    hdr = _hdr(ws)
    field = "RejectionReason" if "RejectionReason" in hdr else "RejectReason"
    for r in _find(ws, SALUTE_INDEX_Q):
        cur = _get(ws, r, field)
        if "NOT A DUPLICATE OF THE PUBLISHED ROW" in cur:
            continue
        _set(ws, r, field, (SALUTE_ARCHIVE_NOTE + cur).strip())
        n += 1
        print("    Weekly_Rejected row %d: salute index row annotated" % r)
    if not n:
        raise RepairNoOp("no Weekly_Rejected row at %s" % SALUTE_INDEX_Q)
    rec = wb["Recalls"]
    m = 0
    for r in _find(rec, SALUTE_INDEX):
        if "UNRESOLVED" in _get(rec, r, "Notes"):
            continue
        _note(rec, r, SALUTE_PUBLISHED_NOTE)
        _set(rec, r, "LastUpdated", TODAY)
        m += 1
        print("    Recalls row %d: listing-index URL flagged UNRESOLVED" % r)
    if not m:
        raise RepairNoOp("no Recalls row at %s" % SALUTE_INDEX)
    return n + m


# ── FAULT D — three language copies of one EFSA news item in Pending ───────

AESAN_EFSA_URLS = (
    "https://www.aesan.gob.es/actualidad/actualidad-noticias/La-EFSA-actualizacion",
    "https://www.aesan.gob.es/ca/actualidad/actualidad-noticias/La-EFSA-actualizacion",
    "https://aesan.gob.es/eu/actualidad/actualidad-noticias/La-EFSA-actualizacion",
)

AESAN_EFSA_REASON = (
    "not a recall notice, and held three times over: Pending carried the "
    "Spanish, Catalan and Basque copies of ONE AESAN page, "
    "\"La EFSA actualiza la evaluación de riesgos de las toxinas de "
    "Alternaria\", which is an EFSA risk-assessment update and not a recall "
    "of any product — it names no firm, no product and no lot, and all three "
    "rows carry an empty Company for that reason. R2 refuses it. All three "
    "are ALREADY refused in Weekly_Rejected on exactly these grounds "
    "(\"not a recall / pre-2026 / non-food\"; \"The page is an EFSA update "
    "on Alternaria toxins, not a food recall.\"; \"No specific official "
    "regulator URL found for the recall\"), so nothing here is a new "
    "verdict. The three URLs differ only by AESAN's language prefix and "
    "\"www.\", and normalise to one dedup key, so reviewer 2 — which "
    "indexes Pending as url_to_idx[url], one slot per URL, last write wins "
    "— could see only the last of them: the other two could never be "
    "corrected, promoted or refused, while the offline promoter reported "
    "all three every morning. Archived by the %s, not deleted."
) % BY


def archive_the_aesan_efsa_copies(wb) -> int:
    ws = wb["Pending"]
    hdr = _hdr(ws)
    rows = []
    for u in AESAN_EFSA_URLS:
        rows.extend(_find(ws, u))
    if len(rows) != 3:
        raise RepairNoOp(
            "expected 3 AESAN EFSA rows in Pending, found %d" % len(rows))
    n = 0
    for r in sorted(set(rows), reverse=True):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(hdr)}
        print("    Pending row %d: %s %r -> Rejected (not a recall notice)"
              % (r, row.get("Source"), str(row.get("Product"))[:40]))
        _archive(wb, row, AESAN_EFSA_REASON)
        ws.delete_rows(r, 1)
        n += 1
    return n


# ── FAULT E — the withdrawn PINTON fiche ───────────────────────────────────

PINTON_OLD = FICHE % "24039"
PINTON_NEW = "https://rappel.conso.gouv.fr/fiche-rappel/24086/interne"

PINTON_NOTE = (
    "[%s: THE REGULATOR WITHDREW THIS FICHE AND RE-PUBLISHED IT UNDER A NEW "
    "NUMBER. %s was opened today and served RappelConso's generic \"Rappel "
    "Consommateur - Accueil\" listing page, which is what that site returns "
    "for a fiche that does not exist; the fiche content this row was built "
    "from on 2026-11-07 is no longer there. The same recall is on the site "
    "this morning as fiche 24086 (Fiche réf. 2026-11-0098 • Version 1, "
    "13/11/2026), read today: Marque \"PINTON\", Modèles ou références "
    "\"Pâté de Sardinelle PINTON en 180 g\", Motif \"Gonflement de certaines "
    "boites lié à un problème au sertissage. Présence de flore anaérobie "
    "élevée.\" and Risques \"Clostridium botulinum (agent responsable du "
    "botulisme)\", \"Clostridium perfringens\", \"Défaut de fabrication, "
    "défaut d'étanchéité…\" — the same firm (Origine de la fiche "
    "\"RACINES RACINES\"), the same product, the same hazard and the same "
    "Motif, word for word, as this row. The URL is repointed at the live "
    "fiche. THE DATE IS NOT CHANGED: 2026-11-06 is when the recall was "
    "published and when this register recorded it, and 13/11/2026 is the "
    "date of the replacement fiche, not of a new recall. Fiche 24086 is "
    "deliberately NOT added as a second row — it is this recall. WHY THE "
    "GUARDIAN MISSED IT: rappel.conso.gouv.fr answers HTTP 200 with its "
    "listing page for a withdrawn fiche, so a status check reads the dead "
    "URL as alive. Fixed in code in this pass, review/url_validator.py.] "
) % (BY, PINTON_OLD)


def repoint_the_pinton_fiche(wb) -> int:
    ws = wb["Recalls"]
    rows = _find(ws, PINTON_OLD)
    if not rows:
        raise RepairNoOp("no Recalls row at %s" % PINTON_OLD)
    n = 0
    for r in rows:
        _set(ws, r, "URL", PINTON_NEW)
        _note(ws, r, PINTON_NOTE)
        _set(ws, r, "LastUpdated", TODAY)
        n += 1
        print("    Recalls row %d: 24039 -> 24086 (fiche withdrawn)" % r)
    return n


# ── FAULT F — two Pending rows repaired from their own notices ─────────────

CFIA_RICE = ("https://recalls-rappels.canada.ca/en/alert-recall/"
             "producers-brand-brown-rice-recalled-due-presence-insects")
CFIA_RICE_NOTE = (
    "[%s: Company was EMPTY and the publish gate held the row on it, for a "
    "recall whose own page names the firm. Read today on %s — \"Recalling "
    "Firm: Tree of Life Canada\", product \"Brown Rice\" 22.6 kg, brand "
    "\"Producers\", UPC 0 72806 00225 0, codes 11/09/26 13326PRM, 11/10/26 "
    "13426PRM, 12/12/26 16626PRM, 12/14/26 16826PRM, issue \"Food - "
    "Extraneous Material\", reason \"Producers brand Brown Rice recalled due "
    "to presence of insects\", recall class 3, recall date 2026-11-10, "
    "identification RA-82943, published by CFIA, distributed in AB, BC, NB, "
    "NL, NS, ON, QC. Pest infestation is in AFTS scope. PATHOGEN WAS ALSO "
    "WRITTEN, and for a second reason: the CFIA scraper set it to the bare "
    "word \"Insect\", which is not this register's label for that hazard. "
    "Six published rows call it \"Foreign material (pest)\"; "
    "pipeline/_publish_gate.py and "
    "tests/test_pest_and_foreign_material_are_one_hazard.py are both "
    "written around that label; and the curator's scope map does not carry "
    "\"Insect\" at all, so publishing the row as it stood turned "
    "tests/test_curator_scope_2026_09_09.py RED — the curator refusing the "
    "hazard of a row this register had just published. Canonicalised here, "
    "and in code for the next one: pipeline/enrich_pending_offline.py now "
    "rewrites a non-canonical hazard WORD on an otherwise filled Pathogen, "
    "which it previously only did for an EMPTY one. Every other field on "
    "the row already matched the page.] "
) % (BY, CFIA_RICE)

FDA_CHEESE = ("https://www.fda.gov/food/outbreaks-foodborne-illness/"
              "outbreak-investigation-e-coli-o26h11-raw-milk-cheese-"
              "september-2026")
FDA_CHEESE_NOTE = (
    "[%s: THREE FIELDS WERE WRONG ON A ROW ABOUT TO BE PUBLISHED. Read today "
    "on the FDA's own investigation page, %s. (1) DATE was 2026-09-01 — a "
    "month start that appears nowhere on the notice and matches only the "
    "words \"september-2026\" in the URL slug. The page says \"On September "
    "24, 2026, FDA contacted the firm\" and the firm \"that same day agreed "
    "to and initiated a recall\"; the Current Update is dated September 25, "
    "2026 and the content is current as of 10/09/2026. Date is set to the "
    "recall date the page gives, 2026-09-24. (2) COMPANY held \"Sierra "
    "Nevada Graziers\", which is the brand; the page names the firm as "
    "\"Sierra Nevada Cheese Company\" and the products as \"Sierra Nevada "
    "Graziers grass-fed raw milk cheeses\". Company is corrected and Brand "
    "is set to Graziers. (3) OUTBREAK was 0 on a page headed \"Outbreak "
    "Investigation\" reporting Total Illnesses 13, Hospitalizations 8, HUS "
    "3, Deaths 0, States with Cases CA, CO, GA, KY, MI, NV, OR, TN, UT, and "
    "9 of 9 people interviewed reporting the cheese; Status: Ongoing. "
    "Outbreak is set to 1. THEN ARCHIVED: with the corrected date in hand "
    "this row is plainly the SAME EVENT as the published Recalls row of "
    "2026-09-29, Sierra Nevada Cheese Company, which carries the FDA's own "
    "per-recall notice, https://www.fda.gov/safety/recalls-market-"
    "withdrawals-safety-alerts/sierra-nevada-cheese-company-recalls-graziers-"
    "raw-milk-cheese-because-possible-health-risk, with Outbreak 1 and Tier "
    "1. This row is the outbreak-INVESTIGATION page for that recall. The "
    "operator reached the same verdict on 2026-10-04 "
    "(\"duplicate_of_published_recall\"), and the re-promotion guard has "
    "vetoed it on every run since — while the offline promoter listed it as "
    "gate-passing every morning, which is how it reached this pass. "
    "Archiving it ends that loop. CORRECTION, recorded deliberately: this "
    "pass first wrote that publishing the row would leave the September 2026 "
    "monthly report understating by one. That was wrong — the recall is "
    "already published and counted, and no closed report changes.] "
) % (BY, FDA_CHEESE)

FDA_CHEESE_ARCHIVE_REASON = (
    "duplicate_of_published_recall: this is the FDA's outbreak-INVESTIGATION "
    "page for the Sierra Nevada Cheese Company raw-milk cheese recall, and "
    "that recall is PUBLISHED in Recalls at 2026-09-29 under the FDA's own "
    "per-recall notice, with Outbreak 1 and Tier 1. The operator recorded "
    "the same verdict on 2026-10-04; the re-promotion guard has vetoed the "
    "row on every run since, while the offline promoter reported it as "
    "gate-passing every morning. Its Date (2026-09-01, a month start taken "
    "from the URL slug), Company (the brand \"Sierra Nevada Graziers\") and "
    "Outbreak (0) were corrected from the FDA page before archiving, so the "
    "archive row records what the notice actually says. Archived by the %s, "
    "not deleted."
) % BY


def repair_the_two_pending_rows(wb) -> int:
    ws = wb["Pending"]
    n = 0

    rows = _find(ws, CFIA_RICE)
    if not rows:
        raise RepairNoOp("no Pending row at %s" % CFIA_RICE)
    for r in rows:
        if _get(ws, r, "Company").strip():
            raise RepairNoOp(
                "Pending row %d: Company is %r, expected empty"
                % (r, _get(ws, r, "Company")))
        _set(ws, r, "Company", "Tree of Life Canada")
        if not _same(_get(ws, r, "Pathogen"), "Insect"):
            raise RepairNoOp(
                "Pending row %d: Pathogen is %r, expected 'Insect'"
                % (r, _get(ws, r, "Pathogen")))
        _set(ws, r, "Pathogen", "Foreign material (pest)")
        _note(ws, r, CFIA_RICE_NOTE)
        n += 1
        print("    Pending row %d: CFIA brown rice Company -> Tree of Life Canada" % r)

    rows = _find(ws, FDA_CHEESE)
    if not rows:
        raise RepairNoOp("no Pending row at %s" % FDA_CHEESE)
    for r in rows:
        if _get(ws, r, "Date")[:10] != "2026-09-01":
            raise RepairNoOp(
                "Pending row %d: Date is %r, expected 2026-09-01"
                % (r, _get(ws, r, "Date")))
        if not _same(_get(ws, r, "Company"), "Sierra Nevada Graziers"):
            raise RepairNoOp(
                "Pending row %d: Company is %r, expected 'Sierra Nevada Graziers'"
                % (r, _get(ws, r, "Company")))
        _set(ws, r, "Date", "2026-09-24")
        _set(ws, r, "Company", "Sierra Nevada Cheese Company")
        _set(ws, r, "Brand", "Graziers")
        _set(ws, r, "Outbreak", 1)
        _note(ws, r, FDA_CHEESE_NOTE)
        n += 1
        print("    Pending row %d: FDA raw-milk cheese Date/Company/Brand/Outbreak" % r)
    # Archived last, after the corrections above, so the archive row carries
    # the notice's own facts. Highest row index first.
    for r in sorted(rows, reverse=True):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(_hdr(ws))}
        _archive(wb, row, FDA_CHEESE_ARCHIVE_REASON)
        ws.delete_rows(r, 1)
        print("    Pending row %d: FDA raw-milk cheese -> Rejected (duplicate "
              "of the published 2026-09-29 recall)" % r)
        n += 1
    return n


# ── The Italian MOSTARDE row: recorded, not changed ────────────────────────

MOSTARDE_NOTE = (
    "[%s: NOT VERIFIED BY THIS PASS AND NOT CHANGED BY IT. The authority "
    "document on this row, the Ministero della Salute's own recall PDF, "
    "could not be read this morning: salute.gov.it answered HTTP 200 with a "
    "Gcore \"Browser Validation Page\" interstitial requiring cookies and "
    "JavaScript, and so did the ministry's pre-authorized listing page "
    "https://www.salute.gov.it/portale/news/p3_2_1_3_1.jsp?lingua=italiano. "
    "No mirror, archive or other route was used. So no field on this row — "
    "Company \"Cascina Balcarino\", Product \"Mustard\", Date 2026-11-04, "
    "Pathogen \"Microbiological risk\" — was confirmed against the notice, "
    "and in particular the PDF's own file name says \"MOSTARDE\", which in "
    "Italian is mostarda, a fruit-and-mustard condiment, not table mustard. "
    "The row is left exactly as the pipeline built it. If the offline "
    "promoter publishes it on its own gate, that is the gate's decision; it "
    "is not a verification by this pass, and the Product should be re-read "
    "against the notice when salute.gov.it is reachable.] "
) % BY

MOSTARDE_URL = ("https://www.salute.gov.it/new/sites/default/files/"
                "external_data/avvisi_sicurezza_alimentare/"
                "Modello_richiamo_Cascina_Balcarino_MOSTARDE_1793729364.pdf")


def record_the_mostarde_block(wb) -> int:
    ws = wb["Pending"]
    rows = _find(ws, MOSTARDE_URL)
    if not rows:
        raise RepairNoOp("no Pending row at the Cascina Balcarino PDF")
    for r in rows:
        _note(ws, r, MOSTARDE_NOTE)
        print("    Pending row %d: MOSTARDE block recorded (no field changed)" % r)
    return len(rows)


# ── the missing recalls ────────────────────────────────────────────────────

def queue_the_missing_rows() -> None:
    """Through tools/add_manual_row.py, the real Pending code path."""
    print("  queueing the missing recalls through add_manual_row ...")
    r = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(MANUAL_ROWS)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr)
    if r.returncode != 0:
        # A near-duplicate refusal is a legitimate verdict and is accepted;
        # the tool names which row did not land and why, above.
        print("  add_manual_row exit %d — see its per-row verdicts above"
              % r.returncode)


# ── main ───────────────────────────────────────────────────────────────────

def main() -> int:
    import openpyxl

    # The manual rows go in first, through their own code path, so that the
    # workbook edits below are made against the workbook the gate produced.
    queue_the_missing_rows()

    wb = openpyxl.load_workbook(XLSX)
    print("A. the five French Products of 12/11 and fiche 24087")
    translate_the_french_rows(wb)
    print("C.1 fiche 24087 published and rejected")
    annotate_the_24087_refusal(wb)
    print("C.2 salute.gov.it listing-index collision")
    annotate_the_salute_index_collision(wb)
    print("D. three language copies of one EFSA news item")
    archive_the_aesan_efsa_copies(wb)
    print("E. the withdrawn PINTON fiche")
    repoint_the_pinton_fiche(wb)
    print("F. two Pending rows repaired from their own notices")
    repair_the_two_pending_rows(wb)
    print("   the Italian MOSTARDE row: block recorded, nothing changed")
    record_the_mostarde_block(wb)
    wb.save(XLSX)
    print("saved %s" % XLSX)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
