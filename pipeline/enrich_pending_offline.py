"""Fill Pending's missing Pathogen from the row's OWN Reason — offline.

WHY THIS EXISTS
---------------
On 2026-09-27 the whole Pending queue was run through the publish gate to see
what could be promoted. Of 50 rows, **one** passed. The dominant blocker was
not review and not a judgement call:

    18 rows  Pathogen is empty
    12 rows  Company is empty

and in most of those rows **the hazard was written in the row's own Reason
field the whole time** — "présence identifiée d'ochratoxine a", "Presence of
Salmonella", "elevated levels of lead", "presence of stones". Nothing needed
fetching, no model needed asking. The same shape as the RASFF notifId that sat
in Notes for six weeks while a test called it unrecoverable.

So this module copies the hazard the row already states into the field the gate
requires, deterministically, with no network and no LLM, so it can run on a
schedule twice a day instead of waiting for an operator.

THE RULES IT WILL NOT BREAK
---------------------------
1. It reads ONLY the row's own Reason (and Product as a fallback for the
   CFIA/FSAI listing rows, whose Reason IS the notice headline). It never
   fetches, never guesses and never consults a model.
2. It writes ONLY Pathogen. It will NOT fill Company, and that is deliberate:
   for RappelConso rows the only company-ish text on the row is the DISTRIBUTOR
   list in Notes ("magasins u", "leclerc thouars"), not the recalling firm.
   Promoting a supermarket as the recalling firm would be a fabricated
   attribution — and the register already refuses it: one such row carries
   "[confirm-agent hold: no recalling firm on the row (Brand 'Unbranded')]".
   Company stays empty and those rows stay in Pending, correctly.
3. Every value it writes is a canonical string ALREADY USED by published rows.
   The vocabulary below was derived by reading the register, not invented:
   "Foreign material (stones)", "Ochratoxin A", "Lead (heavy metal)" and the
   organism names are all spellings that 1,793 published rows already carry.
   0 of those 1,793 rows has an empty Pathogen, so this field is mandatory by
   convention as well as by the gate.
4. It stamps every change into Notes so the edit is auditable from the sheet.
5. Allergen-only hazards are NOT filled. Undeclared allergens are out of AFTS
   scope (policy 2026-07-29), so a row whose only stated hazard is an allergen
   is reported for rejection rather than quietly enriched into publishability.

USAGE
-----
    python -m pipeline.enrich_pending_offline              # report only
    python -m pipeline.enrich_pending_offline --apply      # write
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: (regex, canonical Pathogen). Order matters — first match wins, so specific
#: patterns precede general ones. Every canonical value on the right-hand side
#: is a spelling already present in the published register.
RULES: Tuple[Tuple[str, str], ...] = (
    # ── mycotoxins and chemical contaminants ─────────────────────────────
    (r"ochratox|ochratoxine",                    "Ochratoxin A"),
    (r"aflatox",                                 "Aflatoxin"),
    (r"\bpatulin",                               "Patulin"),
    (r"\blead\b|\bplomb\b",                      "Lead (heavy metal)"),
    (r"\bcadmium\b",                             "Cadmium (heavy metal)"),
    (r"\bmercur|\bmercure\b",                    "Mercury (heavy metal)"),
    (r"\barsenic\b",                             "Arsenic (heavy metal)"),
    # ── biogenic amines / endogenous toxins ──────────────────────────────
    # GAP FOUND 2026-10-14. RappelConso fiche 23765 (13/10/2026, filet de
    # maquereau, SUPER U St Ciers sur Gironde) reached Pending TWICE with
    # Reason "Presence histamine >200mg" and Pathogen empty, and this
    # enricher reported "no hazard term in the row's own text" — so the
    # publish gate held it on "Pathogen is empty" and reviewer 2 then
    # rejected it with a factually wrong reason ("the URL is a listing
    # page"; /fiche-rappel/23765/interne is the per-recall page). The
    # register has published histamine FIVE times already —
    # "Histamine", "Histamine / scombrotoxin", "Histamine (endogenous
    # biotoxin)" — including the near-identical Super U Mirepoix mackerel
    # fillet of 2026-08-20. The scombrotoxin spelling is matched too
    # because RappelConso and the FSA both use it.
    (r"histamin|scombrotox",                     "Histamine"),
    # ── undeclared pharmacological ingredients: IN SCOPE ─────────────────
    # GAP FOUND 2026-10-26. ALLERGEN_ONLY below opened with the bare word
    # "undeclared", so EVERY row whose hazard text contains it was reported
    # as "an undeclared allergen — out of AFTS scope". Undeclared DRUGS are
    # not allergens and are explicitly in scope, and the register proves it:
    # run this module's own infer_pathogen() over Recalls and THREE
    # PUBLISHED ROWS come back refused as out-of-scope —
    #
    #   2026-09-15  FDA  X10 Natural Enhancement Supplement
    #   2026-09-15  FDA  biQ-FEL 500 mL liquid dietary supplement
    #   2026-09-03  FDA  Business Pill and Branch Manager for Men
    #
    # all three carrying Pathogen "Undeclared pharmacological ingredient",
    # plus SZPI (CZ) 2026-10-06 (ELIXIRMAN, undeclared sildenafil) and a
    # Rejected FDA row for Lipofit Extreme Fat Burner 2.0 (undeclared DNP).
    # tools/alert_vocab.py has offered the subscriber term "Undeclared
    # pharmacological ingredient" since 2026-10-10. The register, the gate
    # and the alert vocabulary all treat these as publishable; only this
    # module did not.
    #
    # The canonical spelling is the one those three published rows carry.
    (r"sildenafil|tadalafil|vardenafil|dapoxetin|yohimb|sibutramin|"
     r"phenolphthalein|dimethylamylamine|\bdmaa\b|\bdnp\b|"
     r"dinitrophenol|undeclared (?:drug|pharmaceutical|pharmacolog)|"
     r"unapproved (?:drug|pharmaceutical)|undeclared active pharmaceutical",
                                                 "Undeclared pharmacological ingredient"),
    # ── veterinary residues, and uninspected product ─────────────────────
    # Both added 2026-10-26 with the allergen narrowing above, and for the
    # same reason: each has a PUBLISHED row whose own hazard text mentions
    # an allergen in passing, so the allergen fallback claimed the row was
    # out of scope when the register had already published it.
    #
    #   FSA (UK) 2026-07-22, Graham's Family Dairy semi-skimmed milk:
    #     "…a possible health risk for anyone with an allergy to
    #      antibiotics (penicillin)" — the HAZARD is the residue; the
    #      allergy is the consequence the FSA spells out.
    #   USDA FSIS 2026-03-25, raw beef and pork, false "EST. 1996" mark:
    #     "…may contain undeclared allergens, harmful bacteria or other
    #      contaminants" — FSIS boilerplate listing what uninspected food
    #      MIGHT contain. The hazard is that nothing was inspected.
    #
    # "Uninspected product (hazard not assessed)" is also the label the
    # AFTS scope rule names for FSIS import-eligibility violations, and 14
    # published rows already carry it verbatim.
    (r"veterinary (?:medicin|drug|chemical)|residues? of (?:antibiotic|"
     r"veterinary)|\bpenicillin\b|chloramphenicol|nitrofura",
                                                 "Veterinary drug residue"),
    (r"without (?:the benefit of )?(?:federal |official )?inspection|"
     r"not (?:federally )?inspected|uninspected|false mark of inspection|"
     r"ineligible for import|import eligibility",
                                                 "Uninspected product (hazard not assessed)"),
    # ── bacteria and viruses ─────────────────────────────────────────────
    (r"listeria monocytogenes|list[ée]ria monocytogen",
                                                 "Listeria monocytogenes"),
    (r"\blisteri|\blist[ée]ri",                  "Listeria monocytogenes"),
    (r"e\.?\s?coli\s+stec|stec\b|verotoxin|shiga",
                                                 "Shiga toxin-producing E. coli (STEC)"),
    (r"escherichia coli|e\.?\s?coli",            "E. coli"),
    (r"salmonell",                               "Salmonella"),
    (r"campylobact",                             "Campylobacter"),
    (r"bacillus cereus|cereulid",                "Bacillus cereus"),
    (r"clostridium botulinum|botulis",           "Clostridium botulinum"),
    (r"clostridium perfringens",                 "Clostridium perfringens"),
    (r"staphylococc",                            "Staphylococcus aureus"),
    (r"cronobacter",                             "Cronobacter sakazakii"),
    (r"vibrio",                                  "Vibrio"),
    (r"yersinia",                                "Yersinia enterocolitica"),
    (r"hepatitis a|h[ée]patite a",               "Hepatitis A virus"),
    (r"norovirus|norwalk",                       "Norovirus"),
    (r"\bmould\b|\bmold\b|moisissure",           "Mold"),
    # ── physical / foreign material, with the material named where stated ─
    (r"stone|noyau|pierre",                      "Foreign material (stones)"),
    # GAP FOUND 2026-10-14. This whole block was ENGLISH + FRENCH only,
    # in a register that publishes foreign-material rows from BVL (DE),
    # BLV (CH), AGES (AT), Ministero della Salute (IT), AESAN (ES),
    # NVWA (NL), GIS (PL), SZPI (CZ) and NKFH (HU). Measured cost: the
    # GIS (PL) notice of 13.10.2026 — "możliwa obecność fragmentów szkła
    # w jednej partii wody butelkowanej" (KRYSTYNKA sparkling mineral
    # water 330 ml, batch FS224/3, Uzdrowisko Ciechocinek S.A.) — sat in
    # Pending with Pathogen empty because no rule here speaks Polish.
    #
    # The non-EN/FR additions are QUALIFIED (a fragment/piece word next
    # to the material) on purpose: bare "Glas" (DE), "glas" (NL) and
    # "vetro" (IT) all mean a JAR as often as they mean broken glass,
    # and "szklanka" is a tumbler. The pre-existing bare \bglass\b and
    # \bverre\b are left exactly as they were — this widens the rule,
    # it does not loosen what already worked.
    (r"\bglass\b|\bverre\b|"
     r"(?:fragment|od[łl]am|kawa[łl]|okruch)\w*\s+szk[łl]\w*|"
     r"szk[łl]\w*\s+(?:fragment|od[łl]am)\w*|"
     r"glassplitter|glasscherben|glasfragment|glasst[üu]ck|"
     r"frammenti di vetro|pezzi di vetro|"
     r"(?:trozos|fragmentos|restos) de (?:vidrio|cristal)|"
     r"(?:stukjes|glasdeeltjes|scherven) glas|glasscherven|"
     r"(?:st[řr][íi]pk|[úu]lomk)\w*\s+skla|[üu]vegszil[áa]nk",
                                                 "Foreign material (glass)"),
    (r"m[ée]tall?iqu|\bmetal\b|metallsplitter|metallst[üu]ck|"
     r"frammenti metallici|fragmentos de metal|metaaldeeltjes|"
     r"fragment\w*\s+metal\w*|kov\w*\s+[úu]lomk",
                                                 "Foreign material (metal)"),
    (r"\bplastic\b|plastique|kunststoff|plastik|plastica|pl[áa]stico|"
     r"fragment\w*\s+(?:tworzyw|plastik)",       "Foreign material (plastic)"),
    (r"\bwood\b|\bbois\b|\bholz\b|legno|madera|\bhout\b|drewn",
                                                 "Foreign material (wood)"),
    # The pest rule knew only "insect", "pest" and "nuisible". FSAI alert
    # 2026.65 of 13 Oct 2026 (Ready brek Original 450 g / 750 g and Tesco
    # Super Smooth Porridge 750 g) says "possible presence of flour
    # beetle" — the word the FSAI and the FSA actually use. So do
    # "weevil", "larvae" and "moth"; none of them matched.
    (r"\binsect|\bpest\b|nuisible|\bbeetle|\bweevil|\blarvae?\b|"
     r"\bmaggot|\bmoth\b|\bmites?\b|\bk[äa]fer\b|charan[çc]on|"
     r"coleotter|escarabajo|\bkever\b|chrz[ąa]szcz|owad",
                                                 "Foreign material (pest)"),
    (r"corps [ée]tranger|foreign (?:body|bodies|object|material)",
                                                 "Foreign material"),
    # ── packaging integrity: a real, published hazard class ──────────────
    (r"herm[ée]ticit|[ée]tanch[ée]it|seal (?:defect|failure)|"
     r"perte d[’']?[ée]tanch|defect.{0,20}canette|swollen|bombage",
                                                 "Physical/foreign-body contamination"),
)

#: Hazards that are out of AFTS scope — never enriched into publishability.
#
# NARROWED 2026-10-26. This pattern used to open with the bare word
# "undeclared", which made it fire on every "undeclared <anything>" — and
# the only thing it is allowed to exclude is an undeclared ALLERGEN. Two
# hazard classes that are squarely in AFTS scope were being reported as
# out of scope by that one word:
#
#   undeclared DRUGS       sildenafil, tadalafil, DMAA, DNP — five rows in
#                          the register, three of them PUBLISHED with
#                          Pathogen "Undeclared pharmacological ingredient"
#   unapproved ADDITIVES   "undeclared colour", Sudan dyes, an additive
#                          over its limit — in scope by the AFTS scope rule
#
# So "undeclared"/"non déclaré" now has to be followed by an allergen, and
# the allergen list is the EU Annex II / FDA major list in the languages
# the register actually carries. A standalone "allergen(e)" still matches,
# which is how most notices word it. The RULES table above is consulted
# BEFORE this pattern, so a row naming both a real hazard and an allergen
# keeps its real hazard either way.
_ALLERGEN_NOUN = (
    r"allerg|peanut|arachide|erdnuss|cacahuete|gluten|wheat|bl[ée]\b|weizen|"
    r"\bmilk\b|\blait\b|\bmilch\b|\bleche\b|\blatte\b|lactose|"
    r"\begg\b|\boeuf|\bœuf|\bei\b|huevo|\buovo\b|"
    r"\bsoy\b|soya|\bsoja\b|\bfish\b|poisson|\bfisch\b|pescado|"
    r"crustace|crustáce|shellfish|mollus|\bnuts?\b|tree nut|noix|n[üu]sse|"
    r"almond|amande|hazelnut|noisette|haselnuss|walnut|cashew|pecan|"
    r"pistachio|pistache|macadamia|brazil nut|sesam|s[ée]same|"
    r"\bcelery\b|c[ée]leri|sellerie|mustard|moutarde|senf|"
    r"\blupin|sulphite|sulfite|sulphur dioxide|dioxyde de soufre")

ALLERGEN_ONLY = re.compile(
    r"(?:undeclared|undisclosed|not\s+declared|non[- ]d[ée]clar|"
    r"nicht\s+(?:deklariert|gekennzeichnet)|no\s+declarado)"
    r"[^.;]{0,40}?(?:" + _ALLERGEN_NOUN + r")"
    r"|(?:" + _ALLERGEN_NOUN + r")[^.;]{0,40}?"
    r"(?:undeclared|undisclosed|not\s+declared|non[- ]d[ée]clar)"
    r"|allerg|milk not declared|soy not declared", re.I)

#: Rows whose Reason is the notice HEADLINE rather than a hazard sentence.
#: For these the headline is the only hazard text the row has, so it is the
#: field to read. Identified by the scraper's own Notes stamp.
HEADLINE_SOURCES = ("CFIA HTML listing fallback", "FSAI HTML listing fallback",
                    "FDA HTML fallback")


def hazard_text(row: Dict) -> str:
    """The text this row states its hazard in — its own fields only."""
    parts = [str(row.get("Reason") or "")]
    notes = str(row.get("Notes") or "")
    if any(m in notes for m in HEADLINE_SOURCES):
        # The headline carries the hazard for these; Reason repeats it.
        parts.append(str(row.get("Product") or ""))
    return " ".join(parts)


def infer_pathogen(row: Dict) -> Tuple[Optional[str], str]:
    """(canonical Pathogen, note) inferred from the row's own text.

    Returns (None, reason) when nothing can be inferred without guessing.
    """
    text = hazard_text(row)
    if not text.strip():
        return None, "no hazard text on the row"

    low = text.lower()
    for pattern, canonical in RULES:
        m = re.search(pattern, low, re.I)
        if m:
            if ALLERGEN_ONLY.search(text) and "Foreign" not in canonical:
                # an allergen mentioned alongside a real hazard is fine;
                # only refuse when the allergen IS the hazard
                pass
            return canonical, f"matched {m.group(0)!r}"
    if ALLERGEN_ONLY.search(text):
        return None, ("hazard is an undeclared allergen — out of AFTS scope "
                      "(policy 2026-07-29), not enriched")
    return None, "no hazard term in the row's own text"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(XLSX))
    ap.add_argument("--sheet", default="Pending")
    ap.add_argument("--apply", action="store_true",
                    help="write the inferred Pathogen values into the sheet")
    args = ap.parse_args()

    import openpyxl
    wb = openpyxl.load_workbook(args.xlsx)
    ws = wb[args.sheet]
    head = [str(c.value) for c in ws[1]]
    pcol = head.index("Pathogen") + 1
    ncol = head.index("Notes") + 1

    filled: List[str] = []
    skipped: List[str] = []
    for r in range(2, ws.max_row + 1):
        row = {head[i]: ws.cell(r, i + 1).value for i in range(len(head))}
        if str(row.get("Pathogen") or "").strip():
            continue
        got, why = infer_pathogen(row)
        label = f"{str(row.get('Source'))[:14]:15} {str(row.get('Product'))[:44]}"
        if not got:
            skipped.append(f"{label}  — {why}")
            continue
        filled.append(f"{label}  -> {got}   ({why})")
        if args.apply:
            ws.cell(r, pcol).value = got
            stamp = (f"[offline-enrich {dt.date.today().isoformat()}: Pathogen "
                     f"'' -> {got!r}, read from this row's own hazard text; "
                     f"no fetch, no model]")
            prev = str(ws.cell(r, ncol).value or "")
            if stamp not in prev:
                ws.cell(r, ncol).value = (prev + " " + stamp).strip()

    print(f"FILLED {len(filled)}:")
    for f in filled:
        print("   " + f)
    print(f"\nLEFT ALONE {len(skipped)}:")
    for s in skipped:
        print("   " + s)

    if args.apply and filled:
        wb.save(args.xlsx)
        print(f"\nwrote {len(filled)} Pathogen value(s) to {args.xlsx}")
        print("NOTE: Company is never filled here — see rule 2 in the "
              "module docstring.")
    elif not args.apply:
        print("\n(report only — pass --apply to write)")
    return 0


if __name__ == "__main__":                                 # pragma: no cover
    raise SystemExit(main())
