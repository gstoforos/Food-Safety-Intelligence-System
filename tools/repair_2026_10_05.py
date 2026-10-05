# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-05. Four faults plus one missed recall.

Run on a FRESH clone of main, by tools/rebase_and_verify.py, which then
runs pipeline.promote_gate_passing --apply behind it.

NO ROW IS REMOVED FROM Recalls BY THIS SCRIPT, so the commit message needs
no deletion marker.

Every fact written below was read on an authority page or in the row's own
text/URL, and the Notes say which.


FAULT A — 36 RECALLS BOTH PUBLISHED AND THROWN AWAY
----------------------------------------------------
tests/test_a_recall_is_not_both_published_and_rejected was RED on main
this morning: 36 archive rows share a URL with a PUBLISHED recall and say
nothing about which copy wins — 1 in Weekly_Rejected, 35 in Rejected.

That test was written on 2026-09-28 and the function that is supposed to
keep it green —
``pipeline/promote_gate_passing.supersede_archived_copies`` — was written
the same day. It did not fail at its job; it could not SEE the rows. Three
measured causes, each fixed in code rather than here, with
tests/test_a_silent_archive_row_cannot_outrank_the_register.py holding
them:

  1. IT READ THE REASON COLUMN ONLY. Of the 36, exactly ONE matched on its
     reason column. The other 35 carry RejectReason blank and keep the
     verdict in Notes ("REJECTED: Confirmer: row was at pending_gap_v2 …
     and Pathogen is empty"). merge_master.load_rejected_urls learned this
     on 2026-09-01 and folds Notes in; this function, written after it,
     did not. Folding Notes in resolves 27 of the 36.

  2. THE FIELD LIST WAS A HAND-PICKED SUBSET. Publish-gate rule 4 emits
     "<Field> is empty" for Company, Product, Class, Date, Source and URL.
     SUPERSEDE_IF listed company, pathogen and reason only, so an FSAI
     Wrights of Marino row archived for "Date is empty" matched nothing.

  3. A ROW WITH NO VERDICT AT ALL WAS SKIPPED. Eight gap-finder rows carry
     no reason and no verdict in Notes either, only "Discovered via news:
     <site>" — which says where the row came from, not why it was refused.
     Silence is not a verdict and cannot outrank a published recall.

And the structural cause behind all three: the sweep only ever ran over
the URLs ONE PROMOTER RUN had just promoted. The hourly Pending→Recalls
merge, reviewers 1/2/3, the confirm agent and the gap finders all publish
without calling it. ``supersede_every_published_url`` now reads the
published set out of Recalls, so the sweep no longer depends on who did
the publishing.

This script calls that function rather than stamping the rows by hand, so
what runs here is the same code that will run tomorrow at 15:27.

NOTHING IS DELETED. Weekly_Rejected and Rejected are append-only; the rows
stay and now say which copy wins.


FAULT B — THE EXTRACTOR'S EMPTY-IDENTIFIER TEMPLATE, BACK IN ONE DAY
---------------------------------------------------------------------
    Pending, Mattilsynet, scraped 2026-10-05T05:14:23Z
    Reason: "The product contains pyrrolizidine alkaloids (toxic plant
             substances) above the limit. (Recall ID: N/A)"

pipeline/extractor.py rule 10 gives the model a FORMAT — "<hazard> (Recall
ID <the number printed in the article>)" — and when the article prints no
number the model fills the slot with a placeholder.

tools/repair_2026_10_04.py stripped three such rows YESTERDAY as a one-off
data fix and added nothing to any writer or gate. The defect recurred the
next morning, and in a spelling yesterday's one-off regex could not have
matched either, because this one has a COLON: "(Recall ID: N/A)" against
"(Recall ID N/A)".

Publish-gate rule 2 does not catch it: that rule fires only when the
ENTIRE Reason is a bare identifier, and here the hazard IS described and
the template is trailing noise, so the row passes every rule and would
have published to subscribers verbatim.

The permanent guard is merge_master.strip_empty_identifier_template,
called from _write_sheet AND from promote_gate_passing (which appends to
Recalls directly and skips every guard in _write_sheet — the bypass
documented there on 2026-10-04). This script applies it to the live
sheets once, for the row already sitting in Pending.


FAULT C — A NORWEGIAN ROW ABOUT TO PUBLISH IN NORWEGIAN
--------------------------------------------------------
The same Mattilsynet row passes the full publish gate and would have been
published at 15:27 today as:

    Product   "Pokrazywa"
    Pathogen  "Pyrrolizidine alkaloider exceeding limit"

"alkaloider" is Norwegian. "Pokrazywa" is the brand's product name with no
food in it, and the register's rule since 2026-10-01 is that a published
Product is English.

mattilsynet.no COULD NOT BE READ: WebFetch returned ROBOTS_DISALLOWED
(robots.txt fetch rate-limited, 429) on two attempts, and routing around a
blocked fetch is not permitted. So NOTHING is imported from the page.
Both corrections come from the row's own text and its own URL slug, which
is the regulator's own per-recall address:

    .../herbapol-pokrazywa-te-i-pose-med-smak-av-brennesle-tilbakekalles-
        pa-grunn-av-innhold-av-giftige-plantestoffer

    "te i pose"            tea in bags
    "med smak av brennesle" with nettle flavour

giving Product "Pokrazywa nettle-flavoured tea bags". No lot, weight,
best-before or company detail is added, because none was read. Pathogen
takes the register's existing label for this hazard, "Pyrrolizidine
alkaloids" — the spelling already carried by the published GIS (PL)
Herbapol nettle-tea row of 2026-09-10 — so the register holds one spelling
per hazard. Both originals are kept in Notes.

Checked: the row is reachable by the "Pyrrolizidine alkaloids" subscriber
alert both before and after, via its Reason, so no vocabulary gap.


FAULT D — A PET-FOOD RECALL ARCHIVED WITH NO REASON, BACK IN PENDING
----------------------------------------------------------------------
    Pending, RappelConso fiche 23687, scraped 2026-10-05T01:11:34Z
    "huile 10% de cbd pour chat et huile 5% de cbd pour chat"
    "Le cbd et les extraits de chanvre ont le statut d'additifs non
     autorisé pour l'alimentation animale."

CBD oil for cats. AFTS-FSIS is a HUMAN-food register (R1) — the precedent
is explicit in the archive: a 2026-02-24 row reads "pet_food_out_of_scope
- AFTS-FSIS is a HUMAN-food register."

This is one of a batch of SEVEN CBD pet-oil fiches RappelConso published
on 2026-10-02 (23682–23687 plus a /static/fiches/ copy). All seven were
archived between 2026-10-02 and 2026-10-04 with **RejectReason blank** —
the verdict, where there is one, sits in Notes as a reviewer's
field-defect complaint ("the hazard is not a microbial pathogen", "Pathogen
is empty"), never as a scope judgement.

That is why fiche 23687 is back: the daily RappelConso scrape re-ingested
it this morning and nothing on file says the register has decided it does
not belong. The rows are given the terminal scope reason now, so
merge_master.load_rejected_urls has a verdict to show the next reviewer.

WHAT THIS DOES NOT FIX, SAID PLAINLY: the daily scrape path does not
consult the Rejected archive before writing to Pending, so fiche 23687 may
well arrive again tomorrow. Changing the scrape path is a bigger change
than a morning pass should make unreviewed. The publish gate holds it
either way — Pathogen is empty — so the cost is Pending noise, not a bad
publication.


FAULT E — A LISTERIA RECALL THE PIPELINE MISSED
-------------------------------------------------
    rappel.conso.gouv.fr fiche 23696, published 05/10/2026

Read on the regulator's own per-recall page on 2026-10-05. Searched all
five sheets first — by URL, by fiche number, by company and by product —
and it is in none of them. Its neighbours 23688–23691 (02/10) are all
published, so this is a gap in today's window, not a dead collector.

Fiche 23697 of the same date is NOT added: its "Motif du rappel" is
"Dépassement du seuil d'ABVT" (total volatile basic nitrogen above the
threshold), which is a freshness/decomposition limit — a quality
parameter, excluded by the printed AFTS scope. That is a scope call, not a
verification failure, and it is flagged for the operator rather than
decided quietly here.
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = dt.date.today().isoformat()

LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")

MATTILSYNET_URL = (
    "https://www.mattilsynet.no/tilbakekallinger/herbapol-pokrazywa-te-i-"
    "pose-med-smak-av-brennesle-tilbakekalles-pa-grunn-av-innhold-av-"
    "giftige-plantestoffer")

#: The seven CBD pet-oil fiches RappelConso published 2026-10-02, all
#: archived with a blank RejectReason. Keyed by URL — the only stable key.
CBD_PET_URLS = (
    "https://rappel.conso.gouv.fr/fiche-rappel/23682/interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23683/interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23684/interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23685/interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23686/interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23687/interne",
    "https://rappel.conso.gouv.fr/static/fiches/"
    "0893c31f-880f-4805-8acb-383121fb658c.html",
    "https://rappel.conso.gouv.fr/static/fiches/"
    "d68912b8-a20e-4a36-bac3-9fb2bb3ae7f0.html",
)

CBD_PET_REASON = (
    "pet_food_out_of_scope — operator review " + TODAY + ". RappelConso's "
    "own notice gives the motif as \"Le cbd et les extraits de chanvre ont "
    "le statut d'additifs non autorisé pour l'alimentation animale\" — "
    "unauthorised additives in ANIMAL FEED — and the product is CBD oil "
    "for cats and dogs. AFTS-FSIS is a HUMAN-food register (R1); the "
    "precedent is the 2026-02-24 archive row 'pet_food_out_of_scope - "
    "AFTS-FSIS is a HUMAN-food register.' This is one of seven fiches "
    "(23682-23687 plus two /static/fiches/ copies) RappelConso published "
    "2026-10-02, every one of them archived with the RejectReason column "
    "BLANK and only a reviewer's field complaint in Notes ('the hazard is "
    "not a microbial pathogen', 'Pathogen is empty') — a field defect, "
    "which reads as repairable, not as a scope verdict. Fiche 23687 was "
    "re-ingested into Pending by the daily RappelConso scrape at "
    "2026-10-05T01:11:34Z for exactly that reason. The terminal scope "
    "reason is recorded here so merge_master.load_rejected_urls has a "
    "verdict to show the next reviewer. NOTE: the scrape path does not "
    "consult the Rejected archive before writing to Pending, so this may "
    "arrive again; the publish gate holds it (Pathogen is empty)."
)

#: The row added this morning. Every value below was read on the
#: regulator's own per-recall page on 2026-10-05; nothing is inferred.
FICHE_23696 = {
    "Date": "2026-10-05",
    "Source": "RappelConso (FR)",
    "Company": "SODIMAZ LECLERC",
    # RappelConso prints "Sans marque" — literally "no brand". It is the
    # ABSENCE of a brand, not a brand name, and writing it into Brand makes
    # a one-off firm name out of a French placeholder
    # (tests/test_firm_names_are_uniform caught exactly that). Left empty;
    # the original string is in Notes.
    "Brand": "",
    "Product": ("Pork friton (pork cracklings), GTIN 3385570020915, "
                "lot 0209G244L1"),
    "Pathogen": "Listeria monocytogenes",
    # English only. The French original belongs in Notes, not here —
    # tests/test_language_policy reads Reason and must find English.
    "Reason": ("Presence of Listeria monocytogenes, the agent responsible "
               "for listeriosis."),
    "Class": "Recall",
    "Country": "France",
    "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23696/interne",
    "Notes": (
        "Added by the morning fix pass " + TODAY + " — missed by the "
        "pipeline. Every field was read on the regulator's own per-recall "
        "page, https://rappel.conso.gouv.fr/fiche-rappel/23696/interne, on "
        + TODAY + ". Read there: fiche réf. 2026-10-0025; \"Date de "
        "publication\" 05/10/2026; \"Nom de la marque du produit\" = \"Sans "
        "marque\"; \"Noms des modèles ou références\" = \"Friton de porc\"; "
        "\"Motif du rappel\" = \"Présence de Listeria monocytogenes (agent "
        "responsable de la listériose)\"; \"Risques encourus par le "
        "consommateur\" = \"Listeria monocytogenes (agent responsable de la "
        "listériose)\"; \"Nom de l'entreprise\" = \"SODIMAZ LECLERC\"; "
        "distributeur \"E.LECLERC BOUT DU PONT DE L'ARN\"; GTIN 3385570020915; "
        "lot 0209G244L1; commercialisation 24/09/2026-01/10/2026; zone "
        "\"France entière\". "
        "[original Reason (fr): \"Présence de Listeria monocytogenes (agent "
        "responsable de la listériose)\"] "
        "[original product: Friton de porc] "
        "[original company: SODIMAZ LECLERC] "
        "[original brand: Sans marque — RappelConso's placeholder for NO "
        "brand, so Brand is left empty rather than carrying it as a firm "
        "name] "
        "Searched Recalls, Pending, Weekly_Review, Weekly_Rejected and "
        "Rejected by URL, by fiche number 23696, by company and by product "
        "before adding: present in none. Neighbouring fiches 23688-23691 "
        "(02/10/2026) are all already published, so this is a gap in the "
        "24-hour window rather than a dead collector."),
}


# ── helpers ───────────────────────────────────────────────────────────────
def _head(ws):
    return [str(c.value) for c in ws[1]]


def _col(head, name):
    return head.index(name) + 1 if name in head else None


def _find_rows(ws, url):
    head = _head(ws)
    ucol = _col(head, "URL")
    if not ucol:
        return []
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, ucol).value or "").strip() == url]


def _stamp(ws, r, text):
    ncol = _col(_head(ws), "Notes")
    if not ncol:
        return
    prior = str(ws.cell(r, ncol).value or "").strip()
    ws.cell(r, ncol).value = (prior + " " + text).strip()


def _archive(wb, sheet, row, reason, by):
    if sheet not in wb.sheetnames:
        print(f"  WARNING: no {sheet} sheet — nothing archived")
        return False
    arch = wb[sheet]
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        elif h == "Reviewed":
            out.append("Y")
        elif h == "Week_Added":
            out.append(TODAY)
        else:
            out.append(row.get(h, ""))
    arch.append(out)
    return True


# ── FAULT B ───────────────────────────────────────────────────────────────
def strip_empty_identifier_templates(wb) -> int:
    """Apply the permanent writer-side guard to the rows already on disk."""
    from pipeline.merge_master import strip_empty_identifier_template
    n = 0
    for name in LIVE_SHEETS:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        if "Reason" not in head:
            continue
        rcol = _col(head, "Reason")
        ncol = _col(head, "Notes")
        for r in range(2, ws.max_row + 1):
            row = {"Reason": ws.cell(r, rcol).value,
                   "Notes": ws.cell(r, ncol).value if ncol else ""}
            if strip_empty_identifier_template([row], where=name):
                ws.cell(r, rcol).value = row["Reason"]
                if ncol:
                    ws.cell(r, ncol).value = row["Notes"]
                print(f"  {name} row {r}: template stripped from Reason")
                n += 1
    return n


# ── FAULT C ───────────────────────────────────────────────────────────────
def repair_mattilsynet_row(wb) -> int:
    n = 0
    for name in LIVE_SHEETS:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        pcol, hcol = _col(head, "Product"), _col(head, "Pathogen")
        for r in _find_rows(ws, MATTILSYNET_URL):
            changed = []
            if pcol and str(ws.cell(r, pcol).value or "").strip() == "Pokrazywa":
                ws.cell(r, pcol).value = "Pokrazywa nettle-flavoured tea bags"
                changed.append("Product")
            if hcol and "alkaloider" in str(ws.cell(r, hcol).value or "").lower():
                ws.cell(r, hcol).value = "Pyrrolizidine alkaloids"
                changed.append("Pathogen")
            if not changed:
                continue
            _stamp(ws, r, (
                f"[language-repair {TODAY}: Product read 'Pokrazywa' and "
                f"Pathogen read 'Pyrrolizidine alkaloider exceeding limit' — "
                f"'alkaloider' is Norwegian, and 'Pokrazywa' is a brand "
                f"product name that names no food. mattilsynet.no could NOT "
                f"be read: WebFetch returned ROBOTS_DISALLOWED (robots.txt "
                f"rate-limited, 429) on two attempts {TODAY}, and routing "
                f"around a blocked fetch is not permitted, so NOTHING was "
                f"imported from the page. Both values come from the row's "
                f"own text and its own authority URL slug, "
                f"'herbapol-pokrazywa-te-i-pose-med-smak-av-brennesle' — "
                f"'te i pose' is tea in bags, 'med smak av brennesle' is "
                f"with nettle flavour. Pathogen takes the register's "
                f"existing label for this hazard, already carried by the "
                f"published GIS (PL) Herbapol nettle-tea row of 2026-09-10. "
                f"No lot, weight, best-before or company detail was added, "
                f"because none was read.] "
                f"[original product: Pokrazywa] "
                f"[original Pathogen (no): Pyrrolizidine alkaloider "
                f"exceeding limit]"))
            print(f"  {name} row {r}: {', '.join(changed)} put into English")
            n += 1
    return n


# ── FAULT D ───────────────────────────────────────────────────────────────
def settle_cbd_pet_rows(wb) -> int:
    n = 0
    # 1. The copy that came back into Pending this morning.
    if "Pending" in wb.sheetnames:
        ws = wb["Pending"]
        head = _head(ws)
        for url in CBD_PET_URLS:
            for r in reversed(_find_rows(ws, url)):
                row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
                _archive(wb, "Rejected", row, CBD_PET_REASON,
                         "operator review " + TODAY)
                ws.delete_rows(r, 1)
                print(f"  Pending row {r}: CBD pet oil archived "
                      f"(pet_food_out_of_scope)")
                n += 1
    # 2. The sibling archive rows whose reason column is blank.
    for sheet, col in (("Rejected", "RejectReason"),
                       ("Weekly_Rejected", "RejectionReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        rcol = _col(_head(ws), col)
        if not rcol:
            continue
        for url in CBD_PET_URLS:
            for r in _find_rows(ws, url):
                prior = str(ws.cell(r, rcol).value or "").strip()
                if prior.lower() in ("", "none", "nan"):
                    ws.cell(r, rcol).value = CBD_PET_REASON
                    print(f"  {sheet} row {r}: terminal scope reason recorded")
                    n += 1
                elif "pet_food_out_of_scope" not in prior:
                    ws.cell(r, rcol).value = (
                        CBD_PET_REASON + " [prior reviewer note: " +
                        prior[:300] + "]")
                    print(f"  {sheet} row {r}: terminal scope reason prepended")
                    n += 1
    return n


# ── FAULT E ───────────────────────────────────────────────────────────────
def add_missed_recall() -> int:
    """Queue fiche 23696 through tools/add_manual_row.py, as a person would."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    for sheet in wb.sheetnames:
        ws = wb[sheet]
        rows = list(ws.values)
        if not rows:
            continue
        head = [str(h) for h in rows[0]]
        if "URL" not in head:
            continue
        i = head.index("URL")
        for r in rows[1:]:
            if r and i < len(r) and str(r[i] or "").strip() == FICHE_23696["URL"]:
                print(f"  fiche 23696 already present in {sheet} — not added "
                      f"again")
                wb.close()
                return 0
    wb.close()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(FICHE_23696, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run([sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
                          path], cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip())
    if out.returncode != 0:
        print("  add_manual_row FAILED:", (out.stderr or "")[-500:])
        return 0
    return 1


# ── FAULT A ───────────────────────────────────────────────────────────────
def retire_contradicting_archive_rows() -> int:
    """Run the real sweep, not a hand copy of it."""
    from pipeline.promote_gate_passing import supersede_every_published_url
    return supersede_every_published_url(XLSX)


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1

    print("FAULT B — the extractor's empty-identifier template")
    n_b = strip_empty_identifier_templates(wb)
    print(f"  {n_b} Reason(s) cleaned\n")

    print("FAULT C — a Norwegian row about to publish in Norwegian")
    n_c = repair_mattilsynet_row(wb)
    print(f"  {n_c} row(s) put into English\n")

    print("FAULT D — a pet-food recall archived with no reason")
    n_d = settle_cbd_pet_rows(wb)
    print(f"  {n_d} row(s) settled\n")

    if n_b or n_c or n_d:
        wb.save(XLSX)
    wb.close()

    print("FAULT E — a Listeria recall the pipeline missed")
    n_e = add_missed_recall()
    print(f"  {n_e} row(s) queued into Pending\n")

    print("FAULT A — recalls both published and thrown away")
    n_a = retire_contradicting_archive_rows()
    print(f"  {n_a} archive row(s) marked SUPERSEDED\n")

    total = n_a + n_b + n_c + n_d + n_e
    if not total:
        print("nothing to do")
        print("ROWS_REMOVED=0")
        return 0

    after = openpyxl.load_workbook(XLSX)["Recalls"].max_row - 1
    print(f"Recalls {before} -> {after}")

    from pipeline.merge_master import mirror_json_from_xlsx
    n = mirror_json_from_xlsx(XLSX, JSON)
    print(f"recalls.json re-mirrored: {n} rows")

    print("ROWS_REMOVED=0")      # nothing is removed from Recalls here
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
