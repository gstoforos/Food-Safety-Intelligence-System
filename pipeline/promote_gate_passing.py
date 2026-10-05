"""Promote every Pending row that passes the FULL publish gate — offline.

This is the step that was being done by hand. It adds no judgement of its own:
a row is promoted if and only if `_publish_gate.publish_blockers` returns
nothing for it, and the promotion itself goes through
`merge_master.promote_approved`, so the re-promotion guard, the URL dedup and
the two-axis dedup against Recalls all still apply exactly as they do on the
nightly path.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It never archives. `promote_approved` also returns rows it would evict, and on
2026-09-27 that list held four genuine Tier-1 recalls — Salute cured salami,
GIS High Protein Pudding, Mattilsynet Brie de Melun AOP, Salute brie and smoked
pancetta — whose only problem is a repairable field defect (an empty Reason, a
Region string the vocabulary does not know, the "Recall ID 842632" example text
that leaked out of an extractor prompt). Archiving those is how a repairable
defect becomes a permanent loss, which is the failure this whole day was spent
undoing. So rows that do not pass are LEFT IN PENDING and printed, and nothing
is ever removed from Pending except a row that has just been promoted.

It prints `ROWS_REMOVED=n` so a caller can decide whether the commit message
needs a register-shrink deletion marker. Promotion alone never removes a
published row, so that number is normally 0.
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def supersede_archived_copies(xlsx_path, promoted_urls) -> int:
    """Mark any archive row whose URL has just been PUBLISHED as superseded.

    THE FAULT THIS CLOSES (morning sweep, 2026-09-28)
    -------------------------------------------------
    The sweep found **31 URLs that are simultaneously published and rejected**
    — 21 shared between Recalls and Weekly_Rejected, 10 between Recalls and
    Rejected — and put it exactly right: "A recall cannot be both published
    and thrown away; whichever copy a reader hits first decides what the
    register says."

    Eleven of those twenty-one are mine. On 2026-09-27 I added a narrow
    exception to merge_master's re-promotion guard so that a row archived for
    a REPAIRABLE defect ("Pathogen is empty") could come back once the defect
    was repaired. It works — eleven real recalls returned, five of them Tier 1
    — but it only ever wrote to Recalls. The Weekly_Rejected row that said the
    recall had been thrown away stayed exactly where it was, so the register
    now asserts both things at once. Fixing the promotion without retiring the
    rejection is half a fix.

    WHY THIS ANNOTATES RATHER THAN DELETES. Weekly_Rejected and Rejected are
    append-only by design — test_no_append_only_sheet_loses_rows guards them,
    and the whole point of an archive is that you can read why something was
    once refused. Deleting the row would destroy the audit trail AND trip the
    register-shrink guard. So the row stays, its Reviewed/status column says
    SUPERSEDED, and the reason records which copy now wins.

    NOT EVERY SHARED URL IS A CONTRADICTION. Seven of the twenty-one are the
    RASFF duplicate copies archived on 2026-09-27: after their malformed
    reference URLs were repaired, the archived copy and the kept copy
    legitimately share one address, because they were always the same
    notification. Those already say "Duplicate" in their reason and are left
    alone — marking them superseded would be true but redundant, and rewriting
    settled audit text for no gain is how audit text stops being trusted.

    THREE DEFECTS MEASURED ON THE 2026-10-05 WORKBOOK
    -------------------------------------------------
    The morning sweep counted **36** silent shared URLs — 1 in
    Weekly_Rejected, 35 in Rejected — eight days after this function was
    written to prevent exactly that. Three separate causes, each measured:

    1. **IT READ THE REASON COLUMN ONLY.** Of the 36, exactly **one**
       matched on its reason column. The other 35 carry
       ``RejectReason`` = blank and keep the actual verdict in **Notes**
       ("REJECTED: Confirmer: row was at pending_gap_v2 … and Pathogen is
       empty"). ``merge_master.load_rejected_urls`` learned this on
       2026-09-01 and folds Notes in; this function never did. Folding
       Notes in here resolves **27 of the 36**.

    2. **THE FIELD LIST WAS A HAND-PICKED SUBSET.** Publish-gate rule 4
       emits "<Field> is empty" for Company, Product, Class, Date, Source
       and URL; SUPERSEDE_IF listed company, pathogen and reason only. An
       FSAI Wrights of Marino row archived for "Date is empty" — the same
       repairable field defect, one field over — matched nothing. The
       subset is replaced by a regex over the gate's own field list.

    3. **A ROW WITH NO VERDICT AT ALL WAS SKIPPED.** Eight gap-finder
       rows carry no reason and no verdict in Notes either — only
       "Discovered via news: <site>". An archive row that records no
       reason for refusing a recall cannot outrank the published copy of
       it; silence is not a verdict. Those are now stamped too, saying
       exactly that.

    WHAT IS STILL OPEN, DELIBERATELY. This runs over the whole register on
    an --apply run (see main), not only over the URLs one promoter just
    promoted, because the other publication paths — the hourly
    Pending→Recalls merge, the three reviewers, the confirm agent — never
    call it. Routing all of them through one publication choke point is a
    change worth making on purpose, with its own review. It is not made
    here.
    """
    import openpyxl
    import re as _re
    # Publish-gate rule 4's own field list. A row archived because one of
    # these was missing was archived on a REPAIRABLE field defect, never on
    # a judgement about the recall.
    _FIELD_EMPTY = _re.compile(
        r"\b(?:pathogen|company|product|brand|class|date|source|url|reason)"
        r" is empty\b", _re.IGNORECASE)
    SUPERSEDE_IF = ("product is a fragment", "reason is only a reference number",
                    "llm-extraction-failed", "no official regulator url",
                    "no matching hazard category",
                    # 2026-09-28: verified by reading both copies. Two USDA FSIS
                    # rows were rejected as "fabricated_pathogen_and_out_of_scope"
                    # for carrying Pathogen "Hepatitis A virus" on recalls whose
                    # real hazard was production without inspection. The rows now
                    # PUBLISHED under those same URLs carry the correct
                    # "Uninspected product (hazard not assessed)". So the
                    # rejection is the audit trail of a fabrication and the
                    # published row is its repair — the archive should say so
                    # rather than look like a live contradiction.
                    "fabricated_pathogen",
                    # ── 2026-09-30 ────────────────────────────────────────
                    # "Arrived already marked rejected; confirmer did not
                    # re-review" is not a verdict on the recall at all — it
                    # records that NOBODY looked. When the URL is now
                    # published, the published copy is plainly the one that
                    # wins, and leaving this row silent is exactly the
                    # contradiction test_a_recall_is_not_both_published_and_
                    # rejected was written to catch.
                    "confirmer did not re-review",
                    # 2026-09-30: merge_master.REPAIRABLE_DEFECTS gained this
                    # entry (CFIA R.J. King lobster); the two lists must move
                    # together or a re-promoted row leaves its archive copy
                    # contradicting the register. Held by
                    # tests/test_a_challenge_page_is_not_data.py.
                    "company and brand are the same",
                    # 2026-09-30: the import-violation line was reversed
                    # (operator: "in scope, as uninspected"); a re-promoted
                    # Sempio must retire its archived copy too.
                    "out_of_scope_import_reinspection")
    wb = openpyxl.load_workbook(xlsx_path)
    want = {str(u).strip().lower() for u in promoted_urls if str(u).strip()}
    stamped = 0
    for sheet, reason_col, mark_col in (("Weekly_Rejected", "RejectionReason", "Reviewed"),
                                        ("Rejected", "RejectReason", "Status")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = [str(c.value) for c in ws[1]]
        if "URL" not in head or reason_col not in head:
            continue
        ucol = head.index("URL") + 1
        rcol = head.index(reason_col) + 1
        ncol = head.index("Notes") + 1 if "Notes" in head else None
        mcol = head.index(mark_col) + 1 if mark_col in head else None
        for r in range(2, ws.max_row + 1):
            if str(ws.cell(r, ucol).value or "").strip().lower() not in want:
                continue
            prior = str(ws.cell(r, rcol).value or "")
            # ── DEFECT 1 (2026-10-05): THE VERDICT IS OFTEN IN Notes ─────
            # 35 of 36 silent rows carry a blank reason column and keep the
            # verdict in Notes. load_rejected_urls has folded Notes in since
            # 2026-09-01; this function had not, so it could not see the
            # verdicts it exists to act on. `prior` stays the reason column
            # alone — that is what gets rewritten — and `low` becomes the
            # text the CLASSIFICATION reads.
            notes = (str(ws.cell(r, ncol).value or "")
                     if ncol is not None else "")
            verdict = (prior + " | " + notes).strip(" |")
            low = verdict.lower()
            if "duplicate" in low:            # see the docstring — leave settled text
                continue
            # ── THE REFUSAL CLASS (2026-09-30) ───────────────────────────
            # SUPERSEDE_IF is a list of literal phrases, and it carried
            # exactly ONE spelling of the not-found refusal: "no official
            # regulator url". The reviewers write it at least four other
            # ways — "No official recall page found", "No official regulator
            # page found", "No official regulator page found for this
            # recall", "URL not found and no official page found" — and none
            # of those matched. Measured on the 2026-09-30 workbook right
            # after this morning's promotions: FIVE Weekly_Rejected rows
            # shared a URL with a freshly published recall and said nothing
            # about which copy wins, four of them on a refusal spelling this
            # list did not know.
            #
            # _url_guard owns that vocabulary already — it is the module that
            # decides what counts as a claim about reachability — so ask it
            # rather than growing a second, divergent list here.
            _is_refusal = False
            try:
                from pipeline._url_guard import reject_refusal as _rr
                _is_refusal = bool(_rr(
                    {"URL": str(ws.cell(r, ucol).value or "")}, verdict))
            except Exception:                              # pragma: no cover
                _is_refusal = False
            # DEFECT 2 (2026-10-05): any of publish-gate rule 4's fields.
            _field_empty = bool(_FIELD_EMPTY.search(low))
            # DEFECT 3 (2026-10-05): no recorded verdict anywhere. Eight
            # gap-finder rows carry only "Discovered via news: <site>" —
            # which says where the row came from, not why it was refused.
            # Silence is not a verdict, and it cannot outrank a published
            # recall.
            _no_verdict = not any(
                m in low for m in ("reject", "refus", "out of scope",
                                   "out_of_scope", "not a food", "fabricat",
                                   "allergen", "labelling", "labeling",
                                   "upheld", "superseded"))
            if not (_is_refusal or _field_empty or _no_verdict
                    or any(d in low for d in SUPERSEDE_IF)):
                continue
            if "SUPERSEDED" in prior:
                continue
            # Say WHERE the defect is recorded. On 35 of the 36 rows found
            # on 2026-10-05 the reason column is blank and the verdict is
            # in Notes, so "the defect named below" pointed at nothing.
            _where = "below" if prior.strip() else "in this row's Notes"
            _banner = (
                "[SUPERSEDED " + dt.date.today().isoformat() + " — the defect named "
                + _where + " was repaired and this recall is PUBLISHED in Recalls. "
                "This row is kept only as the audit trail of the original refusal; "
                "the Recalls copy is what the register says.] "
                if not _no_verdict or _is_refusal or _field_empty
                else "[SUPERSEDED " + dt.date.today().isoformat() + " — this archive "
                "row records NO reason for refusing the recall (its Notes say only "
                "where it was discovered), and the same URL is PUBLISHED in Recalls. "
                "Silence is not a verdict: the Recalls copy is what the register "
                "says. Kept as the audit trail of the original refusal.] ")
            ws.cell(r, rcol).value = _banner + prior
            if mcol:
                ws.cell(r, mcol).value = "SUPERSEDED"
            stamped += 1
    if stamped:
        wb.save(xlsx_path)
    return stamped

def supersede_every_published_url(xlsx_path) -> int:
    """Run the sweep over the WHOLE register, not just this run's promotions.

    2026-10-05. ``supersede_archived_copies`` only ever saw the URLs one
    promoter run had just promoted. Every other publication path — the
    hourly Pending→Recalls merge, reviewers 1/2/3, the confirm agent, the
    gap finders — publishes without calling it, so an archive row left
    contradicting the register by any of those paths stayed silent for
    ever. Thirty-six had, by this morning.

    Reading the published URLs out of Recalls makes the sweep independent
    of who did the publishing. It is idempotent: a row already stamped
    SUPERSEDED, or whose reason says "duplicate", is skipped.
    """
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    if "Recalls" not in wb.sheetnames:
        return 0
    rows = wb["Recalls"].values
    try:
        head = [str(h) for h in next(rows)]
    except StopIteration:
        return 0
    if "URL" not in head:
        return 0
    i = head.index("URL")
    published = {str(r[i]).strip() for r in rows
                 if r and i < len(r) and r[i] and str(r[i]).strip()}
    wb.close()
    return supersede_archived_copies(xlsx_path, published)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(XLSX))
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    import pandas as pd
    import openpyxl
    from pipeline._publish_gate import publish_blockers
    from pipeline.merge_master import promote_approved, mirror_json_from_xlsx

    pend = pd.read_excel(args.xlsx, "Pending").to_dict("records")
    rec = pd.read_excel(args.xlsx, "Recalls").to_dict("records")

    # A row only reaches promote_approved's approved branch when its Status is
    # plain 'pending'. Rows parked in a reviewer lane (pending_gap_v2, …) that
    # now satisfy every gate rule are moved to 'pending' here, with the reason
    # stamped, so the promotion is auditable from the sheet.
    ready = [r for r in pend if not publish_blockers(r)]
    print(f"Pending {len(pend)} rows — pass the full publish gate: {len(ready)}")
    for r in ready:
        print(f"   {str(r.get('Date'))[:10]} {str(r.get('Source'))[:14]:15} "
              f"T{r.get('Tier')} {str(r.get('Pathogen'))[:24]:25} "
              f"{str(r.get('Product'))[:40]}")
    blocked = [(r, publish_blockers(r)) for r in pend if publish_blockers(r)]
    print(f"\nLeft in Pending (never archived here): {len(blocked)}")
    for r, b in blocked:
        print(f"   {str(r.get('Source'))[:14]:15} "
              f"{str(r.get('Product'))[:34]:35} | {b[0][:64]}")

    if not args.apply:
        print("\n(report only — pass --apply to write)")
        print("ROWS_REMOVED=0")
        return 0
    if not ready:
        print("\nnothing to promote")
        # The contradiction sweep is NOT conditional on a promotion: the
        # rows it fixes were published by other paths (2026-10-05).
        n_sup = supersede_every_published_url(args.xlsx)
        if n_sup:
            print(f"marked {n_sup} archive row(s) SUPERSEDED — a recall must "
                  f"not be both published and rejected")
        print("ROWS_REMOVED=0")
        return 0

    stamped = []
    for r in ready:
        r = dict(r)
        prev = str(r.get("Status") or "")
        r["Status"] = "pending"
        r["Notes"] = (str(r.get("Notes") or "").strip() +
                      f" [promote {dt.date.today().isoformat()}: status "
                      f"{prev!r}->'pending' — passes every publish-gate rule]"
                      ).strip()
        # Product and firm names are English (operator 2026-10-01). No model
        # here, so stamp for the Morning Fix translation pass — never block.
        try:
            from pipeline._language import (has_non_latin_script,
                                            product_needs_english)
            if product_needs_english(r.get("Product", "")):
                r["Notes"] += " [needs cleanup: Product not in English]"
            if any(has_non_latin_script(r.get(f, ""))
                   for f in ("Company", "Brand")):
                r["Notes"] += " [needs cleanup: Company/Brand not in English]"
        except Exception:                                    # noqa: BLE001
            pass
        stamped.append(r)
    others = [r for r in pend if publish_blockers(r)]
    new, _keep, _arch = promote_approved(stamped + others, rec, {},
                                         archive_immediately=False)
    print(f"\npromote_approved -> {len(new)} row(s) to Recalls "
          f"({len(_arch)} it would archive are deliberately IGNORED)")
    if not new:
        print("ROWS_REMOVED=0")
        return 0

    # ── CANONICALISE THE LABELS THIS PATH BYPASSES (2026-10-04) ──────────
    # This function publishes with R.append(), not with
    # merge_master._write_sheet, so none of the writer's guards have ever
    # run on the rows it promotes. The Italian gap finder's bare "Salute"
    # reached Recalls that way on 2026-10-04 and broke
    # test_monitored_sources::test_every_published_source_is_counted,
    # because the dashboard registry calls that regulator "Ministero della
    # Salute (IT)". Country has the same exposure ("usa" / "United
    # States"), and Country is a join key for Region, the per-country
    # counts in both reports and every subscriber's country filter.
    #
    # The REST of the bypass is not closed here — see the note on
    # merge_master.apply_label_aliases.
    try:
        from pipeline.merge_master import apply_label_aliases
        _n_alias = apply_label_aliases(new, where="promote_gate_passing")
        if _n_alias:
            print(f"canonicalised {_n_alias} Country/Source label(s) on the "
                  f"promoted rows")
    except Exception as _ae:                                 # noqa: BLE001
        print(f"label canonicalisation skipped: "
              f"{type(_ae).__name__}: {str(_ae)[:80]}")

    # Same bypass, same remedy: the extractor's empty-identifier template
    # ("(Recall ID: N/A)") is stripped at _write_sheet, which this writer
    # does not go through. 2026-10-05.
    try:
        from pipeline.merge_master import strip_empty_identifier_template
        _n_tpl = strip_empty_identifier_template(
            new, where="promote_gate_passing")
        if _n_tpl:
            print(f"stripped the extractor's empty-identifier template from "
                  f"{_n_tpl} Reason(s)")
    except Exception as _te:                                 # noqa: BLE001
        print(f"empty-identifier strip skipped: "
              f"{type(_te).__name__}: {str(_te)[:80]}")

    promoted = {str(r.get("URL", "")).strip() for r in new}
    wb = openpyxl.load_workbook(args.xlsx)
    R, P = wb["Recalls"], wb["Pending"]
    rh = [str(c.value) for c in R[1]]
    ph = [str(c.value) for c in P[1]]
    before = R.max_row - 1
    for r in new:
        R.append([r.get(h) for h in rh])
    uc = ph.index("URL") + 1
    drop = [i for i in range(2, P.max_row + 1)
            if str(P.cell(i, uc).value or "").strip() in promoted]
    for i in sorted(drop, reverse=True):
        P.delete_rows(i, 1)
    wb.save(args.xlsx)
    mirror_json_from_xlsx(Path(args.xlsx),
                          Path(args.xlsx).parent / "recalls.json")

    # RECORD THE PROMOTIONS IN Weekly_Review — without this the operator's
    # Sunday email never learns they happened.
    #
    # 2026-09-27: the review email that went out today was headed
    # "Sun 2026-09-20", 15 recalls, generated 2026-09-21T09:02:58 — a week
    # stale. docs/data/weekly-review-latest.json is only rewritten when rows
    # are PROMOTED (reviewer 3 / merge_master call record_promotions), and
    # nothing had been promoted since 2026-09-21, so the mailer had nothing
    # newer and re-sent the last capture it held. A promotion path that
    # skipped this call would reproduce exactly that: rows published on the
    # site, and an operator email still describing last week.
    try:
        from pipeline.weekly_review_capture import record_promotions
        n_rec = record_promotions(new, xlsx_path=Path(args.xlsx))
        print(f"recorded {n_rec} promotion(s) in Weekly_Review and refreshed "
              f"weekly-review-latest.json")
    except Exception as exc:                            # pragma: no cover
        # Never lose a promotion over the email capture, but never let it
        # fail silently either — a stale email is what this whole block is for.
        print(f"WARNING: Weekly_Review capture failed ({exc!r}); the rows ARE "
              f"promoted but today's operator email will not mention them")
    # The whole register, not just `promoted` — see
    # supersede_every_published_url for why (2026-10-05).
    n_sup = supersede_every_published_url(args.xlsx)
    if n_sup:
        print(f"marked {n_sup} archive row(s) SUPERSEDED — a recall must not be "
              f"both published and rejected")

    print(f"Recalls {before} -> {before + len(new)}; "
          f"removed {len(drop)} promoted row(s) from Pending; json mirrored")
    print("ROWS_REMOVED=0")          # promotion never removes a published row
    return 0


if __name__ == "__main__":                                 # pragma: no cover
    raise SystemExit(main())
