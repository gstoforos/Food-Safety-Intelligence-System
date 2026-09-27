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
        stamped.append(r)
    others = [r for r in pend if publish_blockers(r)]
    new, _keep, _arch = promote_approved(stamped + others, rec, {},
                                         archive_immediately=False)
    print(f"\npromote_approved -> {len(new)} row(s) to Recalls "
          f"({len(_arch)} it would archive are deliberately IGNORED)")
    if not new:
        print("ROWS_REMOVED=0")
        return 0

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
    print(f"Recalls {before} -> {before + len(new)}; "
          f"removed {len(drop)} promoted row(s) from Pending; json mirrored")
    print("ROWS_REMOVED=0")          # promotion never removes a published row
    return 0


if __name__ == "__main__":                                 # pragma: no cover
    raise SystemExit(main())
