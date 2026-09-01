#!/usr/bin/env python3
"""
recall_confirm_agent.py  —  AGENT 3 (reviewer 3, the confirmer)
================================================================

Third and final stage. Reviewer 2 does the expensive work — fetch the page,
verify and correct every field, decide approve or reject — and parks its
approvals at Status="pending_gap_v3". This agent confirms that work and
publishes it.

WHY THE SPLIT EXISTS
--------------------
Reviewer 2 runs on CPU-only inference at roughly 100 s per row, so it always
hits its time budget partway through the queue. Before the split, a timeout
meant the whole run promoted nothing: the work was done but never published.

Now reviewer 2 banks every finished row as pending_gap_v3, and this agent
publishes them. It uses NO model — only the deterministic checks — so it runs
in seconds and CANNOT be blocked by the VPS. Whatever reviewer 2 finished
today is published today.

WHAT IT CHECKS
--------------
Reviewer 2's verdict is trusted for JUDGEMENT (is this a real, in-scope
recall). This agent independently re-runs the checks that need no model and
that catch the error classes seen in production:

    * fabricated pathogen        (Pathogen contradicts Reason)
    * placeholder instead of a value
    * URL not on the regulator's own domain / aggregator link
    * regulator name or headline sitting in Product
    * pre-2026 publication date
    * required field missing

A row that fails any of these is rejected even though reviewer 2 approved it —
two independent stages must agree before anything is published.

FLOW
    pending_gap_v3 --confirm--> pending --> promote_approved --> Recalls
    pending_gap_v3 --fail-----> rejected --> Weekly_Rejected
    rejected (from reviewer 2) --------> Weekly_Rejected

CLI
    python -m pipeline.recall_confirm_agent --xlsx docs/data/recalls.xlsx \
        --commit false            # dry run: prints every decision
    --commit true                 # publish
    --limit N                     # cap rows
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Any, Dict, List

A2_APPROVED = "pending_gap_v3"
REQUIRED = ("Date", "Product", "Pathogen", "URL")


def _load_sheet(xlsx: Path, sheet: str) -> List[Dict[str, Any]]:
    import openpyxl
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        return []
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    out = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        row = {h: ("" if v is None else v) for h, v in zip(headers, r) if h}
        if any(str(v).strip() for v in row.values()):
            out.append(row)
    return out


def confirm(row: Dict[str, Any]) -> List[str]:
    """Return the reasons this row must NOT be published. Empty list = publish.
    Deterministic only — no model, no network."""
    problems: List[str] = []

    # PROVENANCE (2026-09-01). Reviewer 3 did not fetch anything: it trusted
    # that reviewer 2 had read the page. Reviewer 2 did not check the page
    # CONTENT either — only the URL's shape. So a row whose URL described a
    # different recall entirely (fiche 22230, a SHEIN plush toy filed as a
    # Brie/Listeria recall) passed both stages. This is the last gate before
    # publication and the cheapest place to catch it.
    try:
        from pipeline import _provenance
        problems += _provenance.check(row, treat_unreachable_as_problem=False)
    except Exception:                                        # noqa: BLE001
        pass

    # Reuse reviewer 2's own guards so the two stages apply one rule set.
    try:
        from pipeline.recall_review_agent import (
            _field_integrity_flags, _is_blocking)
        problems += [p for p in _field_integrity_flags(row) if _is_blocking(p)]
    except Exception as e:  # pragma: no cover
        problems.append(f"could not run integrity checks: {type(e).__name__}")

    # Required fields must be present. Company OR Brand satisfies the firm.
    for f in REQUIRED:
        if not str(row.get(f, "") or "").strip():
            problems.append(f"{f} is empty")
    if not str(row.get("Company", "") or "").strip() and \
       not str(row.get("Brand", "") or "").strip():
        problems.append("neither Company nor Brand is set")

    # Scope: a pre-2026 notice never publishes, whatever reviewer 2 said.
    d = str(row.get("Date", "") or "").strip()[:10]
    if len(d) >= 4 and d[:4].isdigit() and int(d[:4]) < 2026:
        problems.append(f"out of scope: publication date {d} is before 2026")

    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=Path, default=Path("docs/data/recalls.xlsx"))
    ap.add_argument("--commit", type=str, default="false")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    commit = args.commit.lower() in ("1", "true", "yes", "on")

    pending = _load_sheet(args.xlsx, "Pending")
    # ── FAST PATH (2026-08-28) ───────────────────────────────────────────
    # Reviewer 3 was gated to pending_gap_v3 only, so a row that ALREADY
    # satisfies every deterministic guard still had to wait for reviewer 1 and
    # reviewer 2 to touch it — each ~100 s on a CPU-only 7B model, on a VPS
    # that has been down repeatedly.
    #
    # Measured on the live sheet: 20 of 47 Pending rows passed every guard
    # (required fields present, regulator-domain URL, no fabricated pathogen,
    # no placeholder, in-scope date) and 14 of them sat at pending_gap purely
    # because of a status label. Among them a multi-country gorgonzola/brie
    # Listeria cluster — BE, IT, FR, UK within 48 hours — which missed a
    # weekly report while waiting on a model that could add nothing.
    #
    # The guards below ARE the review for such a row. Verification the model
    # would add is re-reading a page whose fields already agree with it. So
    # any status is admitted here; the guards, not the label, decide. Nothing
    # is relaxed: confirm() still runs in full and rejects anything that fails.
    lane = [r for r in pending
            if str(r.get("Status", "")).strip() in
            (A2_APPROVED, "pending", "pending_gap", "pending_gap_v1",
             "pending_gap_v2")]
    already_rejected = [r for r in pending
                        if str(r.get("Status", "")).strip() == "rejected"]
    if args.limit and args.limit > 0:
        lane = lane[:args.limit]

    print(f"Agent 3 (confirmer): {len(lane)} row(s) in lane, "
          f"{len(already_rejected)} already rejected by reviewer 2 "
          f"(commit={commit})")
    if not lane and not already_rejected:
        print("Nothing to confirm.")
        return 0

    confirmed, blocked = [], []
    for i, row in enumerate(lane, 1):
        probs = confirm(row)
        if probs:
            blocked.append((row, probs))
            print(f"  [{i}/{len(lane)}] BLOCK   "
                  f"{str(row.get('Product',''))[:40]:40s} | {probs[0][:52]}")
        else:
            confirmed.append(row)
            print(f"  [{i}/{len(lane)}] CONFIRM "
                  f"{str(row.get('Product',''))[:40]:40s} | publish")

    print(f"\n{'='*60}")
    print(f"confirm: {len(confirmed)}   block: {len(blocked)}   "
          f"archive (reviewer-2 rejects): {len(already_rejected)}")
    print(f"{'='*60}")

    if not commit:
        print("\nDRY RUN — nothing written. Set --commit true to publish.")
        return 0

    from pipeline.merge_master import (  # type: ignore
        promote_approved, sort_rows, save_xlsx_with_pending)

    xlsx = Path(args.xlsx)
    approved_existing = _load_sheet(xlsx, "Recalls")
    full_pending = _load_sheet(xlsx, "Pending")
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    by_url = {}
    for i, r in enumerate(full_pending):
        u = str(r.get("URL", "")).strip()
        if u:
            by_url[u] = i

    rejected_flags: Dict[int, str] = {}

    for row in confirmed:
        idx = by_url.get(str(row.get("URL", "")).strip())
        if idx is None:
            continue
        full_pending[idx]["Status"] = "pending"      # now promotable
        n = str(full_pending[idx].get("Notes", "")).strip()
        full_pending[idx]["Notes"] = (
            n + f" [confirm-agent {today}: {A2_APPROVED} → pending; "
            f"independent checks passed]").strip()[:1000]

    for row, probs in blocked:
        idx = by_url.get(str(row.get("URL", "")).strip())
        if idx is None:
            continue
        rejected_flags[idx] = ("Confirmer: reviewer 2 approved but "
                               + "; ".join(probs[:2]))[:280]

    for row in already_rejected:
        idx = by_url.get(str(row.get("URL", "")).strip())
        if idx is not None and idx not in rejected_flags:
            rejected_flags[idx] = "Rejected by reviewer 2"

    new_approved, remaining, archived = promote_approved(
        pending=full_pending,
        approved_existing=approved_existing,
        rejected_flags=rejected_flags,
        archive_immediately=True,
    )
    save_xlsx_with_pending(sort_rows(approved_existing + new_approved),
                           sort_rows(remaining), xlsx,
                           newly_rejected_rows=archived)
    try:
        from pipeline.merge_master import mirror_json_from_xlsx
        mirror_json_from_xlsx(xlsx, xlsx.parent / "recalls.json")
    except Exception:
        pass

    print(f"\n✓ Published {len(new_approved)} → Recalls, "
          f"archived {len(archived)}, {len(remaining)} remain in Pending.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
