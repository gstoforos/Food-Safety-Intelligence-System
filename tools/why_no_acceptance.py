#!/usr/bin/env python3
"""Why is nothing reaching Recalls?

Answers one question offline, in about a second, with no model and no
network: for every row sitting in Pending, WHICH stage is holding it, and
is that stage's workflow actually running?

WHY THIS EXISTS (2026-08-18)
============================
Between 16 and 18 August the register gained TWO rows while Pending grew
to 24. Nothing was red. Every workflow that scrapes, enriches, validates
and reviews ran on schedule and committed. `merge-master` reported
"+0 approved" on all fifteen runs and that is its correct, documented
behaviour — the hourly CLI is a janitor and has never been allowed to
publish.

The register has two publication paths and both end at a workflow that
has never once appeared in the commit history:

    url-gate -> claude-check -> merge-master        claude-check:  0 runs
    review-agent (banks pending_gap_v3) -> confirm  confirm-agent: 0 runs

There is no alarm for "no row has been published in N days", so this was
invisible for three days while every dashboard stayed green.

Run this after any quiet spell:

    python -m tools.why_no_acceptance
    python -m tools.why_no_acceptance --days 3      # non-zero exit if stalled
"""
from __future__ import annotations

import argparse
import collections
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Status -> the stage that owns the row and must move it on.
OWNER = {
    "pending":            ("reviewer 2 (recall-review-agent) or claude-check",
                           "approve -> pending_gap_v3, or promote directly"),
    "pending_enrichment": ("reviewer 1 (gemini-url-gate) / claude-check",
                           "fill Pathogen, then flip to 'pending'"),
    "pending_gap":        ("reviewer 1 (gemini-url-gate)",
                           "first URL grounding pass -> pending_gap_v1"),
    "pending_gap_v1":     ("reviewer 1 (gemini-url-gate)",
                           "second URL grounding pass -> pending_gap_v2"),
    "pending_gap_v2":     ("reviewer 2 (recall-review-agent)",
                           "content verification -> pending_gap_v3"),
    "pending_gap_v3":     ("reviewer 3 (recall-confirm-agent)",
                           "confirm -> 'pending' -> promote to Recalls"),
    "rejected":           ("archive",
                           "should already be in Weekly_Rejected"),
}

# The only workflows permitted to write a row into Recalls.
PUBLISHERS = ("claude-check", "openrouter-check", "recall-confirm-agent",
              "recall-review-agent")


def _git_log(n=400):
    try:
        out = subprocess.run(
            ["git", "log", f"-{n}", "--format=%ad|%s",
             "--date=format:%Y-%m-%d %H:%M"],
            cwd=ROOT, capture_output=True, text=True, timeout=30)
        return [l for l in out.stdout.splitlines() if l.strip()]
    except Exception as exc:                                  # noqa: BLE001
        print(f"  (git history unavailable: {exc})")
        return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0,
                    help="exit non-zero if Recalls has not grown in N days")
    args = ap.parse_args()

    from pipeline.merge_master import load_existing, load_pending
    from pipeline._publish_gate import publish_blockers

    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    approved = load_existing(xlsx)
    pending = load_pending(xlsx)

    # ── 1. Throughput ────────────────────────────────────────────────
    added = collections.Counter(str(r.get("DateAdded") or "")[:10]
                                for r in approved)
    today = datetime.now(timezone.utc).date()
    print(f"Recalls {len(approved)} | Pending {len(pending)}\n")
    print("rows published per day (last 14):")
    stalled_days = 0
    for i in range(13, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        n = added.get(d, 0)
        print(f"   {d}  {'#' * min(n, 60)}{'' if n else ' —'}  {n}")
    for i in range(0, 60):
        if added.get((today - timedelta(days=i)).isoformat(), 0):
            break
        stalled_days += 1
    print(f"\n   last publication: {stalled_days} day(s) ago")

    # ── 2. Who is holding each Pending row ───────────────────────────
    print("\nPending rows by owning stage:")
    by_status = collections.Counter(str(p.get("Status") or "(blank)")
                                    for p in pending)
    for st, n in by_status.most_common():
        who, what = OWNER.get(st, ("UNKNOWN STAGE", "?"))
        print(f"   {n:3}  {st:20} -> {who}")
        print(f"        {what}")

    # ── 3. Would the deterministic gate let them through? ────────────
    clean = [p for p in pending if not publish_blockers(p)]
    print(f"\n{len(clean)} of {len(pending)} Pending rows pass the "
          f"deterministic publish gate — i.e. the ONLY thing between them "
          f"and Recalls is a reviewer stage.")
    for p in clean:
        print(f"   {str(p.get('Date'))[:10]:10} "
              f"{str(p.get('Status') or ''):18} "
              f"{str(p.get('Source'))[:16]:16} "
              f"{str(p.get('Company') or '—')[:34]}")

    # ── 4. Are the publishing workflows running at all? ──────────────
    print("\nworkflows permitted to write to Recalls:")
    log = _git_log()
    joined = "\n".join(log).lower()
    dead = []
    for wf in PUBLISHERS:
        # Match on the workflow's own commit-message signature.
        needle = wf.replace("-", " ").replace("recall review agent",
                                              "recall review agent")
        hits = sum(1 for l in log if needle in l.lower()
                   or wf.replace("-", "") in l.lower().replace("-", "")
                   or wf.replace("-", " ") in l.lower())
        state = "runs" if hits else "*** NEVER COMMITTED ***"
        if not hits:
            dead.append(wf)
        print(f"   {wf:24} {hits:3} commit(s) in the last {len(log)}  {state}")

    if dead:
        print(f"\n   {len(dead)} publishing workflow(s) have never run: "
              f"{', '.join(dead)}")
        print("   These are workflow_dispatch-only and are fired by the "
              "external Apps Script scheduler (FsisScheduler.gs), which is "
              "not in this repo and has no watchdog. If a workflow is "
              "missing from that script, it never runs and nothing here "
              "goes red.")

    if args.days and stalled_days >= args.days:
        print(f"\nSTALLED: no row published in {stalled_days} day(s) "
              f"(threshold {args.days}).")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
