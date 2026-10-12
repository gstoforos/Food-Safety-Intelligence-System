# -*- coding: utf-8 -*-
"""Operator pass 2026-10-12 (b): sentinel-hazard lane on the signals board.

NOTHING IS REMOVED from Recalls. No register row is touched.

Operator 2026-10-12: "yes do it — C. bot is deadly". The week-41 board
showed one count-only alarm (Salmonella · Europe) and nothing for the two
Clostridium botulinum notifications of the week (BLV (CH) pork shank
terrine, 2 cases in Valais; RASFF honey from Spain). The detector cannot see
them by design: 2 records is below MIN_ABSOLUTE_COUNT = 4 and C. botulinum's
guarded baseline is too thin to score.

pipeline/signal_review_agent.py now carries a SENTINEL channel: every
record of a hazard in SENTINEL_HAZARDS (C. botulinum) and every Outbreak = 1
record is listed at one record, with the regulator's URL, no statistic.
docs/index.html (Signals tab) and docs/signals.html render it first.

Operator 2026-10-12, second instruction: "Flag them" — botulism-risk
indicators are flagged in the same lane: the regulator's own wording of
container swelling (swollen / bulging / blown / bombage / gonflement) or of a
sterilization / thermal-process failure. Week 41: RappelConso canned king
crab (CHATKA, Auchan), "risk of swelling". Across the whole register the rule
matches two records (that one and CFS (HK) 2026-03-09, "Inadequate
sterilization process"). It is labelled an indicator, never a confirmed
C. botulinum finding.

This pass adds the sentinel block to the ALREADY-WRITTEN week-41 review
(docs/data/signals-review.json) without re-running the whole agent, so the
append-only signals-review.jsonl and news-pulse.jsonl are not given a
second week-41 line. It then refreshes the inline snapshot in signals.html.
From the next Monday run the agent writes the block itself.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
REVIEW = ROOT / "docs" / "data" / "signals-review.json"
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def main() -> int:
    import pandas as pd
    from pipeline import signal_detector as S
    from pipeline import signal_review_agent as R

    if not REVIEW.exists():
        print("no signals-review.json — nothing to add to")
        return 0
    rv = json.loads(REVIEW.read_text(encoding="utf-8-sig"))
    week = rv.get("meta", {}).get("week")
    corpus = S.load_corpus(str(XLSX))
    wk = next((w for w in corpus.weeks if str(w) == week), None)
    if wk is None:
        print(f"review week {week} not in corpus — left alone")
        return 0
    sn = R.sentinel(corpus, wk)
    if rv.get("sentinel") == sn:
        print("sentinel block already current")
    else:
        rv["sentinel"] = sn
        ch = rv["meta"].get("channels") or []
        if "sentinel" not in ch:
            rv["meta"]["channels"] = ["sentinel"] + ch
        REVIEW.write_text(json.dumps(rv, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"sentinel added to the {week} review: {sn['n']} record(s) · "
              f"{sn['by_hazard']} · outbreak-flagged {sn['n_outbreak']}")
    from pipeline import build_signals_page
    build_signals_page.main()
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
