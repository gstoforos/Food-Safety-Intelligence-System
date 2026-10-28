#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Queue an operator-found recall into Pending, through the real code path.

    python3 tools/add_manual_row.py path/to/row.json
    python3 tools/add_manual_row.py <file.json> --dry-run      # show, write nothing

WHY THIS EXISTS
---------------
When a scraper misses something and a person finds it by hand, the row
still has to enter the register the same way every other row does: as a
Pending row, subject to the same dedup, the same validation and the same
two review stages. The alternative — typing it straight into the Recalls
sheet — produces a row with no review trail that every later audit has to
take on faith.

So this tool does not write to Recalls. It builds a ``Recall`` exactly as
a scraper would, hands it to ``merge_master.append_to_pending``, and lets
that function apply its own rules: already approved is skipped, already
pending is skipped, a previously rejected key is replaced for
re-validation. Nothing here bypasses any of it.

A ROW THAT DOES NOT LAND SAYS SO (audit 2026-10-28)
---------------------------------------------------
Until today this tool reported one number: ``len(updated) - len(pending)``.
A net count cannot name a row, so when ``append_to_pending`` dropped one
of several rows the tool printed a success line for the others and exited
0, and the dropped row was gone without a word.

That is not hypothetical. On 2026-10-27 the morning-fix pass queued three
operator-verified rows. Two landed. The third — AESAN alert 2026_75,
Salmonella spp. in Arkan Pure Sesame Tahini, Tier 1 — did not, because
the same script had just archived six language copies of that alert to
Rejected stamped ``operator review 2026-10-27``, and
``_is_human_rejection`` reads any ``operator review`` stamp as a terminal
verdict. The terminal-rejection guard then blocked the replacement row at
the same normalised URL. The guard logged it at INFO, which this tool does
not surface. The run printed "Queued 2 row(s)" and exited 0. Nothing in
the register carried that recall the next morning.

So every requested row now gets its own verdict line, keyed on its own
dedup key, and the exit status is non-zero if ANY requested row did not
land. ``--override-terminal`` is the deliberate way past an archived
terminal verdict, and it names the verdict it overrides.

A worked example lives at tests/fixtures/gis-pesto-2026-08-28.json.
There is no conventional home for these files and they are not inputs the
pipeline reads — write the JSON wherever you like and pass the path.

THE JSON
--------
One object, or a list of them. Required: ``Date`` (YYYY-MM-DD), ``Source``,
``Product``, ``URL``. Everything else optional; ``Pathogen`` is
canonicalised and ``Tier``/``Region`` are computed, so do not hand-set
them::

    {
      "Date": "2026-08-28",
      "Source": "GIS (PL)",
      "Company": "MW FOOD Sp. z o.o.",
      "Brand": "Łowicz",
      "Product": "…",
      "Pathogen": "Clostridium botulinum",
      "Reason": "…",
      "Class": "Voluntary",
      "Country": "Poland",
      "Outbreak": 1,
      "URL": "https://…",
      "Notes": "who found it, how it was verified, against which page"
    }

``Notes`` is not decoration. A hand-added row should say who added it and
what they checked, because the reviewers downstream cannot tell a verified
operator row from a guess.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import merge_master as mm                      # noqa: E402
from scrapers._models import (                               # noqa: E402
    Recall,
    assign_tier,
    normalize_pathogen,
)

try:
    from scrapers._models import infer_region, normalize_country
except ImportError:                                          # older layout
    from scrapers._base import infer_region, normalize_country  # type: ignore

DEFAULT_XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
REQUIRED = ("Date", "Source", "Product", "URL")


def build_recall(spec: dict) -> Recall:
    missing = [k for k in REQUIRED if not str(spec.get(k) or "").strip()]
    if missing:
        raise SystemExit("row is missing required field(s): %s" % ", ".join(missing))

    date = str(spec["Date"]).strip()[:10]
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit("Date must be YYYY-MM-DD, got %r" % spec["Date"])

    country = normalize_country(spec.get("Country") or "") or (spec.get("Country") or "")
    pathogen = normalize_pathogen(spec.get("Pathogen") or "") or (spec.get("Pathogen") or "")
    try:
        outbreak = 1 if int(spec.get("Outbreak") or 0) else 0
    except (TypeError, ValueError):
        outbreak = 0

    return Recall(
        Date=date,
        Source=str(spec["Source"]).strip(),
        Company=str(spec.get("Company") or "").strip(),
        Brand=str(spec.get("Brand") or "—").strip() or "—",
        Product=str(spec["Product"]).strip(),
        Pathogen=pathogen,
        Reason=str(spec.get("Reason") or "").strip(),
        Class=str(spec.get("Class") or "Recall").strip(),
        Country=country,
        Region=infer_region(country) if country else "",
        Tier=assign_tier(pathogen, outbreak),
        Outbreak=outbreak,
        URL=str(spec["URL"]).strip(),
        Notes=str(spec.get("Notes") or "").strip(),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("json_file", help="JSON object or list of objects")
    ap.add_argument("--xlsx", default=str(DEFAULT_XLSX))
    ap.add_argument("--dry-run", action="store_true",
                    help="print what would be queued; write nothing")
    ap.add_argument("--override-terminal", action="store_true",
                    help="queue a row even if its URL carries a terminal "
                         "rejection in the archives. Use it only when you "
                         "have just read the authority page yourself, and "
                         "say so in Notes — the overridden verdict is "
                         "printed and logged.")
    args = ap.parse_args()

    spec = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
    specs = spec if isinstance(spec, list) else [spec]
    recalls = [build_recall(s) for s in specs]

    xlsx = Path(args.xlsx)
    if not xlsx.exists():
        raise SystemExit("workbook not found: %s" % xlsx)

    approved = mm.load_existing(xlsx)
    pending = mm.load_pending(xlsx)
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print("workbook : %s" % xlsx)
    print("approved : %d rows   pending: %d rows" % (len(approved), len(pending)))
    for r in recalls:
        print("\n  queueing")
        for f in ("Date", "Source", "Company", "Brand", "Pathogen",
                  "Tier", "Outbreak", "Country", "URL"):
            print("    %-9s %s" % (f, getattr(r, f, "")))
        print("    %-9s %s" % ("Product", str(r.Product)[:96]))

    overrides = set()
    if args.override_terminal:
        overrides = {mm._normalize_url_for_dedup(r.URL) for r in recalls}

    outcomes: list = []
    updated = mm.append_to_pending(pending, approved, recalls, scraped_at,
                                   outcomes=outcomes,
                                   override_terminal=overrides)

    # One verdict line per requested row. A net row count cannot name the
    # row it lost; see the module docstring for the Tier-1 row that cost.
    #
    # THREE OUTCOMES, NOT TWO. "The register already has this row" is a
    # legitimate no-op — re-running the tool on a row that has since been
    # published must stay a success, which is what
    # tests/test_add_manual_row.py::test_running_twice_does_not_duplicate
    # has always required. What must NOT stay a success is a row dropped
    # because something refused it: a gate defect, a near-duplicate, a
    # terminal rejection on file. Those are the ones that vanish.
    LANDED = {"queued", "queued-enrichment", "requeued"}
    BENIGN = {"already-approved", "already-pending"}
    #: A gate reason that means "the register already holds this URL" rather
    #: than "this row is defective".
    DUP_REASONS = ("duplicate url", "already exists", "already in recalls")

    def _bucket(o) -> str:
        if o["outcome"] in LANDED:
            return "landed"
        if o["outcome"] in BENIGN:
            return "benign"
        if (o["outcome"] == "gate-rejected"
                and any(m in str(o.get("detail") or "").lower()
                        for m in DUP_REASONS)):
            return "benign"
        return "lost"

    print("\nper-row result")
    by_key = {o["key"]: o for o in outcomes}
    lost: list = []
    landed = 0
    for r in recalls:
        d = r.to_dict()
        o = by_key.get(mm._dedup_key(d))
        if o is None:                                    # pragma: no cover
            o = {"outcome": "NOT REPORTED", "key": "",
                 "detail": "append_to_pending returned no verdict for this row"}
        bucket = _bucket(o)
        mark = {"landed": "  OK  ", "benign": " SKIP ", "lost": " LOST "}[bucket]
        print("  [%s] %-20s %s" % (mark, o["outcome"], str(r.Product)[:60]))
        print("           %s" % r.URL)
        if o.get("detail"):
            print("           %s" % str(o["detail"])[:220])
        if bucket == "landed":
            landed += 1
        elif bucket == "lost":
            lost.append((r, o))

    if args.dry_run:
        print("\nDRY RUN — %d of %d row(s) would be queued. Nothing written."
              % (landed, len(recalls)))
    elif landed:
        mm.save_xlsx_with_pending(approved, updated, xlsx)
        print("\nQueued %d of %d row(s) to Pending in %s"
              % (landed, len(recalls), xlsx))
        print("They are Status='pending' and go through review like any "
              "other row.")
    elif lost:
        print("\nNothing queued — every requested row was dropped. "
              "Workbook not written.")
    else:
        print("\nNothing queued — the register already holds every requested "
              "row. Workbook not written.")

    if lost:
        print("\n%d REQUESTED ROW(S) DID NOT LAND:" % len(lost))
        for r, o in lost:
            print("  - %s  (%s)" % (str(r.Product)[:70], o["outcome"]))
        if any(o["outcome"] == "terminal-rejection" for _, o in lost):
            print("\n  A terminal-rejection drop is deliberate: the archives "
                  "carry a permanent verdict\n  on that URL. If you have read "
                  "the authority page yourself and the verdict is wrong\n  or "
                  "was only bookkeeping, re-run with --override-terminal and "
                  "say so in Notes.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
