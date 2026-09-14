#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Queue an operator-found recall into Pending, through the real code path.

    python3 tools/add_manual_row.py rows/gis-pesto-2026-08-28.json
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

    updated = mm.append_to_pending(pending, approved, recalls, scraped_at)
    added = len(updated) - len(pending)

    if added <= 0:
        print("\nNothing queued — append_to_pending skipped every row "
              "(already approved, or already waiting in Pending).")
        return 0

    if args.dry_run:
        print("\nDRY RUN — %d row(s) would be queued. Nothing written." % added)
        return 0

    mm.save_xlsx_with_pending(approved, updated, xlsx)
    print("\nQueued %d row(s) to Pending in %s" % (added, xlsx))
    print("They are Status='pending' and go through review like any other row.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
