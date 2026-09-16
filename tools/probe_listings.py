#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ask every live scraper's index URL whether it is actually a listing.

    python3 tools/probe_listings.py                    # all live scrapers
    python3 tools/probe_listings.py --agency "BVL (DE)"
    python3 tools/probe_listings.py --json out.json    # machine-readable
    python3 tools/probe_listings.py --fail-on-broken   # exit 1 for CI

WHY
---
On 2026-09-14 the GIS (PL) scraper was found to have produced zero rows in
eight months without ever reporting an error. Its index URL pointed at
``/web/gis/ostrzezenia-publiczne-dotyczace-zywnosci`` — the singular
*article* slug — while GIS publishes its dated list at
``/web/gis/ostrzezenia``. One word apart.

Every monitor in the repo was blind to it. ``scraper-health.json`` watches
ROWS, so a scraper that has never produced any has no baseline to fall
below; GIS sat at ``SILENT_STALE`` for 102 days and nothing escalated. The
URL audit watches STATUS CODES, and the wrong page returned a cheerful
200. The cost was a Tier-1 botulism recall unseen for seventeen days.

The missing question was never asked: *is the page we are pointing at a
listing of dated entries at all?* That is what this probe asks. It is the
difference between "no recalls this week" and "we have been reading the
wrong page since March", which no existing signal could tell apart.

It is a diagnosis tool, not a fix. It names the scrapers worth an hour
each, in priority order, instead of leaving 57 of them equally suspect.

READ THIS BEFORE BELIEVING A RED LINE
-------------------------------------
A scraper without ``DETAIL_URL_RE`` is judged on raw link and date counts
alone, which is a weak signal — a heavy single article can look like a
listing and a sparse listing can look like an article. Those rows are
reported as ``WEAK``, not as broken. ``BROKEN`` is only claimed where a
pattern is configured and the page contradicts it.

A site behind Cloudflare, a geo-block or a JS-rendered listing will fail
here for reasons that have nothing to do with the URL being wrong. The
HTTP status is printed for exactly that reason.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers import _listing                                # noqa: E402
from scrapers._base import fetch                             # noqa: E402

VERDICT_ORDER = {"BROKEN": 0, "UNREACHABLE": 1, "WEAK": 2, "OK": 3}


def live_scrapers():
    from pipeline.run_all import discover_scrapers
    return discover_scrapers()


def probe_one(scraper, url: str, session) -> dict:
    row = {"agency": scraper.AGENCY, "country": getattr(scraper, "COUNTRY", ""),
           "module": type(scraper).__module__, "url": url,
           "has_pattern": bool(getattr(scraper, "DETAIL_URL_RE", ""))}

    try:
        resp = fetch(session, url)
    except Exception as exc:                                 # noqa: BLE001
        row.update(verdict="UNREACHABLE", status="exception",
                   reason=str(exc)[:160])
        return row

    status = getattr(resp, "status_code", None) if resp is not None else None
    row["status"] = status
    if resp is None or not getattr(resp, "ok", False):
        row.update(verdict="UNREACHABLE",
                   reason="fetch returned %s" % (status or "no response"))
        return row

    pattern = getattr(scraper, "DETAIL_URL_RE", "")
    detail_re = re.compile(pattern, re.I) if pattern else None
    try:
        v = _listing.looks_like_listing(resp.text, url, detail_re)
    except Exception as exc:                                 # noqa: BLE001
        row.update(verdict="UNREACHABLE", reason="parse failed: %s" % exc)
        return row

    row.update(links=v["links"], detail_links=v["detail_links"],
               dated_links=v["dated_links"], dates_on_page=v["dates_on_page"],
               reason=v["reason"])
    if v["is_listing"]:
        row["verdict"] = "OK" if detail_re else "WEAK"
    else:
        row["verdict"] = "BROKEN" if detail_re else "WEAK"
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--agency", action="append", default=[],
                    help="probe only this agency (repeatable)")
    ap.add_argument("--json", metavar="PATH", help="write the full report")
    ap.add_argument("--delay", type=float, default=1.0,
                    help="seconds between requests (default 1.0 — these are "
                         "public regulator sites, do not hammer them)")
    ap.add_argument("--fail-on-broken", action="store_true",
                    help="exit 1 if any scraper is BROKEN (for CI)")
    args = ap.parse_args()

    scrapers = live_scrapers()
    if args.agency:
        want = {a.lower() for a in args.agency}
        scrapers = [s for s in scrapers if s.AGENCY.lower() in want]
    if not scrapers:
        print("no scrapers matched")
        return 2

    rows = []
    for s in scrapers:
        urls = list(getattr(s, "INDEX_URLS", ()) or ())
        if not urls:
            rows.append({"agency": s.AGENCY, "country": getattr(s, "COUNTRY", ""),
                         "module": type(s).__module__, "url": "",
                         "verdict": "OK",
                         "reason": "no INDEX_URLS — hardened scraper, not probed"})
            continue
        for u in urls:
            rows.append(probe_one(s, u, s.session))
            time.sleep(args.delay)

    rows.sort(key=lambda r: (VERDICT_ORDER.get(r.get("verdict"), 9),
                             r.get("agency", "")))

    print("\n%-12s %-22s %-6s %5s %5s %5s  %s" %
          ("VERDICT", "AGENCY", "HTTP", "link", "det", "date", "URL"))
    print("-" * 118)
    for r in rows:
        if not r.get("url"):
            continue
        print("%-12s %-22s %-6s %5s %5s %5s  %s" % (
            r.get("verdict", "?"), r["agency"][:22], r.get("status", "-"),
            r.get("links", "-"), r.get("detail_links", "-"),
            r.get("dated_links", "-"), r["url"][:56]))

    counts = {}
    for r in rows:
        counts[r.get("verdict", "?")] = counts.get(r.get("verdict", "?"), 0) + 1
    print("\n" + "  ".join("%s=%d" % kv for kv in sorted(counts.items())))

    broken = [r for r in rows if r.get("verdict") == "BROKEN"]
    unreachable = [r for r in rows if r.get("verdict") == "UNREACHABLE"]
    if broken:
        print("\nBROKEN — a detail pattern is configured and the page "
              "contradicts it. Fix these first:")
        for r in broken:
            print("  %-22s %s\n      %s" % (r["agency"], r["url"], r["reason"]))
    if unreachable:
        print("\nUNREACHABLE — could be a dead URL, but equally a WAF, a "
              "geo-block, or a JS-rendered page. Check the status:")
        for r in unreachable:
            print("  %-22s %-6s %s" % (r["agency"], r.get("status", "-"), r["url"]))

    print("\nWEAK rows have no DETAIL_URL_RE, so the verdict is a guess from "
          "link and date counts.\nGiving a scraper a pattern turns its guess "
          "into an answer — and turns on the\ndeterministic floor under the "
          "LLM at the same time. See scrapers/europe_eu/gis.py.")

    if args.json:
        Path(args.json).write_text(
            json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
        print("\nwrote %s" % args.json)

    return 1 if (args.fail_on_broken and broken) else 0


if __name__ == "__main__":
    raise SystemExit(main())
