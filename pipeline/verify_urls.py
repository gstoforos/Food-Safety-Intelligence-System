#!/usr/bin/env python3
"""URL verification for the Recalls sheet — structural offline, live in CI.

WHY THIS EXISTS (audit 2026-08-02)
==================================
The daily accuracy brief of 2026-08-02 ended with:

    "URLs: all well-formed, but reachability could not be verified —
     WebFetch requires interactive approval, which isn't available in a
     scheduled/unattended run."

That is an honest limitation and a permanent one: an unattended reviewer has
no interactive fetch, so "did I check the URLs" resolves to "no" on every
scheduled run. A dimension that can never be checked is a dimension that is
not being checked.

Almost none of it actually needs a fetch. The failure modes this database has
really suffered are structural, and every one is visible offline:

  · A row pointing at the WRONG REGULATOR. A gap-finder hands back a plausible
    URL on a different agency's host and the row is published citing a source
    that never mentioned it. Caught by HOST_FOR_SOURCE.
  · A RASFF URL built from the REFERENCE NUMBER instead of the notification
    id — .../screen/notification/2026.4017 rather than .../839115. Six such
    rows are published. The reference number is not a route; those links do
    not resolve.
  · A RASFF URL whose id disagrees with the notifId recorded in the row's own
    Notes — the row and its provenance pointing at two different alerts.
  · A LANDING PAGE instead of a notice (already covered by _publish_gate, and
    re-checked here so one command answers the whole question).
  · An empty or relative URL, a duplicate URL across two rows, or http://
    where the regulator serves https://.

So this runs in two layers:

    --structural   (default)  offline, zero network, safe unattended
    --live                    adds a HEAD/GET reachability check

Use --structural on every scheduled run so the brief can state a real result
instead of "could not verify", and --live in the daily GitHub Actions job,
where outbound HTTPS is available and no approval is needed.

Exit code is 1 when any structural problem is found, so CI can gate on it.

Usage
-----
    python -m pipeline.verify_urls                     # structural, all rows
    python -m pipeline.verify_urls --since 2026-08-01  # today's promotions
    python -m pipeline.verify_urls --live --since 2026-08-01
"""
from __future__ import annotations

import argparse
import collections
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

UA = "AFTS-FSIS/1.0 (+https://fsis.advfood.tech) link-check"

# The host a given Source must cite. A row from RappelConso pointing anywhere
# other than rappel.conso.gouv.fr is either mis-sourced or hallucinated.
# Sources absent from this map are not checked — better silent than wrong.
HOST_FOR_SOURCE: Dict[str, tuple] = {
    "RappelConso (FR)":         ("rappel.conso.gouv.fr",),
    "RASFF (EU)":               ("webgate.ec.europa.eu",),
    "FSANZ (AU)":               ("foodstandards.gov.au",),
    "AESAN (ES)":               ("aesan.gob.es",),
    # recallswiss.admin.ch is the BLV's own public recall portal — the
    # map listed only blv.admin.ch, so every Swiss recall row failed the
    # host check against the regulator that actually published it
    # (audit 2026-08-13). The portal routes by URL fragment
    # (#Recalls/<id>), which no server-side fetch can resolve; that is a
    # citation weakness worth noting in a row's Notes, not grounds for
    # calling the host wrong.
    "BLV (CH)":                 ("blv.admin.ch", "recallswiss.admin.ch"),
    "BVL (DE)":                 ("bvl.bund.de", "lebensmittelwarnung.de"),
    "FSAI (IE)":                ("fsai.ie",),
    "FSA (UK)":                 ("food.gov.uk", "data.food.gov.uk"),
    "FAVV (BE)":                ("favv-afsca.be", "afsca.be", "favv.be"),
    "EFET (GR)":                ("efet.gr",),
    "NCC (ZA)":                 ("thencc.org.za",),
    "USDA FSIS":                ("fsis.usda.gov",),
    "FDA":                      ("fda.gov", "accessdata.fda.gov"),
    "CFIA":                     ("recalls-rappels.canada.ca", "inspection.canada.ca"),
    "CDC":                      ("cdc.gov",),
    "Ministero della Salute":   ("salute.gov.it",),
    "CFS (HK)":                 ("cfs.gov.hk",),
    "MPI (NZ)":                 ("mpi.govt.nz",),
}

# A RASFF notification id is a plain integer. The 2026.NNNN form is the
# notification REFERENCE, which is not addressable as a path segment.
_RASFF_PATH = re.compile(r"/rasff-window/screen/notification/([^/?#]+)")
_RASFF_REF_IN_NOTES = re.compile(r"notifId=(\d+)")


def _host(url: str) -> str:
    """Hostname, lowercased, with a leading 'www.' removed.

    NOT str.lstrip("www.") — that strips any leading run of the CHARACTERS
    {w, .}, so "webgate.ec.europa.eu" becomes "ebgate.ec.europa.eu" and every
    RASFF row in the workbook is reported as citing the wrong regulator. The
    first run of this checker did exactly that: 444 false positives out of
    1305 rows. A checker that cries wolf gets switched off, so the prefix is
    removed explicitly.
    """
    tail = str(url or "").split("://", 1)[-1]
    host = tail.split("/", 1)[0].split("?", 1)[0].lower()
    return host[4:] if host.startswith("www.") else host


def structural_findings(row: Dict[str, Any]) -> List[str]:
    """Every URL problem visible without touching the network."""
    out: List[str] = []
    url = str(row.get("URL") or "").strip()
    source = str(row.get("Source") or "").strip()

    if not url:
        return ["URL is empty"]
    if not url.startswith(("http://", "https://")):
        out.append(f"URL is not absolute ({url[:60]!r})")
        return out
    if url.startswith("http://") and "data.food.gov.uk" not in url:
        # FSA's linked-data IDs are genuinely http:// identifiers, not links.
        out.append("URL uses http:// where the regulator serves https://")

    allowed = HOST_FOR_SOURCE.get(source)
    if allowed:
        host = _host(url)
        if not any(host == a or host.endswith("." + a) or a in host
                   for a in allowed):
            out.append(
                f"host {host!r} does not belong to Source {source!r} "
                f"(expected one of {list(allowed)}) — the row cites a "
                f"regulator that did not publish it")

    m = _RASFF_PATH.search(url)
    if m:
        ident = m.group(1)
        if not ident.isdigit():
            out.append(
                f"RASFF path segment {ident!r} is a notification REFERENCE, "
                f"not a notification id — this address does not resolve; the "
                f"numeric notifId is what the RASFF Window routes on")
        else:
            noted = _RASFF_REF_IN_NOTES.search(str(row.get("Notes") or ""))
            if noted and noted.group(1) != ident:
                out.append(
                    f"RASFF id mismatch: URL says {ident}, the row's own "
                    f"Notes say notifId={noted.group(1)}")

    try:
        from pipeline._publish_gate import publish_blockers
        for blocker in publish_blockers(row):
            if blocker.startswith("URL is a regulator landing page"):
                out.append(blocker)
    except ImportError:                            # pragma: no cover
        pass
    return out


def live_status(url: str, timeout: int = 20) -> str:
    """'200', '404', 'timeout', … — never raises."""
    req = urllib.request.Request(url, method="HEAD",
                                 headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return str(resp.status)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 405, 501):
            # Plenty of regulators refuse HEAD. Retry as a GET before
            # reporting a link dead — a false "dead URL" costs more than the
            # extra request.
            try:
                req = urllib.request.Request(url,
                                             headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return str(resp.status)
            except Exception as exc2:
                return f"{type(exc2).__name__}"
        return str(exc.code)
    except urllib.error.URLError as exc:
        return f"unreachable ({getattr(exc, 'reason', '')})"[:60]
    except Exception as exc:                       # pragma: no cover
        return type(exc).__name__


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--sheet", default="Recalls")
    ap.add_argument("--since", default=None,
                    help="only rows with DateAdded >= YYYY-MM-DD")
    ap.add_argument("--live", action="store_true",
                    help="also check reachability (needs outbound HTTPS)")
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    import openpyxl
    wb = openpyxl.load_workbook(args.xlsx, read_only=True)
    rows = list(wb[args.sheet].values)
    hdr = [str(h) for h in rows[0]]
    data = [dict(zip(hdr, r)) for r in rows[1:] if r]
    if args.since:
        data = [r for r in data if str(r.get("DateAdded") or "") >= args.since]
    if args.limit:
        data = data[:args.limit]

    print(f"Checking {len(data)} row(s) in {args.sheet}"
          f"{' since ' + args.since if args.since else ''}\n")

    flagged = 0
    for row in data:
        problems = structural_findings(row)
        if problems:
            flagged += 1
            print(f"{row.get('Date')}  {str(row.get('Source'))[:18]:20} "
                  f"{str(row.get('Company'))[:34]}")
            for p in problems:
                print(f"    ! {p}")
            print(f"    {str(row.get('URL'))[:100]}")

    dupes = [u for u, n in collections.Counter(
        str(r.get("URL") or "").strip().lower() for r in data).items()
        if n > 1 and u]
    if dupes:
        flagged += len(dupes)
        print(f"\n{len(dupes)} duplicate URL(s):")
        for u in dupes[:10]:
            print(f"    {u[:100]}")

    print(f"\nstructural problems : {flagged}")

    if args.live:
        print("\nreachability:")
        dead = 0
        for row in data:
            url = str(row.get("URL") or "").strip()
            if not url.startswith(("http://", "https://")):
                continue
            status = live_status(url)
            time.sleep(args.delay)
            if status != "200":
                dead += 1
                print(f"    {status:22} {url[:96]}")
        print(f"    non-200 : {dead} of {len(data)}")
    else:
        print("\n(reachability not checked — pass --live in CI, where "
              "outbound HTTPS needs no approval. Structural checks above are "
              "complete and need no network.)")

    return 1 if flagged else 0


if __name__ == "__main__":
    raise SystemExit(main())
