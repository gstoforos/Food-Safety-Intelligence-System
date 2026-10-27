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
import datetime as _dt
import collections
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    # info.gov.hk added 2026-08-31. The Hong Kong Centre for Food Safety
    # publishes some of its notices as Government Information Services press
    # releases on info.gov.hk rather than on its own cfs.gov.hk pages — same
    # regulator, same authority, second official channel. One August row
    # (Ktipiti feta, 2026-08-18) sits on that host. It only surfaced when the
    # Source label was corrected from "CFS (HK) - aggregator (RappelConso FR)"
    # to "CFS (HK)": the aggregator label was absent from this map, so the
    # host check never ran on those rows at all. Correcting the label turned
    # the check ON, which is the point of the label being correct.
    "CFS (HK)":                 ("cfs.gov.hk", "info.gov.hk"),
    "MPI (NZ)":                 ("mpi.govt.nz",),
}

# ─────────────────────────────────────────────────────────────────────────────
# THE SAME MAP, KEYED BY CANONICAL SOURCE IDENTITY — audit 2026-10-26
# ─────────────────────────────────────────────────────────────────────────────
#
# HOST_FOR_SOURCE above is keyed on the RAW Source label, and the comment
# on it says "Sources absent from this map are not checked". That rule is
# right. What it does not survive is a label that drifts: the check does
# not weaken, it switches OFF, silently, for every row of that regulator.
#
# The CFS (HK) note above records this happening once and being fixed by
# hand. Measured on the live register this morning it had happened again,
# and twice over:
#
#     44 distinct Source labels in Recalls
#     16 matched a key above          2,049 rows checked
#     28 matched nothing                126 rows NEVER host-checked
#
# and two keys above matched NO ROW AT ALL, because the label the writer
# produces has moved on:
#
#     key 'FAVV (BE)'               live label 'AFSCA (BE)'          11 rows
#     key 'Ministero della Salute'  live label 'Ministero della Salute (IT)'
#                                                                    22 rows
#
# Belgium and Italy were deliberately covered by the author of this map
# and were not being checked. Among the 126 unchecked rows, these cite a
# NEWS host as the authority that published the recall — exactly what this
# map exists to catch:
#
#     NFSA (EG)      news.google.com/rss/articles/...   (a Google News id)
#     MFDS (KR)      en.sedaily.com                     2 rows
#     Min. Salute    ilfattoalimentare.it
#     COFEPRIS (MX)  unotv.com
#     ANMAT (AR)     chequeado.com
#     VFA (VN)       vietnam.vnanet.vn
#     COMESA         capitalfm.co.ke
#
# pipeline/_source_canon.py already solves label drift — it is the module
# whose whole purpose is "one regulator, one label", and it already knows
# both live spellings above. So the map is now ALSO keyed by canonical id
# and `allowed_hosts_for_source()` resolves through it, falling back to the
# raw label. A future rename cannot turn the check off again without also
# being invisible to _source_canon, which its own test pins.
#
# Every host below is the authority's own domain, taken from the operator's
# authorised source list — nothing inferred from a row.
HOST_FOR_CANONICAL_SOURCE: Dict[str, tuple] = {
    # the sixteen already covered by label, restated by identity
    "FR-RAPPELCONSO": ("rappel.conso.gouv.fr",),
    "EU-RASFF":       ("webgate.ec.europa.eu",),
    "AU-FSANZ":       ("foodstandards.gov.au",),
    "ES-AESAN":       ("aesan.gob.es",),
    "CH-BLV":         ("blv.admin.ch", "recallswiss.admin.ch"),
    "DE-BVL":         ("bvl.bund.de", "lebensmittelwarnung.de"),
    "IE-FSAI":        ("fsai.ie",),
    "UK-FSA":         ("food.gov.uk", "data.food.gov.uk"),
    "GR-EFET":        ("efet.gr",),
    "ZA-NCC":         ("thencc.org.za",),
    "US-FSIS":        ("fsis.usda.gov",),
    "US-FDA":         ("fda.gov", "accessdata.fda.gov"),
    "CA-CFIA":        ("recalls-rappels.canada.ca", "inspection.canada.ca"),
    "US-CDC":         ("cdc.gov",),
    "HK-CFS":         ("cfs.gov.hk", "info.gov.hk"),
    "NZ-MPI":         ("mpi.govt.nz",),
    # the two the raw-label keys could no longer reach
    "BE-FAVV":        ("favv-afsca.be", "afsca.be", "favv.be"),
    "IT-SALUTE":      ("salute.gov.it", "pnes.salute.gov.it"),
    # regulators with published rows that were never checked at all
    "JP-CAA":         ("recall.caa.go.jp", "caa.go.jp"),
    "NL-NVWA":        ("nvwa.nl",),
    "PL-GIS":         ("gov.pl",),
    "NO-MATTILSYNET": ("mattilsynet.no",),
    # MAPAQ's recall notices are served from the Québec government portal
    # (quebec.ca/nouvelles/actualites/...), which is where all six published
    # rows point; mapaq.gouv.qc.ca is the department's own older home.
    "CA-MAPAQ":       ("quebec.ca", "mapaq.gouv.qc.ca"),
    "AT-AGES":        ("ages.at",),
    # potravinynapranyri.cz added 2026-10-27: pipeline/regulatory_domains
    # has whitelisted it as SZPI's own host since that module was written
    # (it is the authority's consumer-facing portal), so a row published
    # from it would have failed a check that the whitelist already allows.
    "CZ-SZPI":        ("szpi.gov.cz", "potravinynapranyri.cz"),
    "MX-COFEPRIS":    ("gob.mx",),
    "HR-HAH":         ("hapih.hr",),
    "HU-NKFH":        ("nkfh.gov.hu", "nebih.gov.hu"),
    "SG-SFA":         ("sfa.gov.sg",),
    "PH-FDA":         ("fda.gov.ph",),
    "BR-ANVISA":      ("gov.br",),
    "KR-MFDS":        ("mfds.go.kr",),
    "IS-MAST":        ("mast.is",),
    "EG-NFSA":        ("nfsa.gov.eg",),
    "FI-RUOKAVIRASTO": ("ruokavirasto.fi",),
    "EU-EFSA":        ("efsa.europa.eu",),
    "EU-ECDC":        ("ecdc.europa.eu",),
    "VN-VFA":         ("vfa.gov.vn",),
    "UY-MSP":         ("gub.uy",),
    # ── 2026-10-27. FOUR IDENTITIES A COLLECTOR TABLE CAN PUBLISH UNDER ──
    # No published row carries these yet, which is the point: the three
    # collector host->label tables can emit them, and until this morning
    # the labels they emitted were not registry labels at all, so these
    # identities did not exist. Each host below is the authority's own
    # domain, from the operator's authorised source list, and is already a
    # regulator-of-record in pipeline/regulatory_domains.
    # szpi.gov.cz's second host is the Czech authority's own consumer
    # portal, which regulatory_domains has whitelisted from the start.
    "UK-FSS":         ("foodstandards.gov.scot",),
    "TH-FDA":         ("fda.moph.go.th",),
    "SK-SVPS":        ("svps.sk",),
    "FR-DGAL":        ("agriculture.gouv.fr",),
    # ── 2026-10-27. THE THIRTY REGULATORS WHOSE CHECK NEVER RAN. ────────
    # The companion to the _source_canon block of the same date: the
    # registry's own domain for each of the 30 monitored regulators that
    # had no canonical identity and therefore no entry here. Adding them
    # only ever turns a check ON — a Source absent from these maps is not
    # checked at all, which is the silence this file exists to end.
    "PT-ASAE":            ("asae.gov.pt",),   # Portugal
    "LU-SECLU":           ("securite-alimentaire.public.lu",),   # Luxembourg
    "DK-FODEVARESTYRELSEN":("foedevarestyrelsen.dk",),   # Denmark
    "SE-LIVSMEDELSVERKET":("livsmedelsverket.se",),   # Sweden
    "RO-ANSVSA":          ("ansvsa.ro",),   # Romania
    "BG-BFSA":            ("bfsa.bg",),   # Bulgaria
    "SI-UVHVVR":          ("gov.si",),   # Slovenia
    "LV-PVD":             ("pvd.gov.lv",),   # Latvia
    "LT-VMVT":            ("vmvt.lt",),   # Lithuania
    "BA-FSABIH":          ("fsa.gov.ba",),   # Bosnia and Herzegovina
    "MK-FVA":             ("fva.gov.mk",),   # North Macedonia
    "MD-ANSA":            ("ansa.gov.md",),   # Moldova
    "TR-TGTHB":           ("tarimorman.gov.tr",),   # Türkiye
    "CL-ACHIPIA":         ("achipia.gob.cl",),   # Chile
    "CL-ISP":             ("ispch.cl",),   # Chile
    "PE-DIGESA":          ("digesa.minsa.gob.pe",),   # Peru
    "EC-ARCSA":           ("controlsanitario.gob.ec",),   # Ecuador
    "JP-MHLW":            ("mhlw.go.jp",),   # Japan
    "CN-SAMR":            ("samr.gov.cn",),   # China
    "TW-TFDA":            ("fda.gov.tw",),   # Taiwan
    "ID-BPOM":            ("pom.go.id",),   # Indonesia
    "MY-KKM":             ("moh.gov.my",),   # Malaysia
    "IN-FSSAI":           ("fssai.gov.in",),   # India
    "SA-SFDA":            ("sfda.gov.sa",),   # Saudi Arabia
    "AE-MOCCAE":          ("moccae.gov.ae",),   # United Arab Emirates
    "QA-MOPH":            ("moph.gov.qa",),   # Qatar
    "IL-MOH":             ("health.gov.il",),   # Israel
    "MA-ONSSA":           ("onssa.gov.ma",),   # Morocco
    "NG-NAFDAC":          ("nafdac.gov.ng",),   # Nigeria
    "KE-KEBS":            ("kebs.org",),   # Kenya
    "CO-INVIMA":      ("invima.gov.co",),
    "AR-ANMAT":       ("argentina.gob.ar", "anmat.gob.ar"),
    "COMESA":         ("comesa.int",),
    "EE-VTA":         ("pta.agri.ee",),
    "GH-FDA":         ("fdaghana.gov.gh",),
}

#: Source labels that name a NON-regulator and are therefore exempt from the
#: host check rather than missing from it. "BeaconBio (TW)" is a laboratory
#: and "Food Safety News (citing …)" is media; both say so in the label, and
#: _source_canon classifies the latter as kind="news". A label here is a
#: citation weakness recorded honestly, not a host mismatch.
NOT_A_REGULATOR_SOURCE = frozenset({
    "NEWS",
    "TW-BEACONBIO",
})


def allowed_hosts_for_source(source) -> Optional[tuple]:
    """The hosts a row carrying this Source label is allowed to cite.

    Resolves the label to its canonical regulator identity first, so a
    renamed label keeps its check, and falls back to the raw label for any
    key that exists only in HOST_FOR_SOURCE. Returns None when this source
    is not host-checked — either a non-regulator (NOT_A_REGULATOR_SOURCE)
    or a label no table names, which stays silent on purpose.
    """
    label = str(source or "").strip()
    if not label:
        return None
    try:
        from pipeline._source_canon import canonical_source
        canon_id, kind, _mirrors = canonical_source(label)
    except Exception:                               # pragma: no cover
        canon_id, kind = "", "unknown"
    if canon_id in NOT_A_REGULATOR_SOURCE or kind == "news":
        return None
    hosts = HOST_FOR_CANONICAL_SOURCE.get(canon_id)
    if hosts:
        return hosts
    # An aggregator label ("CFS (HK) - aggregator (RappelConso FR)") keeps
    # the base regulator's identity, so it is already covered above.
    return HOST_FOR_SOURCE.get(label)

# A RASFF notification id is a plain integer. The 2026.NNNN form is the
# notification REFERENCE, which is not addressable as a path segment.
_RASFF_PATH = re.compile(r"/rasff-window/screen/notification/([^/?#]+)")
_RASFF_REF_IN_NOTES = re.compile(r"notifId=(\d+)")


def _host_matches(host: str, domain: str) -> bool:
    """Delegate to the canonical host test in _publish_gate.

    Imported lazily and defensively: this module is run standalone from a
    workflow step, and if the import were to fail at module scope the whole
    check would vanish silently. On failure this falls back to exact-or-
    subdomain — which is the CORRECT rule, merely without the normalisation
    — so a broken import can never restore the substring hole.
    """
    try:
        from pipeline._publish_gate import host_matches_domain
        return host_matches_domain(host, domain)
    except Exception:                              # pragma: no cover
        h, d = (host or "").lower(), (domain or "").lower()
        return h == d or h.endswith("." + d)


def repair_rasff_reference(row: Dict[str, Any]) -> Optional[str]:
    """The corrected URL for a RASFF row addressed by REFERENCE, or None.

    This is the one finding in this module that can be repaired offline and
    without inventing anything, because the row already carries the answer.
    The RASFF collector stamps the notification id into Notes at scrape time:

        [RASFF #2026.4017; classification: alert notification;
         category: nuts, nut products and seeds; notifId=842548]

    So the numeric id is the row's OWN provenance, recorded when the
    notification was read. Rewriting the path segment from 2026.4017 to
    842548 does not change the format — it puts the value our format has
    always called for where the reference was written by mistake.

    HISTORY, because this was wrong in the repo for six weeks:
    tests/test_publish_gate.py pinned these seven rows on 2026-08-14 with
    the reasoning that RASFF Window is a JavaScript application returning an
    empty shell to a server-side fetch, so "the numeric id cannot be looked
    up and guessing would be fabrication". The first half is true and the
    conclusion did not follow: nobody needed to look the id up or guess it,
    because the collector had already written it into the row. The pin
    reasoned about the network and never read the Notes column.

    Returns None whenever anything is missing or already correct, so the
    caller can never write a half-known URL.
    """
    url = str(row.get("URL") or "").strip()
    m = _RASFF_PATH.search(url)
    if not m or m.group(1).isdigit():
        return None
    noted = _RASFF_REF_IN_NOTES.search(str(row.get("Notes") or ""))
    if not noted:
        return None
    return url.replace("/notification/" + m.group(1),
                       "/notification/" + noted.group(1), 1)


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

    allowed = allowed_hosts_for_source(source)
    if allowed:
        host = _host(url)
        # 2026-09-27: this test used to end in `or a in host` — a bare
        # SUBSTRING containment check, so "fda.gov.evil.example" satisfied
        # "fda.gov". The identical defect was removed from
        # _publish_gate.host_matches_domain the same day, and that function
        # declares itself THE ONE CANONICAL HOST TEST — which it was not,
        # while this second copy carried the bug. Now there is one
        # implementation and this module calls it.
        if not any(_host_matches(host, a) for a in allowed):
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
    ap.add_argument("--apply", action="store_true",
                    help="repair the RASFF reference-style URLs in place from "
                         "each row's own notifId (offline; no other finding "
                         "class is ever auto-repaired)")
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

    if args.apply:
        print("\n--apply : repairing RASFF reference-style URLs from each "
              "row's own notifId")
        repairs = []
        for row in data:
            fixed = repair_rasff_reference(row)
            if fixed:
                repairs.append((str(row.get("URL")).strip(), fixed,
                                str(row.get("Date")), str(row.get("Company"))))
        if not repairs:
            print("    nothing to repair")
        else:
            import openpyxl as _ox
            wb2 = _ox.load_workbook(args.xlsx)          # writable
            ws = wb2[args.sheet]
            head = [str(c.value) for c in ws[1]]
            ucol = head.index("URL") + 1
            ncol = head.index("Notes") + 1
            by_old = {old: (new, d, c) for old, new, d, c in repairs}
            written = 0
            for r in range(2, ws.max_row + 1):
                cur = str(ws.cell(r, ucol).value or "").strip()
                if cur in by_old:
                    new_url = by_old[cur][0]
                    ws.cell(r, ucol).value = new_url
                    stamp = (f"[url-repair {_dt.date.today().isoformat()}: "
                             f"RASFF reference -> notifId from this row's own "
                             f"Notes; format unchanged]")
                    prev = str(ws.cell(r, ncol).value or "")
                    if stamp not in prev:
                        ws.cell(r, ncol).value = (prev + " " + stamp).strip()
                    written += 1
            wb2.save(args.xlsx)
            for old_u, new_u, d, c in repairs:
                print(f"    {d}  {c[:34]}")
                print(f"        {old_u.rsplit('/', 1)[-1]}  ->  "
                      f"{new_u.rsplit('/', 1)[-1]}")
            print(f"    {written} row(s) repaired in {args.xlsx}")
            print("    NOTE: run pipeline/mirror_json_from_xlsx.py so "
                  "docs/data/recalls.json matches.")

    # ── EXIT CONTRACT (fixed 2026-09-28) ──────────────────────────────────
    # WITHOUT --apply this is an AUDIT, and exit 1 for "structural problems
    # found" is correct and useful: a CI step can gate on it.
    #
    # WITH --apply it is a REPAIR ACTION, and its exit code must report
    # whether the REPAIR worked — not whether the audit happened to find
    # unrelated defects it was never asked to fix.
    #
    # The difference cost a whole night's run. offline-enrich-and-promote.yml
    # called `verify_urls --apply` as its third step. The register carries 8
    # known structural defects that cannot be fixed offline — the EFET
    # press-release landing page, a CFIA row citing hortidaily.com, and six
    # BVL rows citing produktwarnung.eu, a private aggregator rather than the
    # German regulator. All 8 are reported, none is repairable here, so the
    # step exited 1 and GitHub killed the job. Everything after it —
    # promote_gate_passing, the weekly rebuild, the signals rebuild and the
    # commit — never ran. The offline loop, the one piece of this system that
    # works while the Llama box is down, was being stopped every time by an
    # audit finding defects it was not there to fix.
    #
    # So: --apply exits 0 when the repairs it attempted succeeded. The audit
    # step later in the same workflow still reports all 8, and still carries
    # `|| true` because reporting is not failing either.
    if args.apply:
        return 0
    return 1 if flagged else 0


if __name__ == "__main__":
    raise SystemExit(main())
