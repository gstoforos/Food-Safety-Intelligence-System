#!/usr/bin/env python3
"""
Audit every scraper's INDEX_URLS against the live web.

Why: scrapers fail silently when an agency restructures its site and the
indexed URL starts returning 404 / 410 / a redirect chain to a homepage.
_base.fetch() logs a warning and returns None; the pipeline happily proceeds
with zero rows. This script is the external check that catches that failure.

Usage:
    python tools/audit_scraper_urls.py              # human-readable table
    python tools/audit_scraper_urls.py --json       # machine-readable JSON
    python tools/audit_scraper_urls.py --bad-only   # only show failing URLs

Signals surfaced per URL:
    * HTTP status (final, after redirects)
    * Final URL (catches silent redirect-to-homepage)
    * Response size in bytes
    * Whether the page carries recall-ish keywords
    * Rough listing-page heuristic (count of dated items / "Rückruf" / "recall" etc.)

Exit code: 0 if all URLs healthy, 1 if any scraper has ONLY failing URLs.
"""
from __future__ import annotations
import argparse
import importlib
import inspect
import json
import pkgutil
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._base import BaseScraper, make_session  # noqa: E402

# Multilingual recall-page markers. Presence of ≥2 on a page is a strong
# signal that the URL is still a real listing. Absence on a 200 response is
# the tell for a soft-404 / redirect-to-homepage.
RECALL_MARKERS = [
    # English
    r"recall", r"food alert", r"withdrawal", r"safety warning",
    # German/Austrian/Swiss
    r"r[üu]ckruf", r"warnung", r"lebensmittelwarnung", r"produktwarnung",
    # French
    r"rappel", r"avertissement",
    # Spanish/Portuguese/Italian
    r"retirada", r"alerta", r"richiamo", r"avviso di sicurezza",
    # Dutch/Flemish
    r"terugroep", r"veiligheidswaarschuwing",
    # Nordic
    r"tilbakekall", r"tilbagetrukn", r"tillbakadragning", r"återkall", r"takaisinveto",
    # Central/Eastern European
    r"stažení", r"wycofan", r"visszahív", r"odpoklic", r"povlačenje", r"odvolanie",
    # Asian languages (Japanese/Korean/Chinese) — mark the presence of Han/hangul/kana
    r"[\u4e00-\u9fff]{2,}.*(回收|召回|下架|リコール)",
    r"리콜", r"회수",
]
MARKER_RE = re.compile("|".join(RECALL_MARKERS), re.IGNORECASE)

# Dated-item heuristic: count occurrences of a date-like token. A healthy
# listing usually shows ≥5 dated items in the first page of HTML.
DATE_RE = re.compile(
    r"\b(20\d{2}[-./]\d{1,2}[-./]\d{1,2}|\d{1,2}[-./]\d{1,2}[-./]20\d{2}|"
    r"\d{1,2}\.\s*(januar|februar|march|märz|april|may|mai|juni|juli|august|"
    r"september|oktober|november|dezember|december|janeiro|fevereiro|março|"
    r"gennaio|febbraio|marzo|aprile|maggio|giugno|luglio)\s*20\d{2})",
    re.IGNORECASE,
)


@dataclass
class UrlResult:
    agency: str
    country: str
    url: str
    status: Optional[int] = None
    final_url: Optional[str] = None
    size_bytes: int = 0
    marker_hits: int = 0
    date_hits: int = 0
    error: Optional[str] = None
    healthy: bool = False
    verdict: str = ""


def discover_scrapers() -> List[BaseScraper]:
    """Mirror run_all.discover_scrapers()."""
    found = []
    import scrapers  # noqa: F401
    regions = ["north_america", "europe_eu", "europe_non_eu", "eu_wide",
               "asia", "oceania", "africa", "latam", "middle_east"]
    for region in regions:
        try:
            pkg = importlib.import_module(f"scrapers.{region}")
        except ImportError:
            continue
        for _, modname, _ in pkgutil.iter_modules(pkg.__path__):
            try:
                mod = importlib.import_module(f"scrapers.{region}.{modname}")
            except Exception as e:
                print(f"  [skip] {region}.{modname}: {e}", file=sys.stderr)
                continue
            for _, cls in inspect.getmembers(mod, inspect.isclass):
                if (issubclass(cls, BaseScraper) and cls is not BaseScraper
                        and cls.__module__ == mod.__name__ and cls.AGENCY):
                    found.append(cls())
    return found


def check_url(session, agency: str, country: str, url: str,
              timeout: int = 30) -> UrlResult:
    res = UrlResult(agency=agency, country=country, url=url)
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        res.status = r.status_code
        res.final_url = r.url
        text = r.text if r.status_code < 400 else ""
        res.size_bytes = len(r.content) if r.content else 0
        res.marker_hits = len(MARKER_RE.findall(text[:200_000]))
        res.date_hits = len(DATE_RE.findall(text[:200_000]))
    except Exception as e:
        res.error = f"{type(e).__name__}: {e}"
        res.verdict = "UNREACHABLE"
        return res

    # Redirected to homepage? (soft-404 pattern)
    in_path = urlparse(url).path.rstrip("/")
    out_path = urlparse(res.final_url or url).path.rstrip("/")
    redirect_to_home = (in_path and in_path != "" and
                       out_path in ("", "/") and
                       urlparse(url).netloc == urlparse(res.final_url or "").netloc)

    if res.status and res.status >= 400:
        res.verdict = f"HTTP {res.status}"
    elif redirect_to_home:
        res.verdict = "REDIRECT_TO_HOME"
    elif res.size_bytes < 5_000:
        res.verdict = "TINY_RESPONSE"
    elif res.marker_hits == 0 and res.date_hits < 3:
        # 200 OK but no recall keywords and no dated items — likely the wrong page
        res.verdict = "NO_RECALL_MARKERS"
    else:
        res.healthy = True
        res.verdict = f"OK ({res.marker_hits} markers, {res.date_hits} dates)"
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of table")
    ap.add_argument("--bad-only", action="store_true", help="Only show non-healthy URLs")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--timeout", type=int, default=30)
    args = ap.parse_args()

    scrapers = discover_scrapers()
    print(f"# Auditing {len(scrapers)} scrapers...", file=sys.stderr)
    session = make_session(timeout=args.timeout)

    tasks = []
    for s in scrapers:
        urls = getattr(s, "INDEX_URLS", None) or []
        # Scrapers that don't define INDEX_URLS (custom scrape() like CFIA,
        # FDA, USDA FSIS, RappelConso, fsa_uk, fsanz, rasff) are skipped here.
        for u in urls:
            tasks.append((s.AGENCY, s.COUNTRY, u))

    results: List[UrlResult] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(check_url, session, ag, co, u, args.timeout): (ag, u)
                for ag, co, u in tasks}
        for f in as_completed(futs):
            results.append(f.result())
    dt = time.time() - t0

    # Aggregate per-agency health (unhealthy means ALL urls for the agency failed)
    by_agency = {}
    for r in results:
        by_agency.setdefault(r.agency, []).append(r)
    fully_broken = [a for a, rs in by_agency.items() if not any(x.healthy for x in rs)]

    if args.bad_only:
        results = [r for r in results if not r.healthy]

    if args.json:
        print(json.dumps({
            "checked": len(results),
            "elapsed_s": round(dt, 1),
            "fully_broken_agencies": sorted(fully_broken),
            "results": [asdict(r) for r in results],
        }, indent=2, ensure_ascii=False))
        return 1 if fully_broken else 0

    # Table output
    results.sort(key=lambda r: (not r.healthy, r.agency))
    print(f"# Audited {len(results)} URLs in {dt:.1f}s across {len(by_agency)} scrapers")
    print(f"# Fully-broken agencies (all URLs failing): {len(fully_broken)}")
    if fully_broken:
        print(f"#   {', '.join(sorted(fully_broken))}")
    print()
    print(f"{'Agency':28s} {'Verdict':22s} {'Sz':>7s} {'M':>3s} {'D':>3s}  URL")
    print("-" * 110)
    for r in results:
        mark = "✓" if r.healthy else "✗"
        print(f"{mark} {r.agency[:26]:26s} {r.verdict[:22]:22s} "
              f"{r.size_bytes:>7d} {r.marker_hits:>3d} {r.date_hits:>3d}  "
              f"{r.url[:70]}")
        if r.final_url and r.final_url != r.url:
            print(f"  {'└─ redirected to:':>48s} {r.final_url[:70]}")
        if r.error:
            print(f"  {'└─ error:':>48s} {r.error[:70]}")

    return 1 if fully_broken else 0


if __name__ == "__main__":
    sys.exit(main())
