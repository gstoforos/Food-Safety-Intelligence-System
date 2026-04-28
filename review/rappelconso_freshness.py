"""
RappelConso freshness check — every-hour deterministic backstop.

WHY THIS EXISTS
---------------
Audit on 2026-04-29 found that the 18:00 Athens daily-scrape silently
failed to capture 4 Listeria recalls published mid-day on 28/04
(RappelConso fiches 22141, 22142, 22143, 22145) plus 3 Alternaria
toxin recalls from 27/04 (fiches 22107, 22108, 22109). The Listeria
ones were on the keyword whitelist; the Alternaria ones were not (now
fixed in scrapers/rappelconso.py). But the underlying lesson is that
ANY single scrape is a single point of failure — silent network errors,
upstream API hiccups, or LLM-prompt regressions can take an entire
batch offline.

This script is a defensive backstop that runs hourly and:

  1. Pulls the LAST 7 DAYS of "Alimentation" recalls from the open-data
     API directly. No LLM, no third-party search, no Google indexing
     latency.
  2. Compares URLs against docs/data/recalls.xlsx (Recalls + Pending).
  3. For each URL not present, decides if it's in pathogen scope using
     the same PATHOGEN_KEYWORDS used by the regular scraper.
  4. Appends each in-scope missing row directly to the Pending sheet
     (the URL Guardian + merge_master will promote them on the next
     pass).

Cost: 0 €. One unauthenticated GET to data.economie.gouv.fr per run.
Latency: < 10 s in the typical case.
Idempotency: URLs already in Recalls or Pending are skipped silently.

SCHEDULING
----------
Add to FsisScheduler.gs as an HOURLY periodic dispatch (same pattern as
merge-master.yml):

    // RappelConso freshness check — hourly, deterministic backstop
    dispatchOnce("rappelconso-freshness.yml", dayKey + ":rcfresh:" + H);

CLI
---
    python -m review.rappelconso_freshness            # default 7-day window
    python -m review.rappelconso_freshness --days 14  # broader sweep
    python -m review.rappelconso_freshness --dry-run  # report only, no write
"""
from __future__ import annotations
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from scrapers.rappelconso import RappelConsoScraper  # noqa: E402  (for keyword list reuse)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending, append_to_pending, save_xlsx_with_pending,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("rappelconso-freshness")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

API_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "rappelconso0/records"
)


# ─────────────────────────────────────────────────────────────────────────
# Live API pull — same query shape as scrapers/rappelconso.py
# ─────────────────────────────────────────────────────────────────────────

def fetch_recent(days: int) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "where": f'date_publication >= "{cutoff}" AND categorie_de_produit = "Alimentation"',
        "limit": 500,                # 500 rows × 7 days = comfortable headroom
        "order_by": "date_publication DESC",
    }
    headers = {
        # data.economie.gouv.fr returns 403 on bare urllib User-Agent.
        # Same UA the production scraper uses.
        "User-Agent": "Mozilla/5.0 (FSIS-freshness/1.0) Python/requests",
        "Accept": "application/json",
    }
    log.info("GET %s (cutoff=%s, limit=%d)", API_URL, cutoff, params["limit"])
    r = requests.get(API_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    body = r.json()
    rows = body.get("results", []) or []
    total = body.get("total_count", "?")
    log.info("API returned %d rows (server total_count=%s)", len(rows), total)
    return rows


# ─────────────────────────────────────────────────────────────────────────
# Filter: only pathogen / mycotoxin scope
# ─────────────────────────────────────────────────────────────────────────

def in_pathogen_scope(rec: dict) -> bool:
    """Same predicate as scrapers/rappelconso.py:scrape() — kept in sync
    by importing the keyword tuple directly. No second source of truth."""
    blob = (
        (rec.get("motif_du_rappel") or "").lower()
        + " "
        + (rec.get("risques_encourus_par_le_consommateur") or "").lower()
    )
    return any(kw in blob for kw in RappelConsoScraper.PATHOGEN_KEYWORDS)


# ─────────────────────────────────────────────────────────────────────────
# Gap detection
# ─────────────────────────────────────────────────────────────────────────

def existing_urls(xlsx_path: Path) -> set[str]:
    """Return every URL already in Recalls or Pending. Case-sensitive,
    trailing whitespace stripped — matches how merge_master de-dupes."""
    urls: set[str] = set()
    for sheet_loader in (load_existing, load_pending):
        for r in sheet_loader(str(xlsx_path)):
            u = (getattr(r, "URL", "") or "").strip()
            if u:
                urls.add(u)
    return urls


def to_recall(rec: dict) -> Recall:
    """Convert a RappelConso API row to a Recall object — same shape the
    regular scraper produces, so downstream code can't tell the difference."""
    pathogen_raw = (rec.get("risques_encourus_par_le_consommateur") or "")[:200]
    motif = (rec.get("motif_du_rappel") or "")[:300]
    # If the "risques" field is generic ("Autres contaminants chimiques"),
    # fall back to extracting the toxin name from the motif. This is what
    # made the Alternaria fiches look like chemicals to the original scraper.
    pathogen = normalize_pathogen(pathogen_raw) or normalize_pathogen(motif)
    country = normalize_country("France")

    fid = (
        rec.get("identifiant_unique_de_l_alerte")
        or rec.get("reference_fiche")
        or rec.get("numero_de_la_fiche")
        or ""
    )
    url = rec.get("lien_vers_la_fiche_rappel") or (
        f"https://rappel.conso.gouv.fr/fiche-rappel/{fid}/Interne" if fid else ""
    )

    return Recall(
        Date=(rec.get("date_publication") or "")[:10],
        Source="RappelConso (FR)",
        Company=rec.get("nom_de_la_societe_responsable_de_la_commercialisation", "") or "",
        Brand=rec.get("nom_de_la_marque_du_produit", "—") or "—",
        Product=(
            rec.get("noms_des_modeles_ou_references", "")
            or rec.get("sous_categorie_de_produit", "")
        )[:300],
        Pathogen=pathogen,
        Reason=motif,
        Class=rec.get("nature_juridique_du_rappel") or "Volontaire",
        Country=country,
        Region=infer_region(country),
        Tier=assign_tier(pathogen, 0),
        Outbreak=0,
        URL=url,
        Notes=(
            f"[freshness backstop {datetime.now(timezone.utc).strftime('%Y-%m-%d')}; "
            f"fiche {fid or '?'}; {(rec.get('distributeurs') or '')[:100]}]"
        ),
    ).normalize()


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=7,
                    help="Look-back window in days (default: 7)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print findings, do NOT write to Pending")
    ap.add_argument("--xlsx", default=str(XLSX_PATH))
    args = ap.parse_args(list(argv) if argv is not None else None)

    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        log.error("recalls.xlsx not found at %s", xlsx_path)
        return 2

    # 1. Pull
    try:
        rows = fetch_recent(args.days)
    except requests.RequestException as e:
        log.error("RappelConso API call failed: %s", e)
        return 1

    # 2. Filter to pathogen scope
    in_scope = [r for r in rows if in_pathogen_scope(r)]
    log.info("Pathogen-scope rows in last %d days: %d / %d total",
             args.days, len(in_scope), len(rows))

    # 3. Compare to existing data
    have = existing_urls(xlsx_path)
    log.info("Existing URLs in Recalls + Pending: %d", len(have))

    missing: list[Recall] = []
    for rec in in_scope:
        url = (rec.get("lien_vers_la_fiche_rappel") or "").strip()
        if not url:
            fid = (rec.get("identifiant_unique_de_l_alerte")
                   or rec.get("reference_fiche") or "")
            url = (
                f"https://rappel.conso.gouv.fr/fiche-rappel/{fid}/Interne"
                if fid else ""
            )
        if not url:
            continue
        if url in have:
            continue
        missing.append(to_recall(rec))

    # 4. Report
    if not missing:
        log.info("No gaps found — RappelConso coverage is complete for the "
                 "last %d days.", args.days)
        return 0

    log.warning("FOUND %d MISSING RAPPELCONSO RECALLS:", len(missing))
    for r in missing:
        log.warning("  %s | %s | %s | %s", r.Date, r.Brand[:25], r.Product[:40], r.URL)

    if args.dry_run:
        log.info("--dry-run: not writing.")
        return 0

    # 5. Append to Pending
    pending = load_pending(str(xlsx_path))
    pending = append_to_pending(pending, missing)
    save_xlsx_with_pending(str(xlsx_path), pending=pending)
    log.info("Appended %d rows to Pending. merge_master will promote on "
             "the next hourly pass.", len(missing))
    return 0


if __name__ == "__main__":
    sys.exit(main())
