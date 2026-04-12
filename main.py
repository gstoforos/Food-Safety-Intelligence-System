"""
FSIS Agent – Main Orchestrator
Runs: fetch → enrich → report
Triggered by GitHub Actions cron or manually.

Usage:
  python main.py                  # normal daily run
  python main.py --full           # re-pull last 90 days (first run / reset)
  python main.py --report-only    # skip fetch/enrich, regenerate report
  python main.py --enrich-only    # only run Gemini enrichment pass
"""
import sys
import logging
import argparse
from datetime import datetime

from database import init_db, log_run
from fda_scraper import fetch_fda
from gemini_enricher import enrich_batch
from report_generator import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fsis.main")


def run(full: bool = False, report_only: bool = False, enrich_only: bool = False):
    log.info("═" * 55)
    log.info("FSIS Agent – FDA Module")
    log.info(f"Run type: {'full' if full else 'report-only' if report_only else 'enrich-only' if enrich_only else 'incremental'}")
    log.info("═" * 55)

    # 1. Initialize DB (idempotent)
    init_db()

    # 2. Fetch
    if not report_only and not enrich_only:
        try:
            if full:
                fetched, new = fetch_fda(date_from=None, date_to=None)  # uses FDA_LOOKBACK_DAYS
            else:
                fetched, new = fetch_fda()   # incremental from last stored date
            log_run("FDA", fetched, new)
        except Exception as e:
            log.error(f"FDA fetch failed: {e}")
            log_run("FDA", 0, 0, status="ERROR", error=str(e))

    # 3. Gemini enrichment
    if not report_only:
        try:
            enrich_batch(limit=200)
        except Exception as e:
            log.error(f"Enrichment failed: {e}")

    # 4. Generate report
    try:
        path = generate_report(days=30)
        log.info(f"Report ready: {path}")
    except Exception as e:
        log.error(f"Report generation failed: {e}")

    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FSIS Agent – FDA Module")
    parser.add_argument("--full",        action="store_true", help="Full re-pull (90 days)")
    parser.add_argument("--report-only", action="store_true", help="Regenerate report only")
    parser.add_argument("--enrich-only", action="store_true", help="Gemini enrichment pass only")
    args = parser.parse_args()

    run(
        full        = args.full,
        report_only = args.report_only,
        enrich_only = args.enrich_only,
    )
