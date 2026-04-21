"""
AFTS FSIS — HTML → PDF converter.

Converts docs/20YY-M<MM>.html  → docs/20YY-M<MM>.pdf  (monthly)
         docs/20YY-W<NN>.html  → docs/20YY-W<NN>.pdf  (weekly)

Only builds PDFs that don't exist yet (or whose HTML is newer than the
existing PDF). Idempotent.

Usage:
    python -m pipeline.html_to_pdf --docs-dir docs --kind monthly
    python -m pipeline.html_to_pdf --docs-dir docs --kind weekly
    python -m pipeline.html_to_pdf --docs-dir docs --kind all

Requires weasyprint (in requirements.txt + apt deps installed by the
workflow).
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("html_to_pdf")

MONTHLY_PAT = re.compile(r"^(20\d{2})-M(\d{2})\.html$")
MONTHLY_ALL_PAT = re.compile(r"^(20\d{2})-M(\d{2})-all\.html$")
WEEKLY_PAT = re.compile(r"^(20\d{2})-W(\d{2})\.html$")


def discover(docs_dir: Path, kind: str) -> List[Path]:
    """List HTML reports to convert. Skips '-all' companion pages — they
    go to index only, not to a separate PDF."""
    out = []
    for p in sorted(docs_dir.glob("20*-*.html")):
        name = p.name
        if MONTHLY_ALL_PAT.match(name):
            continue   # companion — don't PDF
        if kind in ("monthly", "all") and MONTHLY_PAT.match(name):
            out.append(p)
        if kind in ("weekly", "all") and WEEKLY_PAT.match(name):
            out.append(p)
    return out


def needs_rebuild(html: Path, pdf: Path) -> bool:
    if not pdf.exists():
        return True
    return html.stat().st_mtime > pdf.stat().st_mtime


def convert(html: Path, pdf: Path) -> None:
    # Import locally so the script still parses on machines without weasyprint
    # — the workflow installs it before calling us.
    from weasyprint import HTML   # type: ignore
    log.info("PDF: %s -> %s", html.name, pdf.name)
    HTML(filename=str(html)).write_pdf(str(pdf))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs-dir", required=True)
    ap.add_argument("--kind", choices=("monthly", "weekly", "all"), default="all")
    ap.add_argument("--force", action="store_true",
                    help="Rebuild PDFs even if up to date")
    args = ap.parse_args()

    docs_dir = Path(args.docs_dir)
    if not docs_dir.is_dir():
        log.error("docs-dir not found: %s", docs_dir); return 2

    htmls = discover(docs_dir, args.kind)
    if not htmls:
        log.info("No %s HTML reports to convert.", args.kind)
        return 0

    built = 0
    skipped = 0
    failed = 0
    for html in htmls:
        pdf = html.with_suffix(".pdf")
        if not args.force and not needs_rebuild(html, pdf):
            log.info("Skip (up to date): %s", pdf.name)
            skipped += 1
            continue
        try:
            convert(html, pdf)
            built += 1
        except Exception as e:
            log.error("PDF build failed for %s: %s", html.name, e)
            failed += 1

    log.info("Done. built=%d skipped=%d failed=%d", built, skipped, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
