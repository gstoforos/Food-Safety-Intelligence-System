"""
AFTS FSIS — monthly-index.json pdf_url patcher.

After the monthly builder has written docs/YYYY-MMM.html and html_to_pdf
has produced docs/YYYY-MMM.pdf, this script patches every
monthly-index.json entry so its `pdf_url` points to the GitHub Pages URL
of the committed PDF. hub.html ONLY renders a month's card when the
entry has `pdf_url` set — so without this step, auto-generated months
are invisible on the hub even though the HTML and PDF are live.

Legacy Jan/Feb/March entries that already have a non-null pdf_url are
left alone (those are manual Wix-hosted PDFs — don't overwrite).

Usage (called from .github/workflows/afts-monthly-report.yml):

    python -m pipeline.set_pdf_urls \\
        --index docs/data/monthly-index.json \\
        --docs-dir docs \\
        --site-url "https://gstoforos.github.io/Food-Safety-Intelligence-System"
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("set_pdf_urls")


def load_index(path: Path) -> List[dict]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("reports"), list):
        return raw["reports"]
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True)
    ap.add_argument("--docs-dir", required=True)
    ap.add_argument("--site-url", required=True,
                    help="GitHub Pages base URL (no trailing slash)")
    args = ap.parse_args()

    index_path = Path(args.index)
    docs_dir = Path(args.docs_dir)
    site_url = args.site_url.rstrip("/")

    entries = load_index(index_path)
    if not entries:
        log.info("Index is empty — nothing to do.")
        return 0

    changed = 0
    for e in entries:
        filename = e.get("filename") or ""
        if not filename.endswith(".html"):
            continue
        # Marketing one-pager naming: <tag>-marketing.pdf inside docs/marketing/
        # (e.g. docs/marketing/2026-M04-marketing.pdf). The plain <tag>.pdf
        # is the SUBSCRIBER edition at docs/<tag>.pdf — gated, not exposed
        # to hub.html. The PUBLIC pdf_url field that hub.html renders MUST
        # point at the marketing one-pager (lead magnet, top-10 only).
        marketing_pdf_name = filename[:-5] + "-marketing.pdf"   # 2026-M04.html -> 2026-M04-marketing.pdf
        marketing_pdf_path = docs_dir / "marketing" / marketing_pdf_name
        existing = e.get("pdf_url")

        # Skip legacy entries that already point to a Wix (or any external)
        # PDF — don't overwrite George's manual Jan/Feb/Mar uploads.
        if existing and "gstoforos.github.io" not in existing:
            log.info("Keep legacy pdf_url for %s: %s", filename, existing)
            continue

        # Only set pdf_url if the marketing PDF was committed in this run.
        if not marketing_pdf_path.exists():
            log.info("No marketing PDF yet for %s — leaving pdf_url unset.", filename)
            continue

        new_url = f"{site_url}/marketing/{marketing_pdf_name}"
        if existing == new_url:
            continue
        e["pdf_url"] = new_url
        log.info("Set pdf_url for %s -> %s", filename, new_url)
        changed += 1

    if not changed:
        log.info("No pdf_url changes needed.")
        return 0

    # Keep sort order newest-first by month_end (matches builder output).
    entries.sort(key=lambda x: x.get("month_end", ""), reverse=True)
    index_path.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Wrote %s with %d entries (%d changed).", index_path, len(entries), changed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
