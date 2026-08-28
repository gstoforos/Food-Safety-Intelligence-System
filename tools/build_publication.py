#!/usr/bin/env python3
"""Build the single published FSIS report.

ONE DOCUMENT, NOT TWO
---------------------
An earlier pass produced a News article and a separate technical annex.
Two files means two URLs, two places for a figure to drift, and a reader
who has to decide which one to open. This composes both into one page:

    masthead + summary tiles
    capability lead      (unnumbered front matter — what the system caught)
    §1 .. §8             (the full method, unchanged and still cited by number)
    subscription CTA

Every figure on the page, in the lead and in the method alike, comes from
one `gather()` call, so the front and the back cannot disagree.

    python -m tools.build_publication
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools import build_news_piece as lead              # noqa: E402
from tools import build_technical_report as tr          # noqa: E402

# Kept in step with build_news_piece.TITLE — see the note there.
TITLE = "Anomalies in the global food recall record"
EYEBROW = "AFTS-FSIS \u00b7 Food Safety Intelligence System"
OUT = ROOT / "docs" / "reports" / "fsis-signal-detection.html"


def build(d: dict) -> str:
    return tr.render(
        d,
        lead_html=lead.front_matter(d),
        eyebrow=EYEBROW,
        title=TITLE,
        deck=lead.deck(d),
        extra_css=lead.EXTRA_CSS,
    )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args(argv)
    d = tr.gather()
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build(d), encoding="utf-8")
    inw = [r for r in d["replay"] if r["in_window"]]
    share = sum(1 for r in inw if r["channel"] == "proportion")
    print(f"publication: {out}")
    print(f"  {d['n_records']:,} notices / {d['n_weeks']} weeks · "
          f"{share} share signals + {len(inw) - share} diagnostics")
    print(f"  analytical-digest sha256 {tr.analytical_digest(d)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
