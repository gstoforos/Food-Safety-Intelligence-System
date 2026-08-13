"""
AFTS FSIS — monthly SOCIAL cards (LinkedIn / Instagram / X).
============================================================

Three PDFs per month, one per platform aspect ratio, carrying the SAME
content: the month's top 10 critical incidents, in the AFTS marketing visual
language.

WHY THIS IS NOT THE MARKETING PDF
---------------------------------
The marketing one-pager leads with the month's counts — total recalls, Tier-1,
outbreaks, leading pathogen. Those are correct on the day it is built and they
KEEP MOVING afterwards: regulators publish recalls dated inside a closed month
for weeks after it closes. July 2026 went 294 -> 307 in the twelve days after
publication.

That is fine for the website, where the file is regenerated in place. It is
wrong for social, where a post is immutable the moment it goes out. A card
posted on the 1st saying "294 recalls" is simply false by the 10th, and it is
false in public with the AFTS name on it.

So this builder carries NO COUNTS AT ALL. Not the total, not Tier-1, not
outbreaks, not a leading-pathogen tally. Only the ten incidents, each of which
is a fact about a specific recall that does not change. The card stays true
for as long as it exists.

RANKING
-------
Identical to the monthly report and to the Apps Script month-end preview, so
the ten incidents on the card are the ten in the report:

    1. Outbreak flag        confirmed outbreaks above sporadic recalls
    2. C. botulinum         the only pathogen that kills at sub-microgram dose
    3. Tier-1               critical classification
    4. Severity score       pathogen severity + tier + outbreak bonuses
    5. Date                 newer first

The ranking is READ from the summary JSON's `top10`, which the monthly builder
already produced with that rubric — it is not recomputed here. One ranking,
one source.

FORMATS
-------
    linkedin    1080 x 1350   4:5 portrait — LinkedIn document/image post
    instagram   1080 x 1080   1:1 square   — Instagram feed
    x           1600 x  900   16:9         — X / Twitter in-timeline card

Sizes are in POINTS so the PDF's aspect ratio matches the platform's pixel
spec exactly; export to PNG at any scale and the framing is already right.

Brand primitives (colours, embedded font family, pathogen abbreviation) are
IMPORTED from build_monthly_marketing rather than copied. A second private
copy of the palette is how the hazard table drifted into four versions.

USAGE
    python -m pipeline.build_monthly_social \\
        --summary docs/data/monthly-summary-latest.json \\
        --out-dir docs/social
    # writes 2026-M07-linkedin.pdf, -instagram.pdf, -x.pdf
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any, Dict, List, Tuple

from reportlab.pdfgen import canvas

# Imported, not copied — one palette, one font registration, one abbreviator.
from pipeline.build_monthly_marketing import (
    ORANGE, NAVY, INK, MUTED, LINE, BAND, ALT, WHITE,
    H_MONO, _BODY, abbreviate_pathogen, _draw_letter_spaced, _text_w,
    MARGIN_L as _MK_MARGIN, PAGE_W as _MK_PAGE_W,
)

H_BOLD = _BODY["bold"]
H_REG = _BODY["regular"]


# =============================================================================
# FORMATS
# =============================================================================
# (label, width_pt, height_pt, columns, margin, scale)
#   columns=1 → one list of ten
#   columns=2 → two stacked fives, for the wide 16:9 card
FORMATS: Dict[str, Dict[str, Any]] = {
    "linkedin":  {"size": (1080.0, 1350.0), "cols": 1, "margin": 64, "scale": 1.00},
    "instagram": {"size": (1080.0, 1080.0), "cols": 1, "margin": 48, "scale": 0.74},
    "x":         {"size": (1600.0,  900.0), "cols": 2, "margin": 58, "scale": 0.80},
}


# =============================================================================
# DISPLAY LAYER — the register's internal conventions are not public copy
# =============================================================================
# These rewrite what is PRINTED. They never touch the data. Two conventions
# that are correct in the workbook read badly on a public card:
#
#   Company "Origin: Czechia | Notifying: Czechia"
#       The RASFF convention (operator rule: Company = Origin/Notifying,
#       Brand = notifying-state code, Country = origin). Exactly right in the
#       register, meaningless to a LinkedIn reader, and it repeats the country
#       that already appears in the row's tail.
#
#   Company "(not specified in RappelConso fiche 23065)"
#       An honest placeholder — the fiche genuinely names no company. Honest
#       in a database, an embarrassment on a card with the AFTS logo on it.
#
# Neither is a data defect, so neither is repaired upstream.

_PLACEHOLDER_MARKERS = ("not specified", "non spécifié", "unknown", "n/a",
                        "aucune", "sans marque", "unbranded", "—", "-")


def display_company(row: Dict[str, Any]) -> str:
    """What to print in the company slot. Never invents a name."""
    raw = str(row.get("company") or "").strip()
    brand = str(row.get("brand") or "").strip()

    if raw.lower().startswith("origin:"):
        # RASFF: the country is already printed in the row tail, so name the
        # instrument instead of repeating the geography.
        return "RASFF notification"

    low = raw.lower()
    if not raw or any(mark in low for mark in _PLACEHOLDER_MARKERS):
        # A banner letter is not a name. Système U's brand field is literally
        # "U"; printing that as the responsible party on a public card says
        # nothing and looks broken. Require something a reader can act on.
        if (len(brand) >= 3
                and not any(m in brand.lower() for m in _PLACEHOLDER_MARKERS)):
            return brand
        # No company and no usable brand: say what the recall IS rather than
        # printing a parenthetical apology.
        return "Retailer-level recall — no brand named"

    if raw.lower().startswith("multiple establishments"):
        return "Multiple establishments"
    return raw


def _fmt_date(iso: str) -> str:
    """2026-07-18 -> 18 JUL. Short enough for a phone screen."""
    try:
        d = date.fromisoformat(str(iso)[:10])
    except (ValueError, TypeError):
        return str(iso or "")[:10]
    return f"{d.day:02d} {d.strftime('%b').upper()}"


def _clip(text: str, font: str, size: float, max_w: float) -> str:
    """Trim to width with a real ellipsis. Never wraps — a card row is one line."""
    s = str(text or "")
    if _text_w(s, font, size) <= max_w:
        return s
    ell = "…"
    while s and _text_w(s + ell, font, size) > max_w:
        s = s[:-1]
    return (s.rstrip() + ell) if s else ""


def _draw_header(c, W: float, H: float, margin: float, s: float,
                 month_label: str, period: str, stamp: str = "") -> float:
    """Marketing masthead, scaled. Returns the y cursor below it."""
    y = H - margin

    _draw_letter_spaced(c, margin, y - 10 * s,
                        "ADVANCED FOOD-TECH SOLUTIONS · AFTS",
                        H_BOLD, 13 * s, NAVY, tracking=2.6 * s)
    y -= 34 * s

    c.setFont(H_BOLD, 22 * s)
    c.setFillColor(NAVY)
    c.drawString(margin, y - 18 * s, "FOOD SAFETY INTELLIGENCE SYSTEM")
    y -= 52 * s

    c.setFont(H_BOLD, 62 * s)
    c.setFillColor(NAVY)
    c.drawString(margin, y - 46 * s, month_label)

    # UPDATED stamp, right-aligned on the title baseline — exactly as the
    # marketing one-pager does it. Dropping it was an unrequested change: it
    # is how a reader knows which build of the month they are looking at.
    if stamp:
        c.setFont(H_BOLD, 16 * s)
        c.setFillColor(ORANGE)
        c.drawRightString(W - margin, y - 46 * s, stamp)
    y -= 68 * s

    c.setStrokeColor(ORANGE)
    c.setLineWidth(4.0 * s)
    c.line(margin, y, W - margin, y)
    y -= 30 * s

    # Statement band — the section title and the ranking basis. NO COUNTS.
    band_h = 46 * s
    c.setFillColor(BAND)
    c.rect(margin, y - band_h, W - 2 * margin, band_h, fill=1, stroke=0)
    c.setFont(H_BOLD, 17 * s)
    c.setFillColor(ORANGE)
    c.drawString(margin + 16 * s, y - band_h / 2 - 1 * s,
                 "§ TOP 10 CRITICAL INCIDENTS")
    c.setFont(H_BOLD, 12 * s)
    c.setFillColor(MUTED)
    c.drawRightString(W - margin - 16 * s, y - band_h / 2 - 1 * s, period)
    return y - band_h - 26 * s


def _draw_row(c, x: float, y: float, col_w: float, s: float,
              n: int, row: Dict[str, Any], rule: bool = True) -> float:
    """One incident. Returns the y cursor below it."""
    pathogen = abbreviate_pathogen(row.get("pathogen_raw")
                                   or row.get("pathogen") or "")
    company = display_company(row)
    product = str(row.get("product") or "")
    country = str(row.get("country") or "")
    source = str(row.get("source") or "")
    outbreak = bool(row.get("outbreak"))
    tier1 = str(row.get("tier") or "") == "1"

    rank_w = 46 * s
    tx = x + rank_w

    # Rank numeral — the marketing PDF's orange serif-weight figure
    c.setFont(H_BOLD, 34 * s)
    c.setFillColor(ORANGE)
    c.drawRightString(x + rank_w - 14 * s, y - 26 * s, str(n))

    # Line 1 — date · pathogen · chips
    c.setFont(H_MONO, 12 * s)
    c.setFillColor(MUTED)
    dstr = _fmt_date(row.get("date"))
    c.drawString(tx, y - 12 * s, dstr)
    dx = tx + _text_w(dstr, H_MONO, 12 * s) + 12 * s

    c.setFont(H_BOLD, 15 * s)
    c.setFillColor(NAVY)
    c.drawString(dx, y - 13 * s, pathogen)
    dx += _text_w(pathogen, H_BOLD, 15 * s) + 10 * s

    for label, bg in ((("OUTBREAK", ORANGE) if outbreak else (None, None)),
                      (("T1", NAVY) if tier1 else (None, None))):
        if not label:
            continue
        cw = _text_w(label, H_BOLD, 10 * s) + 14 * s
        c.setFillColor(bg)
        c.rect(dx, y - 17 * s, cw, 16 * s, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont(H_BOLD, 10 * s)
        c.drawString(dx + 7 * s, y - 13 * s, label)
        dx += cw + 7 * s

    # Line 2 — company
    c.setFont(H_BOLD, 16 * s)
    c.setFillColor(INK)
    c.drawString(tx, y - 34 * s, _clip(company, H_BOLD, 16 * s,
                                       col_w - rank_w - 8 * s))

    # Line 3 — product
    c.setFont(H_REG, 13 * s)
    c.setFillColor(MUTED)
    c.drawString(tx, y - 52 * s, _clip(product, H_REG, 13 * s,
                                       col_w - rank_w - 8 * s))

    # Line 4 — country · source
    tail = " · ".join([p for p in (country, source) if p])
    c.setFont(H_MONO, 11 * s)
    c.setFillColor(NAVY)
    c.drawString(tx, y - 68 * s, _clip(tail, H_MONO, 11 * s,
                                       col_w - rank_w - 8 * s))

    y -= 82 * s
    if rule:
        c.setStrokeColor(LINE)
        c.setLineWidth(0.9 * s)
        c.line(x, y + 12 * s, x + col_w, y + 12 * s)
    return y


def _draw_footer(c, W: float, H: float, margin: float, s: float) -> float:
    """The marketing one-pager's footer, unchanged.

    Navy band across the full width, white letter-spaced AFTS on the left,
    the two-line tagline right-aligned. The earlier version of this function
    invented a white footer with a thin rule and a "figures at
    fsis.advfood.tech" line — a change nobody asked for, on the one element
    of the page that carries the company identity. Restored to the source.

    Returns the band height so the row area can stop above it.
    """
    foot_h = min(46.0 * s * 1.8, H * 0.075)
    tsize = foot_h * 0.152

    c.setFillColor(NAVY)
    c.rect(0, 0, W, foot_h, fill=1, stroke=0)

    _draw_letter_spaced(c, margin, foot_h / 2 - 2 * s,
                        "ADVANCED FOOD-TECH SOLUTIONS · AFTS",
                        H_BOLD, tsize, WHITE, tracking=0.8 * s)

    right_top = ("Food Process Engineering · Thermal Processing · "
                 "Regulatory Compliance")
    right_bot = "advfood.tech · info@advfood.tech · Athens, Greece"
    c.setFont(H_REG, tsize)
    c.setFillColor(WHITE)
    c.drawString(W - margin - _text_w(right_top, H_REG, tsize),
                 foot_h / 2 + 4 * s, right_top)
    c.drawString(W - margin - _text_w(right_bot, H_REG, tsize),
                 foot_h / 2 - 8 * s, right_bot)
    return foot_h


def render_social_pdf(out_path: str, fmt: str, data: Dict[str, Any]) -> str:
    spec = FORMATS[fmt]
    W, H = spec["size"]
    margin = float(spec["margin"])
    s = float(spec["scale"])
    cols = int(spec["cols"])
    rows: List[Dict[str, Any]] = data["rows"][:10]

    c = canvas.Canvas(out_path, pagesize=(W, H))
    c.setTitle(f"FSIS · Top 10 Critical Incidents · {data['month_label']}")
    c.setAuthor("Advanced Food-Tech Solutions")
    c.setSubject("Monthly pathogen surveillance — top 10 incidents. No counts "
                 "by design: published figures change after publication.")

    c.setFillColor(WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    y0 = _draw_header(c, W, H, margin, s, data["month_label"], data["period"],
                      data.get("updated_stamp", ""))

    if cols == 1:
        # Fill the card. A fixed row pitch left a quarter of the 4:5 portrait
        # blank, which reads as "we ran out of content" rather than "designed".
        col_w = W - 2 * margin
        foot_h = min(46.0 * s * 1.8, H * 0.075)
        avail = y0 - (foot_h + 34 * s)
        pitch = max(94.0 * s, avail / max(len(rows), 1))
        y = y0
        for i, r in enumerate(rows, 1):
            _draw_row(c, margin, y, col_w, s, i, r, rule=False)
            # Anchor the divider to the ROW's own depth. Deriving it from the
            # pitch put the rule through the source line whenever the pitch
            # was tighter than the row — which is exactly the square card.
            c.setStrokeColor(LINE)
            c.setLineWidth(0.9 * s)
            c.line(margin, y - 78 * s, margin + col_w, y - 78 * s)
            y -= pitch
    else:
        gutter = 40 * s
        col_w = (W - 2 * margin - gutter) / 2.0
        half = (len(rows) + 1) // 2
        # Same fill logic as the single-column path — a 16:9 card with five
        # rows bunched at the top and half the height empty reads as broken
        # rather than airy.
        foot_h = min(46.0 * s * 1.8, H * 0.075)
        avail = y0 - (foot_h + 34 * s)
        pitch = max(94.0 * s, avail / max(half, 1))
        for ci, chunk in enumerate((rows[:half], rows[half:])):
            x = margin + ci * (col_w + gutter)
            y = y0
            for j, r in enumerate(chunk):
                _draw_row(c, x, y, col_w, s, ci * half + j + 1, r, rule=False)
                c.setStrokeColor(LINE)
                c.setLineWidth(0.9 * s)
                c.line(x, y - 78 * s, x + col_w, y - 78 * s)
                y -= pitch

    _draw_footer(c, W, H, margin, s)
    c.showPage()
    c.save()
    return out_path


def load(summary_path: str) -> Tuple[Dict[str, Any], str]:
    with open(summary_path, encoding="utf-8") as f:
        s = json.load(f)

    top10 = s.get("top10") or s.get("top_threats") or []
    if not top10:
        raise SystemExit(
            f"{summary_path}: no `top10` array. The social card IS the top 10 "
            f"— there is nothing to render without it. Rebuild the monthly "
            f"report first.")

    ws = date.fromisoformat(s["window_start"])
    we = date.fromisoformat(s["window_end"])
    abbr = ws.strftime("%b").upper()
    month_name = str(s.get("month_name") or "")
    # "July 2026" -> "JULY 2026"; some payloads carry only the month word.
    label = month_name.upper() if month_name else str(s.get("month_tag") or "")
    if str(s.get("year")) and str(s["year"]) not in label:
        label = f"{label} {s['year']}"

    from datetime import date as _d
    def _ordinal(n: int) -> str:
        suf = "th" if 10 <= n % 100 <= 20 else {1:"st",2:"nd",3:"rd"}.get(n % 10,"th")
        return f"{n}{suf}"
    _t = _d.today()

    return ({
        "month_label": label,
        # Same stamp string the marketing one-pager builds.
        "updated_stamp": (f"UPDATED · {_t.strftime('%B')} "
                          f"{_ordinal(_t.day)}, {_t.year}"),
        "period": f"{ws.day:02d} {abbr} – {we.day:02d} {we.strftime('%b').upper()} {we.year}",
        "rows": top10,
    }, str(s.get("month") or "month"))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="AFTS FSIS monthly social cards — top 10, no counts.")
    ap.add_argument("--summary", required=True,
                    help="Path to monthly-summary-latest.json")
    ap.add_argument("--out-dir", required=True,
                    help="Directory to write <month>-<platform>.pdf into")
    ap.add_argument("--formats", default="linkedin,instagram,x",
                    help="Comma-separated subset of: linkedin, instagram, x")
    args = ap.parse_args()

    data, tag = load(args.summary)
    os.makedirs(args.out_dir, exist_ok=True)

    written = []
    for fmt in [f.strip() for f in args.formats.split(",") if f.strip()]:
        if fmt not in FORMATS:
            raise SystemExit(f"unknown format {fmt!r}; "
                             f"choose from {sorted(FORMATS)}")
        out = os.path.join(args.out_dir, f"{tag}-{fmt}.pdf")
        render_social_pdf(out, fmt, data)
        w, h = FORMATS[fmt]["size"]
        written.append(out)
        print(f"wrote {out}  ({int(w)}x{int(h)} pt, "
              f"{FORMATS[fmt]['cols']} column(s))")

    print(f"\n{len(written)} card(s) · {len(data['rows'][:10])} incidents · "
          f"no counts rendered (by design)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
