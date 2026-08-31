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
spec exactly. Each card is written TWICE: a PDF and a PNG.

    PDF   LinkedIn document post; keeps a clickable link annotation on every
          incident row. Also the archival copy.
    PNG   what you actually upload to Instagram and X — neither accepts a
          PDF. Rendered at --png-scale (default 2x → 2160px on a 1080pt card).

A PNG cannot hold a clickable link, so <month>-caption.txt is written
alongside carrying all ten source URLs for pasting into the post body.

Brand primitives (colours, embedded font family, pathogen abbreviation) are
IMPORTED from build_monthly_marketing rather than copied. A second private
copy of the palette is how the hazard table drifted into four versions.

USAGE
    python -m pipeline.build_monthly_social \\
        --summary docs/data/monthly-summary-latest.json \\
        --out-dir docs/social
    # writes <tag>-{linkedin,instagram,x}.{pdf,png} + <tag>-caption.txt
"""
from __future__ import annotations

import argparse
import json
import os
import sys
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


# Column geometry for the one-line row. Fractions apply to the width left
# over after the fixed-width rank / date / flag columns are taken out.
_RANK_W  = 26.0      # right-aligned numeral
_DATE_W  = 58.0      # "18 JUL"
_FLAG_W  = 38.0      # OUTBREAK dot + T1 marker
_TAIL_W  = 178.0     # "United Kingdom · RappelConso (FR)" needs the room
_VIEW_W  = 46.0      # "view →"
_GAP     = 8.0
# Product is the column a reader actually scans, so it gets the largest
# share. The first cut gave pathogen and company 30% each and clipped
# "Fresh jalapeno peppers grown in Sin..." on a 1080pt-wide card.
_FR_PATHOGEN = 0.23
_FR_COMPANY  = 0.31
_FR_PRODUCT  = 0.46


def _row_columns(x: float, col_w: float, s: float):
    """Left edges + widths for one incident row, all on a single baseline.

    2026-08-31 — this replaced a FOUR-LINE block per incident (date+pathogen,
    company, product, country·source stacked over 82pt). Ten incidents that
    way is forty lines of ragged left edges with nothing aligning down the
    card, and it cost the whole height of the 4:5 portrait. One incident is
    now one row of aligned columns, which is what a register looks like.
    """
    rank_x = x
    date_x = x + _RANK_W + _GAP
    flag_x = date_x + _DATE_W + _GAP
    body_x = flag_x + _FLAG_W + _GAP
    right  = x + col_w
    view_x = right - _VIEW_W * s
    tail_x = view_x - _GAP * s - _TAIL_W * s
    body_w = tail_x - body_x - _GAP * s
    return {
        "rank": (rank_x, _RANK_W * s),
        "date": (date_x, _DATE_W * s),
        "flag": (flag_x, _FLAG_W * s),
        "pathogen": (body_x, body_w * _FR_PATHOGEN),
        "company":  (body_x + body_w * _FR_PATHOGEN, body_w * _FR_COMPANY),
        "product":  (body_x + body_w * (_FR_PATHOGEN + _FR_COMPANY),
                     body_w * _FR_PRODUCT),
        "tail": (tail_x, _TAIL_W * s),
        "view": (view_x, _VIEW_W * s),
        "right": right,
    }


def _draw_col_headers(c, x: float, y: float, col_w: float, s: float) -> float:
    """Thin column key above the rows, so the aligned columns read as a table."""
    cols = _row_columns(x, col_w, s)
    c.setFont(H_MONO, 8.5 * s)
    c.setFillColor(MUTED)
    for key, label in (("date", "DATE"), ("pathogen", "HAZARD"),
                       ("company", "COMPANY"), ("product", "PRODUCT"),
                       ("tail", "COUNTRY · SOURCE")):
        cx, cw = cols[key]
        c.drawString(cx, y, _clip(label, H_MONO, 8.5 * s, cw))
    c.setStrokeColor(LINE)
    c.setLineWidth(0.9 * s)
    c.line(x, y - 7 * s, x + col_w, y - 7 * s)
    return y - 7 * s


def _draw_row(c, x: float, y: float, col_w: float, s: float,
              n: int, row: Dict[str, Any], rule: bool = True,
              pitch: float = 0.0) -> float:
    """ONE incident on ONE row. Returns the y cursor below it.

    The whole row is a link to the source notice — see the annotation at the
    bottom. Every incident on this card is a real regulatory action and the
    reader has to be able to get to the notice itself; a card that names a
    company and a pathogen with no way through to the source is an assertion,
    not intelligence.
    """
    pathogen = abbreviate_pathogen(row.get("pathogen_raw")
                                   or row.get("pathogen") or "")
    company = display_company(row)
    product = str(row.get("product") or "")
    country = str(row.get("country") or "")
    source = str(row.get("source") or "")
    outbreak = bool(row.get("outbreak"))
    tier1 = str(row.get("tier") or "") == "1"
    url = str(row.get("url") or "").strip()

    cols = _row_columns(x, col_w, s)
    band = pitch if pitch > 0 else 34.0 * s
    base = y - band * 0.55          # single shared baseline
    fs = 12.5 * s                   # one body size for the whole row

    # Rank
    rx, rw = cols["rank"]
    c.setFont(H_BOLD, 15 * s)
    c.setFillColor(ORANGE)
    c.drawRightString(rx + rw, base, str(n))

    # Date
    dx, dw = cols["date"]
    c.setFont(H_MONO, 10.5 * s)
    c.setFillColor(MUTED)
    c.drawString(dx, base, _clip(_fmt_date(row.get("date")), H_MONO, 10.5 * s, dw))

    # Flags — compact markers, not full pills; a pill per row on ten rows
    # eats the width the product name needs.
    fx, fw = cols["flag"]
    gx = fx
    if outbreak:
        c.setFillColor(ORANGE)
        c.circle(gx + 4 * s, base + 3.5 * s, 4 * s, fill=1, stroke=0)
        gx += 13 * s
    if tier1:
        c.setFillColor(NAVY)
        cw = _text_w("T1", H_BOLD, 8.5 * s) + 8 * s
        c.rect(gx, base - 2 * s, cw, 13 * s, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont(H_BOLD, 8.5 * s)
        c.drawString(gx + 4 * s, base + 1.5 * s, "T1")

    # Hazard
    px, pw = cols["pathogen"]
    c.setFont(H_BOLD, fs)
    c.setFillColor(NAVY)
    c.drawString(px, base, _clip(pathogen, H_BOLD, fs, pw - _GAP * s))

    # Company
    cx, cw2 = cols["company"]
    c.setFont(H_BOLD, fs)
    c.setFillColor(INK)
    c.drawString(cx, base, _clip(company, H_BOLD, fs, cw2 - _GAP * s))

    # Product
    prx, prw = cols["product"]
    c.setFont(H_REG, fs)
    c.setFillColor(MUTED)
    c.drawString(prx, base, _clip(product, H_REG, fs, prw - _GAP * s))

    # Country · source
    tx, tw = cols["tail"]
    tail = " · ".join([p for p in (country, source) if p])
    c.setFont(H_MONO, 10 * s)
    c.setFillColor(NAVY)
    c.drawString(tx, base, _clip(tail, H_MONO, 10 * s, tw))

    # "view →" affordance + underline, mirroring the marketing one-pager.
    if url:
        vx, vw = cols["view"]
        c.setFont(H_REG, 9.5 * s)
        c.setFillColor(ORANGE)
        vtext = "view →"
        c.drawString(vx, base, vtext)
        c.setStrokeColor(ORANGE)
        c.setLineWidth(0.6 * s)
        w = _text_w(vtext, H_REG, 9.5 * s)
        c.line(vx, base - 2 * s, vx + w, base - 2 * s)

    y_bottom = y - band
    # Clickable annotation across the ENTIRE row.
    if url:
        c.linkURL(url, (x, y_bottom, cols["right"], y), relative=0, thickness=0)

    if rule:
        c.setStrokeColor(LINE)
        c.setLineWidth(0.7 * s)
        c.line(x, y_bottom, x + col_w, y_bottom)
    return y_bottom


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

    # Row pitch for a ONE-LINE row. The old block row was 82pt deep and the
    # pitch floor was 94pt; a single line needs a fraction of that. Clamped at
    # both ends and the block centred in the space left over, so the table
    # neither crushes nor floats to the top of a tall card.
    def _pitch(avail: float, n: int) -> float:
        # Fill the height. A 58pt cap left a 320pt dead band under the column
        # headers on the 4:5 portrait and an equal one above the footer, which
        # reads as a broken render rather than a spacious one. Text is centred
        # inside each band, so a tall band is airy, not empty.
        return min(max(avail / max(n, 1), 30.0 * s), 130.0 * s)

    foot_h = min(46.0 * s * 1.8, H * 0.075)

    if cols == 1:
        col_w = W - 2 * margin
        hdr_y = _draw_col_headers(c, margin, y0 - 12 * s, col_w, s)
        avail = hdr_y - (foot_h + 30 * s)
        pitch = _pitch(avail, len(rows))
        # Start directly under the headers. Centring the block split the
        # leftover height into a gap above AND below, which is what made the
        # 16:9 card look like a failed render.
        y = hdr_y - 4 * s
        for i, r in enumerate(rows, 1):
            y = _draw_row(c, margin, y, col_w, s, i, r, rule=True, pitch=pitch)
    else:
        gutter = 40 * s
        col_w = (W - 2 * margin - gutter) / 2.0
        half = (len(rows) + 1) // 2
        hdr_ys = []
        for ci in range(2):
            x = margin + ci * (col_w + gutter)
            hdr_ys.append(_draw_col_headers(c, x, y0 - 12 * s, col_w, s))
        avail = hdr_ys[0] - (foot_h + 30 * s)
        pitch = _pitch(avail, half)
        for ci, chunk in enumerate((rows[:half], rows[half:])):
            x = margin + ci * (col_w + gutter)
            y = hdr_ys[ci] - 4 * s
            for j, r in enumerate(chunk):
                y = _draw_row(c, x, y, col_w, s, ci * half + j + 1, r,
                              rule=True, pitch=pitch)

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
    _t = _d.today()

    return ({
        "month_label": label,
        # Same stamp string the marketing one-pager builds — see the note
        # there. 2026-08-31: ordinal form dropped for "31 August 2026".
        "updated_stamp": f"UPDATED · {_t.strftime('%-d %B %Y')}",
        "period": f"{ws.day:02d} {abbr} – {we.day:02d} {we.strftime('%b').upper()} {we.year}",
        "rows": top10,
    }, str(s.get("month") or "month"))


def rasterise_png(pdf_path: str, png_path: str, scale: float = 2.0) -> str:
    """Render page 1 of the card PDF to PNG.

    2026-08-31 — this did not exist. The module docstring said "export to PNG
    at any scale and the framing is already right", but nothing exported
    anything: the builder emitted three PDFs and stopped. A PDF is a valid
    LinkedIn document post, so that one was postable by luck. Instagram feed
    posts accept image/video ONLY, and X will not render a PDF in-timeline,
    so two of the three cards could not actually be posted as delivered.

    pypdfium2 is preferred: it is a self-contained wheel, so CI needs no
    poppler/apt step. pdf2image is the fallback for environments that already
    have poppler. If neither is present we warn and leave the PDF — a missing
    PNG must never fail the monthly build.
    """
    try:
        import pypdfium2 as pdfium
        pdf = pdfium.PdfDocument(pdf_path)
        page = pdf[0]
        page.render(scale=scale).to_pil().save(png_path)
        return png_path
    except ImportError:
        pass
    except Exception as e:                                   # noqa: BLE001
        print(f"  pypdfium2 render failed ({e}) — trying pdf2image",
              file=sys.stderr)
    try:
        from pdf2image import convert_from_path
        imgs = convert_from_path(pdf_path, dpi=int(72 * scale))
        if imgs:
            imgs[0].save(png_path)
            return png_path
    except Exception as e:                                   # noqa: BLE001
        print(f"  PNG export unavailable ({e}); PDF written, PNG skipped",
              file=sys.stderr)
    return ""


def write_caption(path: str, data: Dict[str, Any], tag: str) -> str:
    """Ready-to-paste post caption carrying the ten source URLs.

    Instagram and X posts are images. An image cannot hold a clickable link,
    so the only place the source notices can live for those two platforms is
    the caption. The LinkedIn PDF keeps its per-row annotations as well; this
    file is the fallback that makes every card's sources reachable.
    """
    rows = data["rows"][:10]
    out = [
        f"AFTS Food Safety Intelligence System — {data['month_label']}",
        f"Top 10 critical incidents · {data['period']}",
        "",
        "Every incident below links to the originating regulatory notice.",
        "",
    ]
    for i, r in enumerate(rows, 1):
        company = display_company(r)
        pathogen = abbreviate_pathogen(r.get("pathogen_raw")
                                       or r.get("pathogen") or "")
        tail = " · ".join([p for p in (str(r.get("country") or ""),
                                       str(r.get("source") or "")) if p])
        out.append(f"{i}. {_fmt_date(r.get('date'))} · {pathogen} · "
                   f"{company} — {tail}")
        url = str(r.get("url") or "").strip()
        if url:
            out.append(f"   {url}")
    out += [
        "",
        "Full month, all records and methodology:",
        "https://fsis.advfood.tech",
        "",
        "#foodsafety #foodrecall #HACCP #foodmanufacturing #qualityassurance",
    ]
    text = "\n".join(out) + "\n"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(
        description="AFTS FSIS monthly social cards — top 10, no counts.")
    ap.add_argument("--summary", required=True,
                    help="Path to monthly-summary-latest.json")
    ap.add_argument("--out-dir", required=True,
                    help="Directory to write <month>-<platform>.pdf into")
    ap.add_argument("--formats", default="linkedin,instagram,x",
                    help="Comma-separated subset of: linkedin, instagram, x")
    ap.add_argument("--png-scale", type=float, default=2.0,
                    help="PNG raster scale (2.0 = 2160px wide for a 1080pt "
                         "card). 0 disables PNG export.")
    ap.add_argument("--no-caption", action="store_true",
                    help="Skip the <month>-caption.txt source-link file.")
    args = ap.parse_args()

    data, tag = load(args.summary)
    os.makedirs(args.out_dir, exist_ok=True)

    written, pngs = [], []
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
        # PNG is what actually gets posted to Instagram and X; the PDF is the
        # LinkedIn document post and the archival copy with live links.
        if args.png_scale > 0:
            png = os.path.join(args.out_dir, f"{tag}-{fmt}.png")
            if rasterise_png(out, png, args.png_scale):
                pngs.append(png)
                print(f"wrote {png}  ({int(w * args.png_scale)}x"
                      f"{int(h * args.png_scale)} px)")

    cap = ""
    if not args.no_caption:
        cap = write_caption(os.path.join(args.out_dir, f"{tag}-caption.txt"),
                            data, tag)
        print(f"wrote {cap}  (10 source links for the image posts)")

    print(f"\n{len(written)} card(s) + {len(pngs)} PNG(s) · "
          f"{len(data['rows'][:10])} incidents · "
          f"no counts rendered (by design)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
