#!/usr/bin/env python3
"""
build_monthly_marketing.py
==========================
Renders the single-page FSIS marketing (lead-magnet) PDF in the canonical
March/April 2026 layout. One entry point: `render_marketing_pdf(out_path, m)`.

Layout contract:
  · header: tiny letter-spaced AFTS line + "FOOD SAFETY INTELLIGENCE SYSTEM"
            + huge MONTH YEAR (navy)
  · orange accent line
  · meta strip (six cells: title, period, recalls, tier-1, outbreaks, leading)
  · three large stat tiles
  · "§ TOP N CRITICAL INCIDENTS · MONTH YEAR" section bar
  · top-N table — black/navy SOURCE labels, orange "view →" link,
                  black/navy OUTBREAK labels (NOT orange)
  · navy two-column footer (AFTS bold left · two-line tagline right)

Source/Outbreak labels are intentionally black-ink (deviation from earlier
March prototype). All other styling matches March.
"""

from __future__ import annotations
import os
import re
from typing import List, Dict, Optional, TypedDict

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import HexColor
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# =============================================================================
# CONSTANTS
# =============================================================================

# Sampled from the canonical March 2026 PDF
ORANGE = HexColor("#E8601A")   # AFTS brand
NAVY   = HexColor("#111827")   # title / footer / source / outbreak
INK    = HexColor("#1F2937")   # body text
MUTED  = HexColor("#6B7280")   # secondary labels
LINE   = HexColor("#E5E7EB")   # row dividers
BAND   = HexColor("#F3F4F6")   # section header / meta strip / table head
ALT    = HexColor("#F9FAFB")   # subtle alt row
WHITE  = HexColor("#FFFFFF")

H_MONO = "Courier"

PAGE_W, PAGE_H = A4
MARGIN_L = 28
MARGIN_R = 28
CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R


# =============================================================================
# EMBEDDED BODY FONT REGISTRATION
# =============================================================================
#
# Strategy: register a Unicode TTF family at module import and use it as the
# primary body family. Embedding the font into the PDF means every viewer —
# Acrobat, Preview, Chrome, Wix iframe, mobile, low-spec readers — renders the
# document identically. Without embedding, PDF-core "Helvetica" gets
# substituted on systems missing it, and Times Roman is the common fallback
# (which is why earlier exports looked serif on some machines).
#
# Family preference, in order:
#   1. Liberation Sans   — metric-compatible with Helvetica/Arial → existing
#                          column coordinates and width math just work; full
#                          Greek + extended-Latin coverage; embeddable.
#   2. DejaVu Sans       — wider than Helvetica (would shift the layout) but
#                          present on the GH-Actions runner via fonts-dejavu-core.
#   3. Helvetica core    — last-resort fallback; NOT embedded, may render as
#                          Times on viewers without Helvetica installed.

_FONT_FAMILIES = [
    # (family_label, regular_paths, bold_paths, italic_paths)
    ("Liberation",
        [
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/liberation/LiberationSans-Regular.ttf",
            "/Library/Fonts/LiberationSans-Regular.ttf",
        ],
        [
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/liberation/LiberationSans-Bold.ttf",
            "/Library/Fonts/LiberationSans-Bold.ttf",
        ],
        [
            "/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf",
            "/usr/share/fonts/liberation/LiberationSans-Italic.ttf",
            "/Library/Fonts/LiberationSans-Italic.ttf",
        ]),
    ("DejaVu",
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans.ttf",
            "/Library/Fonts/DejaVuSans.ttf",
            "C:/Windows/Fonts/DejaVuSans.ttf",
        ],
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
            "/Library/Fonts/DejaVuSans-Bold.ttf",
            "C:/Windows/Fonts/DejaVuSans-Bold.ttf",
        ],
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans-Oblique.ttf",
            "/Library/Fonts/DejaVuSans-Oblique.ttf",
            "C:/Windows/Fonts/DejaVuSans-Oblique.ttf",
        ]),
]


def _register_body_family():
    """Register the first available family. Returns dict with regular/bold/italic font names."""
    for label, reg_paths, bold_paths, ital_paths in _FONT_FAMILIES:
        reg_path  = next((p for p in reg_paths  if os.path.exists(p)), None)
        bold_path = next((p for p in bold_paths if os.path.exists(p)), None)
        if not (reg_path and bold_path):
            continue
        try:
            reg_name  = label
            bold_name = f"{label}-Bold"
            ital_name = f"{label}-Italic"
            registered = pdfmetrics.getRegisteredFontNames()
            if reg_name not in registered:
                pdfmetrics.registerFont(TTFont(reg_name, reg_path))
            if bold_name not in registered:
                pdfmetrics.registerFont(TTFont(bold_name, bold_path))
            ital_resolved = reg_name  # fallback to regular if italic TTF missing
            ital_path = next((p for p in ital_paths if os.path.exists(p)), None)
            if ital_path:
                if ital_name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(ital_name, ital_path))
                ital_resolved = ital_name
            return {"regular": reg_name, "bold": bold_name, "italic": ital_resolved}
        except Exception:
            continue
    return {}


_BODY = _register_body_family()

if _BODY:
    H_REG  = _BODY["regular"]    # "Liberation" or "DejaVu"
    H_BOLD = _BODY["bold"]       # "Liberation-Bold" or "DejaVu-Bold"
    H_ITAL = _BODY["italic"]     # "Liberation-Italic" or "DejaVu-Italic" (or regular fallback)
else:
    # Last-resort fallback — PDF-core Helvetica. NOT embedded. May substitute
    # to Times on viewers missing Helvetica. Production hosts should always
    # have Liberation or DejaVu installed.
    H_REG  = "Helvetica"
    H_BOLD = "Helvetica-Bold"
    H_ITAL = "Helvetica-Oblique"


# =============================================================================
# PATHOGEN ABBREVIATION (matches March convention)
# =============================================================================

# Genera that should be displayed by genus only (per March style)
_GENUS_ONLY = {"Listeria", "Salmonella", "Campylobacter", "Cronobacter", "Vibrio", "Yersinia"}

# Explicit overrides.
# Audit 2026-05-15: STEC variants previously had only three entries
# ("E. coli O157:H7", "Shigatoxin-producing E. coli", "Escherichia coli")
# and the source feed emits at least nine distinct surface forms (FR/EN,
# with and without parens, with and without serogroup). Variants that
# slipped through showed in the marketing PDF as separate rows
# ("Shiga toxin-producing E. coli (STEC)" alongside "E. coli / STEC")
# instead of folding to one tile. Every known surface form is now mapped;
# the regex catch-all in abbreviate_pathogen() backstops future variants.
_PATHOGEN_OVERRIDES = {
    "Listeria monocytogenes":   "Listeria",
    "Salmonella enterica":      "Salmonella",
    "Salmonella spp.":          "Salmonella",
    "Clostridium botulinum":    "C. botulinum",
    "Clostridium perfringens":  "C. perfringens",
    # E. coli / STEC family — every known surface form folds to one label.
    # Bare "Escherichia coli" / "E. coli" map to plain "E. coli" because
    # those forms can refer to non-pathogenic hygiene-indicator counts.
    "Escherichia coli":              "E. coli",
    "Escherichia coli (generic)":    "E. coli",
    "E. coli":                       "E. coli",
    "E. coli (generic)":             "E. coli",
    # STEC variants → "E. coli / STEC"
    "E. coli O157:H7":                                  "E. coli / STEC",
    "E. coli O157":                                     "E. coli / STEC",
    "E. coli O26 (STEC)":                               "E. coli / STEC",
    "E. coli O26":                                      "E. coli / STEC",
    "E. coli O145 (STEC)":                              "E. coli / STEC",
    "E. coli O145":                                     "E. coli / STEC",
    "E. coli O103 (STEC)":                              "E. coli / STEC",
    "E. coli O111 (STEC)":                              "E. coli / STEC",
    "E. coli O121 (STEC)":                              "E. coli / STEC",
    "E. coli STEC":                                     "E. coli / STEC",
    "E. coli STEC (Shiga toxin-producing)":             "E. coli / STEC",
    "STEC":                                             "E. coli / STEC",
    "STEC (Shiga toxin-producing E. coli)":             "E. coli / STEC",
    "Shiga toxin-producing E. coli":                    "E. coli / STEC",
    "Shiga toxin-producing E. coli (STEC)":             "E. coli / STEC",
    "Shigatoxin producing Escherichia coli":            "E. coli / STEC",
    "Shigatoxin producing Escherichia coli (STEC)":     "E. coli / STEC",
    "Shigatoxin-producing E. coli":                     "E. coli / STEC",
    "Shigatoxin-producing Escherichia coli":            "E. coli / STEC",
    "Shigatoxin-producing Escherichia coli (STEC)":     "E. coli / STEC",
    "Escherichia coli STEC":                            "E. coli / STEC",
    "Escherichia coli shiga toxinogène":                "E. coli / STEC",
    "Escherichia coli shiga toxinogène (STEC)":         "E. coli / STEC",
    "Bacillus cereus":          "B. cereus",
    "Bacillus cereus / cereulide": "B. cereus",
    "Bacillus cereus / Cereulide": "B. cereus",
    "Staphylococcus aureus":    "S. aureus",
}

# Regex catch-all for STEC surface forms NOT in the override dict.
# Any string containing 'stec' / 'shiga[ -]?toxin' / 'shigatoxin' /
# 'shigatoxinogène' / 'vtec' (case-insensitive) maps to "E. coli / STEC"
# without us having to enumerate every possible phrasing.
_STEC_REGEX = re.compile(
    r"\b(stec|shiga[\s\-]?toxin|shigatoxin|shigatoxinogène|vtec)\b",
    re.I,
)


def abbreviate_pathogen(name: str) -> str:
    """Apply March-style abbreviation: genus-only or first-initial.species.

    Audit 2026-05-15: added regex backstop for unmapped STEC variants
    so the marketing PDF never shows two rows for the same hazard.
    """
    if not name:
        return ""
    name = name.strip()
    if name in _PATHOGEN_OVERRIDES:
        return _PATHOGEN_OVERRIDES[name]
    # STEC catch-all — fold anything mentioning STEC / shiga-toxin / VTEC
    # to the canonical "E. coli / STEC" label. Excludes bare "Escherichia
    # coli" / "E. coli" (those are non-pathogenic hygiene indicators and
    # don't match this regex anyway).
    if _STEC_REGEX.search(name):
        return "E. coli / STEC"
    parts = name.split()
    # Single-word genus already
    if len(parts) == 1:
        return parts[0]
    # Genus that should display alone
    if parts[0] in _GENUS_ONLY:
        return parts[0]
    # Two-word "Genus species" → abbreviate genus
    return f"{parts[0][0]}. {' '.join(parts[1:])}"


def extract_leading_genus(name: str) -> str:
    """For the meta strip 'LEADING:' label — uppercase genus only."""
    if not name:
        return ""
    return name.strip().split()[0].upper()


# =============================================================================
# DATA SHAPE
# =============================================================================

class IncidentRow(TypedDict, total=False):
    date: str         # ISO "YYYY-MM-DD"
    pathogen: str     # full Latin name; will be auto-abbreviated
    outbreak: bool    # True → "OUTBREAK" tag below pathogen
    company: str      # may contain Greek / extended-Latin
    country: str
    product: str      # long; will wrap
    source: str       # agency label, e.g. "FDA", "EFET (GR)", "RappelConso (FR)"
    url: str          # source URL — wired into clickable "view →" link annotation


class MonthData(TypedDict, total=False):
    month_tag:        str    # "APRIL 2026"
    period_line:      str    # "01 APR – 30 APR 2026"
    total_recalls:    int    # 236
    tier1:            int    # 198
    outbreaks:        int    # 6
    leading_pathogen: str    # full name → genus extracted automatically
    section_title:    str    # OPTIONAL — autogenerated if omitted
    rows:             List[IncidentRow]   # length = 9, 10, etc.


# =============================================================================
# DRAWING HELPERS
# =============================================================================

def _draw_letter_spaced(c, x, y, text, font, size, color, tracking=1.4):
    c.setFont(font, size)
    c.setFillColor(color)
    cx = x
    for ch in text:
        c.drawString(cx, y, ch)
        cx += c.stringWidth(ch, font, size) + tracking


def _text_w(text, font, size):
    return pdfmetrics.stringWidth(text, font, size)


def _wrap(text, font, size, max_w):
    out = []
    for paragraph in text.split("\n"):
        words = paragraph.split(" ")
        if not words:
            out.append("")
            continue
        line = words[0]
        for w in words[1:]:
            test = line + " " + w
            if _text_w(test, font, size) <= max_w:
                line = test
            else:
                out.append(line)
                line = w
        out.append(line)
    return out


# =============================================================================
# MAIN RENDERER
# =============================================================================

def render_marketing_pdf(out_path: str, m: MonthData) -> str:
    """Render the marketing PDF for one month. Returns out_path."""

    section_title = m.get("section_title") or \
                    f"§ TOP {len(m['rows'])} CRITICAL INCIDENTS · {m['month_tag']}"
    leading = extract_leading_genus(m["leading_pathogen"])

    c = canvas.Canvas(out_path, pagesize=A4)
    c.setTitle(f"FSIS · Monthly Pathogen Surveillance · {m['month_tag']}")
    c.setAuthor("Advanced Food-Tech Solutions")

    y = PAGE_H

    # -------- HEADER ----------------------------------------------------------
    y -= 32
    _draw_letter_spaced(c, MARGIN_L, y,
                        "ADVANCED FOOD-TECH SOLUTIONS · AFTS",
                        H_BOLD, 7.2, NAVY, tracking=1.6)
    y -= 22
    c.setFont(H_BOLD, 14); c.setFillColor(NAVY)
    c.drawString(MARGIN_L, y, "FOOD SAFETY INTELLIGENCE SYSTEM")
    y -= 38
    c.setFont(H_BOLD, 32); c.setFillColor(NAVY)
    c.drawString(MARGIN_L, y, m["month_tag"])

    y -= 16
    c.setStrokeColor(ORANGE); c.setLineWidth(2.2)
    c.line(MARGIN_L, y, PAGE_W - MARGIN_R, y)
    y -= 12

    # -------- META STRIP ------------------------------------------------------
    meta_h = 30
    meta_y = y - meta_h
    c.setFillColor(BAND)
    c.rect(MARGIN_L, meta_y, CONTENT_W, meta_h, fill=1, stroke=0)

    cells = [
        ("Monthly Pathogen Surveillance ·", m["month_tag"].title(), 0.00),
        (m["period_line"],                  "",                     0.27),
        (str(m["total_recalls"]),           "RECALLS",              0.43),
        (str(m["tier1"]),                   "TIER-1",               0.53),
        (str(m["outbreaks"]),               "OUTBREAKS",            0.63),
        ("LEADING:",                        leading,                0.78),
    ]
    nums = {str(m["total_recalls"]), str(m["tier1"]), str(m["outbreaks"])}
    for top_text, bot_text, frac_x in cells:
        cx = MARGIN_L + CONTENT_W * frac_x + 8
        c.setFont(H_BOLD, 8.5 if top_text in nums else 7.2)
        c.setFillColor(NAVY)
        c.drawString(cx, meta_y + meta_h / 2 + 1.5, top_text)
        if bot_text:
            c.setFont(H_BOLD, 7.2)
            c.setFillColor(NAVY if bot_text == leading else MUTED)
            c.drawString(cx, meta_y + meta_h / 2 - 8, bot_text)
    y = meta_y - 18

    # -------- THREE STAT TILES -----------------------------------------------
    tile_h = 86
    tile_y = y - tile_h
    third = CONTENT_W / 3.0
    c.setStrokeColor(LINE); c.setLineWidth(0.6)
    c.line(MARGIN_L + third,     tile_y + 12, MARGIN_L + third,     tile_y + tile_h - 12)
    c.line(MARGIN_L + 2 * third, tile_y + 12, MARGIN_L + 2 * third, tile_y + tile_h - 12)

    for i, (num, lbl) in enumerate([
        (str(m["total_recalls"]), "TOTAL RECALLS"),
        (str(m["tier1"]),         "TIER-1 CRITICAL"),
        (str(m["outbreaks"]),     "OUTBREAKS"),
    ]):
        cx = MARGIN_L + third * i + 12
        c.setFont(H_BOLD, 46); c.setFillColor(NAVY)
        c.drawString(cx, tile_y + 28, num)
        c.setFont(H_BOLD, 8.5); c.setFillColor(MUTED)
        c.drawString(cx + 3, tile_y + 14, lbl)
    y = tile_y - 18

    # -------- SECTION HEADER STRIP -------------------------------------------
    sec_h = 22
    sec_y = y - sec_h
    c.setFillColor(BAND)
    c.rect(MARGIN_L, sec_y, CONTENT_W, sec_h, fill=1, stroke=0)
    c.setFont(H_BOLD, 8.5); c.setFillColor(NAVY)
    c.drawString(MARGIN_L + 8, sec_y + sec_h / 2 - 3, section_title)
    y = sec_y - 6

    # -------- TABLE -----------------------------------------------------------
    col_x = [
        MARGIN_L + 0,
        MARGIN_L + 26,
        MARGIN_L + 95,
        MARGIN_L + 215,
        MARGIN_L + 365,
        PAGE_W - MARGIN_R - 78,
    ]
    col_right = PAGE_W - MARGIN_R
    col_widths = [
        col_x[1] - col_x[0] - 6,
        col_x[2] - col_x[1] - 6,
        col_x[3] - col_x[2] - 6,
        col_x[4] - col_x[3] - 8,
        col_x[5] - col_x[4] - 10,
        col_right - col_x[5] - 4,
    ]

    # table header
    th_h = 22
    th_y = y - th_h
    c.setFont(H_BOLD, 7.2); c.setFillColor(MUTED)
    headers = ["#", "DATE", "PATHOGEN", "COMPANY / BRAND", "PRODUCT", "SOURCE"]
    for i, h in enumerate(headers):
        if i == 5:
            w = _text_w(h, H_BOLD, 7.2)
            c.drawString(col_right - w - 2, th_y + th_h / 2 - 2.5, h)
        else:
            c.drawString(col_x[i] + (1 if i == 0 else 0), th_y + th_h / 2 - 2.5, h)
    c.setStrokeColor(LINE); c.setLineWidth(0.5)
    c.line(MARGIN_L, th_y, PAGE_W - MARGIN_R, th_y)
    y = th_y - 4

    # data rows
    PAD_TOP = 5
    PAD_BOT = 5
    PRODUCT_FONT_SIZE = 7.5
    PRODUCT_LEADING = 9.0
    COMPANY_FONT_SIZE = 8.2
    COMPANY_LEADING = 10.0

    for idx, row in enumerate(m["rows"], start=1):
        date     = row["date"]
        pathogen = abbreviate_pathogen(row["pathogen"])
        flag     = "OUTBREAK" if row.get("outbreak") else None
        company  = row["company"]
        country  = row.get("country", "")
        product  = row.get("product", "")
        source   = row["source"]

        prod_lines = _wrap(product, H_REG, PRODUCT_FONT_SIZE, col_widths[4])
        comp_lines = _wrap(company, H_BOLD, COMPANY_FONT_SIZE, col_widths[3])
        src_lines  = _wrap(source, H_BOLD, 8, col_widths[5])

        n_path = 1 + (1 if flag else 0)
        max_h  = max(
            len(prod_lines) * PRODUCT_LEADING,
            (len(comp_lines) + 1) * COMPANY_LEADING,
            n_path * 10,
            (len(src_lines) + 1) * 9.5,
            18,
        )
        row_h = max_h + PAD_TOP + PAD_BOT
        row_y = y - row_h

        if idx % 2 == 0:
            c.setFillColor(ALT)
            c.rect(MARGIN_L, row_y, CONTENT_W, row_h, fill=1, stroke=0)

        c.setStrokeColor(LINE); c.setLineWidth(0.4)
        c.line(MARGIN_L, row_y, PAGE_W - MARGIN_R, row_y)

        text_top_y = y - PAD_TOP - 8

        # #
        c.setFont(H_BOLD, 11); c.setFillColor(NAVY)
        c.drawString(col_x[0] + 2, text_top_y, str(idx))

        # DATE
        c.setFont(H_MONO, 7.5); c.setFillColor(INK)
        c.drawString(col_x[1], text_top_y, date)

        # PATHOGEN (italic) + T1 (black bold) + optional OUTBREAK (black bold)
        c.setFont(H_ITAL, 8.5); c.setFillColor(INK)
        c.drawString(col_x[2], text_top_y, pathogen)
        c.setFont(H_BOLD, 7.5); c.setFillColor(NAVY)
        c.drawString(col_x[2] + _text_w(pathogen, H_ITAL, 8.5) + 5, text_top_y, "T1")
        if flag:
            c.setFont(H_BOLD, 7); c.setFillColor(NAVY)   # OUTBREAK in black
            c.drawString(col_x[2], text_top_y - 11, flag)

        # COMPANY / BRAND (+ country muted)
        cy = text_top_y
        c.setFont(H_BOLD, COMPANY_FONT_SIZE); c.setFillColor(INK)
        for ln in comp_lines:
            c.drawString(col_x[3], cy, ln); cy -= COMPANY_LEADING
        c.setFont(H_REG, 7.2); c.setFillColor(MUTED)
        c.drawString(col_x[3], cy, country)

        # PRODUCT
        py = text_top_y
        c.setFont(H_REG, PRODUCT_FONT_SIZE); c.setFillColor(INK)
        for ln in prod_lines:
            c.drawString(col_x[4], py, ln); py -= PRODUCT_LEADING

        # SOURCE — black bold, right-aligned + orange "view →"
        sy = text_top_y
        c.setFont(H_BOLD, 8); c.setFillColor(NAVY)
        for ln in src_lines:
            w = _text_w(ln, H_BOLD, 8)
            c.drawString(col_right - w - 2, sy, ln); sy -= 10
        c.setFont(H_REG, 7.5); c.setFillColor(ORANGE)
        view_text = "view →"
        vw = _text_w(view_text, H_REG, 7.5)
        view_x = col_right - vw - 2
        view_y = sy - 1
        c.drawString(view_x, view_y, view_text)
        # subtle orange underline so "view →" reads as a link
        c.setStrokeColor(ORANGE); c.setLineWidth(0.5)
        c.line(view_x, view_y - 1.2, view_x + vw, view_y - 1.2)

        # Clickable annotation — covers the entire SOURCE column for this row,
        # so clicking anywhere on the agency label or "view →" opens the recall.
        url = (row.get("url") or "").strip()
        if url:
            c.linkURL(
                url,
                (col_x[5] - 4, row_y, col_right, y),
                relative=0,
                thickness=0,
            )

        y = row_y

    # -------- FOOTER ----------------------------------------------------------
    foot_h = 46
    c.setFillColor(NAVY); c.rect(0, 0, PAGE_W, foot_h, fill=1, stroke=0)
    c.setFillColor(WHITE)
    _draw_letter_spaced(c, MARGIN_L, foot_h / 2 - 2,
                        "ADVANCED FOOD-TECH SOLUTIONS · AFTS",
                        H_BOLD, 7.0, WHITE, tracking=0.8)
    right_top = "Food Process Engineering · Thermal Processing · Regulatory Compliance"
    right_bot = "advfood.tech · info@advfood.tech · Athens, Greece"
    c.setFont(H_REG, 7.0); c.setFillColor(WHITE)
    c.drawString(PAGE_W - MARGIN_R - _text_w(right_top, H_REG, 7.0), foot_h / 2 + 4, right_top)
    c.drawString(PAGE_W - MARGIN_R - _text_w(right_bot, H_REG, 7.0), foot_h / 2 - 8, right_bot)

    c.showPage()
    c.save()
    return out_path


# =============================================================================
# JSON ADAPTER  (monthly-summary-latest.json  →  MonthData)
# =============================================================================
#
# Reads the canonical FSIS summary JSON (produced by docs/build_monthly_report_afts.py)
# and adapts it to the MonthData shape consumed by render_marketing_pdf().
#
# Required JSON keys:
#   month         "2026-M04"           — used for output filename
#   month_name    "April"              — used for big header (uppercased)
#   year          2026
#   window_start  "2026-04-01" (ISO)
#   window_end    "2026-04-30" (ISO)
#   stats         { total, tier1, outbreaks, ... }
#   leading_pathogen { name, ... }
#   top10         [ {date, pathogen, pathogen_raw, company, brand, country,
#                    product, source, tier, outbreak, url}, ... × 10 ]
#
# top10 is preferred over top_threats because top10 has the `source` field
# already populated and contains 10 entries (top_threats has 5 and no source).
# =============================================================================

def _load_summary_json(summary_path: str):
    """Load monthly-summary-latest.json → (MonthData, file_tag)."""
    import json
    from datetime import date

    with open(summary_path, encoding="utf-8") as f:
        s = json.load(f)

    month_name = s["month_name"]                       # "April"
    year       = s["year"]                             # 2026
    file_tag   = s["month"]                            # "2026-M04" (filename stem)
    pretty_tag = f"{month_name.upper()} {year}"        # "APRIL 2026" (big header)

    ws = date.fromisoformat(s["window_start"])
    we = date.fromisoformat(s["window_end"])
    abbr = month_name[:3].upper()                      # "APR"
    period_line = f"{ws.strftime('%d')} {abbr} – {we.strftime('%d')} {abbr} {year}"

    leading = (s.get("leading_pathogen") or {}).get("name", "")

    rows: List[IncidentRow] = []
    # Prefer top10 (10 entries, has `source`). Fall back to top_threats only if missing.
    top_list = s.get("top10") or s.get("top_threats") or []
    for it in top_list:
        rows.append({
            "date":     it.get("date", ""),
            "pathogen": it.get("pathogen_raw") or it.get("pathogen", ""),
            "outbreak": bool(it.get("outbreak")),
            "company":  it.get("company") or "",
            "country":  it.get("country") or "",
            "product":  it.get("product") or "",
            "source":   it.get("source") or "",
            "url":      it.get("url") or "",
        })

    md: MonthData = {
        "month_tag":        pretty_tag,
        "period_line":      period_line,
        "total_recalls":    int((s.get("stats") or {}).get("total",     s.get("total", 0))),
        "tier1":            int((s.get("stats") or {}).get("tier1",     s.get("tier1", 0))),
        "outbreaks":        int((s.get("stats") or {}).get("outbreaks", s.get("outbreaks", 0))),
        "leading_pathogen": leading,
        "rows":             rows,
    }
    return md, file_tag


# =============================================================================
# CLI ENTRY POINT
# =============================================================================
#
# Invoked by .github/workflows/afts-monthly-report.yml as:
#   python -m pipeline.build_monthly_marketing \
#     --summary docs/data/monthly-summary-latest.json \
#     --out-dir docs/marketing
#
# Output: <out-dir>/<month>-marketing.pdf  (e.g. docs/marketing/2026-M04-marketing.pdf)
# This filename is what pipeline/set_pdf_urls.py looks for when wiring
# pdf_url into monthly-index.json (hub.html consumes that field).
# =============================================================================

if __name__ == "__main__":
    import argparse
    import pathlib
    import sys

    p = argparse.ArgumentParser(
        description="Render the FSIS monthly marketing one-pager PDF (lead magnet).",
    )
    p.add_argument("--summary", required=True,
                   help="Path to monthly-summary-latest.json")
    p.add_argument("--out-dir", required=True,
                   help="Directory to write <month>-marketing.pdf into")
    args = p.parse_args()

    if not os.path.exists(args.summary):
        print(f"ERROR: summary file not found: {args.summary}", file=sys.stderr)
        sys.exit(2)

    md, file_tag = _load_summary_json(args.summary)

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = str(out_dir / f"{file_tag}-marketing.pdf")

    render_marketing_pdf(out_path, md)
    print(f"wrote {out_path}")
