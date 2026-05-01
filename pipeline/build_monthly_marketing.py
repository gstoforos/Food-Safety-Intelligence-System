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

H_REG  = "Helvetica"
H_BOLD = "Helvetica-Bold"
H_MONO = "Courier"

PAGE_W, PAGE_H = A4
MARGIN_L = 28
MARGIN_R = 28
CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R


# =============================================================================
# UNICODE / GREEK FONT REGISTRATION (one-time, idempotent)
# =============================================================================

_DEJAVU_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",                      # Debian/Ubuntu
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",                               # Fedora/CentOS
    "/Library/Fonts/DejaVuSans.ttf",                                        # macOS (manual)
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",                 # macOS fallback
    "C:/Windows/Fonts/DejaVuSans.ttf",                                      # Windows (manual)
    "C:/Windows/Fonts/arial.ttf",                                           # Windows fallback
]
_DEJAVU_BOLD_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    "/Library/Fonts/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "C:/Windows/Fonts/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]

UNICODE_FONT_NAME = "DejaVu"
UNICODE_FONT_BOLD = "DejaVu-Bold"
_UNICODE_FONT_OK = False


def _register_unicode_font() -> bool:
    """Register a Unicode TTF for Greek/extended-Latin once. Returns True on success."""
    global _UNICODE_FONT_OK
    if _UNICODE_FONT_OK:
        return True
    if UNICODE_FONT_NAME in pdfmetrics.getRegisteredFontNames():
        _UNICODE_FONT_OK = True
        return True
    reg_path = next((p for p in _DEJAVU_CANDIDATES      if os.path.exists(p)), None)
    bold_path = next((p for p in _DEJAVU_BOLD_CANDIDATES if os.path.exists(p)), None)
    if not reg_path:
        return False
    try:
        pdfmetrics.registerFont(TTFont(UNICODE_FONT_NAME, reg_path))
        if bold_path:
            pdfmetrics.registerFont(TTFont(UNICODE_FONT_BOLD, bold_path))
        _UNICODE_FONT_OK = True
        return True
    except Exception:
        return False


def _needs_unicode(s: str) -> bool:
    """True if string contains chars beyond Latin Extended-B (Helvetica core can't render)."""
    return any(ord(ch) > 0x024F for ch in s)


# =============================================================================
# PATHOGEN ABBREVIATION (matches March convention)
# =============================================================================

# Genera that should be displayed by genus only (per March style)
_GENUS_ONLY = {"Listeria", "Salmonella", "Campylobacter", "Cronobacter", "Vibrio", "Yersinia"}

# Explicit overrides
_PATHOGEN_OVERRIDES = {
    "Listeria monocytogenes":   "Listeria",
    "Salmonella enterica":      "Salmonella",
    "Salmonella spp.":          "Salmonella",
    "Clostridium botulinum":    "C. botulinum",
    "Clostridium perfringens":  "C. perfringens",
    "Escherichia coli":         "E. coli",
    "E. coli O157:H7":          "E. coli / STEC",
    "Shigatoxin-producing E. coli": "E. coli / STEC",
    "Bacillus cereus":          "B. cereus",
    "Bacillus cereus / cereulide": "B. cereus",
    "Staphylococcus aureus":    "S. aureus",
}


def abbreviate_pathogen(name: str) -> str:
    """Apply March-style abbreviation: genus-only or first-initial.species."""
    if not name:
        return ""
    name = name.strip()
    if name in _PATHOGEN_OVERRIDES:
        return _PATHOGEN_OVERRIDES[name]
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

    have_unicode = _register_unicode_font()
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

        comp_font = UNICODE_FONT_NAME if (have_unicode and _needs_unicode(company)) else H_REG

        prod_lines = _wrap(product, H_REG, PRODUCT_FONT_SIZE, col_widths[4])
        comp_lines = _wrap(company, comp_font, COMPANY_FONT_SIZE, col_widths[3])
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

        # PATHOGEN + T1 (+ optional OUTBREAK black)
        c.setFont(H_REG, 8.5); c.setFillColor(INK)
        c.drawString(col_x[2], text_top_y, pathogen)
        c.setFont(H_BOLD, 7.5); c.setFillColor(NAVY)
        c.drawString(col_x[2] + _text_w(pathogen, H_REG, 8.5) + 5, text_top_y, "T1")
        if flag:
            c.setFont(H_BOLD, 7); c.setFillColor(NAVY)   # OUTBREAK in black
            c.drawString(col_x[2], text_top_y - 11, flag)

        # COMPANY / BRAND (+ country muted)
        cy = text_top_y
        c.setFont(comp_font, COMPANY_FONT_SIZE); c.setFillColor(INK)
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
        c.drawString(col_right - vw - 2, sy - 1, view_text)

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
# CLI / standalone test
# =============================================================================

if __name__ == "__main__":
    # smoke test with April 2026 content
    sample: MonthData = {
        "month_tag":        "APRIL 2026",
        "period_line":      "01 APR – 30 APR 2026",
        "total_recalls":    236,
        "tier1":            198,
        "outbreaks":        6,
        "leading_pathogen": "Listeria monocytogenes",
        "rows": [
            {"date": "2026-04-14", "pathogen": "Clostridium botulinum",
             "company": "Liquid Blenz Corp", "country": "USA",
             "product": "Good Brain Tonic 16 oz (UPC 860010984468) and 32 oz (UPC 860010984475) — all codes",
             "source": "FDA"},
            {"date": "2026-04-09", "pathogen": "Listeria monocytogenes", "outbreak": True,
             "company": "Νικόλαος Τσατσούλης & Υιοί Ο.Ε.", "country": "Greece",
             "product": "Feta PDO in barrel (batch ΦΕ-2751, produced 2026-01-24, use-by 2027-07-24)",
             "source": "EFET (GR)"},
            # ... etc
        ],
    }
    out = render_marketing_pdf("test_marketing.pdf", sample)
    print(f"wrote {out}")
