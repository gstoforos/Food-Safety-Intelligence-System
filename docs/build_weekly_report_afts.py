"""
AFTS Food Safety Intelligence System - Weekly Report Generator
Template: 2026-W16.html (gold standard)
Output: docs/YYYY-WW.html + data/weekly-summary-latest.json
Reads: docs/data/recalls.xlsx Recalls sheet ONLY.
Schedule: Friday 10:00 Athens time (dual-DST cron + TZ guard).
"""

import json, logging, os, re, requests, sys, argparse, html as html_mod
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import Counter, OrderedDict
from typing import List, Dict, Any, Tuple
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")

SEVERITY = OrderedDict([
    ("clostridium botulinum",1),("botulinum",1),
    ("listeria monocytogenes",2),("listeria",2),
    ("stec",3),("e. coli o157",3),("e. coli",3),
    ("salmonella",4),("cereulide",5),("bacillus cereus",5),
    ("norovirus",6),("hepatitis",7),
])


# ---------------------------------------------------------------------------
# Plural-aware count phrasing (added 2026-05-08).
# Replaces the legacy "{n} thing(s)" pattern with grammatical phrasing.
# Use as: _count_phrase(n, "incident") -> "1 incident" / "5 incidents".
# Pass zero= to override the n==0 case (e.g. "no confirmed outbreak events").
# ---------------------------------------------------------------------------
def _count_phrase(n: int, singular: str, *, plural: str = None,
                  zero: str = None) -> str:
    if n == 0 and zero is not None:
        return zero
    if n == 1:
        return f"{n} {singular}"
    return f"{n} {plural or singular + 's'}"


# ---------------------------------------------------------------------------
# Pathogen synonym consolidation (added 2026-04-27).
# Mirrors the table in build_monthly_report_afts.py — kept separate to
# preserve the surgical "no shared module" architecture you've used.
# Buckets are CHEMICALLY/EPIDEMIOLOGICALLY distinct: rodenticide is
# anticoagulant chemical poisoning (HiPP), distinct from rodent
# contamination (live pests). Salmonella serovars are deliberately
# preserved.
# ---------------------------------------------------------------------------
_PATHOGEN_SYNONYMS = {
    # Rodenticide chemical poisoning
    "rat poison":                                          "Rodenticide",
    "Rat poison":                                          "Rodenticide",
    "Rodenticide (rat poison)":                            "Rodenticide",
    "rodenticide (rat poison)":                            "Rodenticide",
    "Rodenticide poisoning":                               "Rodenticide",
    "Bromadiolone":                                        "Rodenticide",
    "bromadiolone":                                        "Rodenticide",
    # Rodent / pest contamination (different category)
    "Rodent contamination (physical/microbial hazard)":    "Rodent contamination",
    "Rodent contamination (physical/biological hazard)":   "Rodent contamination",
    "Mouse contamination (physical/biological hazard)":    "Rodent contamination",
    "Mouse contamination":                                 "Rodent contamination",
    # Aflatoxin
    "Aflatoxins":                                          "Aflatoxin",
    # Bacillus cereus / cereulide
    "Bacillus cereus / cereulide":                         "Bacillus cereus / Cereulide",
    "Bacillus cereus (cereulide)":                         "Bacillus cereus / Cereulide",
    "Cereulide (B. cereus toxin)":                         "Bacillus cereus / Cereulide",
    "Cereulide":                                           "Bacillus cereus / Cereulide",
    # E. coli / STEC variants → single bucket
    "STEC (Shiga toxin-producing E. coli)":                "E. coli STEC",
    "Shiga toxin-producing E. coli (STEC)":                "E. coli STEC",
    "E. coli STEC (Shiga toxin-producing)":                "E. coli STEC",
    "STEC / E. coli O157:H7":                              "E. coli STEC",
    "E. coli O157:H7":                                     "E. coli STEC",
    "E. coli":                                             "E. coli STEC",
    # Marine biotoxins
    "Lipophilic biotoxins (DSP)":                          "Marine biotoxins",
    "Lipophilic biotoxins":                                "Marine biotoxins",
    "Paralytic shellfish toxins (PSP)":                    "Marine biotoxins",
    "Paralytic shellfish toxins":                          "Marine biotoxins",
    "Paralytic Shellfish Toxins (saxitoxins)":             "Marine biotoxins",
    "Phytoplankton biotoxins":                             "Marine biotoxins",
    # Salmonella — bare label only; serovars preserved
    "Salmonella":                                          "Salmonella spp.",
    # Histamine
    "Histamine":                                           "Histamine / scombrotoxin",
    "Histamine (biotoxine endogène)":                      "Histamine / scombrotoxin",
    "Scombrotoxin":                                        "Histamine / scombrotoxin",
}


def _consolidate_pathogen_label(label):
    """Map raw pathogen label to canonical bucket. Idempotent."""
    if not label:
        return label
    s = str(label).strip()
    return _PATHOGEN_SYNONYMS.get(s, s)


def _consolidate_counter(c):
    """Return a new Counter with synonymous keys merged."""
    out = Counter()
    for k, v in c.items():
        out[_consolidate_pathogen_label(k)] += v
    return out


def _dot_color(pathogen):
    p = (pathogen or "").lower()
    return "#9333ea" if ("botulinum" in p or "clostridium" in p) else "#dc2626"

def _severity_score(pathogen):
    p = (pathogen or "").lower()
    for k, v in SEVERITY.items():
        if k in p: return v
    return 99

COUNTRY_DISPLAY = {"USA":"United States","UK":"United Kingdom"}
AUTHORITY_DISPLAY = {
    "FDA":"FDA","USDA FSIS":"USDA FSIS","CFIA":"CFIA","MAPAQ QC":"MAPAQ QC",
    "RappelConso (FR)":"RappelConso (FR)","BVL (DE)":"BVL (DE)",
    "FSA (UK)":"FSA (UK)","MPI NZ":"MPI NZ","MPI (NZ)":"MPI NZ",
    "Min. Salute (IT)":"Min. Salute (IT)","EFET (GR)":"EFET (GR)",
    "AESAN (ES)":"AESAN (ES)","RASFF (EU)":"RASFF (EU)",
    "FSANZ (AU)":"FSANZ (AU)","BLV (CH)":"BLV (CH)",
    "AGES (AT)":"AGES (AT)","SZPI (CZ)":"SZPI (CZ)",
    "ANVISA (BR)":"ANVISA (BR)","ANMAT (AR)":"ANMAT (AR)",
    "COFEPRIS (MX)":"COFEPRIS (MX)","NCC (ZA)":"NCC (ZA)",
    "SFA (SG)":"SFA (SG)","CFS (HK)":"CFS (HK)","MFDS (KR)":"MFDS (KR)",
}
GEO_AUTHORITY = {
    "France":"RappelConso / DGCCRF","Germany":"BVL","Canada":"CFIA",
    "United States":"FDA / USDA FSIS","USA":"FDA / USDA FSIS",
    "New Zealand":"MPI / FSANZ","Italy":"Ministero della Salute",
    "United Kingdom":"FSA","UK":"FSA","Spain":"AESAN","Greece":"EFET",
    "Australia":"FSANZ","Switzerland":"BLV","Austria":"AGES",
    "Brazil":"ANVISA","South Africa":"NCC / NRCS","Hong Kong":"CFS",
    "Singapore":"SFA","Japan":"MHLW","South Korea":"MFDS",
    "Taiwan":"MOHW / TFDA","Mexico":"COFEPRIS","Argentina":"ANMAT",
    "Czech Republic":"SZPI","Slovakia":"\u0160VPS","Belgium":"AFSCA",
}

def load_recalls(xlsx_path):
    wb = load_workbook(xlsx_path, data_only=True)
    if "Recalls" not in wb.sheetnames:
        log.error("No Recalls sheet"); return []
    ws = wb["Recalls"]; hdr = [c.value for c in ws[1]]
    return [{h:(v if v is not None else "") for h,v in zip(hdr, row)}
            for row in ws.iter_rows(min_row=2, values_only=True)]

def filter_week(recalls, week_end):
    """Return rows belonging to the weekly report ending Friday `week_end`.

    Audit 2026-05-10: prefer the sticky `report_week` stamp set by
    merge_master.compute_report_week() at promote time. Fall back to
    legacy date-window math ONLY for rows that have no stamp yet — i.e.
    historical rows promoted before 2026-05-10 (everything dated
    before 2026-05-02 per migrate_recalls_report_week.py).

    Why prefer the stamp:
      • Late-arriving rows (scraped Wednesday for an event published the
        prior Thursday) get correctly attributed to LAST week's report
        instead of leaking into THIS week's date window.
      • Friday-dated rows are deferred to NEXT week's report (the AM
        ship-time scrape can't yet capture full Friday data); the legacy
        date math wrongly included them in the same-day report.

    Stamp rule (mirrored in merge_master.compute_report_week):
        report_week = "W{nn}", nn = ISO week of the smallest Friday > Date.

    Legacy fallback window (for rows with empty report_week):
        week_end - 6  ≤  Date  ≤  week_end   (Sat..Fri inclusive, 7 days).
        This is the OLD inclusive-Friday rule under which historical
        weekly reports (W01..W19 of 2026) were originally built. Keeping
        it as fallback preserves identical output for refresh_stale_weeks
        runs against pre-stamp data.
    """
    expected_tag = "W{:02d}".format(week_end.isocalendar()[1])
    legacy_ws = week_end - timedelta(days=6)
    out = []
    for r in recalls:
        stamp = (r.get("report_week") or "").strip()
        if stamp:
            # New path: trust the sticky stamp set at promote time.
            if stamp == expected_tag:
                out.append(r)
            continue
        # Legacy fallback: date-window math (pre-stamp historical rows).
        d = r.get("Date", "")
        if not d:
            continue
        try:
            rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if legacy_ws <= rd <= week_end:
            out.append(r)
    return out


def _display_window(week_end, filtered_recalls):
    """Return (display_start, display_end) for the report period header.

    Audit 2026-05-12: the displayed period depends on whether the report
    was built under the OLD inclusive-Friday rule (Sat→Fri, 7 days) or
    the NEW Friday-strict rule (Fri→Thu, 7 days). A report is treated
    as new-rule iff at least one of its filtered rows carries a
    report_week stamp — that stamp is set only by compute_report_week()
    at promote time and only exists for rows dated ≥ 2026-05-02 per
    migrate_recalls_report_week.py.

    NEW rule (any row stamped):
        display_start = week_end − 7 (previous Friday — first data day)
        display_end   = week_end − 1 (Thursday — last data day)
        ship Friday itself is NOT in the data window (rows dated that
        day get stamped to NEXT week's report).

    LEGACY rule (no stamped rows — pre-migration historicals):
        display_start = week_end − 6 (Saturday)
        display_end   = week_end    (Friday — ship day INCLUDED)
        matches what W01..W18 of 2026 originally shipped, so historical
        reports keep their published display.

    Transitional W19 caveat: the migration intentionally left rows
    dated 2026-05-01 (the prior ship Friday under the OLD rule) blank
    rather than re-stamping them W19, so W19's filtered set starts at
    May 2. display_start therefore derives from week_end − 6 in the
    new-rule branch only if no row predates that — otherwise we let
    the actual min(date) anchor the display start, so the period header
    matches the data it summarises. Same applies to display_end vs
    week_end − 1: if the last stamped row is earlier, use that.
    """
    any_stamp = any((r.get("report_week") or "").strip() for r in filtered_recalls)
    if any_stamp:
        # New-rule window: Friday (week_end − 7) through Thursday (week_end − 1).
        rule_start = week_end - timedelta(days=7)
        rule_end = week_end - timedelta(days=1)
        # Tighten to actual data span — only for new-rule weeks. Handles the
        # transitional W19 case where the rule window starts at Fri May 1
        # but the migration left May 1 blank-stamped, so its earliest
        # stamped row is Sat May 2. Legacy weeks are NEVER tightened —
        # their period header must match what shipped at the time.
        parsed_dates = []
        for r in filtered_recalls:
            d = r.get("Date", "")
            if not d:
                continue
            try:
                parsed_dates.append(datetime.strptime(str(d)[:10], "%Y-%m-%d").date())
            except ValueError:
                continue
        if parsed_dates:
            ds = min(parsed_dates); de = max(parsed_dates)
            display_start = max(rule_start, ds)
            display_end = min(rule_end, de)
            if display_start > display_end:
                display_start, display_end = rule_start, rule_end
        else:
            display_start, display_end = rule_start, rule_end
    else:
        # Legacy rule: Saturday (week_end − 6) through Friday (week_end).
        # No tightening — preserves the published display for W01..W18 of
        # 2026 even if the dataset is later corrected (rows added/removed).
        display_start = week_end - timedelta(days=6)
        display_end = week_end
    return display_start, display_end


def _safe_int(v, default=0):
    try: return int(v)
    except (ValueError, TypeError): return default

def _country_display(c): return COUNTRY_DISPLAY.get(c, c)


# ───────────────────────────────────────────────────────────────────────
# Public helper API — used by docs/build_monthly_report_afts.py.
#
# Historical note: the monthly builder imports six helpers from this
# module — safe_int, severity_score, is_report_grade_url,
# pathogen_badge_color, rank_top_recalls, render_top5_row. Two were
# already implemented as private (_safe_int, _severity_score). The
# other four were authored inside build_html() in past iterations and
# never hoisted to module scope, so the monthly build broke with
# AttributeError until this section was added (2026-05-01).
#
# Exposed here at module scope so:
#   • the monthly builder can call weekly.<name>(…) without changes
#   • unit tests can import them in isolation
#   • the weekly builder itself can use them in build_html() for any
#     future consolidation of duplicated inline code
#
# All functions return primitives (str, int, list, tuple) — no side
# effects, no I/O, no global state.
# ───────────────────────────────────────────────────────────────────────

#: Return value type for `severity_score()` — kept as a 2-tuple so the
#: monthly builder's `_, canon = severity_score(name)` unpack works.
def safe_int(v, default=0):
    """Public alias for _safe_int. Convert v to int, returning default on
    failure. Accepts strings, floats, None, or anything that int() can
    coerce; never raises."""
    return _safe_int(v, default)


def severity_score(pathogen):
    """Return (score, canonical_name) for a pathogen string.

    Score: lower = more severe (1 = botulism, 2 = listeria, ..., 99 = unknown).
    Canonical name: synonym-collapsed display label using _PATHOGEN_SYNONYMS.

    Caller patterns:
      score = severity_score(name)[0]
      _, canon = severity_score(name)
    """
    score = _severity_score(pathogen)
    canonical = _PATHOGEN_SYNONYMS.get(pathogen or "", pathogen or "Unknown")
    return score, canonical


def pathogen_badge_color(canonical):
    """Hex colour used for a pathogen's badge / table dot / progress-bar.

    Uses canonical names (post-synonym-collapse) so synonyms map to the
    same colour. The palette tracks SEVERITY tiers — botulism is the
    most-saturated red, listeria deep amber, salmonella mid-amber, the
    less-acute hazards in muted greys / blues.
    """
    name = (canonical or "").lower()
    # Tier 1 — fatal-risk pathogens
    if "botulin" in name:                            return "#b91c1c"   # deep red
    if "listeria" in name:                           return "#dc2626"   # red
    if "stec" in name or "shiga" in name or "o157" in name:
                                                     return "#ea580c"   # orange-red
    # Tier 2 — common acute pathogens
    if "salmonella" in name:                         return "#f59e0b"   # amber
    if "campylobacter" in name:                      return "#d97706"   # amber-dark
    if "e. coli" in name or "escherichia" in name:   return "#f97316"   # orange
    if "vibrio" in name:                             return "#0891b2"   # cyan
    if "cronobacter" in name:                        return "#0d9488"   # teal
    # Tier 3 — viral / parasitic
    if "norovirus" in name or "norwalk" in name:     return "#7c3aed"   # violet
    if "hepatitis" in name:                          return "#a855f7"   # purple
    if "cyclospora" in name or "crypto" in name or "giardia" in name:
                                                     return "#9333ea"   # purple-dark
    # Tier 4 — toxins / chemical / physical
    if "aflatox" in name or "ochra" in name or "fumon" in name or "patulin" in name:
                                                     return "#854d0e"   # brown
    if "histamine" in name or "scombro" in name:     return "#a16207"   # khaki
    if "rodenticide" in name or "rat poison" in name:
                                                     return "#1e293b"   # near-black
    if any(t in name for t in ("lead", "cadmium", "mercury", "arsenic")):
                                                     return "#475569"   # slate
    if "physical" in name or "glass" in name or "metal" in name:
                                                     return "#525252"   # neutral
    # Default
    return "#6b7280"   # grey


def is_report_grade_url(url):
    """True if the URL points at a primary regulator / official source.

    Used in the monthly summary JSON so subscribers see only links to
    authoritative sources (FDA, USDA, CFIA, RASFF, RappelConso, etc.)
    and not aggregator/news rewrites. Hostname-based — match-anything
    in a small allow-list, no live HTTP check.
    """
    if not url:
        return False
    u = str(url).lower()
    if not u.startswith(("http://", "https://")):
        return False
    # Strip protocol + leading "www." for substring matching
    host = u.split("://", 1)[1].split("/", 1)[0].lstrip("www.")
    REGULATOR_HOSTS = (
        # USA
        "fda.gov", "fsis.usda.gov", "cdc.gov", "epa.gov",
        # Canada
        "canada.ca", "inspection.canada.ca", "gnb.ca", "mapaq.gouv.qc.ca",
        "quebec.ca",
        # EU + member states
        "europa.eu", "efsa.europa.eu", "rasff.eu",
        "rappel.conso.gouv.fr", "economie.gouv.fr",
        "bvl.bund.de", "bund.de",
        "salute.gov.it",
        "aesan.gob.es", "mscbs.gob.es",
        "efet.gr", "minagric.gr",
        "voedselveiligheid.be", "favv.be", "afsca.be",
        "blv.admin.ch",
        "ages.at",
        "szpi.gov.cz",
        # UK
        "food.gov.uk", "fss.scot", "gov.uk",
        # Asia-Pacific
        "mpi.govt.nz", "foodstandards.gov.au", "foodstandards.govt.nz",
        "sfa.gov.sg", "cfs.gov.hk", "mfds.go.kr", "mhlw.go.jp",
        "fda.gov.tw",
        # Latin America + Africa
        "anvisa.gov.br", "anmat.gob.ar", "cofepris.gob.mx",
        "nrcs.gov.za", "nccsa.org.za",
    )
    return any(host == h or host.endswith("." + h) for h in REGULATOR_HOSTS)


def rank_top_recalls(recalls, n=10):
    """Return up to n recalls sorted by composite severity rank.

    Three-phase ordering (lower phase wins outright; ties broken inside
    the phase):

      Phase 0  — VERIFIED OUTBREAKS  (Outbreak truthy)
                 Outbreaks indicate active human illness, so they are
                 escalated above every non-outbreak recall regardless of
                 the underlying pathogen. Inside the phase: severity score
                 (lower = more severe) → tier → date desc → country.
                 Effect: a Listeria outbreak outranks a Salmonella outbreak
                 from the same day, but BOTH outrank a non-outbreak
                 Clostridium botulinum recall.

      Phase 1  — Clostridium botulinum (non-outbreak)
                 Pulled out of the general pool because botulism toxin
                 is fatal-risk even at single-recall scale and warrants
                 prominence on the marketing one-pager. Inside the phase:
                 tier → date desc → country.

      Phase 2  — EVERYTHING ELSE  (by severity → tier → date desc → country)

    Pre-2026-05-01 the ranker used a flat (severity, tier, outbreak, date)
    key. That collapsed the three Apr-2026 botulinum recalls (sev=1)
    to ranks 1-3 and pushed the Good4U Salmonella outbreak (sev=4) out
    of top-10 entirely — which is the wrong editorial signal for the
    public marketing PDF. Phase-based fix mirrors how George curates
    the headline-incidents list in the subscriber report.
    """
    OUTBREAK_TRUTHY = {True, 1, "1", "TRUE", "True", "true", "Y", "Yes"}

    def _rank_key(r):
        pathogen = r.get("Pathogen") or ""
        sev_score, _ = severity_score(pathogen)
        tier        = _safe_int(r.get("Tier"), 99)
        is_outbreak = r.get("Outbreak") in OUTBREAK_TRUTHY
        is_botulin  = "botulin" in pathogen.lower()

        if is_outbreak:
            phase = 0
        elif is_botulin:
            phase = 1
        else:
            phase = 2

        # Negative ordinal → newer-first ordering
        try:
            d = datetime.strptime(str(r.get("Date") or "")[:10], "%Y-%m-%d")
            d_key = -d.toordinal()
        except (ValueError, TypeError):
            d_key = 0

        country_key = str(r.get("Country") or "").lower()
        return (phase, sev_score, tier, d_key, country_key)

    return sorted(recalls, key=_rank_key)[:n]


def render_top5_row(rank, r):
    """Render ONE table row (HTML <tr>) for the top-N recalls table.

    The function name predates the rename to top-10 — kept for backward
    compatibility with existing call sites in the monthly builder.

    Layout (6 columns, must match `<thead>` in build_monthly_report_afts.py):
      1. # (rank)
      2. Date
      3. Pathogen (italic name + tier chip + outbreak chip)
      4. Company / Brand (company on top, brand muted below if present)
      5. Product (truncated to 110 chars)
      6. Jurisdiction & Source (country on top, agency below, link to source)

    Pre-2026-05-01 this emitted only 5 cells in a different column order,
    which silently shifted every td by one column under WeasyPrint and
    produced the broken Apr-2026 subscriber PDF. The 6-cell layout below
    aligns with the <thead> declaration in both §08 and Appendix A.
    """
    canon_name = severity_score(r.get("Pathogen") or "")[1]
    badge_color = pathogen_badge_color(canon_name)
    date_str = str(r.get("Date") or "")[:10] or "—"
    country  = _country_display(r.get("Country") or "—")
    source   = AUTHORITY_DISPLAY.get(
        r.get("Source") or r.get("Agency") or "",
        r.get("Source") or r.get("Agency") or "—"
    )
    company  = r.get("Company") or "—"
    brand    = (r.get("Brand") or "").strip()
    product  = r.get("Product") or r.get("Description") or "—"
    if len(product) > 110:
        product = product[:107] + "…"
    url      = (r.get("URL") or "").strip()

    # Tier / outbreak chips (best-effort; missing fields render nothing)
    chips = []
    tier = _safe_int(r.get("Tier"), 99)
    if   tier == 1: chips.append('<span class="chip chip-tier-1">Tier&nbsp;1</span>')
    elif tier == 2: chips.append('<span class="chip chip-tier-2">Tier&nbsp;2</span>')
    if r.get("Outbreak") in (True, 1, "1", "TRUE", "True", "true", "Y", "Yes"):
        chips.append('<span class="chip chip-outbreak">Outbreak</span>')
    chip_html = " ".join(chips)

    # Brand sub-line only if it's distinct from company and not the placeholder "—"
    brand_html = ""
    if brand and brand not in ("—", company):
        brand_html = f'<div class="brand">{html_mod.escape(brand)}</div>'

    # Source cell: agency name as a link to URL (if present), else plain text
    if url:
        source_html = (
            f'<div class="country">{html_mod.escape(country)}</div>'
            f'<div class="agency"><a href="{html_mod.escape(url)}" target="_blank" '
            f'rel="noopener">{html_mod.escape(source)} &rarr;</a></div>'
        )
    else:
        source_html = (
            f'<div class="country">{html_mod.escape(country)}</div>'
            f'<div class="agency">{html_mod.escape(source)}</div>'
        )

    return (
        f'<tr>'
        f'<td class="num rank">#{rank}</td>'
        f'<td class="date">{html_mod.escape(date_str)}</td>'
        f'<td class="pathogen">'
        f'<span class="path-dot" style="background:{badge_color}"></span>'
        f'<em class="path-name">{html_mod.escape(canon_name)}</em>'
        f'{("&nbsp;" + chip_html) if chip_html else ""}'
        f'</td>'
        f'<td class="company">'
        f'<div class="company-name">{html_mod.escape(company)}</div>'
        f'{brand_html}'
        f'</td>'
        f'<td class="prod">{html_mod.escape(product)}</td>'
        f'<td class="src">{source_html}</td>'
        f'</tr>'
    )


def compute_stats(wr, pr):
    total = len(wr)
    tier1 = sum(1 for r in wr if _safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in wr if _safe_int(r.get("Outbreak")) == 1)
    pc = Counter(); cc = Counter()
    for r in wr:
        p = (r.get("Pathogen") or "").strip()
        if p: pc[p.split("(")[0].strip()] += 1
        cc[_country_display(r.get("Country","") or "Unknown")] += 1
    pt = len(pr); delta = total - pt
    # Apply synonym consolidation BEFORE deriving top_pathogen so the KPI
    # banner and the distribution table never disagree.
    pc_consolidated = _consolidate_counter(pc)
    return {"total":total,"tier1":tier1,"outbreaks":outbreaks,
            "top_pathogen":pc_consolidated.most_common(1)[0] if pc_consolidated else ("\u2014",0),
            "pathogen_counts":pc_consolidated.most_common(20),"country_counts":cc.most_common(20),
            "prev_total":pt,"delta":delta,
            "delta_pct":round(delta/max(pt,1)*100) if pt else 0}

def sort_by_severity(recalls):
    def key(r):
        return (_severity_score(r.get("Pathogen","")),
                -_safe_int(r.get("Outbreak",0)), _safe_int(r.get("Tier",2)),
                -(datetime.strptime(str(r.get("Date","2000-01-01"))[:10],"%Y-%m-%d").toordinal()
                  if str(r.get("Date",""))[:10] else 0))
    return sorted(recalls, key=key)

def esc(s):
    if s is None: return ""
    return html_mod.escape(str(s))

def _fmt_date(d):
    try: return datetime.strptime(str(d)[:10],"%Y-%m-%d").strftime("%-d %b %Y")
    except Exception: return str(d)[:10]

def _fmt_date_short(d):
    try: return datetime.strptime(str(d)[:10],"%Y-%m-%d").strftime("%-d %b")
    except Exception: return str(d)[:10]


def generate_analysis_claude(stats, recalls):
    """Generate §01 Intelligence Analysis.
    P1-P3 come from Claude (or the fallback). P4 Process Authority Note is
    always deterministic — it cites specific CFR/EU/CFIA paragraphs and
    company names, which must be factually stable (not LLM-generated)."""
    tp, tc = stats["top_pathogen"]; t = stats["total"]
    pct = round(tc/max(t,1)*100)
    bot = [r for r in recalls if "botulinum" in (r.get("Pathogen") or "").lower()
           or "clostridium" in (r.get("Pathogen") or "").lower()]

    # Jurisdictions actually present this week — pass to Claude so P3 names them
    auths = _jurisdictions_from_recalls(recalls)
    auth_hint = ", ".join(auths[:5]) if auths else "multiple jurisdictions"

    prompt = """You are a food safety intelligence analyst for AFTS. Generate the Intelligence Analysis section.

DATA: Total={}, Tier-1={}, Outbreaks={}, Leading={} ({}, {}%)
Pathogens: {}
Countries: {}
Jurisdictions this week: {}

Generate EXACTLY these three paragraphs (plain text, no HTML, no headers, no paragraph numbers):
1. Executive overview — total, tier-1, outbreaks, leading pathogen %, interpret as regulatory-pressure signal.
2. Pathogen-specific process-engineering analysis for {} — name the specific product categories at risk (e.g. for Listeria: RTE deli / dairy / cooked-meat; for Salmonella: low-moisture foods / peanut butter / flour / spices; for STEC: raw beef / leafy greens / sprouts), the specific failure modes (e.g. Zone 1 environmental harbourage, sanitation SOP drift, post-lethality recontamination for Listeria), and cite the specific regulatory frameworks (21 CFR 117 Preventive Controls, 21 CFR 113/114 thermal lethality, validated lethality for L. monocytogenes under FDA CPG 555.320 / 9 CFR 430, FDA Produce Safety Rule 21 CFR 112, etc. as applicable).
3. Regulatory/geographic assessment — name the actual authorities active this week ({}). Close with AFTS recommendation to re-verify the single highest-leverage control for the commodity and confirm documentation packages are ready for rapid regulatory response.

Tone: professional, process-engineering voice, no emojis, no bullets, no colons at paragraph starts. 3-5 sentences each. Preserve the word 'AFTS' exactly where referenced. Do NOT write a Process Authority Note — that is appended separately.""".format(
        t, stats["tier1"], stats["outbreaks"], tp, tc, pct,
        dict(stats["pathogen_counts"]), dict(stats["country_counts"]), auth_hint,
        tp, auth_hint)

    claude_out = None
    try:
        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":CLAUDE_API_KEY},
            json={"model":"claude-sonnet-4-20250514","max_tokens":1200,
                  "messages":[{"role":"user","content":prompt}]}, timeout=60)
        if resp.status_code == 200:
            claude_out = resp.json()["content"][0]["text"]
        else:
            log.error("Claude %d", resp.status_code)
    except Exception as e:
        log.error("Claude error: %s", e)

    # Deterministic fallback for P1-P3 if Claude failed (tail call to _fallback
    # without bot so it produces only P1-P3).
    if claude_out is None:
        claude_out = _fallback_p1_to_p3(stats, recalls)

    # Append deterministic P4 Process Authority Note (multi-trigger).
    pa = _process_authority_note(recalls, bot)
    if pa:
        claude_out = claude_out.rstrip() + "\n\n" + pa
    return claude_out


def _fallback_p1_to_p3(stats, recalls):
    """P1-P3 only. The PA Note is always handled by _process_authority_note()."""
    tp, tc = stats["top_pathogen"]; t = stats["total"]
    pct = round(tc/max(t,1)*100)
    p1 = ("This week produced {} food-safety hazard recall incidents across the AFTS "
          "monitoring network, with {} classified as Tier-1 and {}. {} dominated the "
          "surveillance window, accounting for {} of {} "
          "incidents ({}%). The elevated Tier-1 ratio indicates sustained regulatory "
          "pressure and should be read by food manufacturers as a signal of tightening "
          "enforcement.").format(t, stats["tier1"],
                                 _count_phrase(stats["outbreaks"], "confirmed outbreak event",
                                               zero="no confirmed outbreak events"),
                                 tp, tc, t, pct)
    p2 = _pathogen_narrative(tp, pct)
    auths = _jurisdictions_from_recalls(recalls or [])
    if auths:
        auth_clause = ("Regulatory activity this week spanned multiple jurisdictions "
                       "({}{}), signalling continued inspection intensity. ").format(
                           ", ".join(auths),
                           ", and national authorities" if len(auths) >= 3 else "")
    else:
        auth_clause = ("Regulatory activity this week spanned multiple jurisdictions, "
                       "signalling continued inspection intensity. ")
    p3 = (auth_clause +
          "AFTS recommends that food manufacturers use this briefing as a prompt to "
          "re-verify the single highest-leverage control for their commodity this week "
          "and to confirm documentation packages are ready for rapid regulatory response.")
    return "\n\n".join([p1, p2, p3])

# ---------------------------------------------------------------------------
# Pathogen-specific process-engineering narratives (P2 dispatcher).
# Each entry returns the W16-style paragraph: product category → failure modes
# → relevant regulatory framework. Mirrors the W16 gold-standard specificity.
# ---------------------------------------------------------------------------
def _pathogen_narrative(pathogen, pct):
    p = (pathogen or "").lower()
    # The pct token gives us "at this prevalence" / "at this concentration" flavour
    intensity = "at this concentration" if pct >= 30 else "at this prevalence"

    if "listeria" in p:
        return ("Listeria monocytogenes {intensity} points to post-process recontamination "
                "in ready-to-eat deli, dairy, and cooked-meat lines rather than thermal "
                "underprocess. The likely failure modes are Zone 1 environmental harbourage, "
                "sanitation SOP drift, and post-lethality recontamination. The relevant "
                "frameworks for review are the environmental monitoring programme under "
                "21 CFR 117 and the thermal-lethality validation applicable to the product "
                "class (21 CFR 113 / 114 where applicable), supported by qualified process "
                "authority oversight.").format(intensity=intensity)

    if "salmonella" in p:
        return ("Salmonella {intensity} is most consistent with raw-ingredient cross-"
                "contamination or a breakdown in low-moisture-food controls (peanut butter, "
                "flour, spices, herbs, infant formula). The likely failure modes are "
                "raw-ingredient sourcing gaps, cross-contamination at wet-dry transitions, "
                "and insufficient time-temperature control. The relevant frameworks are "
                "21 CFR 117 Preventive Controls, HACCP critical limits on any validated "
                "kill step, and supplier-verification programmes.").format(intensity=intensity)

    if "stec" in p or "e. coli" in p or "escherichia" in p:
        return ("E. coli / STEC {intensity} is most consistent with raw beef, leafy greens, "
                "raw milk/dairy, sprouts, or unpasteurised juice. The likely failure modes "
                "are grinding-plant cross-contamination, irrigation-water contamination, "
                "post-harvest wash-water carry-over, and sprout-seed microbial load. The "
                "relevant frameworks are 21 CFR 117, the FDA Produce Safety Rule (21 CFR 112), "
                "and USDA FSIS zero-tolerance for E. coli O157:H7 in non-intact beef.").format(intensity=intensity)

    if "botulinum" in p or "clostridium" in p:
        return ("Clostridium botulinum {intensity} points directly to a scheduled-thermal-"
                "process failure in a shelf-stable low-acid canned food (LACF), an acidified "
                "food, or a reduced-oxygen-packaged product. The likely failure modes are an "
                "unfiled or outdated scheduled process, a process deviation resolved without "
                "qualified review, container / seal integrity loss, or a formulation change "
                "(pH, a_w, salt, preservative) introduced without re-evaluation. The relevant "
                "frameworks are 21 CFR 113 (LACF), 21 CFR 114 (acidified foods), and the "
                "scheduled-process filing requirements under 21 CFR 108.").format(intensity=intensity)

    if "cereulide" in p or "bacillus cereus" in p:
        return ("Cereulide / Bacillus cereus {intensity} indicates temperature abuse during "
                "cooling or holding of cooked rice, pasta, infant formula, or dairy-based "
                "products. The emetic toxin is heat-stable and is not inactivated by reheating, "
                "so controls must target the cooling-rate critical limit and the cooked-product "
                "hot-hold / chill-hold regime. The relevant frameworks are 21 CFR 117 time-"
                "temperature critical limits and FDA Food Code cooling provisions.").format(intensity=intensity)

    if "norovirus" in p or "hepatitis a" in p or "hav" in p:
        return ("Norovirus / Hepatitis A {intensity} is most consistent with contamination by "
                "an infected food handler or with contaminated raw molluscan shellfish or "
                "ready-to-eat soft fruit. The likely failure modes are sick-worker exclusion "
                "failure, hand-hygiene breakdown, and supplier controls on shellfish-harvest "
                "waters. The relevant frameworks are the FDA Food Code employee-health "
                "provisions and the National Shellfish Sanitation Program.").format(intensity=intensity)

    if "histamine" in p or "scombroid" in p:
        return ("Histamine (scombrotoxin) {intensity} indicates a chill-chain breach on "
                "histidine-rich species (tuna, mackerel, mahi-mahi, sardines, anchovies, "
                "bonito). Histamine is heat-stable and is not removed by cooking or canning, "
                "so the control point is time-temperature at harvest, landing, and the full "
                "cold chain. The relevant framework is 21 CFR 123 (Seafood HACCP) with FDA "
                "action levels of 50 ppm (decomposition) and 500 ppm (hazard).").format(intensity=intensity)

    if "rodenticide" in p or "rat poison" in p or "bromadiolone" in p:
        return ("Rodenticide {intensity} points to a supply-chain integrity or intentional-"
                "adulteration event — a criminal tampering profile rather than a process "
                "failure. The likely failure modes are a compromised inbound-ingredient chain, "
                "a breach in packaging integrity between producer and retailer, or insider "
                "tampering. The relevant frameworks are 21 CFR 121 (FSMA Intentional "
                "Adulteration Rule) for food-defense vulnerability assessment and the "
                "corresponding EU Commission Regulation on food fraud.").format(intensity=intensity)

    if "rodent" in p or "pest" in p:
        return ("Rodent / pest contamination {intensity} indicates a sanitation and GMP "
                "programme failure — pest-management, facility integrity, or raw-material "
                "storage hygiene. The likely failure modes are a lapsed integrated-pest-"
                "management contract, structural entry points, or un-segregated storage of "
                "open-package ingredients. The relevant frameworks are 21 CFR 117 subpart B "
                "(sanitation / pest control) and HACCP GMP prerequisites.").format(intensity=intensity)

    if any(k in p for k in ("glass", "metal", "plastic", "foreign")):
        return ("Foreign-material contamination {intensity} indicates an equipment-wear or "
                "packaging-integrity failure along the line. The likely failure modes are "
                "sieve / filter breakage, equipment fatigue on contact surfaces, or a lapse "
                "in metal-detection or X-ray inspection validation. The relevant frameworks "
                "are HACCP physical-hazard critical control points and the routine re-"
                "qualification of detection equipment at its declared sensitivity.").format(intensity=intensity)

    if any(k in p for k in ("lead", "cadmium", "arsenic", "mercury", "heavy metal")):
        return ("Heavy-metal contamination {intensity} is most consistent with raw-material "
                "sourcing from contaminated soils or waters, or with leaching from processing "
                "equipment and packaging. The likely failure modes are supplier-verification "
                "gaps on agricultural inputs, unmonitored legacy equipment, or incompatible "
                "food-contact materials. The relevant frameworks are FDA action / guidance "
                "levels (Closer to Zero for infant foods), EU Reg. 2023/915 contaminant "
                "maximum levels, and Codex Alimentarius MLs.").format(intensity=intensity)

    if any(k in p for k in ("aflatoxin", "ochratoxin", "patulin", "alternaria", "mycotoxin", "mould", "mold")):
        return ("Mycotoxin contamination {intensity} indicates a post-harvest moisture-control "
                "failure in grains, nuts, dried fruit, or spices, or a storage-humidity "
                "breach along the supply chain. The likely failure modes are inadequate "
                "drying, compromised storage-silo integrity, or supplier-verification gaps "
                "on high-risk commodities. The relevant frameworks are FDA action levels on "
                "aflatoxins, EU Reg. 2023/915 maximum levels, and HACCP supplier-approval "
                "programmes.").format(intensity=intensity)

    # Fallback — keep the old generic line if pathogen not recognised
    return ("{} {} warrants review of the kill step, post-kill recontamination controls, "
            "and supplier-verification programme for the implicated commodity. The relevant "
            "framework is 21 CFR 117 Preventive Controls with HACCP critical limits "
            "appropriate to the product class.").format(pathogen, intensity)


# ---------------------------------------------------------------------------
# P3 — data-driven jurisdiction paragraph.
# Maps Source labels present in the week's data to their authority names.
# ---------------------------------------------------------------------------
_SOURCE_TO_AUTHORITY = {
    "fda": "FDA", "usda fsis": "USDA FSIS", "usda": "USDA FSIS", "cdc": "CDC",
    "cfia": "CFIA", "mapaq": "MAPAQ",
    "rappelconso": "RappelConso", "dgccrf": "DGCCRF", "dgal": "DGAL",
    "fsa": "FSA", "fss": "FSS", "fsai": "FSAI",
    "rasff": "RASFF", "efsa": "EFSA", "dg sante": "DG SANTE",
    "aesan": "AESAN", "bvl": "BVL", "bfr": "BfR", "ages": "AGES",
    "min. salute": "Italian Ministry of Health", "nvwa": "NVWA",
    "favv": "FAVV", "fødevarestyrelsen": "Fødevarestyrelsen",
    "livsmedelsverket": "Livsmedelsverket", "mattilsynet": "Mattilsynet",
    "ruokavirasto": "Ruokavirasto", "gis": "GIS", "nebih": "NEBIH",
    "ansvsa": "ANSVSA", "bfsa": "BFSA", "szpi": "SZPI", "švps": "ŠVPS",
    "svps": "ŠVPS", "mast": "MAST", "blv": "BLV",
    "fsanz": "FSANZ", "mpi nz": "MPI NZ",
    "cfs": "CFS Hong Kong", "mfds": "MFDS", "mhlw": "MHLW",
    "samr": "SAMR", "sfa": "SFA", "fssai": "FSSAI",
    "anvisa": "ANVISA", "cofepris": "COFEPRIS", "invima": "INVIMA",
    "anmat": "ANMAT", "arcsa": "ARCSA", "digesa": "DIGESA", "isp": "ISP",
    "sfda": "SFDA", "moccae": "MOCCAE", "moh": "MOH", "moph": "MOPH",
    "nafdac": "NAFDAC", "kebs": "KEBS", "nfsa": "NFSA", "onssa": "ONSSA",
}

def _jurisdictions_from_recalls(recalls, max_list=5):
    """Return a short list of authority names present in this week's data."""
    seen = []
    for r in recalls:
        src = (r.get("Source") or "").lower().strip()
        if not src: continue
        # Match first token or exact map
        for key, auth in _SOURCE_TO_AUTHORITY.items():
            if key in src and auth not in seen:
                seen.append(auth); break
    return seen[:max_list]


def _process_authority_note(recalls, bot):
    """Return the P4 Process Authority Note string, or '' if no trigger fires.

    Multi-trigger system. A PA Note fires whenever ANY of these hazard-classes
    appear in the week. Each trigger gets its own fully-specified
    process-authority paragraph with the correct GMPs, validation framework,
    and regulatory citations — keyed to the JURISDICTIONS actually present in
    the implicated recalls, not boilerplate FDA citations.

    Triggers (evaluated in severity order; first match wins):
      1. Clostridium / botulinum   - scheduled-thermal-process / LACF filing
      2. Listeria monocytogenes    - RTE sanitation / env-monitoring / validated lethality
      3. Salmonella in low-moisture - kill-step validation, wet-dry segregation
      4. Salmonella (other)        - kill-step + supplier verification
      5. E. coli STEC              - raw-material supplier verification, kill-step
      6. Multi-jurisdiction outbreak OR Tier-1 >=3 across >=3 countries - regulatory-response PA
    """
    if not recalls:
        return ""

    # Helper: recalls implicating a specific pathogen
    def _by_pathogen(*needles):
        out = []
        for r in recalls:
            p = (r.get("Pathogen") or "").lower()
            if any(n in p for n in needles):
                out.append(r)
        return out

    # Low-moisture food heuristic (Salmonella trigger needs both signals)
    def _is_low_moisture(r):
        text = ((r.get("Product") or "") + " " + (r.get("Reason") or "") + " " +
                (r.get("Company") or "")).lower()
        return any(k in text for k in (
            "peanut", "nut butter", "almond", "cashew", "pistachio", "hazelnut",
            "flour", "cereal", "granola", "oat", "rice", "pasta", "grain",
            "powder", "powdered", "infant formula", "formula", "milk powder",
            "spice", "herb", "seasoning", "tea", "dried", "chocolate", "cocoa",
            "seed", "tahini", "sesame",
        ))

    def _names(rows, limit=3):
        return ", ".join("{} ({})".format(
            r.get("Company", ""), _country_display(r.get("Country", "")))
            for r in rows[:limit])

    # ------------------------------------------------------------------
    # Jurisdiction-aware framework picker.
    # Examines the country codes of the implicated rows and returns the
    # appropriate framework citations ordered by dominant jurisdiction.
    # Each hazard trigger calls this to build its regulatory-citation block.
    # ------------------------------------------------------------------
    def _country_buckets(rows):
        us = eu = uk = ca = au_nz = jp = kr = other = 0
        countries = []
        EU_MEMBERS = {
            "France","Germany","Italy","Spain","Netherlands","Belgium","Ireland",
            "Denmark","Sweden","Finland","Austria","Poland","Portugal","Greece",
            "Czech Republic","Slovakia","Hungary","Romania","Bulgaria","Croatia",
            "Slovenia","Lithuania","Latvia","Estonia","Luxembourg","Malta","Cyprus",
        }
        for r in rows:
            c = (r.get("Country") or "").strip()
            if c and c not in countries:
                countries.append(c)
            cl = c.lower()
            if cl in ("united states", "usa", "us"):        us += 1
            elif c in EU_MEMBERS or cl == "european union": eu += 1
            elif cl in ("united kingdom", "uk", "scotland", "england", "wales", "northern ireland"): uk += 1
            elif cl == "canada":                             ca += 1
            elif cl in ("australia", "new zealand"):         au_nz += 1
            elif cl == "japan":                              jp += 1
            elif cl == "south korea":                        kr += 1
            else:                                            other += 1
        return {"US":us,"EU":eu,"UK":uk,"CA":ca,"AU_NZ":au_nz,"JP":jp,"KR":kr,"OTHER":other,
                "countries":countries}

    def _regs_for(hazard, rows):
        """Return a dict of framework-citation strings keyed to the jurisdictions
        actually present. `hazard` is one of: botulinum, listeria, salmonella_lm,
        salmonella, stec, regulatory.
        The returned dict has keys: primary (main framework sentence),
        parallels (comma-joined list of parallel-jurisdiction cites),
        typical_regulators (space-separated authority names for the closing sentence)."""
        b = _country_buckets(rows)

        # Per-hazard framework text per jurisdiction
        FRAMEWORKS = {
            "botulinum": {
                "US": "FDA 21 CFR 113 (LACF) / 21 CFR 114 (acidified foods) with scheduled-process filing under 21 CFR 108 (Form 2541 / 2541e)",
                "EU": "EU Reg. 852/2004 (food hygiene) and Reg. 2073/2005 (microbiological criteria), with national competent-authority oversight of low-acid / shelf-stable production",
                "UK": "UK Food Safety Act 1990, Food Hygiene (England) Regulations 2013 / retained Reg. 852/2004 + 2073/2005, and FSA Food Standards Agency oversight",
                "CA": "CFIA Safe Food for Canadians Regulations (SFCR) with Preventive Control Plan (PCP) requirements for low-acid foods",
                "AU_NZ": "FSANZ Food Standards Code Chapter 3 (Standard 3.2.1 Food Safety Programs, 3.2.2 Practices and General Requirements)",
                "JP": "Japan Food Sanitation Act and MHLW standards for shelf-stable and canned foods",
                "KR": "MFDS Food Sanitation Act with thermal-process validation requirements",
            },
            "listeria": {
                "US": "FDA 21 CFR 117 Subparts B and G (Preventive Controls), FDA CPG 555.320 (L. monocytogenes zero-tolerance in RTE), and USDA FSIS Directive 10,240.4 where meat/poultry is implicated",
                "EU": "EU Reg. 2073/2005 microbiological criteria (absence in 25 g for RTE infant / medical food; ≤100 CFU/g for other RTE at end of shelf-life), Reg. 852/2004 HACCP, and national RASFF notification obligations",
                "UK": "retained EU Reg. 2073/2005, UK Food Hygiene Regulations, and FSA listeria-in-RTE control guidance",
                "CA": "CFIA Policy on Control of Listeria monocytogenes in Ready-to-Eat Foods and the SFCR Preventive Control Plan",
                "AU_NZ": "FSANZ Food Standards Code Standard 1.6.1 (microbiological limits) and industry Listeria management guidelines",
                "JP": "Japan MHLW microbial standards and ready-to-eat food hygiene guidance",
                "KR": "MFDS RTE pathogen standards",
            },
            "salmonella_lm": {
                "US": "FDA 21 CFR 117 Subpart C Preventive Controls with FDA Guidance for Industry: Measures to Address the Risk for Contamination by Salmonella in Low-Moisture Ready-to-Eat Human Foods",
                "EU": "EU Reg. 2073/2005 microbiological criteria (Salmonella absence in 25 g for RTE categories) and Reg. 852/2004 HACCP",
                "UK": "retained EU Reg. 2073/2005 and FSA Salmonella low-moisture food guidance",
                "CA": "CFIA SFCR Preventive Control Plan with validated kill-step requirements for low-moisture foods",
                "AU_NZ": "FSANZ Food Standards Code Standards 1.6.1 and 3.2.2 with the FSANZ Getting Your Food Regulation Right guidance",
                "JP": "Japan MHLW microbial standards",
                "KR": "MFDS low-moisture food hygiene standards",
                "CODEX": "Codex CAC/RCP 75-2015 Code of Hygienic Practice for Low-Moisture Foods",
            },
            "salmonella": {
                "US": "FDA 21 CFR 117 Preventive Controls and, for meat/poultry, USDA FSIS performance standards",
                "EU": "EU Reg. 2073/2005 (Salmonella absence in 25 g for most RTE categories) and Reg. 852/2004 HACCP",
                "UK": "retained EU Reg. 2073/2005 and FSA / Food Hygiene Regulations",
                "CA": "CFIA SFCR Preventive Control Plan",
                "AU_NZ": "FSANZ Food Standards Code Standard 3.2.1 Food Safety Programs and Standard 1.6.1 microbiological limits",
                "JP": "Japan MHLW microbial standards",
                "KR": "MFDS Food Code pathogen limits",
            },
            "stec": {
                "US": "USDA FSIS adulterant declarations for E. coli O157:H7 and the Big-Six non-O157 STECs in non-intact raw beef (9 CFR 318/381), FDA Produce Safety Rule (21 CFR 112), and FDA Juice HACCP rule (21 CFR 120)",
                "EU": "EU Reg. 2073/2005 (STEC limits for RTE sprouted seeds), Reg. 852/2004 HACCP, and national STEC surveillance under Directive 2003/99/EC",
                "UK": "retained EU Reg. 2073/2005 and FSA STEC guidance with sprout-specific controls",
                "CA": "CFIA SFCR Preventive Control Plan with specific E. coli O157:H7 controls for ground beef",
                "AU_NZ": "FSANZ Food Standards Code Standard 1.6.1 (STEC limits in raw unpasteurised milk and cheese) and Standard 4.2.1 primary production",
                "JP": "Japan MHLW STEC control guidance",
                "KR": "MFDS STEC pathogen standards",
            },
            "regulatory": {
                "US": "FSMA §204 traceability (21 CFR 1 Subpart S) with Class I recall effectiveness checks",
                "EU": "EU Reg. 178/2002 Art. 18 traceability one-up / one-back and RASFF INFOSAN rapid-alert obligations",
                "UK": "retained Reg. 178/2002 Art. 18 and FSA Food Law Enforcement Code of Practice",
                "CA": "CFIA SFCR traceability and the Canadian food-recall framework under the Safe Food for Canadians Act",
                "AU_NZ": "FSANZ Food Standards Code 3.2.2 traceability and the Australian Consumer Law mandatory-reporting obligation",
                "JP": "Japan Food Sanitation Act notification obligations",
                "KR": "MFDS recall and traceability provisions",
            },
        }[hazard]

        # Determine ordering. The bucket with the most incidents comes first.
        order = sorted(
            [k for k in ("US","EU","UK","CA","AU_NZ","JP","KR") if b[k] > 0],
            key=lambda k: -b[k])
        if not order:
            # All recalls in "other" countries — default to global framing
            order = ["EU"]  # RASFF is the most common fallback framework

        primary_key = order[0]
        primary_text = FRAMEWORKS[primary_key]

        parallel_keys = order[1:]
        parallels_text = ""
        if parallel_keys:
            parallels_text = "; parallel frameworks apply under " + "; ".join(
                FRAMEWORKS[k] for k in parallel_keys)

        # Codex as a supra-national reference for low-moisture Salmonella
        if hazard == "salmonella_lm":
            if "CODEX" in FRAMEWORKS:
                parallels_text += ("; supranational reference: " + FRAMEWORKS["CODEX"])

        # Regulator names for the closing sentence
        AUTHORITY_NAMES = {
            "US":"FDA and USDA FSIS", "EU":"the national competent authority (with RASFF notification)",
            "UK":"the FSA", "CA":"CFIA", "AU_NZ":"FSANZ",
            "JP":"Japan MHLW", "KR":"MFDS",
        }
        authorities = [AUTHORITY_NAMES[k] for k in order]
        if len(authorities) == 1:
            regulators = authorities[0]
        elif len(authorities) == 2:
            regulators = " and ".join(authorities)
        else:
            regulators = ", ".join(authorities[:-1]) + ", and " + authorities[-1]

        return {"primary": primary_text,
                "parallels": parallels_text,
                "regulators": regulators,
                "buckets": b}

    # ------------------------------------------------------------------
    # Trigger 1 — Clostridium / botulinum (highest severity)
    # ------------------------------------------------------------------
    if bot:
        regs = _regs_for("botulinum", bot)
        return ("This window contains {ip} implicating Clostridium or "
                "botulinum toxin, with {co} cited for {path}. Any shelf-stable "
                "low-acid, acidified, aseptic/UHT, hot-filled, or reduced-oxygen-"
                "packaged product in the affected category must be reviewed to "
                "confirm that the scheduled thermal process has been established, "
                "filed, and validated under the guidance of a qualified process "
                "authority \u2014 required under {primary}{parallels}. The most "
                "common compliance gap behind recalls of this profile is an "
                "unfiled or outdated scheduled process, a process deviation "
                "resolved without qualified process authority review, a container "
                "or seal-integrity lapse, or a formulation change (pH, a_w, salt, "
                "preservative) introduced without re-evaluation. Tier-1 / Class-I "
                "classification on a low-acid product reliably triggers a "
                "regulatory process-filing audit by {regulators} with formal "
                "inspection-findings issuance on the subsequent visit."
                ).format(ip=_count_phrase(len(bot), "incident"), co=_names(bot),
                         path=bot[0].get("Pathogen","Clostridium botulinum"),
                         primary=regs["primary"], parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # ------------------------------------------------------------------
    # Trigger 2 — Listeria monocytogenes
    # ------------------------------------------------------------------
    lst = _by_pathogen("listeria")
    if lst:
        regs = _regs_for("listeria", lst)
        return ("This window contains {ip}, "
                "with {co} among those cited. Ready-to-eat (RTE) manufacturers "
                "\u2014 particularly deli, soft cheese and dairy (including raw-"
                "milk cheese), charcuterie and cured-meat, smoked and cured "
                "seafood, cooked-meat, refrigerated prepared salads, and cut "
                "produce \u2014 should review their environmental monitoring "
                "programme (EMP), Zone 1\u20134 sampling plan, and corrective-"
                "action triggers under {primary}{parallels}. The process-"
                "authority deliverable for this hazard class is a validated "
                "lethality at the kill step (where one exists) or a documented "
                "post-lethality control programme where "
                "the organism cannot be eliminated in-pack. Typical compliance "
                "gaps: incomplete Zone 1 sampling, sanitation SOPs not validated "
                "against worst-case soil load, equipment hollows harbouring "
                "persistent strains, and post-lethality recontamination pathways "
                "not mapped. Tier-1 classification with RTE product categories "
                "routinely triggers an EMP audit and formal inspection-findings "
                "issuance by {regulators}."
                ).format(ip=_count_phrase(len(lst), "Listeria monocytogenes incident"),
                         co=_names(lst),
                         primary=regs["primary"], parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # ------------------------------------------------------------------
    # Trigger 3 — Salmonella in low-moisture foods
    # ------------------------------------------------------------------
    sal = _by_pathogen("salmonella")
    sal_lm = [r for r in sal if _is_low_moisture(r)]
    if sal_lm:
        regs = _regs_for("salmonella_lm", sal_lm)
        return ("This window contains {ip} in low-moisture "
                "food categories, with {co} among those cited. Low-moisture "
                "foods \u2014 peanut and nut butters, flour and grain, milk "
                "powder and infant formula, spices and dried herbs, chocolate "
                "and cocoa, seeds and tahini \u2014 require a validated kill "
                "step because Salmonella survives for months to years at low "
                "water activity (a_w) and is not reliably reduced by ambient "
                "handling. The process-authority deliverable is a kill-step "
                "validation study demonstrating \u22655-log Salmonella reduction "
                "under worst-case product composition (fat, a_w, particulate "
                "size) and worst-case equipment conditions, per {primary}"
                "{parallels}. Typical compliance gaps: unvalidated wet-dry zone "
                "segregation, raw-ingredient supplier programme without "
                "certificate-of-analysis (COA) verification, insufficient "
                "sanitary design at post-kill-step transitions, and the absence "
                "of a documented environmental-monitoring programme for the "
                "dry side. Tier-1 classification in this product class "
                "routinely triggers a kill-step revalidation and formal "
                "inspection-findings issuance by {regulators}."
                ).format(ip=_count_phrase(len(sal_lm), "Salmonella incident"),
                         co=_names(sal_lm),
                         primary=regs["primary"], parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # Trigger 4 — Plain Salmonella (no low-moisture)
    if sal:
        regs = _regs_for("salmonella", sal)
        return ("This window contains {ip}, with {co} "
                "among those cited. The process-authority deliverable for "
                "Salmonella-implicated product is the validated kill step for "
                "the commodity (thermal, high-pressure, or equivalent), the "
                "supplier-verification programme on raw inputs, and the "
                "sanitary separation between raw-handling and ready-to-eat "
                "zones under {primary}{parallels}. Typical compliance gaps: "
                "supplier COAs accepted without independent verification "
                "sampling, shared equipment and tooling across raw-RTE "
                "boundaries, a validated kill-step not re-qualified after "
                "formulation or line changes, and time-temperature CCPs "
                "monitored without recording-rigour sufficient for inspector "
                "reconstruction. Tier-1 classification here triggers HACCP-"
                "plan reassessment and formal enforcement action by "
                "{regulators}."
                ).format(ip=_count_phrase(len(sal), "Salmonella incident"),
                         co=_names(sal),
                         primary=regs["primary"], parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # ------------------------------------------------------------------
    # Trigger 5 — E. coli STEC / O-serogroup
    # ------------------------------------------------------------------
    stec = _by_pathogen("stec", "e. coli", "escherichia")
    if stec:
        regs = _regs_for("stec", stec)
        return ("This window contains {ip}, with {co} among those cited. The "
                "pathogen has an infectious dose as low as ~10 organisms, and "
                "the process-authority deliverable is a validated kill step "
                "demonstrating \u22655-log STEC reduction for non-intact-beef "
                "processors, or a pre-harvest + post-harvest control "
                "programme for leafy-green, sprout, raw-milk cheese, and "
                "unpasteurised-juice producers, per {primary}{parallels}. "
                "Typical compliance gaps: grinding-plant raw-material cross-"
                "contamination without batch segregation, irrigation-water "
                "microbial monitoring not meeting applicable generic E. coli "
                "numeric limits, post-harvest wash-water chemistry (free "
                "chlorine, pH, ORP) not validated to prevent carry-over, "
                "time-temperature CCPs on hot-hold and cook-chill programmes "
                "insufficiently monitored, and sprout-seed treatment "
                "protocols unvalidated. Tier-1 STEC classification in these "
                "product categories routinely triggers formal enforcement "
                "action by {regulators}."
                ).format(ip=_count_phrase(len(stec), "E. coli (STEC / shiga-toxin-producing) incident"),
                         co=_names(stec),
                         primary=regs["primary"], parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # ------------------------------------------------------------------
    # Trigger 6 — Regulatory-response PA note.
    # Fires on weeks with either: (a) an outbreak spanning multiple
    # jurisdictions, or (b) ≥3 Tier-1 recalls across ≥3 countries.
    # ------------------------------------------------------------------
    outbreaks = [r for r in recalls if _safe_int(r.get("Outbreak", 0)) == 1]
    outbreak_countries = set(
        _country_display(r.get("Country", "")) for r in outbreaks
        if (r.get("Country") or "").strip())
    tier1 = [r for r in recalls if _safe_int(r.get("Tier", 2)) == 1]
    tier1_countries = set(
        _country_display(r.get("Country", "")) for r in tier1
        if (r.get("Country") or "").strip())

    multi_jurisdiction_outbreak = outbreaks and len(outbreak_countries) >= 2
    high_tier1_pressure         = len(tier1) >= 3 and len(tier1_countries) >= 3

    if multi_jurisdiction_outbreak or high_tier1_pressure:
        if multi_jurisdiction_outbreak:
            anchor = ("a multi-jurisdiction outbreak pattern ({ep} across "
                      "{n_co} countries: {countries})"
                      ).format(ep=_count_phrase(len(outbreaks), "confirmed outbreak event"),
                               n_co=len(outbreak_countries),
                               countries=", ".join(sorted(outbreak_countries)))
            basis_rows = outbreaks
        else:
            anchor = ("elevated Tier-1 regulatory pressure ({n_t1} Tier-1 "
                      "recalls across {n_co} countries: {countries})").format(
                          n_t1=len(tier1), n_co=len(tier1_countries),
                          countries=", ".join(sorted(tier1_countries)[:6]))
            basis_rows = tier1
        regs = _regs_for("regulatory", basis_rows)
        return ("This window shows {anchor}, signalling either co-ordinated "
                "enforcement under mutual-recognition arrangements (RASFF "
                "INFOSAN, WHO IHR notifications, CFIA-FDA trilateral co-"
                "operation) or independent convergence on the same commodity "
                "class. The process-authority deliverable for affected "
                "manufacturers is a 24-hour regulatory-response readiness "
                "package: scheduled-process documentation, HACCP plan with "
                "all CCP records for the implicated lot window, environmental-"
                "monitoring programme output, supplier COAs, and traceability "
                "records one-up / one-back under {primary}{parallels}. Typical "
                "compliance gaps exposed under cross-jurisdiction scrutiny: "
                "inconsistent batch-coding between markets, labelling mis-"
                "alignment, a recall-effectiveness check not completed within "
                "the inspector-expected window, and a press / retailer "
                "notification lag exceeding 24 hours. Multi-jurisdiction "
                "Tier-1 / Class-I events of this profile routinely escalate "
                "to a full-facility regulatory inspection by {regulators} "
                "and can support import-alert listing, detention-without-"
                "physical-examination, or licence-suspension action depending "
                "on the jurisdiction."
                ).format(anchor=anchor, primary=regs["primary"],
                         parallels=regs["parallels"],
                         regulators=regs["regulators"])

    # No trigger fired — return empty string (no P4 appended)
    return ""

def review_with_claude(text):
    """Optional grammar polish via Claude Haiku 4.5. Returns text unchanged on failure
    or when ANTHROPIC_API_KEY is not set. (Replaced OpenAI gpt-4o-mini, Apr 2026.)"""
    if not CLAUDE_API_KEY: return text
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01",
                     "Content-Type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 1200,
                  "messages": [{"role": "user",
                    "content": "Review this food safety analysis. Fix grammar. Keep structure/facts. Return polished version only.\n\n" + text}]},
            timeout=60)
        if r.status_code == 200:
            return r.json()["content"][0]["text"]
    except Exception as e: log.warning("Claude polish: %s", e)
    return text


def _recall_row(rank, r, top_n=5):
    pathogen = r.get("Pathogen","") or "Unknown"
    tier = _safe_int(r.get("Tier",2)); ob = _safe_int(r.get("Outbreak",0))
    company = r.get("Company","") or "\u2014"
    brand = r.get("Brand","") or "\u2014"
    product = r.get("Product","") or "\u2014"
    country = _country_display(r.get("Country","") or "Unknown")
    source = r.get("Source","") or ""; url = r.get("URL","") or ""
    dt = _fmt_date(r.get("Date",""))
    dot = _dot_color(pathogen); ps = pathogen.split("(")[0].strip()
    chip = '<span class="chip-tier1">T1</span>' if tier==1 else '<span class="chip-tier2">T2</span>'
    obc = ' <span class="chip-outbreak">OUTBREAK</span>' if ob else ""
    rc = "rank-num" if rank<=top_n else "rank-num rank-num--multi"
    sd = AUTHORITY_DISPLAY.get(source, source)
    if url and url.strip():
        lk = '<div class="juris-link"><a class="src-link" href="{}" target="_blank" rel="noopener">View source &rarr;</a></div>'.format(esc(url))
    else:
        lk = '<div class="juris-link"><span class="src-na" title="No verified specific-recall URL available">unverified</span></div>'
    bs = esc(brand) if brand and brand!="\u2014" else "\u2014"
    return """    <tr>
      <td class="{rc}" data-label="#">{rank}</td>
      <td class="date-cell" data-label="Date">{dt}</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:{dot}"></span>
        <span class="path-name">{ps}</span>
        {chip}{obc}
      </td>
      <td class="co-cell" data-label="Company"><strong>{co}</strong><div class="brand-sub">{bs}</div></td>
      <td class="prod-cell" data-label="Product">{prod}</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">{ctry}</div>
        <div class="src-sub">{src}</div>
        {lk}
      </td>
    </tr>""".format(rc=rc, rank=rank, dt=esc(dt), dot=dot, ps=esc(ps),
                    chip=chip, obc=obc, co=esc(company), bs=bs,
                    prod=esc(product), ctry=esc(country), src=esc(sd), lk=lk)


W16_CSS = """
:root {
  --black:#0a0e1a; --orange:#E8601A;
  --ink:#111827; --body:#1f2937; --muted:#6b7280; --dim:#9ca3af;
  --bg:#ffffff; --s1:#f9fafb; --s2:#f3f4f6; --brd:#e5e7eb;
  --red:#dc2626; --amber:#f59e0b; --violet:#9333ea; --green:#059669;
}
* { box-sizing:border-box; }
html, body { margin:0; padding:0; background:var(--bg); }
body {
  font-family:'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color:var(--body); font-size:14px; line-height:1.65;
  max-width:1180px; margin:0 auto; padding:0 40px 60px;
}
a { color:var(--orange); text-decoration:none; }
a:hover { text-decoration:underline; }

.masthead {
  border-top:6px solid var(--black);
  padding:28px 0 22px;
  display:flex; justify-content:space-between; align-items:flex-start;
  border-bottom:1px solid var(--brd);
  margin-bottom:32px;
}
.brand-block .brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:24px;
  color:var(--black); letter-spacing:-0.01em; text-transform:uppercase;
  line-height:1.1;
}
.brand-block .brand em { color:var(--orange); font-style:normal; font-weight:800; }
.brand-block .tagline {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:600;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.14em;
  margin-top:8px;
}
.mast-right { text-align:right; }
.report-label {
  display:inline-block; background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  padding:5px 11px; letter-spacing:0.12em; text-transform:uppercase;
  margin-bottom:10px;
}
.report-meta {
  font-family:'DM Mono', monospace; font-size:11px;
  color:var(--muted); line-height:1.8;
}
.report-meta strong { color:var(--ink); font-weight:700; }

.r-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:38px;
  color:var(--black); letter-spacing:-0.02em; line-height:1.15;
  margin:2px 0 10px;
}
.r-title .accent { color:var(--orange); }
.r-kicker {
  font-family:'Syne', sans-serif; font-weight:800; font-size:13px;
  color:var(--black); letter-spacing:0.08em; text-transform:uppercase;
  margin:8px 0 6px;
}
.r-kicker-dot { color:var(--orange); font-style:normal; margin:0 2px; }
.r-sub {
  color:var(--muted); font-size:14px; margin-bottom:16px;
}
.r-sub strong { color:var(--ink); font-weight:600; }
.r-authority {
  display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  padding:10px 14px; background:var(--s1); border-left:3px solid var(--orange);
  font-family:'DM Mono', monospace; font-size:11px; color:var(--ink);
  margin-bottom:30px;
}
.auth-label {
  font-size:9px; font-weight:700; color:var(--orange);
  text-transform:uppercase; letter-spacing:0.14em;
  border-right:1px solid var(--brd); padding-right:10px;
}

.kpi-strip {
  display:grid; grid-template-columns:repeat(4, 1fr);
  gap:1px; background:var(--brd); border:1px solid var(--brd);
  margin-bottom:32px;
}
.kpi { background:#fff; padding:22px 20px; }
.kpi-label {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.1em;
  margin-bottom:8px;
}
.kpi-value {
  font-family:'Syne', sans-serif; font-weight:800; font-size:42px;
  color:var(--black); line-height:1; letter-spacing:-0.02em;
}
.kpi-value.red { color:var(--red); }
.kpi-value.violet { color:var(--violet); }
.kpi-value.orange { color:var(--orange); font-size:20px; line-height:1.2; font-style:italic; }
.kpi-value a { color:inherit; text-decoration:none; border-bottom:2px solid var(--orange); padding-bottom:1px; }
.kpi-value a:hover { opacity:0.8; text-decoration:none; }
.kpi-delta {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  margin-top:10px; letter-spacing:0.04em;
}
.kpi-top { font-size:11px; color:var(--muted); margin-top:10px; font-style:italic; }

.sec-head {
  display:flex; align-items:baseline; gap:14px;
  margin:40px 0 16px;
}
.sec-num {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.12em;
}
.sec-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--black); letter-spacing:-0.01em;
}
.sec-rule { flex:1; height:1px; background:var(--brd); }
.sec-caption { color:var(--muted); font-size:13px; margin:-4px 0 14px; }
.sec-caption em { color:var(--ink); font-style:italic; }

.analysis {
  background:var(--s1); border-left:4px solid var(--orange);
  padding:26px 30px; margin-bottom:10px;
}
.analysis p { margin:0 0 14px; font-size:14.5px; line-height:1.75; }
.analysis p:last-child { margin-bottom:0; }
.analysis p.pa-note {
  margin:18px -30px 0 -30px; padding:18px 30px 2px 30px;
  background:#fff; border-top:1px solid var(--brd);
  font-size:13.5px; line-height:1.7;
}
.analysis p.pa-note .pa-label {
  display:inline; font-family:'DM Mono', monospace; font-weight:700;
  letter-spacing:0.08em; text-transform:uppercase;
  color:var(--red); font-size:10px; margin-right:8px;
}

table.data {
  width:100%; border-collapse:collapse; margin:0 0 10px;
  background:#fff; border:1px solid var(--brd);
  font-size:13px;
}
table.data th {
  background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  text-transform:uppercase; letter-spacing:0.1em;
  padding:12px 12px; text-align:left; border-bottom:2px solid var(--orange);
}
table.data td {
  padding:14px 12px; border-bottom:1px solid var(--brd);
  vertical-align:top;
}
table.data tr:last-child td { border-bottom:none; }
table.data tr:nth-child(even) td { background:#fafbfc; }
table.data td.num {
  font-family:'DM Mono', monospace; font-weight:600; text-align:right;
  white-space:nowrap;
}
table.data td.empty {
  text-align:center; color:var(--muted); padding:28px; font-style:italic;
}

/* Top 5 column sizing - keeps table within A4 and desktop viewport */
table.top5 { table-layout:fixed; width:100%; }
table.top5 th:nth-child(1), table.top5 td:nth-child(1) { width:5%;  }  /* # */
table.top5 th:nth-child(2), table.top5 td:nth-child(2) { width:9%;  }  /* Date */
table.top5 th:nth-child(3), table.top5 td:nth-child(3) { width:19%; }  /* Pathogen */
table.top5 th:nth-child(4), table.top5 td:nth-child(4) { width:18%; }  /* Company */
table.top5 th:nth-child(5), table.top5 td:nth-child(5) { width:30%; }  /* Product */
table.top5 th:nth-child(6), table.top5 td:nth-child(6) { width:19%; }  /* Jurisdiction+Source */
table.top5 td { word-wrap:break-word; overflow-wrap:break-word; }

.rank-num {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--orange); text-align:center;
  white-space:nowrap; font-variant-numeric:tabular-nums;
  letter-spacing:-0.02em;
}
.rank-num.rank-num--multi { font-size:18px; }
.date-cell {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  white-space:nowrap;
}
.path-dot {
  display:inline-block; width:9px; height:9px; border-radius:50%;
  margin-right:7px; vertical-align:middle;
}
.path-name { font-weight:600; color:var(--ink); font-style:italic; }
.co-cell strong { color:var(--black); font-weight:700; display:block; }
.brand-sub { font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }
.prod-cell { color:var(--body); }
.juris-country { font-weight:600; color:var(--ink); }
.src-sub {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); margin-top:3px;
}
.juris-link { margin-top:6px; }
.chip-tier1 {
  display:inline-block; background:var(--red); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-tier2 {
  display:inline-block; background:var(--amber); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-outbreak {
  display:inline-block; background:var(--violet); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:4px; letter-spacing:0.06em;
}
.src-link {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.02em;
}
.src-na { color:var(--dim); font-family:'DM Mono', monospace; font-size:10px; font-style:italic; }

.dist-grid {
  display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:10px;
}
.dist-grid h3 {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  text-transform:uppercase; letter-spacing:0.1em; margin:0 0 10px;
}
.bar-track {
  width:100%; height:8px; background:var(--s2);
  border-radius:1px; overflow:hidden;
}
.bar-fill { height:100%; }

.cta-box {
  margin:40px 0 30px;
  padding:26px 30px;
  background:var(--black); color:#fff;
  display:flex; justify-content:space-between; align-items:center;
  flex-wrap:wrap; gap:18px;
}
.cta-text { flex:1; min-width:280px; }
.cta-text h3 {
  font-family:'Syne', sans-serif; font-weight:800; font-size:20px;
  margin:0 0 6px; color:#fff; letter-spacing:-0.01em;
}
.cta-text p { margin:0; color:#d1d5db; font-size:13px; }
.cta-btn {
  background:var(--orange); color:#fff; font-family:'DM Mono', monospace;
  font-size:11px; font-weight:700; padding:14px 22px;
  text-transform:uppercase; letter-spacing:0.1em;
  border:none; cursor:pointer; white-space:nowrap;
}
.cta-btn:hover { background:#d35416; text-decoration:none; color:#fff; }

.meth {
  background:var(--s1); border:1px solid var(--brd);
  padding:22px 26px; margin-bottom:24px; font-size:13px;
  color:var(--body);
}
.meth strong { color:var(--black); }
.meth p { margin:0 0 10px; }
.meth p:last-child { margin-bottom:0; }

.footer {
  margin-top:50px; padding-top:26px; border-top:2px solid var(--black);
  display:flex; justify-content:space-between; align-items:flex-start;
  flex-wrap:wrap; gap:20px; font-size:12px;
}
.foot-brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:15px;
  color:var(--black); text-transform:uppercase; letter-spacing:0.02em;
}
.foot-brand em { color:var(--orange); font-style:normal; }
.foot-meta {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); line-height:1.8; margin-top:6px;
}
.foot-legal {
  font-size:11px; color:var(--muted); max-width:440px;
  text-align:right; line-height:1.6;
}

@media print {
  /* Running footer on every printed page: process-authority attribution
     anchors the AFTS differentiator visually throughout the document. */
  @page {
    size: A4;
    margin: 14mm 14mm 18mm 14mm;
    @bottom-left {
      content: "AFTS · Food Safety Validation Intelligence";
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
    @bottom-right {
      content: "Page " counter(page) " / " counter(pages);
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
  }
  body { max-width:none; padding:0; margin:0; font-family:'Times New Roman', Times, serif; font-size:12pt; }
  .cta-box { display:none; }

  /* Lock print-mode layout: even if the browser's print page is narrow,
     these must not collapse into mobile responsive layouts. */
  .masthead { flex-direction:row !important; }
  .mast-right { text-align:right !important; }
  .kpi-strip { grid-template-columns:repeat(4, 1fr) !important; }
  .dist-grid { display:block !important; grid-template-columns:1fr !important; gap:0 !important; }
  .dist-grid > div { width:100% !important; display:block !important; }
  .dist-grid > div:nth-child(2) { margin-top:18px !important; }
  .dist-grid > div { page-break-inside:avoid; break-inside:avoid; }

  /* Page 1 compression: tighten the above-the-fold so the first Intelligence
     Analysis paragraph opens on page 1 rather than orphaning the heading. */
  .masthead { border-top-width:4px; padding:18px 0 12px; margin-bottom:22px; }
  .brand-block .brand { font-size:18px; }
  .brand-block .tagline { font-size:10px; margin-top:5px; letter-spacing:0.12em; }
  .report-label { font-size:9px; padding:4px 10px; margin-bottom:8px; }
  .report-meta { font-size:10px; line-height:1.7; }
  .r-kicker { font-size:12px; margin:6px 0 5px; letter-spacing:0.07em; }
  .r-title { font-size:26px; margin:2px 0 8px; }
  .r-sub { font-size:13px; margin-bottom:12px; line-height:1.55; }
  .r-authority { padding:9px 12px; font-size:11px; margin-bottom:22px; }
  .auth-label { font-size:8px; padding-right:9px; }
  .kpi-strip { margin-bottom:24px; }
  .kpi { padding:16px 14px; }
  .kpi-label { font-size:9px; margin-bottom:6px; }
  .kpi-value { font-size:28px; }
  .kpi-value.orange { font-size:18px; }
  .kpi-delta { font-size:9px; margin-top:7px; }
  .kpi-top { font-size:10px; margin-top:7px; }
  .sec-head { margin:28px 0 12px; page-break-after:avoid; break-after:avoid; }
  .sec-num { font-size:10px; }
  .sec-title { font-size:20px; white-space:nowrap; }
  .analysis { padding:22px 26px; }
  .analysis p { font-size:13px; margin:0 0 12px; line-height:1.7; }
  .analysis p.pa-note { margin:14px -26px 0 -26px; padding:14px 26px 2px 26px; font-size:12px; line-height:1.65; }
  .analysis p.pa-note .pa-label { font-size:9px; }

  table.data th { background:var(--black) !important; color:#fff !important; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
  /* Prevent any table row from splitting across a page break */
  table.data tr { page-break-inside:avoid; break-inside:avoid; }
  .analysis { border-left-width:3px; }
  /* Top-5 print tightening - fit all 6 columns on A4 */
  table.top5 { font-size:9px; page-break-inside:avoid; }
  table.top5 th { padding:6px 5px; font-size:8px; }
  table.top5 td { padding:6px 5px; line-height:1.35; }
  table.top5 tr { page-break-inside:avoid; }
  table.top5 .rank-num { font-size:14px; }
  table.top5 .path-name { font-size:9px; }
  table.top5 .date-cell { font-size:8px; }
  table.top5 .prod-cell { font-size:9px; line-height:1.35; }
  table.top5 .co-cell strong { font-size:9px; }
  table.top5 .juris-country { font-size:9px; }
  table.top5 .brand-sub, table.top5 .src-sub { font-size:8px; margin-top:1px; }
  table.top5 .chip-tier1, table.top5 .chip-tier2, table.top5 .chip-outbreak { font-size:7px; padding:1px 3px; margin-left:3px; }
  table.top5 .src-link { font-size:8px; }
  table.top5 .juris-link { margin-top:3px; }
  /* Tighten the caption above the Top 5 so more room for rows */
  .sec-caption { font-size:10px; margin:-2px 0 8px; }
  /* Force section boundaries on page breaks for clean 4-page distribution:
     P1 = masthead + KPI + § 01 Analysis
     P2 = § 02 Top 5
     P3 = § 03 Distribution
     P4 = § 04 Methodology + Footer */
  section.page-break, div.page-break { page-break-before:always; }
  .sec-head.break-before { page-break-before:always; break-before:page; }

  /* Footer: switch from flex to a clean vertical stack for print.
     WeasyPrint and some browser print engines overlap the two halves
     when flex wraps at narrow widths - block layout avoids it entirely.
     page-break-inside: avoid keeps brand block + disclaimer together on one page. */
  .footer {
    display:block !important;
    margin-top:26px;
    page-break-inside:avoid;
    break-inside:avoid;
  }
  .footer > div { display:block !important; width:auto !important; }
  .footer > div:first-child { margin-bottom:12px; }
  .foot-legal {
    text-align:left !important;
    max-width:none !important;
    padding-top:10px;
    border-top:1px solid var(--brd);
  }
  /* Keep the methodology section with its adjacent section intact */
  .meth { page-break-inside:avoid; break-inside:avoid; }
}

@media screen and (max-width:900px) {
  body { padding:0 20px 40px; }
  .kpi-strip { grid-template-columns:repeat(2,1fr); }
  .dist-grid { grid-template-columns:1fr; }
  .masthead { flex-direction:column; gap:16px; }
  .mast-right { text-align:left; }
  .r-title { font-size:28px; }
}

/* Mobile Top-5: switch from a 6-column table to stacked cards.
   On phones, a horizontal table would either scroll sideways (bad UX) or
   compress columns into unreadable widths. Instead, each row becomes a
   card with labeled fields - all data visible, no horizontal scroll. */
@media screen and (max-width:700px) {
  table.top5, table.top5 thead, table.top5 tbody, table.top5 tr, table.top5 td {
    display:block; width:auto !important;
  }
  /* Kill all fixed column widths - they would make card-mode cells unreadably narrow */
  table.top5 th:nth-child(1), table.top5 td:nth-child(1),
  table.top5 th:nth-child(2), table.top5 td:nth-child(2),
  table.top5 th:nth-child(3), table.top5 td:nth-child(3),
  table.top5 th:nth-child(4), table.top5 td:nth-child(4),
  table.top5 th:nth-child(5), table.top5 td:nth-child(5),
  table.top5 th:nth-child(6), table.top5 td:nth-child(6) {
    width:auto !important;
  }
  table.top5 { border:none; table-layout:auto !important; }
  table.top5 thead { display:none; }
  table.top5 tr {
    border:1px solid var(--brd); border-left:4px solid var(--orange);
    background:#fff; margin-bottom:12px; padding:8px 4px;
    position:relative;
  }
  table.top5 tr:nth-child(even) td { background:transparent; }
  table.top5 td {
    border:none !important; padding:7px 14px 7px 108px !important;
    position:relative; min-height:28px;
    word-wrap:normal; overflow-wrap:normal;
  }
  table.top5 td::before {
    content:attr(data-label);
    position:absolute; left:14px; top:7px; width:88px;
    font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
    color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
  }
  /* Rank number sits in top-right corner as an orange badge */
  table.top5 .rank-num {
    position:absolute; top:8px; right:14px; padding:0 !important;
    font-size:28px; min-height:0; text-align:right;
  }
  table.top5 .rank-num::before { display:none; }
  table.top5 .date-cell { font-size:11px; }
  table.top5 .path-name { font-size:13px; }
  table.top5 .co-cell strong { font-size:13px; }
  table.top5 .prod-cell { line-height:1.45; font-size:13px; }
  table.top5 .juris-country { font-size:13px; }
  table.top5 .juris-link { margin-top:6px; }
}

@media screen and (max-width:480px) {
  body { padding:0 14px 30px; }
  .kpi-strip { grid-template-columns:1fr 1fr; }
  .kpi { padding:16px 14px; }
  .kpi-value { font-size:28px; }
  .r-title { font-size:24px; }
  .analysis { padding:18px 20px; }
  .analysis p { font-size:13px; }
  .analysis p.pa-note { margin:14px -20px 0 -20px; padding:14px 20px 2px 20px; }
}
"""


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Pathogen Intelligence Briefing &middot; Week {wnum}, {year}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
__CSS_PLACEHOLDER__
</style>
</head>
<body>

<header class="masthead">
  <div class="brand-block">
    <div class="brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="tagline">Food Safety Intelligence System &middot; Weekly Briefing</div>
  </div>
  <div class="mast-right">
    <div class="report-label">Subscribers Edition</div>
    <div class="report-meta">
      <strong>ISSUE</strong> &middot; Week {wnum}, {year}<br>
      <strong>PERIOD</strong> &middot; {period}<br>
      <strong>{published_label}</strong> &middot; {published}
    </div>
  </div>
</header>

<div class="r-kicker">AFTS <span class="r-kicker-dot">&middot;</span> Food Safety Validation Intelligence</div>
<h1 class="r-title">Pathogen Surveillance <span class="accent">&middot;</span> Week {wnum}</h1>
<p class="r-sub">
  AI-powered analysis of <strong>{total}</strong> regulatory recall actions across
  <strong>{n_jurisdictions}</strong> jurisdictions, aggregated from 66 primary sources
  monitored continuously by the AFTS intelligence platform.
</p>

<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value"><a href="#all-recalls">{total}</a></div>
    {delta_html}
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">{tier1}</div>
    <div class="kpi-delta" style="color:var(--muted)">Immediate public-health risk</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Active Outbreaks</div>
    <div class="kpi-value violet">{outbreaks}</div>
    <div class="kpi-delta" style="color:var(--muted)">Confirmed cluster events</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange">{top_pathogen_name}</div>
    <div class="kpi-top">{top_cnt} recall incidents &middot; {top_pct}% of total</div>
  </div>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 01</span>
  <h2 class="sec-title">Intelligence Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="analysis">
{analysis_html}
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 02</span>
  <h2 class="sec-title">Top 5 Critical Threats</h2>
  <span class="sec-rule"></span>
</div>
<p class="sec-caption">
  Ranked by pathogen severity (<em>C. botulinum</em> &rarr; <em>Listeria</em> &rarr; STEC &rarr; <em>Salmonella</em>), outbreak status, and tier classification.
  Each row links to the originating regulatory notice.
</p>
<table class="data top5">
  <thead><tr><th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th></tr></thead>
  <tbody>
    {top5_rows}
  </tbody>
</table>

<div class="sec-head">
  <span class="sec-num">&sect; 03</span>
  <h2 class="sec-title">Distribution Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="dist-grid">
  <div>
    <h3>Pathogen Profile</h3>
    <table class="data">
      <thead><tr><th>Pathogen</th><th class="num">Cases</th><th class="num">%</th><th>Share</th></tr></thead>
      <tbody>
{pathogen_rows}
      </tbody>
    </table>
  </div>
  <div>
    <h3>Geographic &middot; Regulatory</h3>
    <table class="data">
      <thead><tr><th>Country</th><th>Authority</th><th class="num">Cases</th><th class="num">%</th></tr></thead>
      <tbody>
{country_rows}
      </tbody>
    </table>
  </div>
</div>

<div class="cta-box">
  <div class="cta-text">
    <h3>Live Dashboard &middot; Full Dataset Access</h3>
    <p>Filter by pathogen, country, tier, and source. Download the accumulative XLSX dataset. Set custom alerts.</p>
  </div>
  <a class="cta-btn" href="https://www.advfood.tech/fsis-recalls" target="_blank" rel="noopener">Access Portal &rarr;</a>
</div>

<div id="all-recalls" class="sec-head">
  <span class="sec-num">&sect; 04</span>
  <h2 class="sec-title">All {total} Recalls &middot; {period}</h2>
  <span class="sec-rule"></span>
</div>
<p class="sec-caption">
  Complete record for the reporting period. Sorted by pathogen severity, outbreak status, and tier classification.
  Each row links to the originating regulatory notice.
</p>
<table class="data top5">
  <thead><tr><th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th></tr></thead>
  <tbody>
    {all_rows}
  </tbody>
</table>

<div class="sec-head">
  <span class="sec-num">&sect; 05</span>
  <h2 class="sec-title">Methodology &amp; Sources</h2>
  <span class="sec-rule"></span>
</div>
<div class="meth">
  <p><strong>Process authority.</strong> Analytical frameworks, severity rubrics, pathogen classification, and the engineering interpretation of each recall are developed by the AFTS process-authority practice, drawing on in-house expertise in food process engineering, thermal processing, and regulatory compliance. Every view is grounded in validated process engineering: thermal processing (21 CFR 113/114), pasteurisation (PMO), aseptic and UHT, hold-tube and F-value lethality, and HACCP. This is what the AFTS platform brings that pure data feeds do not &mdash; interpretation under engineering authority.</p>
  <p><strong>Data &amp; AI pipeline.</strong> The system aggregates regulatory recall notices from 66 primary sources across 60+ countries (FDA, USDA FSIS, RASFF, FSA, FSANZ, CFIA, RappelConso, BVL, AESAN, EFET, and national authorities) and processes each record through Gemini (extraction), OpenAI GPT (normalisation), and Claude (Tier-1 validation). Records are de-duplicated and harmonised into the accumulative dataset.</p>
  <p><strong>This briefing.</strong> Statistical analysis filters the accumulative dataset to the reporting week ({period}). AI-generated narrative is produced against AFTS process-authority prompts and edited for publication. Figures and pathogen names are preserved verbatim from source data.</p>
</div>

<footer class="footer">
  <div>
    <div class="foot-brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="foot-meta">Food Safety Validation Intelligence<br>advfood.tech &middot; info@advfood.tech &middot; Athens, Greece<br>&copy; {year} Advanced Food Tech Solutions</div>
  </div>
  <div class="foot-legal">This briefing is provided for informational purposes only and does not constitute regulatory, legal, or medical advice. Subscribers should verify recall status with the originating regulatory authority before taking action. Next issue: Friday, {next_issue}.</div>
</footer>

</body>
</html>"""


def build_html(week_end, recalls, prev_week, original_published=None):
    """Build a weekly report HTML.

    original_published:  None  → fresh publish; header reads
                                 "PUBLISHED · {week_end + 1}".
                         str   → rebuild; header reads
                                 "UPDATED · {today}". The string itself
                                 (returned by _extract_published_from_html)
                                 is just the "this is a rebuild" signal —
                                 its content is no longer used in render.

    Audit 2026-05-09 — added original_published with label-flip semantics.
    Pre-this change, every rebuild silently overwrote the PUBLISHED date
    with the new build's week_end+1, which on Wednesday rebuilds is
    identical to the original Saturday publish — so the user couldn't see
    that the report had been revised. Now rebuilds carry an explicit
    UPDATED label with today's date.
    """
    stats = compute_stats(recalls, prev_week)
    # Display window (audit 2026-05-12): under the new-rule (any row in
    # `recalls` carries a report_week stamp), the period runs Fri→Thu
    # (week_end−7 .. week_end−1) — the ship Friday is NOT part of the
    # data window. Legacy reports (no stamped rows) keep Sat→Fri
    # (week_end−6 .. week_end). See _display_window() for full reasoning.
    ws, display_end_date = _display_window(week_end, recalls)
    wnum = week_end.isocalendar()[1]; year = week_end.year
    total = stats["total"]
    sr = sort_by_severity(recalls)

    raw = generate_analysis_claude(stats, recalls)
    final = review_with_claude(raw)

    paras = [p.strip() for p in final.strip().split("\n\n") if p.strip()]
    pa_html = ""; reg = []
    for p in paras:
        pl = p.lower()
        # PA Note detection — any paragraph using the process-authority idiom
        # and citing a regulatory-enforcement framework. Jurisdiction-neutral
        # since the primary regulator block varies by week.
        is_pa = (
            ("process-authority deliverable" in pl) or
            ("qualified process authority" in pl) or
            ("regulatory-response readiness" in pl) or
            ("environmental monitoring programme (emp)" in pl) or
            ("kill-step validation study" in pl) or
            ("\u22655-log" in p) or
            ("multi-jurisdiction" in pl and ("escalate" in pl or "import-alert" in pl))
        )
        if is_pa:
            pa_html = '<p class="pa-note"><span class="pa-label">Process Authority Note:</span> ' + esc(p) + '</p>'
        else:
            reg.append(p)
    analysis = "\n".join("  <p>{}</p>".format(esc(p)) for p in reg)
    if pa_html: analysis += "\n" + pa_html

    t5rows = "\n".join(_recall_row(i+1, r, 5) for i,r in enumerate(sr[:5]))
    allrows = "\n".join(_recall_row(i+1, r, 5) for i,r in enumerate(sr))
    if not t5rows: t5rows = '<tr><td class="empty" colspan="6">No recalls this week</td></tr>'
    if not allrows: allrows = t5rows

    d = stats["delta"]; dp = stats["delta_pct"]
    if d < 0:
        dh = '<div class="kpi-delta" style="color:#059669">&#9660; {} ({}%) vs prior week</div>'.format(d, dp)
    elif d > 0:
        dh = '<div class="kpi-delta" style="color:#dc2626">&#9650; +{} (+{}%) vs prior week</div>'.format(d, dp)
    else:
        dh = '<div class="kpi-delta" style="color:var(--muted)">No change vs prior week</div>'

    tp, tc = stats["top_pathogen"]
    tpct = round(tc/max(total,1)*100) if total else 0

    prows = ""
    for path, cnt in stats["pathogen_counts"]:
        pct = round(cnt/max(total,1)*100); dot = _dot_color(path)
        prows += '        <tr>\n          <td><span class="path-dot" style="background:{}"></span><em class="path-name">{}</em></td>\n          <td class="num">{}</td>\n          <td class="num">{}%</td>\n          <td><div class="bar-track"><div class="bar-fill" style="width:{}%;background:{}"></div></div></td>\n        </tr>\n'.format(dot, esc(path), cnt, pct, pct, dot)
    if not prows: prows = '        <tr><td class="empty" colspan="4">No pathogen data</td></tr>\n'

    crows = ""
    for country, cnt in stats["country_counts"]:
        pct = round(cnt/max(total,1)*100)
        auth = GEO_AUTHORITY.get(country, "National Authority")
        crows += '        <tr>\n          <td>{}</td>\n          <td>{}</td>\n          <td class="num">{}</td>\n          <td class="num">{}%</td>\n        </tr>\n'.format(esc(country), esc(auth), cnt, pct)
    if not crows: crows = '        <tr><td class="empty" colspan="4">No geographic data</td></tr>\n'

    wsd = _fmt_date_short(ws); wed = _fmt_date(display_end_date)
    period = "{} &ndash; {}".format(wsd, wed)
    # Header label flips between two states (audit 2026-05-09):
    #   First publish (original_published is None) →
    #     "PUBLISHED · {week_end + 1}" — the formulaic Saturday-after-week-close.
    #   Rebuild (original_published is set) →
    #     "UPDATED · {today}" — the actual rebuild date.
    # The label change is the visible signal that the user is looking at
    # a revised version of a previously-published weekly briefing.
    if original_published:
        published_label = "UPDATED"
        pub = datetime.now(timezone.utc).strftime("%-d %b %Y")
    else:
        published_label = "PUBLISHED"
        pub = (week_end + timedelta(days=1)).strftime("%-d %b %Y")
    nf = _fmt_date(week_end + timedelta(days=7))

    html = HTML_TEMPLATE.format(
        wnum=wnum, year=year, period=period,
        published=pub, published_label=published_label, total=total,
        n_jurisdictions=len(stats["country_counts"]), delta_html=dh,
        tier1=stats["tier1"], outbreaks=stats["outbreaks"],
        top_pathogen_name=esc(tp), top_cnt=tc, top_pct=tpct,
        analysis_html=analysis, top5_rows=t5rows, pathogen_rows=prows,
        country_rows=crows, all_rows=allrows, next_issue=nf,
    )
    html = html.replace("__CSS_PLACEHOLDER__", W16_CSS)
    return html, stats

def update_dashboard_data(week_end, stats, all_recalls=None):
    """Write docs/data/weekly-index.json — the dashboard's loadReports()
    fetches this on tab open. Always writes the FULL accumulated history
    (every Friday-ending week that has at least one recall in the dataset),
    sorted newest-first. Dashboard handles slicing to 4 rich cards + per-
    year archive client-side, so we never have to touch index.html.

    Audit 2026-04-29 — switched from regex-mutating index.html (fragile,
    silently produced stale "outbreaks=3" cards when the dataset was
    corrected to outbreaks=1) to writing a JSON the dashboard re-fetches
    on every load. Single source of truth: docs/data/recalls.xlsx Recalls
    sheet → docs/data/weekly-index.json → dashboard cards.
    """
    out = ROOT / "docs" / "data" / "weekly-index.json"

    def _make_entry(we, st, wr_for_display=None):
        wn = we.isocalendar()[1]; yr = we.year
        # Display window via _display_window so JSON matches HTML period.
        # New-rule (stamped) reports: ws=we−7 (Fri), we_display=we−1 (Thu).
        # Legacy reports: ws=we−6 (Sat), we_display=we (Fri).
        ws, we_display = _display_window(we, wr_for_display or [])
        tp_name = st["top_pathogen"][0] if st["top_pathogen"] else "Mixed"
        return {"filename":"{}-W{:02d}.html".format(yr,wn),"week_num":wn,"year":yr,
                "week_start":ws.strftime("%Y-%m-%d"),"week_end":we_display.strftime("%Y-%m-%d"),
                "generated":datetime.now(timezone.utc).isoformat(),
                "total":st["total"],"tier1":st["tier1"],"outbreaks":st["outbreaks"],
                "top_pathogen":tp_name,
                "summary":"Week {} saw {} pathogen recalls with {} as primary concern.".format(
                    wn, st["total"], tp_name)}

    entries = []

    if all_recalls:
        # Find every distinct ISO week represented in the recall set, snap
        # each to its Friday end, build an entry. Only keep weeks whose
        # Friday is on or before today (no future weeks).
        today = date.today()
        week_ends_seen = set()
        for r in all_recalls:
            d = r.get("Date", "")
            if not d: continue
            try: rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
            except ValueError: continue
            # Snap rd's ISO week to that week's Friday (ISO weekday 5 = Fri)
            iso_year, iso_week, iso_dow = rd.isocalendar()
            # Friday of the same ISO week:
            mon = rd - timedelta(days=iso_dow - 1)
            fri = mon + timedelta(days=4)
            if fri <= today:
                week_ends_seen.add(fri)

        for we in sorted(week_ends_seen, reverse=True):
            prev_we = we - timedelta(days=7)
            wr = filter_week(all_recalls, we)
            if not wr: continue
            pr = filter_week(all_recalls, prev_we)
            st = compute_stats(wr, pr)
            entries.append(_make_entry(we, st, wr_for_display=wr))
    else:
        # Fallback: just the current week
        entries = [_make_entry(week_end, stats)]

    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(entries, indent=2, ensure_ascii=False),
                        encoding="utf-8")
        wk_list = ", ".join("W{}".format(e["week_num"]) for e in entries[:6])
        suffix = "" if len(entries) <= 6 else f" (+{len(entries)-6} more)"
        log.info("Wrote %s — %d entries: %s%s",
                 out, len(entries), wk_list, suffix)
    except Exception as e:
        log.error("weekly-index.json write failed: %s", e)

def write_weekly_summary_json(week_end, recalls, stats, data_dir):
    wnum = week_end.isocalendar()[1]; year = week_end.year
    # Audit 2026-05-12: week_end in this JSON is the DISPLAY end (Thu for
    # new-rule, Fri for legacy) so the dashboard formats it correctly.
    # The HTML filename and isocalendar-derived wnum still anchor to the
    # ship Friday — only the displayed window shifts.
    ws, we_display = _display_window(week_end, recalls)
    tp = stats.get("top_pathogen")
    leading = {"name":tp[0],"cases":tp[1],"pct":round(tp[1]/max(stats["total"],1)*100)} if tp and len(tp)>=2 else {"name":"Mixed","cases":0,"pct":0}
    sr = sort_by_severity(recalls); threats = []
    for i,r in enumerate(sr[:5],1):
        threats.append({"rank":i,"date":str(r.get("Date",""))[:10],
            "pathogen":str(r.get("Pathogen","")),"pathogen_raw":str(r.get("Pathogen","")),
            "tier":_safe_int(r.get("Tier",2)),"outbreak":bool(_safe_int(r.get("Outbreak",0))),
            "company":str(r.get("Company","")),"brand":str(r.get("Brand","\u2014")),
            "product":str(r.get("Product","")),"country":str(r.get("Country","")),
            "source":str(r.get("Source","")),"url":str(r.get("URL",""))})
    summary = {"filename":"{}-W{:02d}.html".format(year,wnum),
        "report_url":"https://gstoforos.github.io/Food-Safety-Intelligence-System/{}-W{:02d}.html".format(year,wnum),
        "dashboard_url":"https://www.advfood.tech/fsis-recalls",
        "week_num":wnum,"year":year,"week_start":ws.isoformat(),"week_end":we_display.isoformat(),
        "week_start_display":ws.strftime("%-d %b"),"week_end_display":we_display.strftime("%-d %b %Y"),
        "generated_utc":datetime.now(timezone.utc).isoformat(),
        "stats":{"total":stats["total"],"tier1":stats["tier1"],"outbreaks":stats["outbreaks"],
                 "delta":stats.get("delta",0),"delta_pct":stats.get("delta_pct",0)},
        "leading_pathogen":leading,"ai_lead_paragraph":"","top_threats":threats,
        "country_count":len(set(str(r.get("Country","")) for r in recalls if r.get("Country")))}
    out = data_dir / "weekly-summary-latest.json"
    out.write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding="utf-8")
    log.info("Wrote %s",out)

def _extract_total_from_html(path):
    """Read an existing report HTML and extract the total-recalls KPI.
    Returns int or None if the file doesn't exist or can't be parsed."""
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = re.search(r'<a href="#all-recalls">(\d+)</a>', html)
        if m: return int(m.group(1))
        # Fallback: look for "All N Recalls" in §04 heading
        m2 = re.search(r'All (\d+) Recalls', html)
        if m2: return int(m2.group(1))
    except Exception:
        pass
    return None


def _extract_published_from_html(path):
    """Read an existing report HTML and detect whether it has a header
    date marker (either "PUBLISHED" or "UPDATED").

    Returns the verbatim date string (e.g. "9 May 2026") or None.

    The CONTENT of the returned string is no longer used by build_html
    after the 2026-05-09 label-flip change — but the FACT that it
    returned non-None is the signal "this is a rebuild, flip the label
    to UPDATED and use today's date." Successive rebuilds keep flipping
    the date forward as expected.

    Two patterns supported (an already-rebuilt report carries UPDATED):
      • Fresh publish:  "PUBLISHED</strong> &middot; 9 May 2026"
      • After rebuild:  "UPDATED</strong> &middot; 13 May 2026"
    """
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = re.search(
            r'<strong>(?:PUBLISHED|UPDATED)</strong>\s*&middot;\s*([^<]+?)<',
            html,
        )
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return None


def _extract_label_from_html(path):
    """Read an existing report HTML and return its header label —
    either "PUBLISHED" or "UPDATED" — or None.

    Companion to _extract_published_from_html. Used by the Wednesday
    weekly-updates check (audit 2026-05-09) to detect the case where
    a report's count matches the current dataset BUT the label is
    still "PUBLISHED" — typically because a previous rebuild fired
    before this label-flip code was deployed and overwrote the file
    with a stale "PUBLISHED" header. The Wednesday flow uses this
    signal to force a one-shot rebuild that flips the label to
    "UPDATED" without waiting for a count change.
    """
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = re.search(
            r'<strong>(PUBLISHED|UPDATED)</strong>\s*&middot;\s*[^<]+<',
            html,
        )
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def refresh_stale_weeks(all_recalls, current_week_end, n_previous=1):
    """Check up to n_previous weeks before current_week_end.
    If the recall count in the dataset differs from what's baked into
    the existing HTML report, rebuild that week's report.
    Returns list of rebuilt week-end dates."""
    rebuilt = []
    for offset in range(1, n_previous + 1):
        prev_end = current_week_end - timedelta(days=7 * offset)
        prev_prev_end = prev_end - timedelta(days=7)
        wnum = prev_end.isocalendar()[1]
        year = prev_end.year
        report_path = ROOT / "docs" / "{}-W{:02d}.html".format(year, wnum)

        dataset_recalls = filter_week(all_recalls, prev_end)
        dataset_total = len(dataset_recalls)

        existing_total = _extract_total_from_html(report_path)

        if existing_total is None:
            log.info("W%02d: no existing report at %s — building fresh", wnum, report_path)
        elif existing_total == dataset_total:
            log.info("W%02d: report matches dataset (%d recalls) — no refresh needed", wnum, dataset_total)
            continue
        else:
            log.info("W%02d: STALE — report has %d recalls, dataset has %d — rebuilding",
                      wnum, existing_total, dataset_total)

        prev_week_recalls = filter_week(all_recalls, prev_prev_end)
        # Preserve the original publish date so the rebuilt HTML shows
        # "PUBLISHED · <orig> (updated <today>)" instead of overwriting
        # with today's date. Falls back to None (fresh build) if the
        # existing HTML doesn't carry a parseable PUBLISHED marker.
        orig_pub = _extract_published_from_html(report_path)
        html, stats = build_html(prev_end, dataset_recalls, prev_week_recalls,
                                 original_published=orig_pub)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(html, encoding="utf-8")
        log.info("W%02d refreshed -> %s (%d bytes, %d recalls, original_pub=%r)",
                  wnum, report_path, len(html), dataset_total, orig_pub)
        rebuilt.append(prev_end)

    return rebuilt


def main():
    ap = argparse.ArgumentParser(description="Build AFTS weekly report")
    ap.add_argument("--week-end", required=True, help="Friday YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT/"docs"/"data"/"recalls.xlsx"))
    ap.add_argument("--output", default=None)
    ap.add_argument("--refresh-previous", type=int, default=1, metavar="N",
                    help="Check N previous weeks for stale data and rebuild if needed (default: 1)")
    ap.add_argument("--no-refresh", action="store_true",
                    help="Skip stale-week refresh (build current week only)")
    args = ap.parse_args()
    week_end = datetime.strptime(args.week_end,"%Y-%m-%d").date()
    log.info("Building report for %s", week_end)
    all_r = load_recalls(Path(args.xlsx))
    log.info("Loaded %d recalls", len(all_r))

    # --- Refresh stale previous weeks ---
    if not args.no_refresh and args.refresh_previous > 0:
        log.info("Checking %d previous week(s) for stale data...", args.refresh_previous)
        rebuilt = refresh_stale_weeks(all_r, week_end, args.refresh_previous)
        if rebuilt:
            log.info("Refreshed %d stale report(s): %s",
                      len(rebuilt), ", ".join(d.strftime("W%V") for d in rebuilt))

    # --- Build current week ---
    wr = filter_week(all_r, week_end)
    pr = filter_week(all_r, week_end - timedelta(days=7))
    log.info("This week: %d  Prev: %d", len(wr), len(pr))
    html, stats = build_html(week_end, wr, pr)
    wnum = week_end.isocalendar()[1]
    out = Path(args.output) if args.output else ROOT/"docs"/"{}-W{:02d}.html".format(week_end.year, wnum)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    log.info("Report -> %s (%d bytes)", out, len(html))
    update_dashboard_data(week_end, stats, all_r)
    write_weekly_summary_json(week_end, wr, stats, Path(args.xlsx).parent)
    return 0

if __name__ == "__main__":
    sys.exit(main())
