"""
scrapers/_models.py
===================
AFTS FSIS — Recall data model + normalization helpers.

Exports
-------
    Recall              Dataclass for a single recall row.
    PATHOGEN_RULES      Ordered list of (compiled regex, canonical name, tier) tuples.
    normalize_pathogen  Raw string -> canonical pathogen name, or "" if not a pathogen.
    normalize_country   Raw country string -> canonical country name.
    infer_region        Country -> region bucket (Africa, Asia, Europe, ...).
    assign_tier         Canonical pathogen -> 1 | 2 | 3 (higher severity = lower number).

Consumers
---------
    scrapers._base                     imports Recall, normalize_pathogen,
                                       normalize_country, infer_region, assign_tier
    enrichment.gemini_client           imports normalize_pathogen, PATHOGEN_RULES
    review.claude_client               imports Recall
    review.openai_client               imports Recall
    pipeline.merge_master              imports Recall
    pipeline.run_all                   imports Recall
    docs.build_weekly_report_afts      imports PATHOGEN_RULES (for tier lookup)
    docs.build_monthly_report_afts     imports PATHOGEN_RULES
    tools.cleanup_dataset              imports normalize_pathogen, assign_tier
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# Recall dataclass
# ---------------------------------------------------------------------------

@dataclass
class Recall:
    """One normalized recall row, ready to write to the Pending sheet."""

    date: str = ""                  # ISO date of the recall (YYYY-MM-DD)
    agency: str = ""                # e.g. "FDA", "USDA FSIS", "RASFF"
    country: str = ""               # canonical country name
    region: str = ""                # Africa | Asia | Europe | Latin America | ...
    company: str = ""               # recalling company / brand
    product: str = ""               # product name / description
    pathogen: str = ""              # canonical pathogen name
    tier: int = 3                   # 1 = critical, 2 = major, 3 = minor
    url: str = ""                   # direct link to the recall detail page
    description: str = ""           # 1–2 sentence summary
    source_url: str = ""            # agency listing page (optional, for debugging)
    scraped_at: str = ""            # ISO timestamp (UTC) when extracted
    notes: str = ""                 # free-form; holds rejection reason on validation fail

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Pathogen normalization
# ---------------------------------------------------------------------------
# Rules are evaluated IN ORDER; first match wins. Order matters:
#   STEC / O157 patterns MUST come before the generic E. coli rule.
#   Biotoxins / cereulide MUST come before generic B. cereus.

_PATHOGEN_RULES_RAW: List[Tuple[str, str, int]] = [
    # --- Tier 1 (critical) ---
    (r"\blisteria(\s*monocytogenes)?\b|\bl\.?\s*monocytogenes\b",
     "Listeria monocytogenes", 1),
    (r"\b(shiga[\s-]?toxin|stec|vtec|ehec)\b|\be\.?\s*coli\s*o\d{2,3}\b",
     "Shiga toxin-producing E. coli (STEC)", 1),
    (r"\bc(lostridium)?\.?\s*botulinum\b|\bbotulism\b|\bbotulinum\s*toxin\b",
     "Clostridium botulinum", 1),
    (r"\bcereulide\b|\bemetic\s*toxin\b",
     "Cereulide (B. cereus toxin)", 1),
    (r"\bbiotoxin|\bsaxitoxin|\bdomoic\s*acid|\btetrodotoxin|\bciguatoxin|\bokadaic\s*acid\b",
     "Marine biotoxin", 1),

    # --- Tier 2 (major) ---
    (r"\bsalmonella\b", "Salmonella", 2),
    (r"\bhepatitis\s*a\b|\bhav\b", "Hepatitis A virus", 2),
    (r"\bnorovirus\b", "Norovirus", 2),

    # --- Tier 3 (minor) ---
    (r"\be\.?\s*coli\b", "Escherichia coli (generic)", 3),  # after STEC check
    (r"\bcampylobacter\b", "Campylobacter", 3),
    (r"\bvibrio\b", "Vibrio", 3),
    (r"\bcyclospora\b", "Cyclospora cayetanensis", 3),
    (r"\byersinia\b", "Yersinia enterocolitica", 3),
    (r"\bb(acillus)?\.?\s*cereus\b", "Bacillus cereus", 3),
    (r"\bbrucella\b", "Brucella", 3),
    (r"\baflatoxin\b", "Aflatoxin", 3),
    (r"\bochratoxin\b", "Ochratoxin", 3),
    (r"\bmycotoxin\b|\bfumonisin\b|\bzearalenone\b|\bdeoxynivalenol\b|\bDON\b",
     "Mycotoxin", 3),
    (r"\bcronobacter\b|\benterobacter\s*sakazakii\b",
     "Cronobacter sakazakii", 3),
    (r"\bstaphylococcus\b|\bstaph\s*aureus\b|\bstaphylococcal\s*enterotoxin\b",
     "Staphylococcus enterotoxin", 3),
    (r"\bshigella\b", "Shigella", 3),
]

PATHOGEN_RULES: List[Tuple[re.Pattern, str, int]] = [
    (re.compile(pat, re.IGNORECASE), name, tier)
    for pat, name, tier in _PATHOGEN_RULES_RAW
]

# Non-pathogen markers — if the raw string is dominated by these, drop the row
# unless a pathogen is ALSO named.
_NON_PATHOGEN_MARKERS: Tuple[str, ...] = (
    "allergen", "undeclared", "mislabel", "label error",
    "foreign material", "foreign object", "packaging defect",
    "glass fragment", "metal fragment", "plastic fragment",
    "soft plastic", "rubber",
)


def normalize_pathogen(raw: str) -> str:
    """
    Map a free-form string to a canonical pathogen name.
    Returns "" when the string does NOT describe a pathogen that AFTS tracks —
    allergens, label errors, foreign material, packaging defects all return "".
    """
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""

    low = s.lower()
    non_pathogen_hit = any(marker in low for marker in _NON_PATHOGEN_MARKERS)
    if non_pathogen_hit:
        # Still allow if a pathogen is ALSO named (rare: "Salmonella + undeclared milk")
        for pattern, name, _ in PATHOGEN_RULES:
            if pattern.search(s):
                return name
        return ""

    for pattern, name, _ in PATHOGEN_RULES:
        if pattern.search(s):
            return name
    return ""


def assign_tier(pathogen: str) -> int:
    """Canonical pathogen name -> severity tier (1/2/3). Returns 3 if unknown."""
    if not pathogen:
        return 3
    # Direct lookup by canonical name
    for _, name, tier in _PATHOGEN_RULES_RAW:
        if name.lower() == pathogen.lower():
            return tier
    # Fallback: match regex against the pathogen string itself
    for pattern, _, tier in PATHOGEN_RULES:
        if pattern.search(pathogen):
            return tier
    return 3


# ---------------------------------------------------------------------------
# Country + region normalization
# ---------------------------------------------------------------------------

_COUNTRY_ALIASES: Dict[str, str] = {
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "great britain": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "northern ireland": "United Kingdom",
    "republic of ireland": "Ireland",
    "eire": "Ireland",
    "south korea": "South Korea",
    "republic of korea": "South Korea",
    "korea": "South Korea",
    "prc": "China",
    "people's republic of china": "China",
    "roc": "Taiwan",
    "chinese taipei": "Taiwan",
    "uae": "United Arab Emirates",
    "czech republic": "Czechia",
    "holland": "Netherlands",
    "the netherlands": "Netherlands",
    "russian federation": "Russia",
    "eu": "European Union",
    "european union": "European Union",
    "hellas": "Greece",
    "hellenic republic": "Greece",
}


def normalize_country(raw: str) -> str:
    """Map common country aliases to a canonical name. Unknown input returned as-is (stripped)."""
    if not raw:
        return ""
    s = raw.strip()
    return _COUNTRY_ALIASES.get(s.lower(), s)


_REGION_MAP: Dict[str, str] = {
    # North America
    "United States": "North America",
    "Canada": "North America",
    "Mexico": "North America",

    # Europe
    "United Kingdom": "Europe", "Ireland": "Europe", "France": "Europe",
    "Germany": "Europe", "Spain": "Europe", "Italy": "Europe",
    "Portugal": "Europe", "Netherlands": "Europe", "Belgium": "Europe",
    "Luxembourg": "Europe", "Austria": "Europe", "Switzerland": "Europe",
    "Denmark": "Europe", "Sweden": "Europe", "Norway": "Europe",
    "Finland": "Europe", "Iceland": "Europe", "Poland": "Europe",
    "Czechia": "Europe", "Slovakia": "Europe", "Hungary": "Europe",
    "Romania": "Europe", "Bulgaria": "Europe", "Greece": "Europe",
    "Cyprus": "Europe", "Malta": "Europe", "Estonia": "Europe",
    "Latvia": "Europe", "Lithuania": "Europe", "Slovenia": "Europe",
    "Croatia": "Europe", "Serbia": "Europe", "Bosnia and Herzegovina": "Europe",
    "North Macedonia": "Europe", "Albania": "Europe", "Montenegro": "Europe",
    "Kosovo": "Europe", "Moldova": "Europe", "Ukraine": "Europe",
    "Belarus": "Europe", "Russia": "Europe", "Turkey": "Europe",
    "European Union": "Europe",

    # Asia
    "China": "Asia", "Japan": "Asia", "South Korea": "Asia", "Taiwan": "Asia",
    "Hong Kong": "Asia", "Singapore": "Asia", "Malaysia": "Asia",
    "Thailand": "Asia", "Vietnam": "Asia", "Philippines": "Asia",
    "Indonesia": "Asia", "India": "Asia", "Pakistan": "Asia",
    "Bangladesh": "Asia", "Sri Lanka": "Asia", "Nepal": "Asia",
    "Myanmar": "Asia", "Cambodia": "Asia", "Laos": "Asia",

    # Oceania
    "Australia": "Oceania", "New Zealand": "Oceania",
    "Fiji": "Oceania", "Papua New Guinea": "Oceania",

    # Middle East
    "Israel": "Middle East", "Saudi Arabia": "Middle East",
    "United Arab Emirates": "Middle East", "Qatar": "Middle East",
    "Kuwait": "Middle East", "Bahrain": "Middle East", "Oman": "Middle East",
    "Jordan": "Middle East", "Lebanon": "Middle East", "Iran": "Middle East",
    "Iraq": "Middle East", "Yemen": "Middle East", "Syria": "Middle East",
    "Palestine": "Middle East",

    # Africa
    "South Africa": "Africa", "Egypt": "Africa", "Morocco": "Africa",
    "Tunisia": "Africa", "Algeria": "Africa", "Libya": "Africa",
    "Nigeria": "Africa", "Kenya": "Africa", "Ethiopia": "Africa",
    "Ghana": "Africa", "Tanzania": "Africa", "Uganda": "Africa",
    "Zimbabwe": "Africa", "Zambia": "Africa", "Mozambique": "Africa",
    "Senegal": "Africa", "Ivory Coast": "Africa", "Cameroon": "Africa",

    # Latin America
    "Brazil": "Latin America", "Argentina": "Latin America",
    "Chile": "Latin America", "Peru": "Latin America",
    "Colombia": "Latin America", "Venezuela": "Latin America",
    "Uruguay": "Latin America", "Paraguay": "Latin America",
    "Bolivia": "Latin America", "Ecuador": "Latin America",
    "Costa Rica": "Latin America", "Panama": "Latin America",
    "Guatemala": "Latin America", "Honduras": "Latin America",
    "El Salvador": "Latin America", "Nicaragua": "Latin America",
    "Cuba": "Latin America", "Dominican Republic": "Latin America",
    "Puerto Rico": "Latin America", "Jamaica": "Latin America",
    "Trinidad and Tobago": "Latin America", "Haiti": "Latin America",
}


def infer_region(country: str) -> str:
    """Map canonical country name to AFTS region bucket. Returns 'Unknown' if not mapped."""
    if not country:
        return "Unknown"
    return _REGION_MAP.get(country, "Unknown")


__all__ = [
    "Recall",
    "PATHOGEN_RULES",
    "normalize_pathogen",
    "normalize_country",
    "infer_region",
    "assign_tier",
]
