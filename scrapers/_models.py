"""
scrapers/_models.py
===================
AFTS FSIS — Recall data model + normalization helpers.

Schema (14 fields, matches pipeline.merge_master.SCHEMA):
    Date, Source, Company, Brand, Product, Pathogen, Reason, Class,
    Country, Region, Tier, Outbreak, URL, Notes

Exports
-------
    Recall              Dataclass for a single recall row.
    PATHOGEN_RULES      List of (canonical_name, regex_source_string) 2-tuples.
    normalize_pathogen  Raw string -> canonical pathogen name, or "" if not a pathogen.
    normalize_country   Raw country string -> canonical country name.
    infer_region        Country -> region bucket.
    assign_tier         (pathogen, outbreak) -> 1 | 2 | 3.

Consumers
---------
    scrapers._base                         imports everything above
    scrapers.news_feeds._news_base         imports normalize_pathogen, PATHOGEN_RULES
    enrichment.enrich_rows                 imports Recall + all helpers
    enrichment.gemini_client               (indirect, through enrich_rows)
    pipeline.merge_master                  imports Recall
    pipeline.run_all                       imports Recall
    pipeline.gap_finder_claude             imports Recall + all helpers
    pipeline.gap_finder_openai             imports Recall + all helpers
    tools.cleanup_dataset                  imports normalize_pathogen, normalize_country, assign_tier, infer_region
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Tuple


# ---------------------------------------------------------------------------
# Recall dataclass — 14 fields matching the Recalls sheet schema
# ---------------------------------------------------------------------------

@dataclass
class Recall:
    """One normalized recall row, ready to write to the Pending sheet."""

    Date: str = ""          # ISO date (YYYY-MM-DD)
    Source: str = ""        # agency name, e.g. "FDA", "USDA FSIS", "RASFF"
    Company: str = ""       # recalling company
    Brand: str = "—"        # product brand (em-dash when unknown — matches existing data)
    Product: str = ""       # product description
    Pathogen: str = ""      # canonical pathogen name (after normalize_pathogen)
    Reason: str = ""        # short reason text
    Class: str = "Recall"   # FDA-style recall class or generic "Recall"
    Country: str = ""       # canonical country name
    Region: str = ""        # region bucket (Europe, North America, etc.)
    Tier: int = 3           # 1 = critical, 2 = major, 3 = minor
    Outbreak: int = 0       # 0 / 1 — is an associated outbreak/illness reported
    URL: str = ""           # direct link to the recall detail page
    Notes: str = ""         # free-form; also holds rejection reason on validation fail

    # ---- instance methods ----
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def normalize(self) -> "Recall":
        """
        Apply canonical normalization to pathogen, country, region, tier and
        trim whitespace. Mutates self in place and returns self.
        """
        # Strip every string field
        for f in ("Date", "Source", "Company", "Brand", "Product",
                  "Pathogen", "Reason", "Class", "Country", "Region",
                  "URL", "Notes"):
            v = getattr(self, f, "")
            if isinstance(v, str):
                setattr(self, f, v.strip())
            elif v is None:
                setattr(self, f, "")

        # Outbreak must be int 0/1
        try:
            self.Outbreak = 1 if int(self.Outbreak or 0) else 0
        except (TypeError, ValueError):
            self.Outbreak = 0

        # Pathogen canonicalization (keep original if already canonical)
        if self.Pathogen:
            norm = normalize_pathogen(self.Pathogen)
            if norm:
                self.Pathogen = norm

        # Country + region
        if self.Country:
            self.Country = normalize_country(self.Country)
        if self.Country and not self.Region:
            self.Region = infer_region(self.Country)

        # Tier derivation
        self.Tier = assign_tier(self.Pathogen, self.Outbreak)

        # Safe defaults
        if not self.Brand:
            self.Brand = "—"
        if not self.Class:
            self.Class = "Recall"

        return self


# ---------------------------------------------------------------------------
# Pathogen taxonomy
# ---------------------------------------------------------------------------
# PATHOGEN_RULES is (canonical_name, regex_source_string) — a 2-tuple list.
# Consumers iterate with: `for name, pattern in PATHOGEN_RULES:` so the 2-tuple
# shape is part of the public API — do not change it.
#
# Order matters. Specific patterns (STEC / O-serotype) must come BEFORE generic
# patterns (plain "E. coli") so first-match-wins produces the correct canonical.
#
# Tiers are stored separately in _TIERS so PATHOGEN_RULES stays 2-tuple-shaped.

PATHOGEN_RULES: List[Tuple[str, str]] = [
    # --- Tier 1 (critical) ---
    ("Listeria monocytogenes",
        r"\blisteria(\s*monocytogenes)?\b|\bl\.?\s*monocytogenes\b"),
    ("Shiga toxin-producing E. coli (STEC)",
        r"\b(shiga[\s-]?toxin|stec|vtec|ehec)\b|\be\.?\s*coli\s*o\d{2,3}\b"),
    ("Clostridium botulinum",
        r"\bc(lostridium)?\.?\s*botulinum\b|\bbotulism\b|\bbotulinum\s*toxin\b"),
    ("Cereulide (B. cereus toxin)",
        r"\bcereulide\b|\bemetic\s*toxin\b"),
    ("Marine biotoxin",
        r"\bbiotoxin|\bsaxitoxin|\bdomoic\s*acid|\btetrodotoxin|\bciguatoxin|\bokadaic\s*acid\b"),

    # --- Tier 2 (major) ---
    ("Salmonella", r"\bsalmonella\b"),
    ("Hepatitis A virus", r"\bhepatitis\s*a\b|\bhav\b"),
    ("Norovirus", r"\bnorovirus\b"),

    # --- Tier 3 (minor) ---
    ("Escherichia coli (generic)", r"\be\.?\s*coli\b"),  # after STEC check
    ("Campylobacter", r"\bcampylobacter\b"),
    ("Vibrio", r"\bvibrio\b"),
    ("Cyclospora cayetanensis", r"\bcyclospora\b"),
    ("Yersinia enterocolitica", r"\byersinia\b"),
    ("Bacillus cereus", r"\bb(acillus)?\.?\s*cereus\b"),
    ("Brucella", r"\bbrucella\b"),
    ("Aflatoxin", r"\baflatoxin\w*\b"),
    ("Ochratoxin", r"\bochratoxin\w*\b|\bocratoxin\w*\b|\bocratossin\w*\b"),
    ("Alternaria toxins",
        r"\balternariol\b|\btenuazonic\b|\balternaria[\s\-]+toxin|toxines?[\s\-]+(?:d['e\s]+)?alternaria"),
    ("T-2 / HT-2 toxin", r"\b(?:ht|t)[\s\-]?2[\s\-]?toxin\b"),
    ("Ergot alkaloids",
        r"\bergot\b|\bclaviceps\b|\bmutterkorn\b|"
        r"\balcaloid\w*\s+(?:de|of|d['e]?)\s*ergot"),
    ("Citrinin", r"\bcitrinin\b"),
    ("Mycotoxin",
        r"\bmycotoxin\b|\bmykotoxin\b|\bmicotoxin\w*\b|\bmicotossin\w*\b|"
        r"\bfumonisin\w*\b|\bzearalenon\w*\b|\bdeoxynivalenol\b|\bnivalenol\b|"
        r"\bpatulin\w*\b|\bfusarium\s+toxin"),
    ("Cronobacter sakazakii",
        r"\bcronobacter\b|\benterobacter\s*sakazakii\b"),
    ("Staphylococcus enterotoxin",
        r"\bstaphylococcus\b|\bstaph\s*aureus\b|\bstaphylococcal\s*enterotoxin\b"),
    ("Shigella", r"\bshigella\b"),
    ("Histamine / scombrotoxin", r"\bhistamine\b|\bscombro(toxin|id)?\b"),
]

_TIERS: Dict[str, int] = {
    # Tier 1
    "Listeria monocytogenes": 1,
    "Shiga toxin-producing E. coli (STEC)": 1,
    "Clostridium botulinum": 1,
    "Cereulide (B. cereus toxin)": 1,
    "Marine biotoxin": 1,
    # Tier 2
    "Salmonella": 2,
    "Hepatitis A virus": 2,
    "Norovirus": 2,
    # Tier 3
    "Escherichia coli (generic)": 3,
    "Campylobacter": 3,
    "Vibrio": 3,
    "Cyclospora cayetanensis": 3,
    "Yersinia enterocolitica": 3,
    "Bacillus cereus": 3,
    "Brucella": 3,
    "Aflatoxin": 3,
    "Ochratoxin": 3,
    "Mycotoxin": 3,
    "Alternaria toxins": 3,
    "T-2 / HT-2 toxin": 3,
    "Ergot alkaloids": 3,
    "Citrinin": 3,
    "Cronobacter sakazakii": 3,
    "Staphylococcus enterotoxin": 3,
    "Shigella": 3,
    "Histamine / scombrotoxin": 3,
}

# Pre-compiled regexes for fast matching (internal)
_COMPILED_RULES: List[Tuple[str, "re.Pattern[str]"]] = [
    (name, re.compile(src, re.IGNORECASE)) for name, src in PATHOGEN_RULES
]

# Non-pathogen markers — if the raw string is dominated by these, drop the row
# unless a pathogen is ALSO named (rare: "Salmonella plus undeclared milk").
_NON_PATHOGEN_MARKERS: Tuple[str, ...] = (
    "allergen", "undeclared", "mislabel", "label error",
    "packaging defect", "soft plastic", "rubber",
)


def normalize_pathogen(raw: Any) -> str:
    """
    Map a free-form string to a canonical pathogen name.
    Returns "" when the string does NOT describe a pathogen AFTS tracks.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""

    low = s.lower()
    non_pathogen_hit = any(marker in low for marker in _NON_PATHOGEN_MARKERS)
    if non_pathogen_hit:
        # Still allow if a pathogen is ALSO named
        for name, pattern in _COMPILED_RULES:
            if pattern.search(s):
                return name
        return ""

    for name, pattern in _COMPILED_RULES:
        if pattern.search(s):
            return name
    return ""


def assign_tier(pathogen: Any, outbreak: Any = 0) -> int:
    """
    Map (canonical pathogen name, outbreak flag) to severity tier.

        Tier 1 = critical
        Tier 2 = major
        Tier 3 = minor (default when pathogen unknown)

    Outbreak=1 bumps the tier up by one (minimum tier 1) — an outbreak with
    even a Tier-3 pathogen is materially more serious than a recall alone.
    """
    if not pathogen:
        base = 3
    else:
        p = str(pathogen).strip()
        base = _TIERS.get(p, 0)
        if base == 0:
            # Not in lookup by canonical; try matching as raw string
            for name, pattern in _COMPILED_RULES:
                if pattern.search(p):
                    base = _TIERS.get(name, 3)
                    break
            if base == 0:
                base = 3

    try:
        outbreak_int = 1 if int(outbreak or 0) else 0
    except (TypeError, ValueError):
        outbreak_int = 0

    if outbreak_int:
        return max(1, base - 1)
    return base


# ---------------------------------------------------------------------------
# Country + region normalization
# ---------------------------------------------------------------------------

_COUNTRY_ALIASES: Dict[str, str] = {
    "usa": "USA",
    "us": "USA",
    "u.s.": "USA",
    "u.s.a.": "USA",
    "united states": "USA",
    "united states of america": "USA",
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
    "korea, south": "South Korea",
    "korea, republic of": "South Korea",
    "prc": "China",
    "people's republic of china": "China",
    "mainland china": "China",
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
    "deutschland": "Germany",
    "españa": "Spain",
    "brasil": "Brazil",
    "méxico": "Mexico",
    "türkiye": "Turkey",
}


def normalize_country(raw: Any) -> str:
    """Map common country aliases to a canonical name. Unknown input returned stripped."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    return _COUNTRY_ALIASES.get(s.lower(), s)


_REGION_MAP: Dict[str, str] = {
    # North America
    "USA": "North America", "Canada": "North America", "Mexico": "North America",

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
    "Côte d'Ivoire": "Africa",

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


def infer_region(country: Any) -> str:
    """Canonical country name -> region bucket. Returns 'Unknown' if not mapped."""
    if not country:
        return "Unknown"
    return _REGION_MAP.get(str(country).strip(), "Unknown")


__all__ = [
    "Recall",
    "PATHOGEN_RULES",
    "normalize_pathogen",
    "normalize_country",
    "infer_region",
    "assign_tier",
]
