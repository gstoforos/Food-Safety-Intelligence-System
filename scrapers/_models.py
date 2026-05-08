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
from typing import Any, Dict, List, Optional, Tuple


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

        # Translate non-English Class values to canonical English short form.
        # Added 2026-05-07 after audit found 8 raw "Volontaire" + 82 verbose
        # "Mandatory recall (prefectural order)" rows in the Recalls sheet —
        # neither matched the operator-stated rule that all fields except
        # Company/Brand must be in English (US). RappelConso's API returns
        # `nature_juridique_du_rappel` in French; no scraper translated it
        # and the prompt-level rule never made it into code.
        self.Class = _normalize_class_language(self.Class)

        return self


# ---------------------------------------------------------------------------
# Class language normalization
# ---------------------------------------------------------------------------
# Operator rule (2026-05-07): every field except Company and Brand must be
# stored in English (US). Class values come straight from regulator APIs,
# many of which publish in their native language. Map them here so the
# Recalls sheet stays English regardless of source.
#
# Match strategy:
#   1. Exact case-insensitive lookup (CLASS_TRANSLATIONS) — fastest path,
#      covers the literal API enum values.
#   2. Substring fallback (CLASS_SUBSTRING_RULES) — catches noisy variants
#      like "Volontaire (sans arrêté préfectoral)" or "Rappel volontaire".
#   3. If neither matches, the value is returned unchanged. We do NOT
#      attempt heuristic translation — that's the LLM gate's job.
#
# Canonical English short forms:
#   "Voluntary"  — producer-initiated recall, no regulator order
#   "Mandatory"  — regulator-imposed recall (prefectural order, ministerial
#                  decree, administrative action — collapsed to one label)
#   Existing English values ("Class I", "Recall", "Alert", etc.) pass through.

CLASS_TRANSLATIONS: Dict[str, str] = {
    # --- French (RappelConso, AFSCA-FR, etc.) ---
    "volontaire":                                  "Voluntary",
    "rappel volontaire":                           "Voluntary",
    "volontaire (sans arrêté préfectoral)":        "Voluntary",
    "imposé par arrêté préfectoral":               "Mandatory",
    "rappel imposé":                               "Mandatory",
    "impératif":                                   "Mandatory",
    # Verbose English already produced by older translations — collapse to
    # the short form per operator preference.
    "mandatory recall (prefectural order)":        "Mandatory",
    "mandatory recall":                            "Mandatory",
    # --- Italian (Min. Salute) ---
    "richiamo volontario":                         "Voluntary",
    "richiamo":                                    "Recall",
    # --- Spanish (AESAN) ---
    "retirada voluntaria":                         "Voluntary",
    "alerta sanitaria":                            "Alert",
    # --- German (BVL) ---
    "freiwilliger rückruf":                        "Voluntary",
    "rückruf":                                     "Recall",
    # --- Portuguese (ASAE) ---
    "recolhimento voluntário":                     "Voluntary",
}

# Substring rules run after exact lookup misses. Order matters — first
# matching substring wins, so "imposé"/"mandatory" must be checked before
# "volontaire"/"voluntary" (an "Imposé volontairement" hypothetical would
# be Mandatory, not Voluntary).
CLASS_SUBSTRING_RULES: Tuple[Tuple[str, str], ...] = (
    ("imposé par arrêté",      "Mandatory"),
    ("arrêté préfectoral",     "Mandatory"),
    ("prefectural order",      "Mandatory"),
    ("rappel imposé",          "Mandatory"),
    ("mandatory",              "Mandatory"),
    ("volontaire",             "Voluntary"),
    ("rappel volontaire",      "Voluntary"),
    ("retirada voluntaria",    "Voluntary"),
    ("richiamo volontario",    "Voluntary"),
    ("recolhimento voluntário","Voluntary"),
    ("freiwilliger rückruf",   "Voluntary"),
)


def _normalize_class_language(value: str) -> str:
    """Map a regulator-supplied Class value to its English short form.

    Returns the input unchanged if no rule matches — never raises.
    """
    if not value:
        return value
    key = value.strip().lower()
    if key in CLASS_TRANSLATIONS:
        return CLASS_TRANSLATIONS[key]
    for needle, replacement in CLASS_SUBSTRING_RULES:
        if needle in key:
            return replacement
    return value


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


# ---------------------------------------------------------------------------
# Tier assignment — HYBRID framework (v2.0, 2026-05-04)
# ---------------------------------------------------------------------------
# Per docs/fsis_reviewer_prompts.md:
#   1. If regulator's Class field matches a known severity label → use that
#   2. Otherwise apply FDA Class I/II/III framework based on pathogen +
#      product type (RTE vs cooking-required)
#   3. Apply Outbreak bump: outbreak=1 → bump tier one level up (max 1)
# ---------------------------------------------------------------------------

# Regulator class strings (lower-cased) → base tier. Grounded in actual
# Class values that appear in recalls.xlsx across 28+ regulators.
_CLASS_TIER_MAP: Dict[str, int] = {
    # --- Tier 1 (CRITICAL — regulator marked highest severity) ---
    "class i":                                1,
    "class 1":                                1,
    "public health alert":                    1,
    "public health alert (phl)":              1,
    "mandatory recall":                       1,
    "mandatory recall (prefectural order)":   1,
    "imposé par arrêté préfectoral":          1,
    "impératif":                              1,
    "imperative":                             1,
    "imperativo":                             1,
    "outbreak":                               1,
    "administrative action":                  1,

    # --- Tier 3 (MINOR — regulator marked lowest severity) ---
    "class iii":                              3,
    "class 3":                                3,
    "advisory":                               3,
    "information":                            3,
    "information notification":               3,
    "information notification for attention": 3,
    "border rejection":                       3,
    "border rejection notification":          3,
    "notification":                           3,

    # --- Tier 2 (MAJOR — regulator marked mid severity, OR generic "Recall" / "Alert") ---
    "class ii":                               2,
    "class 2":                                2,
    "recall":                                 2,
    "alert":                                  2,
    "alert notification":                     2,
    "voluntary":                              2,
    "voluntary recall":                       2,
    "conseillé":                              2,
    "food alert":                             2,
    "product recall information notice":      2,
    "regional public warning":                2,
}


# Ready-to-eat (RTE) product keywords. Used for the FDA framework fallback:
# Salmonella, Hep A, Norovirus in RTE foods → Tier 1; in cooking-required
# foods → Tier 2.
_RTE_KEYWORDS: List[str] = [
    # English
    "ready-to-eat", "ready to eat", "rte ", " rte",
    "deli ", "deli-", "delicatessen",
    "peanut butter", "almond butter", "cashew butter", "nut butter",
    "ice cream", "ice-cream", "frozen yogurt", "gelato",
    "soft cheese", "fresh cheese", "queso fresco", "feta", "ricotta",
    "brie", "camembert", "blue cheese", "roquefort",
    "pâté", "pate ", "rillettes", "terrine",
    "salad", "coleslaw", "guacamole", "hummus", "salsa", "dip",
    "sandwich", "wrap", "burrito", "sushi", "nigiri", "sashimi",
    "smoked fish", "smoked salmon", "lox", "gravlax",
    "cold-smoked", "cured meat", "salami", "chorizo", "prosciutto",
    "ham", "mortadella", "bologna",
    "raw oyster", "oysters",   # eaten raw
    "frozen berries", "frozen berry", "frozen fruit",   # often eaten raw in smoothies
    "sprouts", "alfalfa sprouts", "bean sprouts",  # eaten raw in salads
    "leafy greens", "lettuce", "spinach", "kale", "arugula", "rocket",
    "dry pet food", "kibble",  # children/elderly handle without washing hands
    # German
    "verzehrfertig", "rohwurst", "räucherlachs",
    # French
    "prêt à consommer", "prêt-à-consommer", "charcuterie", "saumon fumé",
    # Spanish
    "listo para consumo", "embutido", "queso fresco",
    # Italian
    "pronto al consumo", "salume",
]

# Cooking-required product keywords. Salmonella/Campylobacter in these
# is Tier 2 — consumer cooks before eating.
_COOKING_REQUIRED_KEYWORDS: List[str] = [
    "raw chicken", "raw poultry", "raw turkey", "raw duck",
    "raw beef", "raw pork", "raw lamb", "raw veal",
    "ground beef", "ground turkey", "ground chicken", "ground pork",
    "minced meat", "mince",
    "raw egg", "in-shell egg", "shell egg",
    "uncooked", "raw meat",
    "fresh chicken", "fresh poultry", "fresh beef", "fresh pork",
]

# Vulnerable-population product keywords. Foreign body / contaminants in
# these → Tier 1 (FDA framework).
_VULNERABLE_POPULATION_KEYWORDS: List[str] = [
    "infant", "baby food", "infant formula", "follow-on formula",
    "toddler", "baby snack", "baby cereal",
    "medical food", "enteral nutrition", "nutritional supplement for elderly",
    "Säuglingsnahrung", "Babynahrung",                # German
    "aliments pour bébé", "lait infantile",           # French
    "alimentos para bebé",                            # Spanish
    "alimenti per bebè", "latte per neonati",         # Italian
]

# Always-Tier-1 hazards. Hard rules H1-H6 — these override product type.
#
# Audit 2026-05-07 — added "Salmonella" per reviewer-prompts-v2 (locked
# 2026-05-01): "Salmonella always Tier 1". The previous HYBRID FDA
# framework treated Salmonella as RTE-conditional (Tier 1 if RTE product,
# Tier 2 otherwise), which produced 4 Tier-2 Salmonella rows in today's
# Tavily gap-finder run — a locked-rule violation. Moving Salmonella into
# the always-Tier-1 set short-circuits the RTE-borderline path so all
# Salmonella recalls are Tier 1 regardless of product type.
_ALWAYS_TIER_1_PATHOGENS: set = {
    "Listeria monocytogenes",
    "Shiga toxin-producing E. coli (STEC)",
    "Clostridium botulinum",
    "Cereulide (B. cereus toxin)",
    "Marine biotoxin",
    "Salmonella",
}


def _match_class_to_tier(klass: Any) -> Optional[int]:
    """Try to map a regulator's Class string to a tier. Returns None if
    no match — caller falls back to FDA framework."""
    if not klass:
        return None
    s = str(klass).strip().lower()
    if not s:
        return None

    # Direct lookup
    if s in _CLASS_TIER_MAP:
        return _CLASS_TIER_MAP[s]

    # Fuzzy: check if any keyword is in the string
    # Order matters — check Tier 1 markers first, then Tier 3, then Tier 2.
    if any(t1 in s for t1 in (
        "class i", "class 1", "public health alert", "mandatory",
        "impératif", "imperative", "imperativo", "outbreak",
        "administrative action", "imposé par arrêté",
    )):
        return 1
    if any(t3 in s for t3 in (
        "class iii", "class 3", "information", "border rejection",
        "notification", "advisory",
    )):
        return 3
    if any(t2 in s for t2 in (
        "class ii", "class 2", "recall", "alert", "voluntary",
        "conseillé", "food alert", "warning",
    )):
        return 2
    return None


def _is_rte_product(product: Any) -> bool:
    if not product:
        return False
    p = str(product).strip().lower()
    return any(kw in p for kw in _RTE_KEYWORDS)


def _is_cooking_required_product(product: Any) -> bool:
    if not product:
        return False
    p = str(product).strip().lower()
    return any(kw in p for kw in _COOKING_REQUIRED_KEYWORDS)


def _is_vulnerable_population_product(product: Any) -> bool:
    if not product:
        return False
    p = str(product).strip().lower()
    return any(kw in p for kw in _VULNERABLE_POPULATION_KEYWORDS)


def _fda_framework_tier(pathogen_canonical: str, product: Any) -> int:
    """
    Apply FDA Class I/II/III framework when no regulator class is present.
    Pure pathogen + product type reasoning — no outbreak bump applied here
    (caller does that).
    """
    # H1-H5: always Tier 1 hazards (override product type)
    if pathogen_canonical in _ALWAYS_TIER_1_PATHOGENS:
        return 1

    # Audit 2026-05-07 — Salmonella removed from this list because it now
    # matches _ALWAYS_TIER_1_PATHOGENS above and never reaches this code
    # path. Hep A and Norovirus remain RTE-conditional per FDA framework.
    rte_borderline = {"Hepatitis A virus", "Norovirus"}
    if pathogen_canonical in rte_borderline:
        if _is_rte_product(product):
            return 1
        if _is_cooking_required_product(product):
            return 2
        # Neither marker — assume Tier 2 (default for these pathogens
        # without explicit RTE indicator). Conservative.
        return 2

    # Campylobacter, Yersinia, Vibrio etc. — always Tier 2 (FDA Class II)
    tier_2_pathogens = {
        "Campylobacter", "Vibrio", "Cyclospora cayetanensis",
        "Yersinia enterocolitica", "Cronobacter sakazakii",
        "Bacillus cereus", "Brucella", "Shigella",
        "Staphylococcus enterotoxin", "Histamine / scombrotoxin",
        "Escherichia coli (generic)",
        "Aflatoxin", "Ochratoxin", "Mycotoxin", "Alternaria toxins",
        "T-2 / HT-2 toxin", "Ergot alkaloids", "Citrinin",
    }
    if pathogen_canonical in tier_2_pathogens:
        return 2

    # Foreign body / physical hazards
    if pathogen_canonical and "foreign body" in pathogen_canonical.lower():
        if _is_vulnerable_population_product(product):
            return 1   # H13: foreign body in baby food = Tier 1
        return 3       # H14: foreign body in adult food = Tier 3

    # Heavy metals, rodenticide, chemical contaminants — Tier 1
    if pathogen_canonical and any(m in pathogen_canonical.lower() for m in (
        "heavy metal", "lead ", "mercury", "arsenic",
        "rodenticide", "bromadiolone", "warfarin", "cyanide", "methanol",
    )):
        return 1

    # Default — unknown pathogen / mild contaminant → Tier 3
    return 3


def assign_tier(
    pathogen: Any,
    outbreak: Any = 0,
    klass: Any = "",
    product: Any = "",
) -> int:
    """
    Map (pathogen, outbreak, regulator-class, product) to severity tier
    using the HYBRID framework defined in docs/fsis_reviewer_prompts.md.

        Tier 1 = critical (FDA Class I)
        Tier 2 = major    (FDA Class II)
        Tier 3 = minor    (FDA Class III)

    Algorithm:
      1. If regulator's Class field is recognized, use that as base tier.
      2. Otherwise apply FDA framework: pathogen + product type.
      3. Apply Outbreak bump: outbreak=1 → max(1, base_tier - 1).

    Backward compatibility: callers that pass only (pathogen, outbreak)
    still work — klass and product default to "". When class+product are
    not provided, the FDA framework defaults to a pathogen-only heuristic
    via the closed Tier-1 list and the legacy _TIERS pathogen lookup.
    """
    # Step 1 — Try regulator class first
    base = _match_class_to_tier(klass)

    # Normalize pathogen to canonical name (used by both paths below)
    canonical = ""
    if pathogen:
        p = str(pathogen).strip()
        canonical = p
        if p not in _TIERS:
            for name, pattern in _COMPILED_RULES:
                if pattern.search(p):
                    canonical = name
                    break

    # H1-H5 OVERRIDE: always-Tier-1 pathogens win even when a regulator
    # marked them as Class II/III. Regulators occasionally mis-classify
    # (e.g., Listeria flagged as just "Recall" without severity tag).
    # The pathogen severity is non-negotiable for these 5.
    if canonical in _ALWAYS_TIER_1_PATHOGENS:
        base = 1
    elif base is None:
        # Step 2 — Fall back to FDA framework when no regulator class match
        if not pathogen:
            base = 3
        else:
            base = _fda_framework_tier(canonical, product)

    # Step 3 — Outbreak bump
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
