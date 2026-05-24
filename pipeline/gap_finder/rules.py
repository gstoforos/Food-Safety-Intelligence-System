"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 1: Accept/Reject Rules Engine

Locked rule set (2026-05-15):
  ACCEPT: pathogens, microbial-origin toxins (mycotoxins), natural plant/fungal toxins
  REJECT: allergens, foreign matter, synthetic/environmental chemicals, heavy metals
  Manual override possible per item, but default is the rule.

Bilingual: matches Greek and English keywords against Pathogen/Reason free text.

Usage:
    from rules import classify
    result = classify(pathogen="Listeria monocytogenes", reason="presence of Listeria")
    # → {'verdict': 'accept', 'category': 'pathogen', 'tier': 1, 'rule': '...'}
"""

from __future__ import annotations
import re
import unicodedata
from dataclasses import dataclass, asdict
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD LEXICONS (bilingual Greek + English)
# ─────────────────────────────────────────────────────────────────────────────

# ACCEPT — pathogens (Tier assigned per locked rules)
PATHOGENS_TIER_1 = {
    # Salmonella → always Tier 1 (locked rule)
    "salmonella", "σαλμονέλα", "σαλμονελα",
    # Listeria monocytogenes → Tier 1
    "listeria monocytogenes", "listeria", "λιστέρια", "λιστερια",
    # STEC / E. coli O157 → Tier 1
    "stec", "e. coli o157", "e.coli o157", "escherichia coli o157",
    "shiga toxin-producing", "shiga-toxin", "shigatoxin",
    # Bacillus cereus / cereulide → always Tier 1 (locked rule)
    "bacillus cereus", "cereulide", "κερευλίδη", "κερευλιδη",
    # Botulism — high severity
    "clostridium botulinum", "botulinum toxin", "botulism", "αλλαντίαση", "αλλαντιαση",
    # Other high-severity
    "cronobacter sakazakii", "cronobacter",
}

PATHOGENS_TIER_2 = {
    "campylobacter", "καμπυλοβακτηρίδιο", "καμπυλοβακτηριδιο",
    "yersinia", "γερσίνια", "γερσινια",
    "shigella", "σιγκέλλα", "σιγκελλα",
    "staphylococcus aureus", "σταφυλόκοκκος",
    "norovirus", "νοροϊός", "νοροιος", "norwalk virus",
    # Hepatitis A & E — foodborne viruses
    "hepatitis a", "ηπατίτιδα α", "ηπατιτιδα α",
    "hepatitis a virus", "hav",
    "epatite a", "virus dell'epatite a", "virus dell epatite a",
    "hepatitis e", "ηπατίτιδα ε", "ηπατιτιδα ε",
    "hepatitis e virus", "hev",
    "epatite e", "virus dell'epatite e", "virus dell epatite e",
    # Rotavirus
    "rotavirus",
    "clostridium perfringens",
    "vibrio", "δονακιοειδή", "δονακιοειδη",
    # Generic E. coli without O157 specification → Tier 2 by default
    "e. coli", "e.coli", "escherichia coli", "κολοβακτηρίδιο", "κολοβακτηριδιο",
}

# ACCEPT — microbial-origin toxins (mycotoxins)
# Greek nouns inflect (αφλατοξίνη → αφλατοξινών gen.pl.), Italian uses 'aflatossina'/
# 'aflatossine'. We include stems where helpful to match all inflected forms.
MICROBIAL_TOXINS = {
    # Aflatoxin
    "aflatoxin", "aflatoxins", "αφλατοξιν",
    "aflatossin",          # Italian stem (aflatossina/aflatossine)
    # Ochratoxin
    "ochratoxin", "ωχρατοξιν",
    "ocratossin",          # Italian
    # Patulin
    "patulin", "πατουλιν",
    "patulin",
    # Fumonisin
    "fumonisin", "φουμονισιν",
    "fumonisin",
    # Deoxynivalenol
    "deoxynivalenol", "don", "δεοξυνιβαλενολ",
    "deossinivalenol",     # Italian
    # Zearalenone
    "zearalenone", "zen", "ζεαραλενον",
    "zearalenone",
    # T-2 toxin
    "t-2 toxin", "t2 toxin",
    "tossina t-2",
    # Ergot alkaloids
    "ergot alkaloid", "αλκαλοειδη ερυσιβης",
    "alcaloidi della segale cornuta", "alcaloidi ergot",
}

# ACCEPT — natural plant / fungal toxins
NATURAL_TOXINS = {
    "amanita muscaria", "muscarine", "muscimol", "μουσκαρίνη", "μουσκιμόλη",
    "muscarina", "muscimolo",            # Italian
    "solanine", "σολανίνη",
    "solanina",                          # Italian
    "cyanogenic glycoside", "κυανογόνο γλυκοζίδιο",
    "glucoside cianogenico", "glicoside cianogenico",
    "tetrodotoxin", "τετροδοτοξίνη",
    "tetrodotossina",
    "scombroid", "histamine poisoning",
    "sgombroide", "intossicazione da istamina",
    "lectin", "ricin", "ρικίνη",
    "lectina", "ricina",
    "grayanotoxin", "γκραγιανοτοξίνη",
    "grayanotossina",
}

# REJECT — allergens (undeclared)
# Multi-lingual: English / Greek / Italian terms all merged. The classifier
# doesn't care which language matched, just which hazard category fired.
ALLERGENS = {
    "milk", "γάλα", "γαλα", "γαλακτος", "lactose", "λακτόζη", "λακτοζη",
    "latte", "lattosio", "caseina", "siero",
    "wheat", "σιτάρι", "σιταρι", "άλευρο σίτου", "αλευρο σιτου",
    "grano", "frumento", "farina di frumento", "orzo", "segale", "farro", "avena",
    "gluten", "γλουτένη", "γλουτενη",
    "glutine",
    "soy", "σόγια", "σογια", "soya",
    "soia",
    "peanut", "φιστίκι", "φιστικι", "αραχίδα", "αραχιδα",
    "arachide", "arachidi", "noccioline",
    "tree nut", "καρπός με κέλυφος", "καρπος με κελυφος",
    "frutta a guscio",
    "almond", "αμύγδαλο", "αμυγδαλο",
    "mandorla", "mandorle",
    "hazelnut", "φουντούκι", "φουντουκι",
    "nocciola", "nocciole",
    "walnut", "καρύδι", "καρυδι",
    "noce", "noci",
    "cashew", "κάσιους", "κασιους",
    "anacardi",
    "pistachio", "φιστίκι αιγίνης", "φιστικι αιγινης",
    "pistacchi", "pistacchio",
    "egg", "αυγό", "αυγο", "αυγού",
    "uovo", "uova",
    "fish", "ψάρι", "ψαρι",
    "pesce",
    "shellfish", "οστρακοειδή", "οστρακοειδη",
    "crostaceo", "crostacei",
    "crustacean", "καρκινοειδή", "καρκινοειδη",
    "sesame", "σουσάμι", "σουσαμι",
    "sesamo",
    "sulfite", "θειώδη", "θειωδη",
    "solfiti", "solfito",
    "mustard", "μουστάρδα", "μουσταρδα",
    "senape",
    "celery", "σέλινο", "σελινο",
    "sedano",
    "lupin", "λούπινο", "λουπινο",
    "lupini",
    "mollusc", "μαλάκια", "μαλακια",
    "molluschi",
    "allergen", "αλλεργιογόνο", "αλλεργιογονο", "αλλεργιογόνα",
    "allergene", "allergeni",
    "undeclared", "μη δηλωμένο", "μη δηλωμενο", "μη δηλωμένη",
    "non dichiarato", "non dichiarata", "non indicato",
}

# REJECT — synthetic / environmental chemicals & additives
SYNTHETIC_CHEMICALS = {
    "coumarin", "κουμαρίνη", "κουμαρινη", "cumarina",
    "pesticide", "φυτοφάρμακο", "φυτοφαρμακο", "pesticida", "pesticidi", "fitofarmaco",
    "chlorpyrifos", "χλωροπυριφός", "clorpirifos",
    "glyphosate", "γλυφοσάτη", "glifosato",
    "additive", "πρόσθετο", "προσθετο", "additivo", "additivi",
    "preservative over limit", "συντηρητικό υπέρβαση", "conservante oltre limite",
    "colorant over limit", "χρωστική υπέρβαση", "colorante oltre limite",
    "nitrite over limit", "νιτρώδη υπέρβαση", "nitriti oltre limite",
    "nitrate over limit", "νιτρικά υπέρβαση", "nitrati oltre limite",
    "sudan dye", "sudan i", "sudan ii", "sudan iii", "sudan iv",
    "rhodamine", "ροδαμίνη", "rodamina",
    "ethylene oxide", "οξείδιο αιθυλενίου", "ossido di etilene",
    "melamine", "μελαμίνη", "melamina",
    "acrylamide", "ακρυλαμίδιο", "acrilammide",
    "pah", "polycyclic aromatic", "πολυκυκλικοί αρωματικοί υδρογονάνθρακες",
    "idrocarburi policiclici aromatici",
    "dioxin", "διοξίνη", "diossina", "diossine",
    "pcb", "πολυχλωριωμένα διφαινύλια", "policlorobifenili",
}

# REJECT — heavy metals
HEAVY_METALS = {
    "cadmium", "κάδμιο", "καδμιο", "cadmio",
    "lead", "μόλυβδος", "μολυβδος", "piombo",
    "mercury", "υδράργυρος", "υδραργυρος", "mercurio",
    "arsenic", "αρσενικό", "αρσενικο", "arsenico",
    "chromium", "χρώμιο", "χρωμιο", "cromo",
    "nickel", "νικέλιο", "νικελιο", "nichel",
    "tin", "κασσίτερος", "κασσιτερος", "stagno",
    "heavy metal", "βαρύ μέταλλο", "βαρυ μεταλλο", "metallo pesante", "metalli pesanti",
}

# REJECT — foreign matter
FOREIGN_MATTER = {
    "glass", "γυαλί", "γυαλι", "verre", "vetro", "frammenti di vetro",
    "metal fragment", "θραύσμα μετάλλου", "μέταλλο", "μεταλλο",
    "frammento metallico", "frammenti metallici", "metallo",
    "plastic", "πλαστικό", "πλαστικο", "plastica", "frammenti di plastica",
    "foreign body", "foreign matter", "ξένο σώμα", "ξενο σωμα",
    "corps étranger", "stone", "πέτρα", "πετρα",
    "wood fragment", "θραύσμα ξύλου",
    "insect", "έντομο", "εντομο",
    "rubber", "ελαστικό", "λάστιχο",
}


# ─────────────────────────────────────────────────────────────────────────────
# OUTBREAK DETECTION (locked rule: Outbreak=1 needs case counts, not "linked to")
# ─────────────────────────────────────────────────────────────────────────────

# Phrase patterns that DO NOT qualify as outbreak (mere association language)
OUTBREAK_WEAK_PHRASES = {
    "linked to", "associated with", "possibly linked", "may be linked",
    "συνδέεται με", "ενδεχομένως συνδέεται",
}

# Pattern that indicates outbreak with confirmed case counts
OUTBREAK_CASE_PATTERN = re.compile(
    r"(\d+)\s*(confirmed|reported|κρούσμα|κρούσματα|cases?)",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace. Bilingual-safe."""
    if not text:
        return ""
    text = text.lower().strip()
    # Strip Greek + Latin diacritics, keep base letters
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_set(keywords: set[str]) -> set[str]:
    return {_normalize(k) for k in keywords}


# Pre-normalize all lexicons once at module load (faster matching)
_PATHOGENS_T1_N = _normalize_set(PATHOGENS_TIER_1)
_PATHOGENS_T2_N = _normalize_set(PATHOGENS_TIER_2)
_MICROBIAL_TOXINS_N = _normalize_set(MICROBIAL_TOXINS)
_NATURAL_TOXINS_N = _normalize_set(NATURAL_TOXINS)
_ALLERGENS_N = _normalize_set(ALLERGENS)
_SYNTHETIC_CHEMICALS_N = _normalize_set(SYNTHETIC_CHEMICALS)
_HEAVY_METALS_N = _normalize_set(HEAVY_METALS)
_FOREIGN_MATTER_N = _normalize_set(FOREIGN_MATTER)


def _contains_any(haystack: str, needles: set[str]) -> Optional[str]:
    """Return first matching needle, or None."""
    for needle in needles:
        if needle and needle in haystack:
            return needle
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Classification:
    verdict: str          # 'accept' | 'reject'
    category: str         # 'pathogen' | 'microbial_toxin' | 'natural_toxin'
                          # | 'allergen' | 'synthetic_chemical' | 'heavy_metal'
                          # | 'foreign_matter' | 'unknown'
    tier: Optional[int]   # 1, 2, 3, or None
    matched_term: Optional[str]
    rule: str             # human-readable explanation
    outbreak_qualifies: bool  # True if case counts present

    def to_dict(self) -> dict:
        return asdict(self)


def detect_outbreak(text: str) -> bool:
    """
    Locked rule: Outbreak=1 needs case counts (not 'linked to').
    Returns True only if numeric case count is present.
    """
    if not text:
        return False
    n = _normalize(text)
    # Reject weak association language
    for weak in OUTBREAK_WEAK_PHRASES:
        if _normalize(weak) in n and not OUTBREAK_CASE_PATTERN.search(n):
            return False
    # Require numeric case count
    return bool(OUTBREAK_CASE_PATTERN.search(n))


def classify(
    pathogen: str = "",
    reason: str = "",
    product: str = "",
) -> Classification:
    """
    Apply the locked accept/reject rules to a recall.

    Matching priority (first hit wins):
      1. REJECT — foreign matter      (glass anywhere = out of scope)
      2. REJECT — heavy metals        (cadmium anywhere = out of scope)
      3. REJECT — synthetic chemicals (coumarin/pesticide = out of scope)
      4. ACCEPT — pathogens Tier 1
      5. ACCEPT — pathogens Tier 2
      6. ACCEPT — microbial-origin toxins (Tier 2)
      7. ACCEPT — natural toxins (Tier 2)
      8. REJECT — allergens           (only if no pathogen/toxin found above)
      9. UNKNOWN — defer to manual review

    Why allergens are checked LAST among rejects: allergen lexicon words
    ("peanut", "milk", "wheat") are also common food INGREDIENTS. A recall
    that says "Aflatoxin found in peanuts" is an aflatoxin recall, not a
    peanut-allergen recall. Pathogens/toxins are more specific signals
    than mere allergen-ingredient mentions, so they take priority.

    Foreign matter, heavy metals, and synthetic chemicals stay first because
    they're unambiguous hazards: glass-in-Listeria-product is still out of
    scope under Rule B.
    """
    blob = _normalize(f"{pathogen} {reason} {product}")

    # ── UNAMBIGUOUS REJECT path (glass, heavy metals, synthetic chemicals) ─
    m = _contains_any(blob, _FOREIGN_MATTER_N)
    if m:
        return Classification(
            verdict="reject", category="foreign_matter", tier=None,
            matched_term=m, rule="Foreign matter — out of scope (Rule B reject).",
            outbreak_qualifies=False,
        )

    m = _contains_any(blob, _HEAVY_METALS_N)
    if m:
        return Classification(
            verdict="reject", category="heavy_metal", tier=None,
            matched_term=m, rule="Heavy metal — out of scope (Rule B reject).",
            outbreak_qualifies=False,
        )

    m = _contains_any(blob, _SYNTHETIC_CHEMICALS_N)
    if m:
        return Classification(
            verdict="reject", category="synthetic_chemical", tier=None,
            matched_term=m,
            rule="Synthetic/environmental chemical — out of scope (Rule B reject).",
            outbreak_qualifies=False,
        )

    # ── ACCEPT path (pathogens & toxins — more specific than allergen mention) ─
    outbreak = detect_outbreak(reason)

    m = _contains_any(blob, _PATHOGENS_T1_N)
    if m:
        return Classification(
            verdict="accept", category="pathogen", tier=1,
            matched_term=m,
            rule="Tier-1 pathogen (Salmonella / Listeria / STEC / B. cereus-cereulide / "
                 "C. botulinum / Cronobacter).",
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _PATHOGENS_T2_N)
    if m:
        # Outbreak with case counts elevates to Tier 1
        tier = 1 if outbreak else 2
        return Classification(
            verdict="accept", category="pathogen", tier=tier,
            matched_term=m,
            rule=f"Tier-{tier} pathogen" + (" (outbreak case counts elevate to Tier 1)." if outbreak else "."),
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _MICROBIAL_TOXINS_N)
    if m:
        return Classification(
            verdict="accept", category="microbial_toxin", tier=2,
            matched_term=m,
            rule="Microbial-origin toxin (mycotoxin) — accepted per Rule B.",
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _NATURAL_TOXINS_N)
    if m:
        return Classification(
            verdict="accept", category="natural_toxin", tier=2,
            matched_term=m,
            rule="Natural plant/fungal toxin — accepted per Rule B.",
            outbreak_qualifies=outbreak,
        )

    # ── ALLERGEN REJECT (last resort — only if no pathogen/toxin matched) ──
    m = _contains_any(blob, _ALLERGENS_N)
    if m:
        return Classification(
            verdict="reject", category="allergen", tier=None,
            matched_term=m, rule="Undeclared allergen — out of scope (Rule B reject).",
            outbreak_qualifies=False,
        )

    # ── UNKNOWN ────────────────────────────────────────────────────────────
    return Classification(
        verdict="reject", category="unknown", tier=None,
        matched_term=None,
        rule="No matching hazard category — defer to manual review.",
        outbreak_qualifies=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST: today's 2 EFET cases + 3 historical Greek accepts
# Run: python rules.py
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cases = [
        # Today's cases (2026-05-14) — both should REJECT
        {
            "name": "Today #1 — Παξιμάδια κανέλας (Karagiannakis)",
            "pathogen": "",
            "reason": "Παρουσία μη δηλωμένου αλλεργιογόνου: άλευρο σίτου (gluten)",
            "expected": ("reject", "allergen"),
        },
        {
            "name": "Today #2 — Strudito strudel μήλο/κανέλα",
            "pathogen": "",
            "reason": "Coumarin exceeding maximum permitted level for the category",
            "expected": ("reject", "synthetic_chemical"),
        },
        # Historical Greek accepts — all should ACCEPT under Rule B
        {
            "name": "Hist #1 — Feta ΒΥΤΙΝΑΣ ΠΟΠ",
            "pathogen": "Listeria monocytogenes",
            "reason": "Presence of Listeria monocytogenes in feta cheese",
            "expected": ("accept", "pathogen"),
        },
        {
            "name": "Hist #2 — Psillys mushroom",
            "pathogen": "Amanita muscaria toxin (muscimol)",
            "reason": "Presence of muscimol — natural fungal toxin",
            "expected": ("accept", "natural_toxin"),
        },
        {
            "name": "Hist #3 — AB roasted peanuts",
            "pathogen": "Aflatoxins",
            "reason": "Aflatoxin contamination exceeding limit",
            "expected": ("accept", "microbial_toxin"),
        },
        # Edge cases for additional coverage
        {
            "name": "Edge #1 — Cadmium in chocolate (heavy metal)",
            "pathogen": "",
            "reason": "Cadmium exceeding regulatory limit",
            "expected": ("reject", "heavy_metal"),
        },
        {
            "name": "Edge #2 — Glass fragments (foreign matter)",
            "pathogen": "",
            "reason": "Presence of glass fragments / γυαλί",
            "expected": ("reject", "foreign_matter"),
        },
        {
            "name": "Edge #3 — Salmonella outbreak with 12 cases",
            "pathogen": "Salmonella",
            "reason": "Outbreak with 12 confirmed cases linked to product",
            "expected": ("accept", "pathogen"),  # Tier 1 + outbreak
        },
        {
            "name": "Edge #4 — Listeria 'linked to' (no case count)",
            "pathogen": "Listeria monocytogenes",
            "reason": "Recall linked to broader concerns",
            "expected": ("accept", "pathogen"),  # Tier 1, outbreak=False
        },
        {
            "name": "Edge #5 — Bacillus cereus / cereulide (locked Tier 1)",
            "pathogen": "Bacillus cereus producing cereulide",
            "reason": "Presence of cereulide emetic toxin",
            "expected": ("accept", "pathogen"),  # MUST be Tier 1
        },
    ]

    print("=" * 78)
    print("AFTS Greek Gap Finder — Rules Engine Self-Test")
    print("=" * 78)
    passed = failed = 0
    for tc in test_cases:
        result = classify(pathogen=tc["pathogen"], reason=tc["reason"])
        exp_verdict, exp_category = tc["expected"]
        ok = result.verdict == exp_verdict and result.category == exp_category
        # Extra check: Bacillus cereus / cereulide MUST be Tier 1
        if "cereulide" in tc["name"].lower() or "cereus" in tc["name"].lower():
            ok = ok and result.tier == 1
        status = "✓ PASS" if ok else "✗ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"\n{status}  {tc['name']}")
        print(f"        verdict={result.verdict}  category={result.category}  "
              f"tier={result.tier}  outbreak={result.outbreak_qualifies}")
        print(f"        matched: {result.matched_term!r}")
        print(f"        rule:    {result.rule}")
        if not ok:
            print(f"        EXPECTED: verdict={exp_verdict}  category={exp_category}")

    print("\n" + "=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 78)
