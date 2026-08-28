"""
product_axes.py — ProcessType, ConsumptionState, StorageCondition,
PackagingType for the FSIS statistical schema.

SCOPE. This module owns ONLY the four axes that describe the product itself.
It does NOT map food categories: pipeline/regulator_fields.py owns
CATEGORY_MAP and is the single source of truth for FoodCategory. An earlier
draft of mine re-invented that map and mixed two variables in one column —
RASFF "poultry meat" (a commodity) and CFIA "...- processed" (a process) both
landing in FoodCategory — which is the same defect as the legacy Class column.
That map is deleted. Nothing here writes FoodCategory.

THE ONE RULE THAT PREVENTS THAT DEFECT RECURRING
    A regulator category names the COMMODITY and nothing else.
    Where a regulator concatenates process information into its category
    string (CFIA: "Food - Meat and poultry - Processed"), the suffix is
    process evidence and must be routed to ProcessType, never allowed to
    change FoodCategory.
    -> split_cfia_category() below does exactly that.

CONFIDENCE. Every function returns (value, confidence, evidence):
    "high"  the source states it, or a legally reserved marker implies it
    "low"   inferred from product wording; usable but reviewable
    "none"  unknown
"unknown" is always permitted. Never return the modal value to fill a gap.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, Iterable, Optional, Tuple

Result = Tuple[str, str, Optional[str]]


def _n(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s.lower())


# Trailing regulator metadata inside Reason. RASFF rows carry the notifying
# authority's own taxonomy appended to the free text:
#
#   "Listeria monocytogenes in halloumi from Cyprus; risk: serious;
#    category: milk and milk products"
#
# 490 of the 1,532 published rows carry it. It is already parsed, correctly,
# by pipeline/regulator_fields.py — this module must not read it a second
# time as if it were product wording. See _strip_regulator_metadata().
_META_LABEL = re.compile(
    r"^\s*(?:risk|category|classification|hazard category|product category)\s*:",
    re.I,
)
_RASFF_ID = re.compile(r"^\s*rasff\s*#", re.I)


def _strip_regulator_metadata(reason: str) -> str:
    """Drop the `; risk: … ; category: …` tail from a Reason string.

    WHY (audit 2026-08-28). `_text()` fed the whole Reason to the keyword
    matchers, so the regulator's COMMODITY FAMILY label was treated as
    product wording. The RASFF family "meat and meat products (other than
    poultry)" contains the bare token "meat", which sat in
    CONSUMPTION_TERMS["cook-before-eating"] — so every RASFF meat row was
    labelled cook-before-eating on the strength of a category name, cooked
    ham and cold-smoked salami included.

    Measured on the live register before this change: 475 rows labelled
    cook-before-eating, 261 of them matched on a bare commodity word, and
    113 of those name a ready-to-eat food in the Product field — 42 of the
    113 carrying Listeria monocytogenes, which is precisely the RTE/Listeria
    stratum the schema exists to isolate.

    Only segments whose leading label is a known regulator field are
    removed. Free text is never touched, and a Reason with no metadata is
    returned unchanged.
    """
    if not reason:
        return ""
    kept = []
    for seg in str(reason).split(";"):
        if _META_LABEL.match(seg) or _RASFF_ID.match(seg):
            continue
        kept.append(seg)
    return ";".join(kept)


def _text(row: Dict) -> str:
    """Product + Reason free text ONLY.

    Notes is the pipeline's audit trail — "gemini check pass", "gate",
    "enrich", "shortcut", "claude" — not product data. Including it swamped
    the real vocabulary (the top 25 tokens in every unknown bucket were audit
    words) and risks false matches on words like "clean" or "fixed". The
    structured RASFF fields that DO live in Notes are parsed by
    pipeline/regulator_fields.py, which is where that belongs.

    The same rule now applies to Reason: its trailing `risk:` / `category:`
    segments are regulator metadata, not wording, and are stripped before
    any keyword runs over the text.
    """
    return _n(f"{row.get('Product','')} "
              f"{_strip_regulator_metadata(row.get('Reason', ''))}")


# Phrases that contain a commodity term but mean something else. Checked
# before the term that would otherwise fire, per axis.
#
# "pate" is the worst of them. Accent-stripped, French pâté (a meat product),
# pâte à tartiner (a spread) and pâtes (pasta) all normalise to "pate". A
# pistachio chocolate spread — "pâte à tartiner dulci dubaï 40 % pistache" —
# was filed as meat-other, and then heat-treated, ready-to-eat, chilled and
# chilled-rte behind it, because one accent-free word did the work of three.
# Same shape as "the" in PROCESS_TERMS["dried"] and "meat" in the RASFF
# category label: one string, two meanings, no guard.
_FALSE_FRIENDS = {
    "pate": ("pate a tartiner", "pate a sucre", "pate feuilletee",
             "pate brisee", "pate sablee", "pates", "pate de fruit",
             "pate d amande", "pate a pizza", "pate a crepes"),
    "the": ("the vert", "the noir"),
    "date": ("date limite", "use by date", "best before date", "date de"),
    "case": ("in case of", "case of illness", "cases reported"),
}


def _blocked(text: str, term: str) -> bool:
    """True if this term only appears inside a phrase that means otherwise."""
    friends = _FALSE_FRIENDS.get(term)
    if not friends:
        return False
    for phrase in friends:
        if phrase in text:
            # the term is present ONLY as part of the decoy phrase
            stripped = text.replace(phrase, " ")
            if not re.search(r"(?<![a-z0-9])" + re.escape(term) + r"(?:s|es|x)?(?![a-z0-9])",
                             stripped):
                return True
    return False


def _find(text: str, terms: Iterable[str]) -> Optional[str]:
    """Word-boundary match, plural tolerant.

    Boundaries matter: a bare substring test makes "cru" fire inside
    "crustaces" and "raw" inside "strawberry". Plural tolerance matters too —
    an earlier version missed 418 rows because \\bcrevette\\b does not match
    "crevettes".
    """
    for t in terms:
        tn = _n(t)
        if not tn:
            continue
        pat = r"(?<![a-z0-9])" + re.escape(tn) + r"(?:s|es|x)?(?![a-z0-9])"
        if re.search(pat, text) and not _blocked(text, tn):
            return t
    return None


# ── ProcessType ─────────────────────────────────────────────────────────
# Process words appear in the PRODUCT's language, not English. FSAI's
# "Peklimar Metka Lososiowa — kielbasa surowa metka" is a RAW spreadable
# sausage: "surowa" is Polish for raw. An English-only pass sees "sausage"
# and returns cured-smoked, which inverts the risk.
PROCESS_TERMS: Dict[str, Tuple[str, ...]] = {
    "raw": (
        "raw", "cru", "crue", "crudo", "cruda", "roh", "rauw", "surowa",
        "surowe", "surowy", "cig", "nyers", "syrove", "tartare", "sashimi",
        "carpaccio", "unpasteurised", "unpasteurized", "non pasteurise",
        "lait cru", "leche cruda", "latte crudo", "rohmilch", "rauwe melk",
        "viande hachee", "steak hache", "minced meat", "ground beef",
        "ground pork", "ground turkey", "fresh meat", "metka",
        # mined from the corpus 2026-08-28: RASFF titles name the commodity
        # in a raw state without ever using the word "raw".
        "poultry meat", "chicken meat", "turkey meat", "duck meat",
        "chicken breast", "turkey breast", "chicken leg", "chicken wing",
        "poultry cut", "fresh poultry", "carcass", "mince", "hache",
        "viande fraiche", "escalope", "paupiette", "pilon", "cuisse",
        # "Meat preparation" is a defined EU term (Reg. 853/2004 Annex I 1.15)
        # for RAW meat with seasoning or additives — it has had no heat step.
        # "Salmonella Infantis in chicken preparations from Slovakia" came
        # back with no process at all.
        "meat preparation", "meat preparations", "chicken preparation",
        "poultry preparation", "preparazioni di", "preparation de viande",
        "fleischzubereitung", "vleesbereiding", "preparado de carne",
        "filet de poulet", "filet de dinde", "brochette", "merguez crue",
        "saucisse fraiche", "chair a saucisse", "steak", "roti cru",
        "lamb meat", "pork meat", "beef meat", "bovine meat", "veal",
        "egg", "oeuf", "shell egg", "sprout", "germe", "alfalfa",
        "porc", "boeuf", "agneau", "dinde", "canard", "lapin",
        "poisson frais", "maquereau", "sardine", "anchois", "hareng",
        "crevette crue", "coquillage", "huitre", "moule", "bulot",
        "oyster", "mollusc", "molluscs", "bivalve", "clam", "scallop",
        "sausage", "saucisse", "chipolata", "kebab", "brochette",
        "white meat", "red meat", "offal", "abat",
        "tripe", "liver", "foie", "rognon", "gizzard", "gesier",
        "enoki", "shiitake",
        # BARE SPECIES REMOVED 2026-08-28 — the same defect that was fixed in
        # CONSUMPTION_TERMS on the same day and left standing here. A species
        # name says what the animal was, not what was done to it. Measured on
        # a 20-row hand review, this bucket called three cooked products raw:
        #   "Beef-snout salad, modified-atmosphere tray"   -> raw, on "beef"
        #   "Chicken Caesar Wrap, clear plastic wrapped"   -> raw, on "chicken"
        #   "CURRY CHICKEN SALAD, 8-oz plastic packages"   -> raw, on "chicken"
        # All three are cooked, ready-to-eat, and two of them are Listeria
        # notices — the RTE/Listeria stratum again.
        # Removed: meat poultry chicken beef pork lamb turkey duck veal viande
        #          porc boeuf poulet dinde volaille mushroom champignon halal
        #          onigiri (onigiri is cooked rice, never raw)
        # The specific phrases above ("poultry meat", "chicken breast",
        # "filet de dinde", "viande fraiche", ...) still carry the raw rows
        # that matter, and they say something the bare word does not.
    ),
    "heat-treated": (
        "cooked", "cuit", "cuite", "cocido", "cocida", "cotto", "gekocht",
        "gekookt", "gotowane", "pasteurised", "pasteurized", "pasteurise",
        "pasteurizado", "pastorizzato", "pasteurisiert", "sterilised",
        "sterilized", "uht", "baked", "cuit au four", "roasted", "roti",
        "boiled", "bouilli", "fried", "frit", "canned", "conserve",
        "appertise", "retort", "blanched", "blanchi", "precooked", "precuit",
        "gegaard", "gebacken",
        "roti", "rotie", "grille", "grilled", "poele", "seared",
        "smoked cooked", "cooked ham", "jambon blanc", "boudin blanc",
        "pate", "rillettes", "terrine", "mousse de", "galantine",
        "corn dog", "nugget", "burger cuit", "meatball", "boulette",
        "roasted nut", "roasted peanut", "roasted almond", "grillees",
        "boudin", "andouille", "crevette cuite", "cuites", "cooked prawn",
        "infant formula", "follow-on formula", "lait infantile",
        "preparation pour nourrisson", "baby food", "petit pot",
        "compote", "puree", "soupe", "sauce", "conserve de",
        "yorkham", "ham cooked", "gekookte ham", "ready meal", "meal",
        "plat", "cooked meal", "prepared meal", "tray", "barquette repas",
    ),
    "fermented": (
        "fermented", "fermente", "fermentado", "fermentato", "fermentiert",
        "gefermenteerd", "affine", "affinato", "ripened", "matured",
        "cheese", "fromage", "queso", "formaggio", "kase", "kaas",
        "yoghurt", "yaourt", "yogurt", "kefir", "skyr", "quark",
        "salami", "saucisson", "chorizo", "sobrasada", "sourdough",
        "levain", "kimchi", "choucroute", "sauerkraut", "miso", "tempeh",
        "gorgonzola", "brie", "camembert", "roquefort", "feta", "halloumi",
        "mozzarella", "reblochon", "munster", "nectaire",
    ),
    "cured-smoked": (
        "smoked", "fume", "fumee", "ahumado", "affumicato", "gerauchert",
        "gerookt", "wedzon", "cured", "sale", "salee", "saumure", "curado",
        "stagionato", "jambon sec", "jambon cru", "prosciutto", "bacon",
        "lardon", "pastrami", "biltong", "jerky", "gravlax", "salaison",
        "speck", "bresaola", "coppa",
    ),
    "dried": (
        "dried", "seche", "sechee", "deshidratado", "essiccato",
        "getrocknet", "gedroogd", "suszony", "powder", "poudre", "polvo",
        "polvere", "pulver", "poeder", "flour", "farine", "harina",
        "farina", "mehl", "bloem", "dehydrated", "lyophilise", "freeze-dried",
        "raisin sec", "figue seche", "abricot sec",
        # 116 RASFF rows are "nuts, nut products and seeds" and 55 are figs;
        # these arrive dried by definition and the notice never says so.
        "dried fig", "dried apricot", "dried date", "dried fruit",
        "sultana", "raisin", "prune", "cranberry dried",
        "peanut", "groundnut", "pistachio", "almond", "cashew", "walnut",
        "hazelnut", "pecan", "macadamia", "brazil nut", "pine nut",
        "sesame", "sesame seed", "sunflower seed", "pumpkin seed",
        "poppy seed", "chia", "linseed", "flaxseed", "melon seed",
        "nut kernel", "kernel", "nutmeg", "spice", "spices", "pepper corn",
        "herb", "herbs", "moringa", "capsule", "powder", "poudre",
        "supplement", "complement alimentaire", "protein powder",
        # "the" was here as the French "the" (the) with its accent already
        # stripped. _n() strips accents from the TEXT too, so the term became
        # indistinguishable from the English article and matched every Reason
        # containing it - "Ochratoxin A above the regulatory limit" was
        # classified as a dried product. 118 rows, 29% of the whole "dried"
        # bucket, assigned by a definite article (measured 2026-08-28).
        # Accent-stripping cannot be undone at match time, so the term is
        # replaced by forms that cannot collide. Guarded by
        # test_no_term_collides_with_a_stopword.
        "tea", "the vert", "the noir", "the glace", "sachet de the",
        "tisane", "infusion", "coffee", "cafe", "cacao", "cocoa",
        "flour", "farine", "semoule", "couscous", "rice", "riz", "pasta",
        "pates", "noodle", "cereal", "muesli", "granola", "biscuit",
    ),
    "fresh-cut": (
        "fresh-cut", "fresh cut", "predecoupe", "precoupe", "pret a l emploi",
        "ready washed", "prelave", "bagged salad", "salade en sachet",
        "shredded", "rape", "sliced fresh", "iv gamma", "quarta gamma",
        "mesclun", "mixed salad", "fruits and vegetables", "fresh fruit",
        "fresh vegetable", "salad leaves", "leafy green", "baby leaf",
        "salade", "salad", "lettuce", "laitue", "roquette", "epinard",
        "spinach", "tomato", "tomate", "concombre", "cucumber", "carotte",
        "carrot", "melon", "berry", "berries", "fraise", "framboise",
        "myrtille", "pomme", "apple", "poire", "mangue", "avocat",
    ),
    "composite": (
        "sandwich", "ready meal", "plat cuisine", "plat prepare", "lasagne",
        "pizza", "quiche", "salade composee", "wrap", "sushi", "traiteur",
        "fertiggericht", "kant-en-klaar", "piatto pronto", "bouchee a la reine",
        "taboule", "coleslaw", "tray meal", "meal kit", "plateau repas",
        "assortment", "assortiment", "mixed lots", "multi-component",
    ),
}

# Order matters where a product legitimately matches two: a smoked salmon is
# both cured and heat-treated in some processes, but the cure is the
# controlling step for Listeria. Raw is checked first because it is the
# highest-risk claim and the one an English-only pass gets wrong.
PROCESS_ORDER = ("raw", "cured-smoked", "fermented", "dried",
                 "fresh-cut", "heat-treated", "composite")


# A raw state stated as a PHRASE, not inferred from a species name.
#
# Removing the bare species from PROCESS_TERMS["raw"] was right for cooked
# products and wrong for these: "frozen chicken thigh meat from Ukraine" and
# "fresh goose meat" are RASFF titles that name a raw commodity without ever
# using the word "raw", and they were only being caught by the bare tokens
# "chicken" and "meat". This says the same thing without the collateral —
# a preservation state or the word "raw", then up to two words, then a cut
# or the word meat.
_RAW_STATE = re.compile(
    r"(?<![a-z])(fresh|frozen|raw|chilled|surgele|congele|frais|fraiche|cru|crue)"
    r"(?:\s+[a-z]+){0,2}"
    r"\s+(meat|thigh|thighs|breast|breasts|leg|legs|wing|wings|carcass|"
    r"fillet|fillets|cut|cuts|mince|viande|filet)(?![a-z])")

# Dish names whose protein is cooked by definition. These sit in
# heat-treated, and heat-treated is tested before fresh-cut and composite,
# because "CURRY CHICKEN SALAD" and "Chicken Caesar Wrap" were landing on
# the salad/assembly buckets while the chicken in them is unambiguously
# cooked — and both are Listeria notices, so the RTE stratum depends on it.
_COOKED_DISH = (
    "chicken salad", "turkey salad", "ham salad", "egg salad", "tuna salad",
    "caesar", "curry chicken", "coronation chicken", "pastrami",
    "corned beef", "pulled pork", "rotisserie", "roti cuit",
    "museau", "salade museau", "pied de porc cuit", "tete pressee",
    "chicken caesar", "club sandwich", "bouchee a la reine",
    # The English rendering of the same products. RappelConso rows are
    # translated into English per the output rule, so the French term alone
    # misses them: "Beef-snout salad" is salade de museau, a cooked
    # charcuterie product, and "white ham" is jambon blanc — cooked ham.
    "beef-snout", "beef snout", "snout salad", "white ham",
    "cooked pig", "pressed head", "brawn",
)


def process_type(row: Dict) -> Result:
    t = _text(row)
    hit = _find(t, _COOKED_DISH)
    if hit:
        return "heat-treated", "low", hit
    for label in PROCESS_ORDER:
        hit = _find(t, PROCESS_TERMS[label])
        if hit:
            return label, "low", hit
    m = _RAW_STATE.search(t)
    if m:
        return "raw", "low", m.group(0)
    return "unknown", "none", None


# ── PreservationSystem ──────────────────────────────────────────────────
# THE VARIABLE PackagingType SHOULD HAVE BEEN
#
# Operator instruction, 2026-08-28: "if we have a can or a retort or aseptic
# package, think like that more — maybe we have to take off the packaging and
# put something else."
#
# He is right, and the measurement agrees. PackagingType asks what the food is
# WRAPPED IN. That is a materials fact and it is nearly silent about safety: a
# retort pouch and a steel can are different materials and the same food-safety
# object — both commercially sterile, both fail the same way, both implicate
# Clostridium botulinum when they fail. An aseptic carton and a chilled carton
# are the same material and completely different objects.
#
# What matters is the HURDLE: which barrier is keeping this food safe, and
# therefore which organism is in play when the barrier fails.
#
#   commercially sterile, ambient   ->  C. botulinum, spoilage spore-formers
#   low-moisture / low aw           ->  Salmonella, Cronobacter, aflatoxin
#   chilled, post-lethality exposed ->  Listeria monocytogenes
#   chilled, raw                    ->  Salmonella, Campylobacter, STEC
#   fermented / acidified           ->  Listeria, STEC in raw-milk cheese
#
# Measured on the register the day it was added: PackagingType filled 11.4%
# and its ceiling is about 25% because most notices never print a material.
# PreservationSystem fills 86.6%, and the pathogen split falls out exactly as
# food microbiology predicts — aflatoxin 95% low-moisture, Bacillus 62%
# low-moisture, Salmonella split between chilled-raw and low-moisture,
# Listeria 56% across the chilled and fermented classes. A variable that
# reproduces the textbook without being told it is a variable worth having.
#
# PackagingType is KEPT, not deleted: when a notice does state a material it
# is 100% accurate and it is the right answer to a descriptive question. It is
# simply no longer the stratification variable.

# Only two systems are ever stated in words, and both are unambiguous when
# they are. Vacuum and modified atmosphere are deliberately NOT here — they
# are pack atmospheres, PackagingType already records them, and a
# vacuum-packed chilled ham is a chilled-rte risk object first.
PRESERVATION_TERMS: Dict[str, Tuple[str, ...]] = {
    "retort-sterilised": (
        "retort", "appertise", "appertisation", "sterilised", "sterilized",
        "autoclave", "canned", "tinned", "en conserve", "boite de conserve",
        "commercially sterile", "low-acid canned", "shelf stable",
        "shelf-stable", "in scatola", "konserve",
    ),
    "aseptic-uht": (
        "uht", "aseptic", "ultra high temperature", "ultra-high temperature",
        "tetra brik", "longue conservation", "long life", "haltbare milch",
    ),
}
PRESERVATION_KEYWORD_ORDER = ("retort-sterilised", "aseptic-uht")


def preservation_system(food: str, proc: str, stor: str, cons: str,
                        row: Dict) -> Result:
    """The hurdle keeping this food safe.

    Two passes. A stated system wins — "canned", "UHT", "retort" are said
    explicitly or not at all. Otherwise the class already settles it: the
    commodity family, the process and the storage condition between them name
    the barrier without anyone having to write it down.

    Returns "unknown" freely. An environmental swab has no preservation
    system, and neither does a row whose product text is unusable.
    """
    t = _text(row)
    for label in PRESERVATION_KEYWORD_ORDER:
        hit = _find(t, PRESERVATION_TERMS[label])
        if hit:
            return label, "high", hit

    if _find(t, _NOT_A_FOOD_SAMPLE):
        return "unknown", "none", None

    if stor == "frozen":
        return "frozen", "high", "storage:frozen"
    if proc == "dried" or food in ("nuts-seeds", "dried-fruit", "herbs-spices",
                                   "supplements", "infant-food"):
        return "low-moisture-dried", "low", f"class:{food}|process:{proc}"
    if proc == "cured-smoked":
        return "cured-smoked", "low", "process:cured-smoked"
    if proc == "fermented" or food == "dairy-soft-cheese":
        return "fermented-acidified", "low", f"class:{food}|process:{proc}"
    if stor == "ambient":
        if food in ("beverages", "sauces-condiments", "dairy-other",
                    "confectionery-snacks"):
            return "ambient-stable", "low", f"class:{food}|storage:ambient"
        return "ambient-other", "low", "storage:ambient"
    # Confectionery is shelf-stable by composition — sugar and fat, low water
    # activity — so it does not need a storage statement to be placed. The
    # exception is the frozen dessert, which is caught by stor == "frozen"
    # above, and by the explicit terms here for the case where the notice
    # names the product but never its storage.
    if food == "confectionery-snacks":
        if _find(t, ("ice cream", "glace", "sorbet", "gelato", "frozen dessert",
                     "eis", "frozen yoghurt", "ice lolly")):
            # "fruit bar" is deliberately absent: Outshine fruit bars are
            # frozen and a cereal fruit bar is ambient, and the words are the
            # same. Guessing from the brand is knowledge, not evidence.
            return "frozen", "low", "frozen dessert"
        return "ambient-stable", "low", "class:confectionery-snacks"
    if stor == "chilled":
        if cons == "ready-to-eat":
            # The post-lethality-exposed class. This is the Listeria stratum
            # and the single most useful cell in the whole variable.
            return "chilled-rte", "low", "storage:chilled|ready-to-eat"
        if proc == "raw" or cons == "cook-before-eating":
            return "chilled-raw", "low", "storage:chilled|raw"
        return "chilled-other", "low", "storage:chilled"
    return "unknown", "none", None


# ── FoodCategory, keyword path ──────────────────────────────────────────
# WHY THIS EXISTS (audit 2026-08-28)
#
# FoodCategory had no keyword path at all. enrich_schema read it from the
# RASFF `category:` field, or from a CFIA category string, and nothing else.
# RASFF is 490 of 1,532 rows, so every FDA, RappelConso, FSANZ, BLV and USDA
# row was "unknown" BY CONSTRUCTION. The 32.4% fill was not a tuning problem
# and no amount of term-list work on the other axes would have moved it.
#
# Measured on a 20-row hand review: 19 rows were categorisable from their own
# text and the module answered 4. Fifteen misses on one axis — more than
# every other axis combined.
#
# This is a TIER-3 path: it runs only when the regulator stated nothing, and
# enrich_schema records the tier, so a keyword category can never be mistaken
# for a notifying authority's own classification.
#
# The vocabulary is the one already in use (regulator_fields.CATEGORY_MAP
# plus the wider set the tests allow). Order is most-specific-first, because
# a cheese is dairy before it is anything else and an infant formula is
# infant-food before it is dairy.
# A COMPOSITE DISH IS A DISH, NOT ITS MEAT. prepared-meals sits ahead of the
# commodity buckets because "Chicken Caesar Wrap" and "CURRY CHICKEN SALAD"
# were landing in meat-poultry on the word "chicken" — the same mistake, one
# level up, as calling a cooked ham raw because it is made of pork. RASFF
# itself keeps "prepared dishes and snacks" separate from the meat families
# for exactly this reason.
#
# The known cost of this order: "Beef-snout salad" (salade de museau) is a
# charcuterie product that now files as prepared-meals rather than meat-other.
# Both readings are defensible and it is recorded here rather than hidden.
CATEGORY_ORDER = (
    "infant-food", "supplements", "prepared-meals",
    "dairy-soft-cheese", "dairy-other", "eggs-egg-products",
    "meat-poultry", "meat-other", "fish-seafood",
    "herbs-spices", "nuts-seeds", "dried-fruit",
    "bakery-cereal", "confectionery-snacks", "sauces-condiments",
    "beverages", "fresh-produce",
)

CATEGORY_TERMS: Dict[str, Tuple[str, ...]] = {
    "infant-food": (
        "infant formula", "baby food", "petit pot", "lait infantile",
        "preparation pour nourrisson", "follow-on formula", "sauglingsnahrung",
        "baby cereal", "compote pour bebe",
    ),
    "supplements": (
        "supplement", "complement alimentaire", "food supplement", "capsule",
        "softgel", "gelule", "nahrungserganzung", "vitamin", "multivitamin",
        "protein powder", "superfood", "moringa", "spirulina", "ashwagandha",
        "collagen", "probiotic",
    ),
    # Soft and fresh cheeses are the Listeria vehicle of record, so they get
    # their own bucket rather than being folded into dairy-other.
    "dairy-soft-cheese": (
        "soft cheese", "fromage a pate molle", "raw milk cheese",
        "fromage au lait cru", "brie", "camembert", "reblochon", "munster",
        "epoisses", "livarot", "pont l eveque", "maroilles", "roquefort",
        "gorgonzola", "feta", "mozzarella", "burrata", "ricotta", "halloumi",
        "crescenza", "stracchino", "taleggio", "nectaire", "chevre frais",
        "queso fresco", "fromage frais", "faisselle", "brousse",
    ),
    "dairy-other": (
        "cheese", "fromage", "kaese", "queso", "formaggio", "milk", "lait",
        "leche", "latte", "melk", "milch", "yoghurt", "yaourt", "yogurt",
        "butter", "beurre", "cream", "creme", "kefir", "tomme", "comte",
        "emmental", "cheddar", "gouda", "raclette", "morbier", "fourme",
        "picodon", "sainte maure", "dairy", "laitier", "creme fraiche",
    ),
    "eggs-egg-products": (
        "egg", "oeuf", "huevo", "uovo", "ei ", "shell egg", "egg product",
        "ovoproduct", "egg laying", "laying facility", "mayonnaise",
    ),
    "meat-poultry": (
        "poultry", "chicken", "turkey", "duck", "goose", "volaille", "poulet",
        "dinde", "canard", "oie", "pollo", "pavo", "gallina", "hahnchen",
        "kip", "drob", "chicken meat", "poultry meat", "foie gras",
    ),
    "meat-other": (
        "beef", "pork", "lamb", "veal", "mutton", "rabbit", "game",
        "viande", "boeuf", "porc", "agneau", "lapin", "gibier", "carne",
        "manzo", "maiale", "rind", "schwein", "ham", "jambon", "bacon",
        "salami", "saucisson", "chorizo", "sausage", "saucisse", "merguez",
        "pate", "rillettes", "andouille", "boudin", "coppa", "pancetta",
        "pastrami", "charcuterie", "museau", "steak", "mince", "hache",
        "offal", "liver", "foie", "tripe", "abat", "kebab",
    ),
    "fish-seafood": (
        "fish", "poisson", "pescado", "pesce", "fisch", "vis ", "salmon",
        "saumon", "trout", "truite", "tuna", "thon", "mackerel", "maquereau",
        "herring", "hareng", "sardine", "anchovy", "anchois", "cod", "cabillaud",
        "surimi", "shrimp", "crevette", "prawn", "gamba", "crab", "crabe",
        "lobster", "homard", "oyster", "huitre", "mussel", "moule", "clam",
        "scallop", "coquille", "mollusc", "crustacean", "seafood", "fruits de mer",
        "sole", "ling", "julienne", "bulot", "poke",
    ),
    "herbs-spices": (
        "spice", "epice", "herb", "herbe", "aromate", "zaatar", "za atar",
        "paprika", "cumin", "curry powder", "pepper corn", "poivre",
        "cinnamon", "cannelle", "oregano", "basil", "basilic", "thyme",
        "curcuma", "turmeric", "chili powder", "chilipulver", "seasoning",
        "assaisonnement", "spice mix", "melange d epices",
    ),
    "nuts-seeds": (
        "nut", "noix", "nuez", "noce", "peanut", "cacahuete", "groundnut",
        "arachide", "pistachio", "pistache", "almond", "amande", "cashew",
        "noix de cajou", "hazelnut", "noisette", "walnut", "pecan",
        "macadamia", "sesame", "tahini", "seed", "graine", "semi di",
        "sunflower seed", "poppy seed", "flax", "lin", "chia", "pine nut",
    ),
    "dried-fruit": (
        "dried fig", "dried apricot", "dried fruit", "figue seche",
        # A recall of organic figs in a kraft bag was uncategorised because
        # only the two-word English phrase was here. Figs reach this register
        # dried far more often than fresh, and the fresh case is caught by
        # fresh-produce below if the wording says so.
        "figue", "figues", "fig", "figs", "higo", "fico", "feige",
        "abricot sec", "raisin sec", "sultana", "date", "datte", "prune",
        "pruneau", "cranberry seche", "fruit sec",
    ),
    "prepared-meals": (
        "ready meal", "plat prepare", "plat cuisine", "sandwich", "wrap",
        "salad", "salade", "sushi", "onigiri", "bowl", "mezze", "falafel",
        "hummus", "houmous", "guacamole", "dip", "tartinable", "quiche",
        "pizza", "lasagne", "pasta salad", "couscous", "tabbouleh",
        "spring roll", "nem", "samosa", "bouchee a la reine", "traiteur",
        "prepared dish", "fertiggericht", "kant en klaar",
    ),
    "bakery-cereal": (
        "bread", "pain", "brot", "pane", "baguette", "brioche", "viennoiserie",
        "croissant", "cake", "gateau", "tart", "tarte", "pastry", "patisserie",
        "flour", "farine", "mehl", "harina", "cereal", "cereale", "oat",
        "avoine", "muesli", "granola", "rice", "riz", "pasta", "pates",
        "noodle", "nouille", "semoule", "crepe", "gaufre", "biscuit", "cracker",
    ),
    "confectionery-snacks": (
        "chocolate", "chocolat", "schokolade", "cioccolato", "candy", "bonbon",
        "confectionery", "confiserie", "sweets", "praline", "nougat", "halva",
        "ice cream", "glace", "sorbet", "eis ", "gelato", "dessert",
        "fruit bar", "cereal bar", "barre", "crisps", "chips", "snack",
        "cookie", "wafer", "gaufrette", "delights", "pistazie",
    ),
    "sauces-condiments": (
        "sauce", "soup", "soupe", "broth", "bouillon", "condiment", "ketchup",
        "moutarde", "mustard", "vinaigrette", "dressing", "salsa", "pesto",
        "chutney", "relish", "oil", "huile", "olive oil", "vinegar", "vinaigre",
        "honey", "miel", "jam", "confiture", "spread", "tartinade",
    ),
    "beverages": (
        "juice", "jus", "smoothie", "drink", "boisson", "beverage", "water",
        "eau", "soda", "cola", "beer", "biere", "wine", "vin", "coffee",
        "cafe", "tea", "the vert", "infusion", "cocoa", "cacao", "milkshake",
    ),
    "fresh-produce": (
        "lettuce", "laitue", "salade verte", "mesclun", "mache", "spinach",
        "epinard", "rocket", "roquette", "sprout", "germe", "alfalfa",
        "cucumber", "concombre", "tomato", "tomate", "carrot", "carotte",
        "onion", "oignon", "potato", "pomme de terre", "mushroom", "champignon",
        "enoki", "berry", "fraise", "strawberry", "raspberry", "framboise",
        "blueberry", "myrtille", "melon", "pasteque", "mango", "mangue",
        "avocado", "avocat", "pepper", "poivron", "jalapeno", "chilli",
        "courgette", "aubergine", "brocoli", "broccoli", "chou", "cabbage",
        "fruit", "vegetable", "legume", "crudite", "fresh produce",
    ),
}


def food_category(row: Dict) -> Result:
    """Commodity family from the product wording. TIER 3 — keyword only.

    Never overrides a regulator's own category; enrich_schema calls this only
    when RASFF and CFIA both gave nothing, and stamps the tier accordingly.
    """
    t = _text(row)
    for label in CATEGORY_ORDER:
        hit = _find(t, CATEGORY_TERMS[label])
        if hit:
            return label, "low", hit
    return "unknown", "none", None


# ── ConsumptionState ────────────────────────────────────────────────────
CONSUMPTION_TERMS: Dict[str, Tuple[str, ...]] = {
    # BARE SPECIES NAMES ARE NOT IN THIS LIST — audit 2026-08-28.
    #
    # This tuple used to end with "meat", "poultry", "chicken", "beef",
    # "pork", "lamb", "turkey", "duck", "veal", "viande", "porc", "boeuf",
    # "poulet", "dinde", "egg", "rice", "pasta", "mushroom", "flour" and
    # the bare fish names. A species name states what the animal was, not
    # how the food is eaten: cooked ham, cold-smoked salami, pork pâté,
    # pastrami and duck liver mousse are all ready-to-eat, and all of them
    # were being labelled cook-before-eating by the word "pork" or "duck".
    # Oysters are the sharpest case — "huitre" was here, and an oyster is
    # the archetypal raw-consumption food.
    #
    # Measured on the 1,532 published rows before the change: 475 labelled
    # cook-before-eating, 261 on a bare commodity token, 113 of those
    # naming a ready-to-eat food in Product, 42 of the 113 Listeria
    # monocytogenes. Same failure shape as the definite article "the" in
    # PROCESS_TERMS["dried"], which classified 118 rows before it was found.
    #
    # Every term below states a process, a form, or a legally reserved
    # instruction. "unknown" is the correct answer for a bare species name;
    # this module's rule is that unknown is always permitted and the modal
    # value is never used to fill a gap.
    "cook-before-eating": (
        "cook before", "cook thoroughly", "a cuire", "cuire avant",
        "bien cuire", "cocinar antes", "cuocere prima", "durchgaren",
        "not ready-to-eat", "must be cooked", "raw poultry", "raw chicken",
        "viande hachee", "steak hache", "minced meat", "ground beef",
        "corn dog", "nugget", "raw sausage to cook",
        "poultry meat", "chicken meat", "turkey meat", "chicken breast",
        "turkey breast", "chicken leg", "chicken wing", "poultry cut",
        "carcass", "fresh poultry", "raw beef", "raw pork", "raw lamb",
        "raw meat", "viande crue", "fresh meat", "viande fraiche",
        "escalope", "paupiette", "pilon", "cuisse", "brochette crue",
        "saucisse fraiche", "merguez", "chair a saucisse",
        # Restored as SPECIFIC phrases after the bare species names came
        # out. Bare "cru" is deliberately absent: jambon cru is prosciutto,
        # eaten exactly as sold, and cook-before-eating is tested first, so
        # a bare "cru" would relabel every cured raw ham in the register.
        "viande hache", "hache de boeuf", "boeuf hache",
        "dinde cru", "dinde crue", "poulet cru", "volaille crue",
        "filet de dinde", "filet de poulet", "blanc de poulet",
        "saucisse de poulet", "saucisses de poulet",
        "chicken sausage", "raw sausage",
        "aile de poulet", "cuisse de poulet", "pilon de poulet",
        "escalope de dinde", "escalope de poulet", "souvlaki",
        "lardon cru", "raw flour", "wheat flour", "farine de ble",
        "plain flour", "self-raising flour", "dough", "pate crue",
        "porc cru", "boeuf cru", "sprout", "alfalfa", "germe",
        "shell egg", "oeuf coquille", "poisson frais",
        "enoki", "shiitake",
    ),
    "ready-to-heat": (
        "ready-to-heat", "rechauffer", "reheat", "a rechauffer",
        "micro-ondes", "microwave", "oven ready", "au four", "calentar",
        "riscaldare", "erhitzen", "opwarmen", "ready meal", "plat cuisine",
    ),
    "ingredient": (
        "ingredient", "raw material", "matiere premiere", "materia prima",
        "for further processing", "pour transformation",
        "zutat", "grondstof", "bulk industrial",
        # "semi-finished" REMOVED 2026-08-28. It describes where a sample was
        # taken, not what was sold: "Listeria monocytogenes detected on
        # analysis of a semi-finished product" appears in the Reason of a
        # recall whose PRODUCT is a ready-to-eat mezze bowl. The phrase was
        # reading the sampling stage and relabelling the finished good.
        # A flour or a meal made from a ready-to-eat commodity is not itself
        # ready-to-eat: "Aflatoxin B1 in Groundnut flours" and "jojoba nut
        # meal" were both labelled ready-to-eat by the commodity word.
        # "ingredient" is tested before "ready-to-eat", so these win.
        "groundnut flour", "nut flour", "nut meal", "almond powder",
        "amandes en poudre", "poudre d amande", "farine de", "seed meal",
    ),
    # WHAT IS NOT IN THIS LIST, AND WHY — audit 2026-08-28.
    #
    # A consumption state is a claim about whether the food gets a kill step
    # before it is eaten. Three kinds of word were in here that make no such
    # claim, and each was deciding rows on its own:
    #
    #   packaging   "tray", "sachet", "pack" — raw chicken comes in a tray.
    #               That axis is PackagingForm's, and it already answers it.
    #   sales channel "rayon traditionnel", "counter", "deli", "deli counter"
    #               — a delicatessen counter sells raw turkey fillet and
    #               cooked ham side by side. "filet de dinde CRU vendu au
    #               rayon traditionnel" was labelled ready-to-eat by the
    #               words after the comma.
    #   form/species "sliced", "tranche", "salmon", "truite", "milk", "nut",
    #               "seeds", "herb", "meal", "date" — "Sliced unsmoked RAW
    #               ham", "raw bovine milk", "Cook at Home Chicken Fillets",
    #               "Brocoli Calabrese SEEDS" (planting seed), "7 herb-
    #               flavoured sausages". "date" was the worst: it matched
    #               the word inside "use-by date" and "best-before date",
    #               so fourteen rows — raw chicken wings, iceberg lettuce,
    #               infant formula — were ready-to-eat because they carried
    #               a date field.
    #
    # Every term kept below names a food that reaches the consumer without a
    # kill step, or a legally reserved phrase that says so. Specific nut and
    # cheese names stay; the bare category words do not. Smoked and cooked
    # markers stay and already carry the smoked-trout and cooked-prawn rows
    # that "truite" and "tranche" were claiming.
    "ready-to-eat": (
        "ready-to-eat", "ready to eat", "rte", "pret a consommer",
        "pret a manger", "consommer en l etat", "listo para consumir",
        "pronto al consumo", "verzehrfertig", "kant-en-klaar",
        "metka", "a tartiner", "spreadable", "charcuterie",
        "smoked salmon", "saumon fume", "salad", "salade",
        "dessert", "snack", "cheese", "fromage", "yoghurt",
        "sandwich", "pate", "rillettes", "mousse",
        # mined: the nuts/seeds/dried-fruit bucket (238 rows) is eaten
        # without a kill step, and so are cheeses and cured meats.
        "peanut", "groundnut", "pistachio", "almond", "cashew",
        "walnut", "hazelnut", "pecan", "macadamia", "sesame", "tahini",
        "brazil nut", "nut butter", "pine nut",
        "dried fig", "dried apricot", "raisin", "sultana",
        "seed mix", "trail mix", "granola", "muesli", "cereal bar",
        "chocolate", "biscuit", "cookie", "crisps", "chips", "confectionery",
        "ice cream", "glace", "sorbet", "juice", "jus", "smoothie",
        "drink", "boisson", "water", "eau", "yaourt", "fromage frais",
        "ham", "jambon", "salami", "saucisson", "chorizo", "bacon cooked",
        "coppa", "olive", "hummus", "houmous", "dip", "spread", "tartinade",
        "gorgonzola", "brie", "camembert", "roquefort", "feta", "halloumi",
        "mozzarella", "cheddar", "gouda", "comte", "emmental", "raclette",
        "morbier", "fourme", "picodon", "livarot", "reblochon",
        "sainte maure", "pont l eveque", "tomme",
        # "nectaire" was dropped when this list was pruned on 2026-08-28 —
        # a regression, caught by the hand review: Saint Nectaire fermier AOP
        # came back unknown while storage_condition still answered
        # "structural:nectaire" for the same row. Two axes of one module
        # disagreeing about whether a cheese is a cheese.
        "nectaire", "crescenza", "stracchino", "taleggio", "chevre",
        "roquefort", "bleu", "gorgonzola", "munster", "epoisses",
        "crevette cuite", "cuites", "cooked prawn", "smoked", "fume",
        "boudin", "fresh fruit", "berry", "melon",
        "moringa", "capsule", "supplement", "complement alimentaire",
        "infant formula", "baby food", "petit pot", "compote",
        "saucisson sec", "fuet", "pancetta", "guanciale", "oyster",
        "mollusc", "molluscs", "bivalve", "clam", "scallop",
        "smoked fish", "poisson fume", "surimi",
        "onigiri",
    ),
}
# Specific instructions beat generic product nouns: a "raw chicken" that also
# contains the word "salad" is cook-before-eating, not ready-to-eat.
CONSUMPTION_ORDER = ("cook-before-eating", "ready-to-heat", "ingredient",
                     "ready-to-eat")


def consumption_state(row: Dict) -> Result:
    t = _text(row)
    for label in CONSUMPTION_ORDER:
        hit = _find(t, CONSUMPTION_TERMS[label])
        if hit:
            return label, "low", hit
    return "unknown", "none", None


# ── StorageCondition ────────────────────────────────────────────────────
# Use-by PROVES chilled: EU FIC reserves it for microbiologically highly
# perishable food. Best-before does NOT prove ambient — the FSAI Peklimar
# sausages are raw, chilled and carry one. So best-before is low confidence
# and must be reviewable, not asserted.
FROZEN_TERMS = ("surgele", "surgelee", "congele", "congelee", "frozen",
                "tiefgefroren", "diepvries", "congelado", "surgelato",
                "iqf", "deep frozen", "-18")
CHILLED_TERMS = ("dlc", "date limite de consommation", "use by", "use-by",
                 "a consommer jusqu", "te gebruiken tot", "tgt",
                 "verbrauchen bis", "consumir antes de",
                 "da consumarsi entro", "keep refrigerated",
                 "conserver au frais", "a conserver entre", "gekoeld",
                 "refrigere", "refrigerated", "chilled", "im kuhlschrank")
AMBIENT_TERMS = ("dluo", "ddm", "best before", "a consommer de preference",
                 "tht", "ten minste houdbaar", "mhd", "mindestens haltbar",
                 "consumir preferentemente", "long life", "uht",
                 "shelf stable", "ambient", "store in a cool dry place")


# Shelf-stable by nature: low water activity or hermetically preserved.
AMBIENT_BY_NATURE = (
    "dried fig", "dried apricot", "dried date", "dried fruit", "raisin",
    "sultana", "prune", "peanut", "groundnut", "pistachio", "almond",
    "cashew", "walnut", "hazelnut", "pecan", "macadamia", "brazil nut",
    "pine nut", "nut kernel", "kernel", "sesame", "tahini",
    "sunflower seed", "pumpkin seed", "poppy seed", "chia", "linseed",
    "flaxseed", "melon seed", "seed mix",
    "spice", "spices", "herb", "herbs", "pepper corn", "nutmeg",
    "cinnamon", "paprika", "cumin", "curry", "turmeric", "moringa",
    "flour", "farine", "semoule", "couscous", "rice", "riz", "pasta",
    "pates", "noodle", "cereal", "muesli", "granola", "biscuit",
    "cookie", "cracker", "crisps", "chips", "chocolate", "confectionery",
    "powder", "poudre", "capsule", "supplement", "complement alimentaire",
    "tea", "infusion", "coffee", "cafe", "cocoa", "cacao",
    "canned", "conserve", "appertise", "honey", "miel", "oil", "huile",
    "infant formula", "follow-on formula", "lait infantile",
)

# Requires refrigeration by nature: raw animal product, fresh dairy, or a
# ready-to-eat product supporting pathogen growth (EC 853/2004 Annex III).
CHILLED_BY_NATURE = (
    "poultry meat", "chicken meat", "turkey meat", "chicken breast",
    "turkey breast", "chicken leg", "chicken wing", "poultry cut",
    "carcass", "fresh poultry", "raw beef", "raw pork", "raw lamb",
    "viande hachee", "steak hache", "minced meat", "ground beef",
    "escalope", "paupiette", "pilon", "cuisse", "merguez", "boudin",
    "saucisse fraiche", "porc", "boeuf", "agneau", "dinde", "canard",
    "milk", "lait", "cheese", "fromage", "gorgonzola", "brie",
    "camembert", "roquefort", "feta", "halloumi", "mozzarella",
    "cheddar", "gouda", "comte", "emmental", "raclette", "yoghurt",
    "yaourt", "creme", "butter", "beurre", "egg", "oeuf", "shell egg",
    "poisson frais", "maquereau", "sardine", "anchois", "hareng",
    "crevette", "coquillage", "huitre", "moule", "bulot", "saumon",
    "ham", "jambon", "pate", "rillettes", "terrine", "mousse de",
    "salad", "salade", "sprout", "germe", "alfalfa", "sandwich",
    "oyster", "mollusc", "molluscs", "bivalve", "clam", "scallop",
    "truite", "salmon", "saumon", "smoked fish", "poisson fume",
    "surimi", "fish", "poisson", "seafood", "fruits de mer",
    "reblochon", "nectaire", "fermier", "savoie", "saucisson",
    "sausage", "saucisse", "chipolata", "halal", "kebab", "volaille",
    "poulet", "white meat", "red meat", "offal", "abat", "foie",
    "ready meal", "meal", "plat", "tray", "prepared meal",
    "fruits and vegetables", "fresh fruit", "fresh vegetable",
    "lettuce", "laitue", "spinach", "epinard", "tomato", "tomate",
    "cucumber", "concombre", "carrot", "carotte", "melon", "berry",
    "berries", "fraise", "framboise", "myrtille",
    "meat", "poultry", "chicken", "beef", "pork", "lamb", "turkey",
    "duck", "veal", "viande", "charcuterie", "mushroom", "champignon",
    "enoki", "shiitake", "onigiri", "smoked", "fume",
)


# Not a food. A swab of a production environment has no storage condition,
# and inferring one from the commodity word that happens to be in the title
# is an assertion about a product that does not exist. Row 9 of the hand
# review — "Salmonella Enteritidis in environment of egg laying facility" —
# was filed as chilled on the strength of the word "egg".
_NOT_A_FOOD_SAMPLE = (
    "environment of", "environmental sample", "environmental swab",
    "laying facility", "processing environment", "production environment",
    "swab", "prelevement d environnement", "surface sample",
)


# Preserved by water activity, whatever the commodity. "Salmonella spp. in
# salted and dried fish from Thailand" was filed as chilled — from the
# commodity — while PreservationSystem said low-moisture-dried for the same
# row. Two axes of one module contradicting each other on one product.
_SHELF_STABLE_BY_AW = (
    "salted and dried", "sale et seche", "dried and salted", "salt-cured",
    "sun-dried", "seche au soleil", "air-dried", "biltong", "jerky",
    "stockfish", "morue salee", "bacalao", "baccala", "klippfisk",
    "dried fish", "poisson seche", "salaison seche",
)


def storage_condition(row: Dict) -> Result:
    t = _text(row)
    if _find(t, _NOT_A_FOOD_SAMPLE):
        return "unknown", "none", None
    hit = _find(t, _SHELF_STABLE_BY_AW)
    if hit:
        return "ambient", "high", hit
    hit = _find(t, FROZEN_TERMS)
    if hit:
        return "frozen", "high", hit
    hit = _find(t, CHILLED_TERMS)
    if hit:
        return "chilled", "high", hit
    hit = _find(t, AMBIENT_TERMS)
    if hit:
        # best-before alone. Real but weak: national practice varies and
        # chilled products carry it in some markets.
        return "ambient", "low", hit

    # ── STRUCTURAL inference, medium confidence ─────────────────────────
    # Storage is rarely STATED (11% of the corpus states it), but for some
    # products it is determined by what the product IS, not by a label:
    # a dried fig is shelf-stable because it is dried; raw meat and fresh
    # dairy require refrigeration by law (EC 853/2004) whether or not the
    # notice says so.
    #
    # This is food science, not a guess about the label — but it is one step
    # removed from the source, so it is marked "medium" and never "high".
    # A consumer of this column can exclude medium if they need only stated
    # facts. Nothing here overrides a stated marker: the checks above run
    # first and win.
    hit = _find(t, AMBIENT_BY_NATURE)
    if hit:
        return "ambient", "medium", f"structural:{hit}"
    hit = _find(t, CHILLED_BY_NATURE)
    if hit:
        return "chilled", "medium", f"structural:{hit}"
    return "unknown", "none", None


# ── PackagingType ───────────────────────────────────────────────────────
# The schema proposal warns: "Expect ~50%. Above 70% fill means the reviewer
# is inferring." Only explicit packaging words fire here. A cheese is not
# assumed vacuum-packed; a salad is not assumed bagged.
PACKAGING_TERMS: Dict[str, Tuple[str, ...]] = {
    "vacuum": ("vacuum", "sous vide", "sous-vide", "vakuum",
               "envasado al vacio", "sottovuoto", "vacuumverpakt"),
    "map": ("modified atmosphere", "atmosphere modifiee",
            "atmosphere protectrice", "schutzatmosphare",
            "atmosfera protettiva", "beschermende atmosfeer"),
    "canned": ("canned", "boite de conserve", "conserve", "appertise",
               "lata", "scatoletta", "dose", "blik", "retort pouch",
               "tinned", "tin", "puszka", "lattina",
               # NOT bare "can" — it is the English modal verb. It matched
               # "symptoms can include severe and bloody diarrhea" and
               # "hydrocyanic acid can release cyanide".
               "in a can", "g can", "oz can", "canned"),
    "glass": ("glass jar", "glass bottle", "bocal", "pot en verre",
              "bouteille en verre", "verrine", "vaso de vidrio",
              "glasflasche", "glazen pot", "jar",
              # NOT bare "glass". Hand review of the 30 most recent rows,
              # 2026-08-28: all four rows it matched were foreign-material
              # glass, not glass packaging — "contamination with glass",
              # "small glass fragments in the jam", "a shard of brown glass".
              # It classified a glass-in-food hazard as glass packaging.
              "in glass", "glass container"),
    "rigid-plastic": ("barquette", "tray", "tub", "punnet", "pot plastique",
                      "gobelet", "bandeja", "vaschetta", "becher", "kuipje",
                      # mined from the corpus 2026-08-28: tokens actually
                      # present in rows the vocabulary was missing.
                      "pot", "alveole", "plastic container", "plastic tray",
                      "clamshell", "bak", "tarrina",
                      # Qualified only. Bare "plastic" is deliberately absent:
                      # it matched four rows where a plastic FRAGMENT was the
                      # hazard, which is the opposite of a packaging fact.
                      "plastic package", "plastic packages", "plastic pot",
                      "plastic tub", "plastic cup"),
    "flexible": ("sachet", "pouch", "flowpack", "doypack", "sous film",
                 # NOT bare "wrap": "chicken fajita wrap" is a food, not a
                 # package. Only the participle and explicit forms.
                 "wrapped", "plastic wrap", "shrink wrap", "flow wrap",
                 "sleeve",
                 "paquet", "opakowanie",
                 "poche", "beutel", "zakje", "busta", "bag", "film",
                 "sachet fraicheur", "packet", "wrapper", "bagged",
                 "in bags", "en sachet", "stand-up pouch", "sac"),
    "carton": ("carton", "brique", "tetra", "boite carton", "cardboard",
               "karton", "cartone", "cardboard box", "boite en carton",
               "case of", "caisse", "boite", "caja",
               # NOT bare "box" or "case". "box" matched the brand "Green Box
               # Limited" on every row; "case" matched illness counts —
               # "55 reported cases", "1,644 laboratory-confirmed cases".
               "cardboard box", "box of", "in boxes", "carton box"),
    "loose": ("vrac", "en vrac", "loose", "unpackaged", "a la coupe",
              "rayon traditionnel", "self-service", "non emballe",
              "poids variable", "granel", "sfuso", "lose", "counter"),
}
# Specific technologies before generic containers: a vacuum-packed tray is
# vacuum, not rigid-plastic.
PACKAGING_ORDER = ("vacuum", "map", "canned", "glass", "loose",
                   "rigid-plastic", "flexible", "carton")


# ── PackagingForm ───────────────────────────────────────────────────────
# A SECOND, COARSER axis, and the honest answer to "packaging coverage must
# be higher".
#
# Measured on the register 2026-08-28: only 86 of the 1,383 rows that
# packaging_type cannot place contain ANY container word, in any of the
# eight languages the vocabulary covers. The hard ceiling for a seven-value
# packaging axis from Product + Reason text is 15.3%. No amount of
# vocabulary work moves it, because the text does not say.
#
# What the text DOES carry often enough to be useful is whether the product
# was sold as a sealed unit or off a counter. That distinction is not
# cosmetic: an unpackaged product cut and handled at the point of sale has a
# different Listeria cross-contamination profile from a sealed pack, which
# is exactly the kind of thing the register exists to stratify on.
#
# A declared net weight or a multi-unit format ("4 tranches 80G", "x20") is
# treated as evidence of a pre-packed unit. That is an inference, and it is
# labelled "low" confidence so it can be excluded from any stratum that
# needs certainty.
_LOOSE_TERMS = (
    "vrac", "en vrac", "loose", "unpackaged", "non emballe", "non conditionne",
    "a la coupe", "vendu a la coupe", "rayon traditionnel", "a la demande",
    "self-service", "granel", "sfuso", "lose", "poids variable", "bulk",
    "sold loose", "deli counter", "counter",
)
_PACKED_TERMS = (
    "sachet", "pouch", "bag", "sac", "busta", "bolsa", "zak", "beutel",
    "barquette", "tray", "punnet", "tub", "pot", "gobelet", "vaschetta",
    "bandeja", "kuipje", "becher", "boite", "carton", "brique",
    "tetra", "karton", "cartone", "caja", "bocal", "jar", "bottle",
    "bouteille", "botella", "flasche", "fles", "flacon", "tin",
    "lattina", "puszka", "conserve", "lata", "blik", "dose", "film",
    "flowpack", "doypack", "sous vide", "sottovuoto", "vakuum", "vacuum",
    "modified atmosphere", "confezione", "envase", "emballage",
    "verpakking", "pack", "packet", "alveole", "multipack", "sleeve",
    "wrapper", "sachet fraicheur", "wrapped", "case of", "cardboard box",
    # A dosage form IS its package — capsules and softgels are not sold loose.
    "capsule", "softgel", "gelule", "blister",
    # Single-serve vessels sold as a unit.
    "bowl", "cup", "goblet", "coupelle",
)
# Structural evidence of a retail unit. None of these name a MATERIAL, which
# is why the material vocabulary above misses them and why PackagingForm was
# stuck well under its ceiling.
#
# Measured 2026-08-28 on a 20-row hand review: 11 rows were determinable as
# packaged or unpackaged and the module answered 6. Every one of the five
# misses carried structural evidence and no material word —
#   "(GTIN 3760373402593, all lots, use-by ...)"          a falafel wrap
#   "Lot 2604201C, best-before 2026-05-16"                a branded cheese
#   "Green Superfood Capsules"                            a supplement
#   "bowl mezze"                                          a chilled bowl
#   "8-oz plastic packages, USE BY 07/30/2026"            a chicken salad
_NET_WEIGHT = re.compile(
    # metric
    r"(?<![a-z0-9])\d+[.,]?\d*\s?(g|gr|kg|ml|cl|l)(?![a-z])"
    # imperial — two USDA FSIS rows in twenty were missed for want of this:
    # "8-oz plastic packages" and "8.7-oz. clear plastic wrapped packages".
    r"|(?<![a-z0-9])\d+[.,]?\d*\s?-?\s?(oz|lb|lbs|fl\.?\s?oz)(?![a-z])")

_MULTI_UNIT = re.compile(
    r"(?<![a-z0-9])x\s?\d+(?![a-z0-9])"
    r"|(?<![a-z0-9])\d+\s?(tranches|pieces|units|pcs|sachets|pots|bouteilles)(?![a-z])"
    # "6-count", "24-count", "12 ct", "4-pack" — a count IS a pack format.
    r"|(?<![a-z0-9])\d+\s?-?\s?(count|ct|pack|packs|portions)(?![a-z])")

# A GTIN / EAN / UPC is, by definition, a Global Trade Item Number: it
# identifies a packaged trade item. A product sold loose at a counter does
# not carry one. This is the strongest packaging signal in the corpus and
# nothing was reading it.
_TRADE_ITEM_CODE = re.compile(
    r"(?<![a-z])(gtin|ean|upc|sku|barcode|code[- ]?barres)(?![a-z])")


def packaging_form(row: Dict) -> Result:
    """packaged | unpackaged | unknown — the axis the text can actually support."""
    t = _text(row)
    hit = _find(t, _LOOSE_TERMS)
    if hit:
        return "unpackaged", "high", hit
    hit = _find(t, _PACKED_TERMS)
    if hit:
        return "packaged", "high", hit
    if _TRADE_ITEM_CODE.search(t):
        return "packaged", "high", "trade item code (GTIN/EAN/UPC)"
    if _MULTI_UNIT.search(t):
        return "packaged", "low", "multi-unit format"
    if _NET_WEIGHT.search(t):
        return "packaged", "low", "declared net weight"
    return "unknown", "none", None


def packaging_type(row: Dict) -> Result:
    t = _text(row)
    for label in PACKAGING_ORDER:
        hit = _find(t, PACKAGING_TERMS[label])
        if hit:
            return label, "low", hit
    return "unknown", "none", None


# ── CFIA category: commodity vs process, kept apart ─────────────────────
_CFIA_PROCESS = {"processed": "heat-treated", "raw": "raw",
                 "fresh": "raw", "ready-to-eat": None}


def split_cfia_category(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """CFIA files 'Food - Meat and poultry - Processed'.

    The trailing token is PROCESS information, not commodity. Returns
    (commodity_string, process_type_or_None). The commodity string is handed
    to pipeline/regulator_fields.py for FoodCategory; the process token is
    tier-1 evidence for ProcessType. Never let one field write two axes.
    """
    parts = [p.strip() for p in str(raw or "").split("-") if p.strip()]
    if not parts:
        return None, None
    tail = _n(parts[-1])
    if tail in _CFIA_PROCESS:
        return " - ".join(parts[:-1]) or None, _CFIA_PROCESS[tail]
    return " - ".join(parts) or None, None


def enrich(row: Dict) -> Dict[str, object]:
    """All four axes plus evidence, for one row. No FoodCategory here."""
    pt, pc, pe = process_type(row)
    cs, cc, ce = consumption_state(row)
    st, sc, se = storage_condition(row)
    pk, kc, ke = packaging_type(row)
    ev = "; ".join(f"{k}:{v}" for k, v in
                   (("proc", pe), ("cons", ce), ("stor", se), ("pack", ke))
                   if v)
    return {"ProcessType": pt, "ProcessConfidence": pc,
            "ConsumptionState": cs, "ConsumptionConfidence": cc,
            "StorageCondition": st, "StorageConfidence": sc,
            "PackagingType": pk, "PackagingConfidence": kc,
            "AxesEvidence": ev[:200]}
