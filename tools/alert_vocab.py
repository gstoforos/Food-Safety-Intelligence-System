# -*- coding: utf-8 -*-
"""FSIS alert vocabulary — the single source of truth.

THE PROBLEM THIS SOLVES
-----------------------
`docs/alerts.html` offers a subscriber a fixed list of words. The Apps Script
mailer then decides, for each new register row, whether that word matches. Up
to 2026-09-14 the two sides did not share a definition:

  * alerts.html said, in a comment, "the backend matches on exact string
    equality so the Apps Script reuses this list". It does not, and never did.
  * The Apps Script truncated the subscriber's word to its first token
    (`v.split(/[\\s/()]/)[0]`), so the rule "Clostridium botulinum" became
    "clostridium" and matched *Clostridium perfringens*. That is the wrong
    email George received on 13 Sep.
  * Several offered words could never match anything the pipeline emits —
    "Undeclared allergen" (blocked by the publish gate), "Rotavirus",
    "Toxoplasma", "Trichinella", "Cryptosporidium" (no PATHOGEN_RULES entry,
    no register row, ever).

So a subscriber could pick a word that silently never fires, or pick a word
and be emailed about a different organism.

THE CONTRACT
------------
Every term offered in alerts.html is a key of one of the maps below, and the
Apps Script matches using exactly these token lists. Nothing is inferred, no
word is split at runtime, and a term may only be offered if it is *producible*:

  provenance "rules"  — the label is in scrapers/_models.PATHOGEN_RULES or
                        _TIERS, so normalize_pathogen() can emit it whether or
                        not a row exists yet (Yersinia, Shigella, Brucella).
  provenance "rows"   — no normaliser entry, but the register demonstrably
                        holds rows with this hazard as free text written by the
                        review agents (foreign material, heavy metals, ...).

tests/test_alert_vocab.py enforces both halves: producibility, and that the
generated .gs and alerts.html arrays are byte-identical to what this file
generates.

MATCHING RULE (identical in Python and in the .gs)
--------------------------------------------------
A token matches when it occurs in the haystack on word boundaries, tolerating
a trailing plural "s":

    \\b<token>s?\\b      (leading \\b only if the token starts with a word
                         character; trailing s?\\b only if it ends with one)

The boundary is load-bearing. Without it an "E. coli" rule matches
"Coliform / total bacterial count" through the substring "coli"; a "nut" token
matches "minute"; a "cod" token matches most of the seafood register.

Haystacks:
    pathogen  -> row["Pathogen"]
    product   -> row["Product"] + " " + row["Reason"]
    country   -> row["Country"]
    brand     -> row["Brand"] + " " + row["Company"]   (free text, not a vocab)
"""

from __future__ import annotations

import re

# --------------------------------------------------------------------------
# Pathogen / hazard
# --------------------------------------------------------------------------
# Ordered as the subscriber sees them: bacteria, viruses, parasites, then
# toxins and chemical / physical hazards. Coverage figures in the comments are
# against the 1,693 published rows as of 2026-09-14; 0 is fine when provenance
# is "rules" — it means the organism is recognised but has not appeared yet.

PATHOGEN: dict[str, list[str]] = {
    # --- bacteria -------------------------------------------------------
    "Listeria": ["listeria"],                                       # 616
    "Salmonella": ["salmonella", "salmonelle"],                     # 506
    "E. coli / STEC": ["e. coli", "e.coli", "escherichia coli",
                       "stec", "vtec", "ehec", "shiga toxin",
                       "o157", "o104", "o121", "o26", "o45",
                       "o103", "o111", "o145", "o168"],             # 123
    "Clostridium botulinum": ["botulinum", "botulism", "botulisme"],  # 19
    "Clostridium perfringens": ["perfringens"],                     # 2
    "Bacillus cereus / cereulide": ["bacillus cereus", "b. cereus",
                                    "cereulide", "emetic toxin"],   # 42
    "Campylobacter": ["campylobacter"],                             # 3
    "Vibrio": ["vibrio", "vulnificus", "parahaemolyticus",
               "cholerae", "cholera", "alginolyticus"],             # 1
    "Cronobacter": ["cronobacter", "sakazakii"],                    # 2
    "Staphylococcus aureus / enterotoxin": ["staphylococcus",
                                            "staph. aureus", "s. aureus",
                                            "staphylococcal",
                                            "entérotoxine"],        # 7
    "Yersinia": ["yersinia", "enterocolitica"],                     # 0  (rules)
    "Shigella": ["shigella"],                                       # 0  (rules)
    "Brucella": ["brucella", "brucellosis"],                        # 0  (rules)

    # --- viruses --------------------------------------------------------
    "Norovirus": ["norovirus", "norwalk"],                          # 19
    "Hepatitis A": ["hepatitis a"],                                 # 4

    # --- parasites ------------------------------------------------------
    "Cyclospora": ["cyclospora"],                                   # 1

    # --- mycotoxins -----------------------------------------------------
    "Aflatoxin": ["aflatoxin"],                                     # 149
    "Ochratoxin": ["ochratoxin"],                                   # 91
    "Patulin": ["patulin"],                                         # 2
    "T-2 / HT-2 toxin": ["t-2", "ht-2", "t2 toxin"],                # 10
    "Deoxynivalenol (DON)": ["deoxynivalenol", "vomitoxin", "(don)"],  # 1
    "Zearalenone": ["zearalenone"],                                 # 2
    "Alternaria toxins": ["alternaria", "tenuazonic"],              # 5
    # Ergot alkaloids and Citrinin are PATHOGEN_RULES labels with no rows
    # and no expected traffic; they live here rather than as their own
    # subscriber terms.
    "Mycotoxins (other)": ["mycotoxin", "fumonisin", "citrinin",
                           "ergot"],                                # 14

    # --- natural toxins -------------------------------------------------
    "Histamine / scombrotoxin": ["histamine", "scombrotox"],        # 11
    "Marine / shellfish biotoxins": ["marine biotoxin", "biotoxin",
                                     "shellfish toxin",
                                     "paralytic shellfish",
                                     "lipophilic", "okadaic", "domoic",
                                     "saxitoxin", "tetrodotoxin",
                                     "ciguatoxin", "ciguatera",
                                     "phytoplankton"],              # 8
    "Mushroom / plant toxins": ["mushroom toxin", "amanita", "muscimol",
                                "muscaria", "hydrocyanic", "cyanogenic",
                                "solanine", "tropane"],             # 3

    # --- chemical -------------------------------------------------------
    "Heavy metals": ["heavy metal", "cadmium", "lead (", "mercury",
                     "arsenic", "molybdenum"],                      # 9
    "Pesticide / veterinary residues": ["pesticide", "veterinary medicine",
                                        "veterinary chemical", "penicillin",
                                        "nitrofurazone", "chloramphenicol",
                                        "rodenticide", "residue"],  # 6
    "Industrial chemical contaminant": ["pfoa", "pfas", "dioxin",
                                        "ethylene oxide", "mineral oil",
                                        "moah", "mosh", "acrylamide",
                                        "melamine", "chemical hazard"],  # 3
    "Undeclared pharmacological ingredient": ["pharmacological", "yohimbine",
                                              "sildenafil", "tadalafil"],  # 1

    # --- physical / other -----------------------------------------------
    "Foreign material / physical hazard": ["foreign material", "foreign body",
                                           "physical/foreign", "physical hazard",
                                           "glass", "metal fragment",
                                           "hard plastic", "shell fragment",
                                           "stones", "sand)"],      # 41
    "Mold / spoilage": ["mold", "spoilage", "organoleptic", "coliform",
                        "total bacterial count"],                   # 9
    "Rodent / pest contamination": ["rodent", "mouse contamination",
                                    "rat poison", "insect",
                                    "pest infestation"],            # 5
    "Process deviation (sterilisation / pasteurisation)": [
        "sterilization", "sterilisation", "pasteurization",
        "pasteurisation", "process deviation"],                     # 2

    # --- inspection ------------------------------------------------------
    # Added 2026-09-25, with the hazard class itself. Without a subscriber
    # word, FSIS recall 022-2026 — Star Meat Delivery, 167,639 lb of raw
    # pork, beef and goat distributed nationwide under a counterfeit "EST.
    # 1363" mark, Class I — would have sat in the register reaching no
    # subscriber at all. test_every_row_matches_at_least_one_pathogen_term
    # is the test that says so, and it is the right test to have.
    #
    # NO BARE "inspection" TOKEN. The Prime Line / Ferrarini Listeria row
    # (2026-09-06) says "confirmed by FSIS routine import re-inspection
    # sampling", and "insanitary conditions found during inspection" is
    # standard wording on genuine pathogen notices. Every token below states
    # that inspection did NOT happen, or that its mark was faked.
    "Uninspected product / false inspection mark": [
        "uninspected", "without the benefit of inspection",
        "without the benefit of federal inspection",
        "without benefit of inspection", "produced without inspection",
        "false inspection mark", "false mark of inspection",
        "false usda mark", "hazard not assessed"],                  # 4
}

# Terms REMOVED from the old alerts.html list on 2026-09-14 and why. Kept here
# so the removal is not silently re-added by a later edit, and so the Apps
# Script can tell a legacy subscriber what happened to their rule.
RETIRED_PATHOGEN: dict[str, str] = {
    "Rotavirus": "not in PATHOGEN_RULES and never collected — no row can match",
    "Toxoplasma": "not in PATHOGEN_RULES and never collected — no row can match",
    "Trichinella": "not in PATHOGEN_RULES and never collected — no row can match",
    "Cryptosporidium": "not in PATHOGEN_RULES and never collected — no row can match",
    "Undeclared allergen": (
        "allergen-only notices are blocked by pipeline/_publish_gate.py "
        "(scope blocks when hazard classes are a subset of {allergen, "
        "fermentation}), so they never reach recalls.json"),
    "Chemical contaminant": "split into Heavy metals / Pesticide / Industrial",
    "Physical hazard": "renamed Foreign material / physical hazard",
    "Ochratoxin A": "register writes both 'Ochratoxin' and 'Ochratoxin A'; "
                    "the term is now 'Ochratoxin', which matches both",
    "Marine biotoxins": "renamed Marine / shellfish biotoxins",
    "Staphylococcus aureus": "renamed Staphylococcus aureus / enterotoxin — "
                             "the register mostly writes the toxin, not the organism",
}
# "Bacillus cereus / Cereulide" changed only in case. Term lookup is
# case-insensitive on both sides, so nothing is retired and nothing is aliased.

# Legacy subscriber rules that must keep working. Maps the old stored string to
# the new term whose tokens should be used.
PATHOGEN_LEGACY: dict[str, str] = {
    "ochratoxin a": "Ochratoxin",
    "marine biotoxins": "Marine / shellfish biotoxins",
    "staphylococcus aureus": "Staphylococcus aureus / enterotoxin",
    "bacillus cereus / cereulide": "Bacillus cereus / cereulide",
    "chemical contaminant": "Industrial chemical contaminant",
    "physical hazard": "Foreign material / physical hazard",
}

# --------------------------------------------------------------------------
# Product category
# --------------------------------------------------------------------------
# Free-text, multilingual product fields (fr/pl/it/es/de all occur). Tokens are
# deliberately plain nouns; the word-boundary + plural rule does the rest.
# Coverage 2026-09-14: 1,368 of 1,693 rows fall into at least one category
# (80.8%). The remainder are lot references and regional cheese names that no
# category list can reach — a product filter is a narrowing tool, not a census.

PRODUCT: dict[str, list[str]] = {
    "Dairy — raw milk": ["raw milk", "lait cru", "leche cruda", "latte crudo",
                         "rohmilch", "mleko surowe", "unpasteurised",
                         "unpasteurized"],
    "Dairy — cheese": ["cheese", "fromage", "queso", "formaggio", "käse",
                       "kaese", "brie", "camembert", "mozzarella", "gorgonzola",
                       "roquefort", "feta", "halloumi", "raclette", "reblochon",
                       "tomme", "morbier", "comté"],
    "Dairy — other": ["milk", "lait", "leche", "latte", "milch", "yoghurt",
                      "yogurt", "butter", "beurre", "cream", "crème",
                      "ice cream", "glace"],
    "Meat — poultry": ["poultry", "chicken", "turkey meat", "duck", "volaille",
                       "poulet", "drobiowym", "drób", "pollo", "geflügel"],
    "Meat — beef": ["beef", "bœuf", "boeuf", "veal", "steak", "manzo", "rind"],
    "Meat — pork": ["pork", "porc", "ham", "jambon", "bacon", "lardon",
                    "salami", "sausage", "saucisse", "wurst", "prosciutto",
                    "guanciale", "pancetta", "chorizo"],
    "Meat — cooked / ready-to-eat": ["ready-to-eat", "cooked meat",
                                     "charcuterie", "deli meat", "pâté",
                                     "rillettes", "foie gras"],
    "Seafood — fin-fish": ["fish", "poisson", "salmon", "saumon", "tuna",
                           "thon", "mackerel", "sardine", "anchovy", "trout",
                           "truite"],
    "Seafood — shellfish / molluscs": ["oyster", "huître", "huitre", "mussel",
                                       "moule", "clam", "scallop", "shellfish",
                                       "coquillage", "crab", "shrimp", "prawn",
                                       "crevette", "lobster"],
    "Seafood — smoked / cured": ["smoked fish", "saumon fumé", "smoked salmon",
                                 "gravlax", "hot-smoked", "cold-smoked",
                                 "fumé"],
    "Eggs & egg products": ["egg", "œuf", "oeuf", "huevo", "uova", "jaja"],
    "Fresh produce — leafy greens": ["lettuce", "spinach", "salad", "salade",
                                     "rocket", "arugula", "kale", "leafy"],
    "Fresh produce — sprouts": ["sprout", "germe", "alfalfa"],
    "Fresh produce — berries": ["berry", "berries", "strawberry", "raspberry",
                                "blueberry", "fraise", "framboise"],
    "Fresh produce — melons": ["melon", "watermelon", "cantaloupe", "pastèque"],
    "Fresh produce — herbs": ["parsley", "persil", "coriander", "cilantro",
                              "basil", "basilic", "mint", "herb"],
    "Fresh produce — other": ["vegetable", "légume", "legume", "tomato",
                              "cucumber", "carrot", "onion", "pepper", "fruit",
                              "apple", "pomme"],
    "Dried fruit / nuts": ["dried fig", "fig", "raisin", "dried date",
                           "apricot", "prune", "nut", "walnut", "almond",
                           "pistachio", "peanut", "cashew", "hazelnut", "noix",
                           "amande", "arachide"],
    "Seeds & grains": ["seed", "grain", "cereal", "wheat", "maize", "corn",
                       "rice", "oat", "barley", "flour", "farine", "sesame",
                       "quinoa"],
    "Spices & seasonings": ["spice", "épice", "epice", "paprika", "cumin",
                            "turmeric", "curry", "seasoning", "cinnamon",
                            "nutmeg"],
    "Bakery & confectionery": ["bread", "pain", "pastry", "cake", "gâteau",
                               "biscuit", "cookie", "chocolate", "chocolat",
                               "candy", "sweets", "bakery", "boulangerie"],
    "Infant formula & baby food": ["infant formula", "baby food",
                                   "follow-on formula", "lait infantile",
                                   "babynahrung", "infant cereal"],
    "Beverages": ["juice", "soft drink", "bottled water", "eau minérale",
                  "beer", "bière", "wine", "tea", "thé", "coffee", "café"],
    "Canned / jarred / vacuum-packed": ["canned", "conserve", "jarred",
                                        "vacuum-packed", "sous-vide",
                                        "sous vide", "low-acid", "bocal"],
    "Ready meals / sandwiches / salads": ["ready meal", "ready-to-eat meal",
                                          "sandwich", "wrap", "prepared salad",
                                          "plat préparé", "soup", "soupe",
                                          "broth"],
    "Sauces, condiments & dressings": ["sauce", "condiment", "dressing",
                                       "mayonnaise", "pesto", "hummus",
                                       "houmous", "tahini"],
    "Dietary supplements": ["supplement", "complément", "vitamin", "capsule",
                            "food supplement"],
    "Ingredients / raw materials": ["ingredient", "raw material", "additive",
                                    "matière première"],
}

# "Pet food" was offered and has never matched a row — the scrapers do not
# ingest companion-animal recalls. Removed rather than left to fire never.
RETIRED_PRODUCT: dict[str, str] = {
    "Pet food (human-adjacent recalls)": "no scraper ingests pet-food notices",
}

# --------------------------------------------------------------------------
# Country
# --------------------------------------------------------------------------
# Exactly the values the register writes, so the match is an equality test in
# the normal case. Generated from docs/data/recalls.json; the test regenerates
# and compares, so a new source country shows up as a failing test rather than
# a filter that silently never fires.

COUNTRY: list[str] = [
    "Afghanistan", "Argentina", "Australia", "Austria", "Azerbaijan",
    "Belgium", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil",
    "Bulgaria", "Canada", "Chile", "China", "Colombia", "Croatia", "Cyprus",
    "Czechia", "Denmark", "Ecuador", "Egypt", "Estonia", "Ethiopia",
    "Finland", "France", "Georgia", "Germany", "Greece", "Hong Kong",
    "Hungary", "India", "Indonesia", "Iran", "Ireland", "Italy", "Japan",
    "Kenya", "Korea, South", "Kosovo", "Latvia", "Lithuania", "Madagascar",
    "Malawi", "Mexico", "Morocco", "Nepal", "Netherlands", "New Zealand",
    "Nicaragua", "Nigeria", "Norway", "Pakistan", "Panama", "Peru",
    "Philippines", "Poland", "Portugal", "Romania", "Rwanda", "Serbia",
    "Singapore", "Slovakia", "Slovenia", "South Africa", "Spain", "Sri Lanka",
    "Sweden", "Switzerland", "Syria", "Taiwan", "Thailand", "Turkey",
    "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom",
    "United States", "Uruguay", "Uzbekistan", "Vietnam",
    "EU-wide / multi-country",
]

# Spellings a subscriber may already have stored, or may reasonably type,
# mapped to the substrings that find the register's own spelling.
COUNTRY_ALIASES: dict[str, list[str]] = {
    "Czechia": ["czechia", "czech republic"],
    "Korea, South": ["korea, south", "south korea", "republic of korea"],
    "Turkey": ["turkey", "türkiye", "turkiye"],
    "United Kingdom": ["united kingdom", "great britain",
                       "northern ireland"],
    "United States": ["united states", "u.s.a"],
    "EU-wide / multi-country": ["european union", "multiple eu", "eu/eea",
                                "comesa", "eu-wide"],
}

CATEGORIES = ("pathogen", "product", "country", "brand")


# --------------------------------------------------------------------------
# The matching rule — mirrored exactly in AftsAlertVocab.gs
# --------------------------------------------------------------------------

def token_pattern(token: str) -> str:
    """Regex source for one vocabulary token: word-boundaried, plural-tolerant."""
    pat = re.escape(token)
    if re.match(r"\w", token):
        pat = r"\b" + pat
    if re.search(r"\w$", token):
        pat = pat + r"s?\b"
    return pat


def _tokens_for(category: str, term: str) -> list[str]:
    key = (term or "").strip()
    low = key.lower()
    if category == "pathogen":
        for name, toks in PATHOGEN.items():
            if name.lower() == low:
                return toks
        mapped = PATHOGEN_LEGACY.get(low)
        if mapped:
            return PATHOGEN[mapped]
        return [low] if low else []
    if category == "product":
        for name, toks in PRODUCT.items():
            if name.lower() == low:
                return toks
        return [low] if low else []
    if category == "country":
        for name, toks in COUNTRY_ALIASES.items():
            if name.lower() == low:
                return toks
        return [low] if low else []
    return [low] if low else []


def _haystack(category: str, row: dict) -> str:
    if category == "pathogen":
        return str(row.get("Pathogen") or "")
    if category == "product":
        return str(row.get("Product") or "") + " " + str(row.get("Reason") or "")
    if category == "country":
        return str(row.get("Country") or "")
    if category == "brand":
        return str(row.get("Brand") or "") + " " + str(row.get("Company") or "")
    return ""


def matches(row: dict, category: str, term: str) -> bool:
    """True when `row` satisfies the subscriber rule (`category`, `term`).

    Free-text categories (brand, and any term not in the vocabulary) fall back
    to a word-boundaried search for the term as written — never to a split.
    """
    toks = _tokens_for(category, term)
    if not toks:
        return False
    hay = _haystack(category, row)
    if not hay.strip():
        return False
    return any(re.search(token_pattern(t), hay, re.I) for t in toks)
