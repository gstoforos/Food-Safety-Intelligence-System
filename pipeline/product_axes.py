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


def _text(row: Dict) -> str:
    """Product + Reason ONLY.

    Notes is the pipeline's audit trail — "gemini check pass", "gate",
    "enrich", "shortcut", "claude" — not product data. Including it swamped
    the real vocabulary (the top 25 tokens in every unknown bucket were audit
    words) and risks false matches on words like "clean" or "fixed". The
    structured RASFF fields that DO live in Notes are parsed by
    pipeline/regulator_fields.py, which is where that belongs.
    """
    return _n(f"{row.get('Product','')} {row.get('Reason','')}")


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
        if re.search(pat, text):
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
        "filet de poulet", "filet de dinde", "brochette", "merguez crue",
        "saucisse fraiche", "chair a saucisse", "steak", "roti cru",
        "lamb meat", "pork meat", "beef meat", "bovine meat", "veal",
        "egg", "oeuf", "shell egg", "sprout", "germe", "alfalfa",
        "porc", "boeuf", "agneau", "dinde", "canard", "lapin",
        "poisson frais", "maquereau", "sardine", "anchois", "hareng",
        "crevette crue", "coquillage", "huitre", "moule", "bulot",
        "oyster", "mollusc", "molluscs", "bivalve", "clam", "scallop",
        "sausage", "saucisse", "chipolata", "halal", "kebab", "brochette",
        "poulet", "volaille", "white meat", "red meat", "offal", "abat",
        "tripe", "liver", "foie", "rognon", "gizzard", "gesier",
        "meat", "poultry", "chicken", "beef", "pork", "lamb", "turkey",
        "duck", "veal", "viande", "porc", "boeuf", "poulet", "dinde",
        "mushroom", "champignon", "enoki", "shiitake", "onigiri",
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


def process_type(row: Dict) -> Result:
    t = _text(row)
    for label in PROCESS_ORDER:
        hit = _find(t, PROCESS_TERMS[label])
        if hit:
            return label, "low", hit
    return "unknown", "none", None


# ── ConsumptionState ────────────────────────────────────────────────────
CONSUMPTION_TERMS: Dict[str, Tuple[str, ...]] = {
    "cook-before-eating": (
        "cook before", "cook thoroughly", "a cuire", "cuire avant",
        "bien cuire", "cocinar antes", "cuocere prima", "durchgaren",
        "not ready-to-eat", "must be cooked", "raw poultry", "raw chicken",
        "viande hachee", "steak hache", "minced meat", "ground beef",
        "corn dog", "nugget", "raw sausage to cook",
        "poultry meat", "chicken meat", "turkey meat", "chicken breast",
        "turkey breast", "chicken leg", "chicken wing", "poultry cut",
        "carcass", "fresh poultry", "raw beef", "raw pork", "raw lamb",
        "escalope", "paupiette", "pilon", "cuisse", "brochette crue",
        "saucisse fraiche", "merguez", "chair a saucisse", "steak hache",
        "lardon cru", "flour", "farine", "raw flour", "dough", "pate crue",
        "porc cru", "boeuf cru", "agneau", "veal", "carcass", "sprout",
        "alfalfa", "germe", "egg", "oeuf", "shell egg", "rice", "riz",
        "pasta", "pates", "noodle", "semoule", "couscous", "poisson frais",
        "maquereau", "sardine", "anchois", "coquillage", "huitre", "moule",
        "meat", "poultry", "chicken", "beef", "pork", "lamb", "turkey",
        "duck", "veal", "viande", "porc", "boeuf", "poulet", "dinde",
        "mushroom", "champignon", "enoki", "shiitake",
    ),
    "ready-to-heat": (
        "ready-to-heat", "rechauffer", "reheat", "a rechauffer",
        "micro-ondes", "microwave", "oven ready", "au four", "calentar",
        "riscaldare", "erhitzen", "opwarmen", "ready meal", "plat cuisine",
    ),
    "ingredient": (
        "ingredient", "raw material", "matiere premiere", "materia prima",
        "for further processing", "pour transformation", "semi-finished",
        "zutat", "grondstof", "bulk industrial",
    ),
    "ready-to-eat": (
        "ready-to-eat", "ready to eat", "rte", "pret a consommer",
        "pret a manger", "consommer en l etat", "listo para consumir",
        "pronto al consumo", "verzehrfertig", "kant-en-klaar",
        "metka", "a tartiner", "spreadable", "deli", "charcuterie",
        "tranche", "sliced", "smoked salmon", "saumon fume", "salad",
        "salade", "dessert", "snack", "cheese", "fromage", "yoghurt",
        "sandwich", "pate", "rillettes", "mousse",
        # mined: the nuts/seeds/dried-fruit bucket (238 rows) is eaten
        # without a kill step, and so are cheeses and cured meats.
        "nut", "peanut", "groundnut", "pistachio", "almond", "cashew",
        "walnut", "hazelnut", "pecan", "macadamia", "sesame", "tahini",
        "dried fig", "dried apricot", "raisin", "sultana", "date",
        "seed mix", "trail mix", "granola", "muesli", "cereal bar",
        "chocolate", "biscuit", "cookie", "crisps", "chips", "confectionery",
        "ice cream", "glace", "sorbet", "juice", "jus", "smoothie",
        "drink", "boisson", "water", "eau", "yaourt", "fromage frais",
        "ham", "jambon", "salami", "saucisson", "chorizo", "bacon cooked",
        "olive", "hummus", "houmous", "dip", "spread", "tartinade",
        "gorgonzola", "brie", "camembert", "roquefort", "feta", "halloumi",
        "mozzarella", "cheddar", "gouda", "comte", "emmental", "raclette",
        "fromage", "cheese", "milk", "lait", "yoghurt", "creme",
        "rayon traditionnel", "a la demande", "counter", "deli counter",
        "crevette cuite", "cuites", "cooked prawn", "smoked", "fume",
        "boudin", "fruits and vegetables", "fresh fruit", "berry", "melon",
        "moringa", "capsule", "supplement", "complement alimentaire",
        "herb", "spice", "spices", "tea", "infusion", "coffee",
        "infant formula", "baby food", "petit pot", "compote",
        "reblochon", "nectaire", "fermier", "savoie", "aop", "igp",
        "saucisson sec", "fuet", "pancetta", "guanciale", "oyster",
        "mollusc", "molluscs", "bivalve", "clam", "scallop", "truite",
        "salmon", "saumon", "smoked fish", "poisson fume", "surimi",
        "ready meal", "meal", "plat", "tray", "sachet", "pack",
        "onigiri", "charcuterie", "seeds", "nuts",
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


def storage_condition(row: Dict) -> Result:
    t = _text(row)
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
                      "clamshell", "bak", "tarrina"),
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
)
_NET_WEIGHT = re.compile(r"(?<![a-z0-9])\d+[.,]?\d*\s?(g|gr|kg|ml|cl|l)(?![a-z])")
_MULTI_UNIT = re.compile(
    r"(?<![a-z0-9])x\s?\d+(?![a-z0-9])"
    r"|(?<![a-z0-9])\d+\s?(tranches|pieces|units|pcs|sachets|pots|bouteilles)(?![a-z])")


def packaging_form(row: Dict) -> Result:
    """packaged | unpackaged | unknown — the axis the text can actually support."""
    t = _text(row)
    hit = _find(t, _LOOSE_TERMS)
    if hit:
        return "unpackaged", "high", hit
    hit = _find(t, _PACKED_TERMS)
    if hit:
        return "packaged", "high", hit
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
