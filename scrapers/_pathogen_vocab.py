"""
Centralised pathogen / hazard vocabulary — single source of truth.
==================================================================

Every scraper, gap-finder and freshness check imports keywords from
HERE rather than maintaining its own list. That's the only way to
guarantee a fiche written in Greek (EFET), Hungarian (Nébih), Polish
(GIS) or Korean (MFDS) cannot be silently dropped because someone
forgot to translate "Listeria" or "Salmonella" into that language.

REFACTORED 2026-05-10 — TWO CATEGORIES, NOT ONE
================================================
Until 2026-05-10, every keyword (specific hazard names AND generic
recall-verbs) lived in a single CORE bucket. That was convenient for
gap-finder queries — a search for "recall" finds recall pages, a search
for "Salmonella" finds pathogen pages, both pulled from one vocab.

But it had a serious downstream cost. Scrapers using the vocab as a
PATHOGEN filter (fda_press, fda_listing, usda_fsis, cfia, fsanz, fsa_uk,
…) were matching on the verb "recall" and tagging EVERY misbranding /
allergen / mechanical-defect recall as Pathogen="recall" Tier=3. They
landed in Pending; claude-check rejected them downstream — wasted
reviewer API calls, diluted Pending signal-to-noise.

Fix: split the vocab into two categories. Same source of truth, two
clean lanes.

  • PATHOGENS         specific hazard names (Salmonella, ochratoxin,
                      glass fragment, lead contamin, …) — what scrapers
                      should match on when filtering for actual hazards.
  • RECALL_SIGNALS    page-context verbs and food terms (recall, alert,
                      warning, rappel, lebensmittel, 召回, …) — useful
                      for gap-finder and "is this a recall page" checks,
                      but should NOT be used as a pathogen filter.

Public API surface (post-refactor)
----------------------------------
  pathogens(*langs)         → tuple of pathogen-only keywords
  recall_signals(*langs)    → tuple of recall-signal-only keywords
  for_languages(*langs)     → tuple of pathogens ∪ recall_signals
                              (BACKWARD-COMPATIBLE — existing callers
                              keep working unchanged. They get the same
                              keyword set they got before this refactor.)

Migration path for scraper authors
----------------------------------
Old (broken — over-matches):
    PATHOGEN_KEYWORDS = for_languages("en")

New (correct — pathogen-only):
    PATHOGEN_KEYWORDS = pathogens("en")

Migrate scrapers one at a time and watch for regressions in Pending
volume. The vocab module itself is fully backward-compatible — no
scraper change is required to deploy this refactor; behavior is
identical to pre-refactor for any caller still using for_languages().

Layered data structures (kept for backward compat)
--------------------------------------------------
  CORE                Tuple — union of PATHOGENS and RECALL_SIGNALS,
                      preserving order: pathogens first, recall signals
                      after. Existing callers see this as before.
  BY_LANGUAGE         dict — per-language union of PATHOGENS_BY_LANGUAGE
                      and RECALL_SIGNALS_BY_LANGUAGE.
  for_languages()     unchanged behavior: CORE + BY_LANGUAGE[lang]
                      union, deduplicated.

NEGATIVE-FILTER VOCABULARY
--------------------------
``NON_PATHOGEN_REJECTS`` — unchanged from pre-refactor. Lists
"looks-like-a-recall-but-isn't-pathogen" patterns (undeclared
allergens that are pure-allergen, labelling errors, mechanical issues
with no contamination claim).
"""
from __future__ import annotations
from typing import Tuple


# ─────────────────────────────────────────────────────────────────────
# LAYER 1A — PATHOGENS (universal/scientific names)
# Specific hazard names. Use this when filtering for "what's the hazard".
# ─────────────────────────────────────────────────────────────────────
PATHOGENS: Tuple[str, ...] = (
    # ─── Bacterial pathogens ───
    "listeria", "listeria monocytogenes",
    "salmonella", "salmonella enterica", "salmonella spp",
    "e. coli", "e.coli", "escherichia coli",
    "stec", "vtec",
    "o157", "o26", "o103", "o111", "o121", "o145",
    "shiga", "shigatoxin", "shigatox",
    "clostridium botulinum", "botulin", "botulism",
    "norovirus", "norwalk",
    "hepatitis a", "hepatitis e", "hav", "hev",
    "campylobacter", "campylobacter jejuni",
    "cyclospora", "cyclospora cayetanensis",
    "vibrio", "vibrio parahaemolyticus", "vibrio cholerae",
    "cronobacter", "cronobacter sakazakii", "enterobacter sakazakii",
    "bacillus cereus", "cereulide",
    "shigella",
    "yersinia", "yersinia enterocolitica",
    "brucella", "brucellosis",
    "staphylococcus aureus", "staph enterotoxin",
    "trichinella", "trichinosis",
    "clostridium perfringens",

    # ─── Marine / biological toxins ───
    "biotoxin",
    "histamine", "scombroid", "scombrotoxin",
    "tetrodotoxin", "saxitoxin", "domoic acid", "okadaic acid",
    "ciguatera", "ciguatoxin",
    "dsp", "psp", "asp", "azp",  # diarrhetic / paralytic / amnesic / azaspiracid

    # ─── Mycotoxins ───
    "mycotoxin", "mykotoxin", "micotoxin", "micotossin",
    "aflatoxin", "aflatoxine",
    "ochratoxin", "ochratoxin a", "ochratoxine", "ocratoxin", "ocratossin",
    "patulin", "patuline",
    "alternaria", "alternaria toxins", "alternariol", "tenuazonic",
    "fumonisin", "fumonisine",
    "zearalenone", "zéaralénone",
    "deoxynivalenol", "déoxynivalénol", "nivalenol",
    "t-2 toxin", "ht-2 toxin", "trichothecene", "trichothécène",
    "citrinin",
    "ergot", "claviceps", "ergot alkaloid", "mutterkorn",
    "fusarium",

    # ─── Physical / foreign-body hazards ───
    "glass fragment", "glass piece", "glass shard",
    "metal fragment", "metal piece", "metal shard", "metal shavings",
    "plastic fragment", "plastic piece",
    "foreign object", "foreign body", "foreign material",
    "wood fragment", "stone fragment",

    # ─── Chemical hazards ───
    "ethylene oxide", "eto",
    "dioxin", "pcb",
    "mineral oil", "moah", "mosh",
    "chlorate", "perchlorate",
    "sudan dye", "sudan ",          # space-anchored to avoid matching "sudan-related"
    "melamine",
    "rodenticide", "rat poison",
    "bromadiolone", "brodifacoum", "difethialone", "difenacoum", "chlorophacinone",

    # ─── Heavy metals (anchored phrases to reduce false positives) ───
    "lead contamin", "elevated lead", "levels of lead", "lead in product",
    "excess lead",
    "cadmium",
    "arsenic",
    "mercury contamin", "mercury level",
    "heavy metal",

    # ─── Pest contamination ───
    "rodent contamination", "rodent dropping", "mouse droppings",
    "insect contamination", "insect infestation",
    "pest contamination",

    # ─── Mould / spoilage (specific hazard, distinct from generic "alert") ───
    "mould", "mold",
)


# ─────────────────────────────────────────────────────────────────────
# LAYER 1B — RECALL_SIGNALS (universal English)
# Generic recall-event verbs and warning words. Useful for "is this
# a recall page" detection but should NOT be used as a pathogen filter.
# ─────────────────────────────────────────────────────────────────────
RECALL_SIGNALS: Tuple[str, ...] = (
    "recall", "recalled", "recalls", "recalling",
    "withdrawal", "withdrawn", "withdraws",
    "alert", "warning",
)


# ─────────────────────────────────────────────────────────────────────
# LAYER 1 — CORE (legacy, computed)
# Backward-compat alias: CORE = PATHOGENS + RECALL_SIGNALS, deduplicated
# preserving order. Existing imports of CORE continue to work unchanged.
# ─────────────────────────────────────────────────────────────────────
def _ordered_union(*tuples: Tuple[str, ...]) -> Tuple[str, ...]:
    """Concatenate tuples preserving order, dropping duplicates."""
    seen: dict = {}
    for t in tuples:
        for kw in t:
            seen[kw] = None
    return tuple(seen)


CORE: Tuple[str, ...] = _ordered_union(PATHOGENS, RECALL_SIGNALS)


# ─────────────────────────────────────────────────────────────────────
# LAYER 2A — PATHOGENS_BY_LANGUAGE
# Per-language hazard names (translations of pathogen / toxin / foreign-
# body / chemical / pest terms). NOT recall verbs.
# ─────────────────────────────────────────────────────────────────────
PATHOGENS_BY_LANGUAGE: dict = {
    # ─── French ───
    "fr": (
        "salmonelle", "salmonelles", "salmonellose",
        "listériose", "listeriose",
        "hépatite", "hépatit",
        "moisissure", "moisissures",
        "toxine", "toxines",
        "contamination", "contamine", "contaminé",
        "microbiologique",
        "corps étranger", "fragment", "morceau de verre",
        "raticide", "mort-aux-rats",
    ),

    # ─── German ───
    "de": (
        "salmonellen", "listeriose",
        "schimmel", "schimmelpilz",
        "kontamination", "verunreinigung",
        "keim", "keime", "bakterien",
        "fremdkörper", "glassplitter", "metallspäne",
        "rattengift",
    ),

    # ─── Spanish ───
    "es": (
        "salmonelosis",
        "listeriosis",
        "moho", "mohos",
        "toxina", "toxinas",
        "contaminación", "contaminado", "contaminada",
        "microbiológica",
        "cuerpo extraño", "fragmento de vidrio",
        "raticida", "veneno para ratas",
    ),

    # ─── Italian ───
    "it": (
        "salmonellosi",
        "listeriosi",
        "muffa", "muffe",
        "tossina", "tossine",
        "contaminazione", "contaminato",
        "microbiologica",
        "corpo estraneo", "frammento di vetro",
        "rodenticida",
    ),

    # ─── Portuguese ───
    "pt": (
        "salmonelose",
        "listeriose",
        "bolor", "mofo",
        "toxina", "toxinas",
        "contaminação", "contaminado",
        "microbiológica",
        "corpo estranho", "fragmento de vidro",
        "raticida",
    ),

    # ─── Dutch ───
    "nl": (
        "salmonellose",
        "listeriose",
        "schimmel",
        "verontreiniging", "besmetting",
        "vreemd voorwerp", "glassplinter",
        "rattengif",
    ),

    # ─── Swedish ───
    "sv": (
        "salmonellos",
        "listerios",
        "mögel",
        "förorening", "kontamination",
        "bakterie", "bakterier",
    ),

    # ─── Danish ───
    "da": (
        "salmonellose",
        "listeriose",
        "skimmel",
        "forurening",
        "fremmedlegeme",
        "rottegift",
    ),

    # ─── Norwegian ───
    "no": (
        "salmonellose",
        "listeriose",
        "mugg",
        "forurensning",
        "fremmedlegeme",
        "rottegift",
    ),

    # ─── Finnish ───
    "fi": (
        "salmonella",
        "listeria",
        "home",
        "saastuminen",
        "vierasesine",
        "rotanmyrkky",
    ),

    # ─── Polish ───
    "pl": (
        "salmonelloza",
        "listerioza",
        "pleśń",
        "skażenie", "zanieczyszczenie",
        "ciało obce",
        "trutka na szczury",
    ),

    # ─── Czech ───
    "cs": (
        "salmonelóza",
        "listerióza",
        "plíseň",
        "kontaminace",
        "cizí těleso",
        "jed na potkany",
    ),

    # ─── Slovak ───
    "sk": (
        "salmonelóza",
        "listerióza",
        "pleseň",
        "kontaminácia",
        "cudzie teleso",
        "jed na potkany",
    ),

    # ─── Hungarian ───
    "hu": (
        "szalmonellózis",
        "listeriózis",
        "penész",
        "szennyeződés",
        "idegen test",
        "patkányméreg",
    ),

    # ─── Slovenian ───
    "sl": (
        "salmoneloza",
        "listerioza",
        "plesen",
        "kontaminacija", "onesnaženje",
        "tujek",
        "rodenticid",
    ),

    # ─── Romanian ───
    "ro": (
        "salmoneloză",
        "listerioză",
        "mucegai",
        "contaminare",
        "corp străin",
        "raticid", "otravă pentru șobolani",
    ),

    # ─── Bulgarian ───
    "bg": (
        "салмонелоза",
        "листериоза",
        "мухъл",
        "замърсяване",
        "чуждо тяло",
        "родентицид",
    ),

    # ─── Croatian ───
    "hr": (
        "salmoneloza",
        "listerioza",
        "plijesan",
        "kontaminacija",
        "strano tijelo",
        "rodenticid",
    ),

    # ─── Estonian ───
    "et": (
        "salmonelloos",
        "listerioos",
        "hallitus",
        "saastumine",
        "võõrkeha",
        "rotimürk",
    ),

    # ─── Latvian ───
    "lv": (
        "salmoneloze",
        "lisriozes",
        "pelējums",
        "piesārņojums",
        "svešķermenis",
        "rodenticīds",
    ),

    # ─── Lithuanian ───
    "lt": (
        "salmoneliozė",
        "listeriozė",
        "pelėsis",
        "užterštumas",
        "svetimkūnis",
        "rodenticidas",
    ),

    # ─── Greek ───
    "el": (
        "σαλμονέλλωση", "σαλμονέλα",
        "λιστερίωση", "λιστέρια",
        "μούχλα",
        "μόλυνση", "επιμόλυνση",
        "ξένο σώμα",
        "ποντικοφάρμακο",
    ),

    # ─── Turkish ───
    "tr": (
        "salmonelloz",
        "listeriyoz",
        "küf",
        "kontaminasyon", "bulaşma",
        "yabancı madde",
        "fare zehiri",
    ),

    # ─── Arabic ───
    "ar": (
        "سالمونيلا", "ليستريا",
        "عفن",
        "تلوث",
        "جسم غريب",
        "سم الفئران",
    ),

    # ─── Hebrew ───
    "he": (
        "סלמונלה", "ליסטריה",
        "עובש",
        "זיהום",
        "גוף זר",
        "רעל עכברים",
    ),

    # ─── Japanese ───
    "ja": (
        "サルモネラ", "リステリア",
        "カビ",
        "汚染", "混入",
        "異物",
        "殺鼠剤",
    ),

    # ─── Korean ───
    "ko": (
        "살모넬라", "리스테리아",
        "곰팡이",
        "오염",
        "이물질", "이물",
        "쥐약",
    ),

    # ─── Chinese-Simplified ───
    "zh": (
        "沙门氏菌", "李斯特菌",
        "霉菌",
        "污染",
        "异物",
        "灭鼠药",
    ),

    # ─── Chinese-Traditional ───
    "zh-Hant": (
        "沙門氏菌", "李斯特菌",
        "黴菌",
        "污染",
        "異物",
        "滅鼠藥",
    ),

    # ─── Indonesian ───
    "id": (
        "salmonella", "listeria",
        "jamur",
        "kontaminasi",
        "benda asing",
        "racun tikus",
    ),

    # ─── Malay ───
    "ms": (
        "salmonella", "listeria",
        "kulat",
        "pencemaran",
        "bendasing",
        "racun tikus",
    ),

    # ─── Thai ───
    "th": (
        "ซาลโมเนลลา", "ลิสทีเรีย",
        "เชื้อรา",
        "การปนเปื้อน",
        "วัตถุแปลกปลอม",
        "ยาเบื่อหนู",
    ),

    # ─── Vietnamese ───
    "vi": (
        "salmonella", "listeria",
        "nấm mốc",
        "ô nhiễm",
        "vật thể lạ",
        "thuốc diệt chuột",
    ),

    # ─── Icelandic ───
    "is": (
        "salmonellosi",
        "listeríusýking",
        "mygla",
        "mengun",
        "aðskotahlut",
        "rottueitur",
    ),
}


# ─────────────────────────────────────────────────────────────────────
# LAYER 2B — RECALL_SIGNALS_BY_LANGUAGE
# Per-language recall-event verbs, warning words, and food-context
# nouns. Useful for gap-finder queries and recall-page detection.
# ─────────────────────────────────────────────────────────────────────
RECALL_SIGNALS_BY_LANGUAGE: dict = {
    # ─── French ───
    "fr": (
        "rappel", "rappels", "retrait", "retiré",
        "alimentaire", "alimentation",
    ),
    # ─── German ───
    "de": (
        "rückruf", "rückrufe", "rückgerufen",
        "warnung", "warnungen",
        "lebensmittel", "lebensmittelvergiftung", "lebensmittelwarnung",
    ),
    # ─── Spanish ───
    "es": (
        "retiro", "retirada", "retira", "retirar",
        "alerta", "alertas",
        "alimentaria", "alimentario", "alimentación",
    ),
    # ─── Italian ───
    "it": (
        "richiamo", "richiami", "ritiro", "ritirato",
        "allerta", "allarme",
        "alimentare", "alimentari", "alimento",
    ),
    # ─── Portuguese ───
    "pt": (
        "recolha", "recolhe", "retirada", "retirar",
        "alerta", "alertas",
        "alimentar", "alimento",
    ),
    # ─── Dutch ───
    "nl": (
        "terugroep", "terugroepen", "teruggeroepen",
        "waarschuwing",
        "voedsel", "voedselveiligheid",
    ),
    # ─── Swedish ───
    "sv": (
        "återkallar", "återkallas", "återkallelse",
        "varning",
        "livsmedel", "livsmedelsburen", "livsmedelsförgiftning",
    ),
    # ─── Danish ───
    "da": (
        "tilbagekald", "tilbagekaldelse", "tilbagekalder",
        "advarsel",
        "fødevare", "fødevareforgiftning",
    ),
    # ─── Norwegian ───
    "no": (
        "tilbakekall", "tilbakekaller",
        "advarsel",
        "matvare", "matforgiftning",
    ),
    # ─── Finnish ───
    "fi": (
        "takaisinveto", "takaisinkutsu",
        "varoitus",
        "elintarvike", "ruokamyrkytys",
    ),
    # ─── Polish ───
    "pl": (
        "wycofanie", "wycofuje",
        "ostrzeżenie",
        "żywność", "zatrucie pokarmowe",
    ),
    # ─── Czech ───
    "cs": (
        "stažení", "stahuje",
        "varování",
        "potravina", "otrava jídlem",
    ),
    # ─── Slovak ───
    "sk": (
        "stiahnutie", "sťahuje",
        "varovanie",
        "potravina", "otrava jedlom",
    ),
    # ─── Hungarian ───
    "hu": (
        "visszahívás", "visszahív",
        "figyelmeztetés",
        "élelmiszer", "ételmérgezés",
    ),
    # ─── Slovenian ───
    "sl": (
        "umik", "umika",
        "opozorilo",
        "živilo", "zastrupitev s hrano",
    ),
    # ─── Romanian ───
    "ro": (
        "rechemare", "retragere",
        "alertă",
        "aliment", "alimentar", "intoxicație alimentară",
    ),
    # ─── Bulgarian ───
    "bg": (
        "изтегляне",
        "предупреждение",
        "храна", "хранително отравяне",
    ),
    # ─── Croatian ───
    "hr": (
        "povlačenje", "povlači",
        "upozorenje",
        "hrana", "hrane", "trovanje hranom",
    ),
    # ─── Estonian ───
    "et": (
        "tagasikutsumine", "tagasi kutsutud",
        "hoiatus",
        "toit", "toiduainete", "toidumürgistus",
    ),
    # ─── Latvian ───
    "lv": (
        "atsaukšana", "atsauc", "atsaukts",
        "brīdinājums",
        "pārtika", "pārtikas", "saindēšanās ar pārtiku",
    ),
    # ─── Lithuanian ───
    "lt": (
        "atšaukimas", "atšauktas",
        "įspėjimas",
        "maistas", "maisto", "apsinuodijimas maistu",
    ),
    # ─── Greek ───
    "el": (
        "ανάκληση", "ανακαλεί", "ανακλήθηκε",
        "προειδοποίηση",
        "τρόφιμο", "τρόφιμα", "τροφική δηλητηρίαση",
    ),
    # ─── Turkish ───
    "tr": (
        "geri çağırma", "geri çağırıldı",
        "uyarı",
        "gıda", "gıda zehirlenmesi",
    ),
    # ─── Arabic ───
    "ar": (
        "استدعاء", "سحب", "تحذير",
        "غذاء", "غذائي", "تسمم غذائي",
    ),
    # ─── Hebrew ───
    "he": (
        "ריקול", "החזרה", "אזהרה",
        "מזון", "הרעלת מזון",
    ),
    # ─── Japanese ───
    "ja": (
        "リコール", "回収", "自主回収",
        "警告", "注意喚起",
        "食品", "食中毒",
    ),
    # ─── Korean ───
    "ko": (
        "리콜", "회수", "자진회수",
        "경고",
        "식품", "식중독",
    ),
    # ─── Chinese-Simplified ───
    "zh": (
        "召回", "下架",
        "警告",
        "食品", "食物中毒",
    ),
    # ─── Chinese-Traditional ───
    "zh-Hant": (
        "回收", "下架",
        "警告", "警示",
        "食品", "食物中毒",
    ),
    # ─── Indonesian ───
    "id": (
        "penarikan", "tarik kembali",
        "peringatan",
        "makanan", "keracunan makanan",
    ),
    # ─── Malay ───
    "ms": (
        "tarik balik", "panggilan semula",
        "amaran",
        "makanan", "keracunan makanan",
    ),
    # ─── Thai ───
    "th": (
        "เรียกคืน", "เรียกเก็บ",
        "เตือน", "คำเตือน",
        "อาหาร", "อาหารเป็นพิษ",
    ),
    # ─── Vietnamese ───
    "vi": (
        "thu hồi", "rút khỏi",
        "cảnh báo",
        "thực phẩm", "ngộ độc thực phẩm",
    ),
    # ─── Icelandic ───
    "is": (
        "innköllun", "innkallað",
        "viðvörun",
        "matvæli", "matareitrun",
    ),
}


# ─────────────────────────────────────────────────────────────────────
# LAYER 2 — BY_LANGUAGE (legacy, computed)
# Backward-compat alias: per-lang union of PATHOGENS_BY_LANGUAGE and
# RECALL_SIGNALS_BY_LANGUAGE. Existing imports of BY_LANGUAGE see the
# same shape as before this refactor.
# ─────────────────────────────────────────────────────────────────────
def _build_legacy_by_language() -> dict:
    out: dict = {}
    keys = set(PATHOGENS_BY_LANGUAGE) | set(RECALL_SIGNALS_BY_LANGUAGE)
    for lang in keys:
        p = PATHOGENS_BY_LANGUAGE.get(lang, ())
        s = RECALL_SIGNALS_BY_LANGUAGE.get(lang, ())
        out[lang] = _ordered_union(p, s)
    return out


BY_LANGUAGE: dict = _build_legacy_by_language()


# ─────────────────────────────────────────────────────────────────────
# LAYER 3 — Helpers
# ─────────────────────────────────────────────────────────────────────
def _collect(*langs: str, base: Tuple[str, ...],
             per_lang: dict) -> Tuple[str, ...]:
    """Return ordered union of `base` + per-language tuples for `langs`.

    Lang-code normalization: tolerates "fr-FR", "zh-Hant", "en_US",
    uppercase, etc. Unknown / empty codes are silently ignored.
    """
    seen: dict = {}
    for kw in base:
        seen[kw] = None
    for lang in langs:
        if not lang:
            continue
        key = lang.replace("_", "-")
        bucket = per_lang.get(key) or per_lang.get(key.split("-")[0].lower())
        if not bucket:
            continue
        for kw in bucket:
            seen[kw] = None
    return tuple(seen)


def pathogens(*langs: str) -> Tuple[str, ...]:
    """Return pathogen-only keywords (no recall verbs).

    Use this when filtering for "what's the actual hazard". Returns
    PATHOGENS plus any per-language pathogen translations.

    >>> kws = pathogens("fr")
    >>> "salmonella" in kws and "salmonelle" in kws
    True
    >>> "rappel" in kws or "recall" in kws
    False
    """
    return _collect(*langs, base=PATHOGENS, per_lang=PATHOGENS_BY_LANGUAGE)


def recall_signals(*langs: str) -> Tuple[str, ...]:
    """Return recall-signal-only keywords (no pathogen names).

    Use this for "is this a recall page" detection or gap-finder
    queries. Returns RECALL_SIGNALS plus per-language verbs/food terms.

    >>> kws = recall_signals("fr")
    >>> "rappel" in kws and "recall" in kws
    True
    >>> "salmonella" in kws or "salmonelle" in kws
    False
    """
    return _collect(*langs, base=RECALL_SIGNALS,
                    per_lang=RECALL_SIGNALS_BY_LANGUAGE)


def for_languages(*langs: str) -> Tuple[str, ...]:
    """Return tuple of all keywords (pathogens ∪ recall_signals).

    BACKWARD-COMPAT shim: returns the same set of keywords this
    function returned before the 2026-05-10 vocab refactor. Existing
    callers continue to work unchanged.

    For new code, prefer ``pathogens(*langs)`` for hazard filtering and
    ``recall_signals(*langs)`` for recall-event detection — they give
    cleaner, less false-positive-prone matching.

    >>> kws = for_languages("fr")
    >>> "listeria" in kws and "rappel" in kws
    True
    >>> "rückruf" in kws
    False
    """
    return _ordered_union(pathogens(*langs), recall_signals(*langs))


# ─────────────────────────────────────────────────────────────────────
# Negative filters — items matching ONLY these (and no pathogen) are
# dropped. Unchanged from pre-refactor.
# ─────────────────────────────────────────────────────────────────────
NON_PATHOGEN_REJECTS: Tuple[str, ...] = (
    "undeclared milk", "undeclared egg", "undeclared peanut", "undeclared soy",
    "undeclared wheat", "undeclared gluten", "undeclared nut", "undeclared fish",
    "undeclared shellfish", "undeclared sesame", "undeclared sulphite",
    "allergen labelling", "allergen labeling",
    "labelling error", "labeling error",
    "anomalie d'étiquetage", "etikettierungsfehler", "error de etiquetado",
    "errore di etichettatura",
)


__all__ = [
    # New public API (preferred for new code)
    "PATHOGENS", "RECALL_SIGNALS",
    "PATHOGENS_BY_LANGUAGE", "RECALL_SIGNALS_BY_LANGUAGE",
    "pathogens", "recall_signals",
    # Backward-compat exports (unchanged behavior)
    "CORE", "BY_LANGUAGE", "for_languages",
    "NON_PATHOGEN_REJECTS",
]
