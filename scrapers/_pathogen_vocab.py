"""
Centralised pathogen / hazard vocabulary — single source of truth.
==================================================================

Every scraper, gap-finder and freshness check imports keywords from
HERE rather than maintaining its own list. That's the only way to
guarantee a fiche written in Greek (EFET), Hungarian (Nébih), Polish
(GIS) or Korean (MFDS) cannot be silently dropped because someone
forgot to translate "Listeria" or "Salmonella" into that language.

THE THREE LAYERS
----------------

1. ``CORE``
   Universal English + scientific Latin names. Every scraper uses
   these regardless of country. Long-tail mycotoxins go here too —
   "alternaria", "fumonisin", "deoxynivalenol", "T-2 toxin", "ergot",
   etc., because their Latin / scientific spellings are language-
   independent. This is also where chemical hazards (rodenticide,
   ethylene-oxide, MOAH, lead-contamin) live.

2. ``BY_LANGUAGE``
   ISO-639 language code → tuple of native-language pathogen and
   recall vocabulary specific to that locale. Only words that are
   genuinely language-specific (e.g., "tilbagekaldelse" in Danish for
   "recall", or "リコール" in Japanese) — universal scientific
   spellings stay in CORE.

3. ``for_languages(*langs)``
   Helper. Returns CORE + every BY_LANGUAGE[lang] tuple unioned and
   de-duplicated. Used both by RSS scrapers (auto-extended via
   their ``LANGUAGE`` class attribute) and by the freshness checks
   that hit a single agency's API.

NEGATIVE-FILTER VOCABULARY
--------------------------

``NON_PATHOGEN_REJECTS`` lists "looks-like-a-recall-but-isn't-pathogen"
patterns (undeclared allergens that are pure-allergen, labelling
errors, mechanical issues with no contamination claim). RSS base
class drops items matching these exclusively — but if an item has
BOTH a pathogen keyword AND a reject keyword, the pathogen wins.

USAGE EXAMPLES
--------------

Inside ``_rss_base.py``::

    from scrapers._pathogen_vocab import CORE, for_languages
    PATHOGEN_KEYWORDS = CORE                       # base list
    # Subclasses' EXTRA_PATHOGEN_KEYWORDS are automatically extended
    # with for_languages(self.LANGUAGE) — see _rss_base.py.

Inside an agency-specific freshness/scraper::

    from scrapers._pathogen_vocab import for_languages
    KEYWORDS = for_languages("fr")                 # FR-only sweep

Inside a multi-language regulator (RASFF, CFIA EN+FR)::

    KEYWORDS = for_languages("en", "fr")
"""
from __future__ import annotations
from typing import Tuple

# ─────────────────────────────────────────────────────────────────────────
# LAYER 1 — CORE
# Universal English + scientific Latin + chemical-name vocabulary.
# Lower-cased; consumers compare ``haystack.lower()`` against these.
# ─────────────────────────────────────────────────────────────────────────
CORE: Tuple[str, ...] = (
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

    # ─── Mycotoxins (Latin / scientific names) ───
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

    # ─── Mould / spoilage ───
    "mould", "mold",

    # ─── Generic recall verbs (English — present in nearly every notice) ───
    "recall", "recalled", "recalls", "recalling",
    "withdrawal", "withdrawn", "withdraws",
    "alert", "warning",
)


# ─────────────────────────────────────────────────────────────────────────
# LAYER 2 — BY_LANGUAGE
# ISO-639 code → tuple of locale-specific terms.
# Only words that genuinely don't appear in CORE — universal Latin /
# scientific spellings already cover most pathogens worldwide.
# ─────────────────────────────────────────────────────────────────────────
BY_LANGUAGE: dict = {
    # ─── French (RappelConso, MAPAQ-QC, AFSCA-BE, BLV-CH-FR, ASN-LU) ───
    "fr": (
        "rappel", "rappels", "retrait", "retiré",
        "alimentaire", "alimentation",
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

    # ─── German (BVL, AGES, BLV-CH-DE) ───
    "de": (
        "rückruf", "rückrufe", "rückgerufen",
        "warnung", "warnungen",
        "lebensmittel", "lebensmittelvergiftung", "lebensmittelwarnung",
        "salmonellen", "listeriose",
        "schimmel", "schimmelpilz",
        "kontamination", "verunreinigung",
        "keim", "keime", "bakterien",
        "fremdkörper", "glassplitter", "metallspäne",
        "rattengift",
    ),

    # ─── Spanish (AESAN, ANMAT-AR, COFEPRIS-MX, ARCSA-EC, DIGESA-PE,
    #             INVIMA-CO, ISP-CL, MSP-UY) ───
    "es": (
        "retiro", "retirada", "retira", "retirar",
        "alerta", "alertas",
        "alimentaria", "alimentario", "alimentación",
        "salmonelosis",
        "listeriosis",
        "moho", "mohos",
        "toxina", "toxinas",
        "contaminación", "contaminado", "contaminada",
        "microbiológica",
        "cuerpo extraño", "fragmento de vidrio",
        "raticida", "veneno para ratas",
    ),

    # ─── Italian (Min. Salute) ───
    "it": (
        "richiamo", "richiami", "ritiro", "ritirato",
        "allerta", "allarme",
        "alimentare", "alimentari", "alimento",
        "salmonellosi",
        "listeriosi",
        "muffa", "muffe",
        "tossina", "tossine",
        "contaminazione", "contaminato",
        "microbiologica",
        "corpo estraneo", "frammento di vetro",
        "rodenticida",
    ),

    # ─── Portuguese (ASAE-PT, ANVISA-BR) ───
    "pt": (
        "recolha", "recolhe", "retirada", "retirar",
        "alerta", "alertas",
        "alimentar", "alimento",
        "salmonelose",
        "listeriose",
        "bolor", "mofo",
        "toxina", "toxinas",
        "contaminação", "contaminado",
        "microbiológica",
        "corpo estranho", "fragmento de vidro",
        "raticida",
    ),

    # ─── Dutch (NVWA, FAVV-AFSCA-BE-NL) ───
    "nl": (
        "terugroep", "terugroepen", "teruggeroepen",
        "waarschuwing",
        "voedsel", "voedselveiligheid",
        "salmonellose",
        "listeriose",
        "schimmel",
        "verontreiniging", "besmetting",
        "vreemd voorwerp", "glassplinter",
        "rattengif",
    ),

    # ─── Swedish (Livsmedelsverket) ───
    "sv": (
        "återkallar", "återkallas", "återkallelse",
        "varning",
        "livsmedel", "livsmedelsburen", "livsmedelsförgiftning",
        "salmonellos",
        "listerios",
        "mögel",
        "förorening", "kontamination",
        "bakterie", "bakterier",
        "toxin",
        "främmande föremål", "glassplitter",
        "råttgift",
    ),

    # ─── Danish (Fødevarestyrelsen) ───
    "da": (
        "tilbagekaldelse", "tilbagekaldt", "tilbagetrækning",
        "advarsel",
        "fødevare", "fødevarer", "fødevarebårne", "madforgiftning",
        "salmonellose",
        "listeriose",
        "skimmel",
        "forurening", "kontamination",
        "fremmedlegeme", "glasskår",
        "rottegift",
    ),

    # ─── Norwegian (Mattilsynet) ───
    "no": (
        "tilbakekaller", "tilbakekalles", "tilbakekalling",
        "advarsel",
        "matvare", "mat", "matforgiftning",
        "salmonellose",
        "listeriose",
        "muggsopp",
        "forurensning", "kontaminasjon",
        "fremmedlegeme",
        "rottegift",
    ),

    # ─── Finnish (Ruokavirasto) ───
    "fi": (
        "takaisinveto", "takaisinvedot", "vetää takaisin",
        "varoitus",
        "elintarvike", "elintarvikkeet", "ruokamyrkytys",
        "salmonelloosi",
        "listerioosi",
        "home",
        "saastunut", "kontaminaatio",
        "vieras esine", "lasinsiru",
        "rotanmyrkky",
    ),

    # ─── Polish (GIS) ───
    "pl": (
        "wycofanie", "wycofany", "wycofuje",
        "ostrzeżenie", "alarm",
        "żywność", "żywnościowy", "zatrucie pokarmowe",
        "salmonelloza",
        "listerioza",
        "pleśń",
        "skażenie", "zanieczyszczenie",
        "ciało obce", "odłamki szkła",
        "trutka na szczury",
    ),

    # ─── Hungarian (Nébih) ───
    "hu": (
        "visszahívás", "visszahívja", "visszahívták",
        "figyelmeztetés", "riasztás",
        "élelmiszer", "élelmiszerbiztonság", "ételmérgezés",
        "szalmonellózis",
        "listeriózis",
        "penész",
        "szennyeződés", "fertőzés",
        "idegen anyag",
        "patkányméreg",
    ),

    # ─── Romanian (ANSVSA) ───
    "ro": (
        "retragere", "retras", "retrage",
        "avertizare", "alertă",
        "aliment", "alimentar", "intoxicaţie alimentară",
        "salmoneloză",
        "listerioză",
        "mucegai",
        "contaminare",
        "corp străin",
        "raticid",
    ),

    # ─── Bulgarian (BFSA / БАБХ) ───
    "bg": (
        "изтегляне", "изтегля", "изтеглен",
        "предупреждение",
        "храна", "хранителен", "хранително отравяне",
        "салмонелоза",
        "листериоза",
        "мухъл",
        "замърсяване", "контаминация",
        "чуждо тяло",
        "родентицид",
    ),

    # ─── Czech (SZPI) ───
    "cs": (
        "stažení", "stažen", "stahuje",
        "varování",
        "potravina", "potraviny", "otrava jídlem",
        "salmonelóza",
        "listerióza",
        "plíseň",
        "kontaminace", "znečištění",
        "cizí těleso",
        "rodenticid",
    ),

    # ─── Slovak (ŠVPS) ───
    "sk": (
        "stiahnutie", "stiahnutý",
        "varovanie",
        "potravina", "potraviny", "otrava jedlom",
        "salmonelóza",
        "listerióza",
        "pleseň",
        "kontaminácia",
        "cudzie teleso",
        "rodenticíd",
    ),

    # ─── Slovenian (UVHVVR) ───
    "sl": (
        "odpoklic", "odpoklicano",
        "opozorilo",
        "živilo", "živila", "zastrupitev s hrano",
        "salmoneloza",
        "listerioza",
        "plesen",
        "onesnaženje", "kontaminacija",
        "tujek",
        "rodenticid",
    ),

    # ─── Croatian (HAH) ───
    "hr": (
        "povlačenje", "povlači", "povučen",
        "upozorenje",
        "hrana", "hrane", "trovanje hranom",
        "salmoneloza",
        "listerioza",
        "plijesan",
        "kontaminacija",
        "strano tijelo",
        "rodenticid",
    ),

    # ─── Estonian (VTA) ───
    "et": (
        "tagasikutsumine", "tagasi kutsutud",
        "hoiatus",
        "toit", "toiduainete", "toidumürgistus",
        "salmonelloos",
        "listerioos",
        "hallitus",
        "saastumine",
        "võõrkeha",
        "rotimürk",
    ),

    # ─── Latvian (PVD) ───
    "lv": (
        "atsaukšana", "atsauc", "atsaukts",
        "brīdinājums",
        "pārtika", "pārtikas", "saindēšanās ar pārtiku",
        "salmoneloze",
        "lisriozes",
        "pelējums",
        "piesārņojums",
        "svešķermenis",
        "rodenticīds",
    ),

    # ─── Lithuanian (VMVT) ───
    "lt": (
        "atšaukimas", "atšauktas",
        "įspėjimas",
        "maistas", "maisto", "apsinuodijimas maistu",
        "salmoneliozė",
        "listeriozė",
        "pelėsis",
        "užterštumas",
        "svetimkūnis",
        "rodenticidas",
    ),

    # ─── Greek (EFET) ───
    "el": (
        "ανάκληση", "ανακαλεί", "ανακλήθηκε",
        "προειδοποίηση",
        "τρόφιμο", "τρόφιμα", "τροφική δηλητηρίαση",
        "σαλμονέλλωση", "σαλμονέλα",
        "λιστερίωση", "λιστέρια",
        "μούχλα",
        "μόλυνση", "επιμόλυνση",
        "ξένο σώμα",
        "ποντικοφάρμακο",
    ),

    # ─── Turkish (TGTHB) ───
    "tr": (
        "geri çağırma", "geri çağırıldı",
        "uyarı",
        "gıda", "gıda zehirlenmesi",
        "salmonelloz",
        "listeriyoz",
        "küf",
        "kontaminasyon", "bulaşma",
        "yabancı madde",
        "fare zehiri",
    ),

    # ─── Arabic (NFSA-EG, MOCCAE-AE, SFDA-SA, MOPH-QA, ONSSA-MA) ───
    "ar": (
        "استدعاء", "سحب", "تحذير",
        "غذاء", "غذائي", "تسمم غذائي",
        "سالمونيلا", "ليستريا",
        "عفن",
        "تلوث",
        "جسم غريب",
        "سم الفئران",
    ),

    # ─── Hebrew (MOH-IL) ───
    "he": (
        "ריקול", "החזרה", "אזהרה",
        "מזון", "הרעלת מזון",
        "סלמונלה", "ליסטריה",
        "עובש",
        "זיהום",
        "גוף זר",
        "רעל עכברים",
    ),

    # ─── Japanese (MHLW) ───
    "ja": (
        "リコール", "回収", "自主回収",
        "警告", "注意喚起",
        "食品", "食中毒",
        "サルモネラ", "リステリア",
        "カビ",
        "汚染", "混入",
        "異物",
        "殺鼠剤",
    ),

    # ─── Korean (MFDS) ───
    "ko": (
        "리콜", "회수", "자진회수",
        "경고",
        "식품", "식중독",
        "살모넬라", "리스테리아",
        "곰팡이",
        "오염",
        "이물질", "이물",
        "쥐약",
    ),

    # ─── Chinese-Simplified (SAMR) ───
    "zh": (
        "召回", "下架",
        "警告",
        "食品", "食物中毒",
        "沙门氏菌", "李斯特菌",
        "霉菌",
        "污染",
        "异物",
        "灭鼠药",
    ),

    # ─── Chinese-Traditional (CFS-HK, TFDA-TW) ───
    "zh-Hant": (
        "回收", "下架",
        "警告", "警示",
        "食品", "食物中毒",
        "沙門氏菌", "李斯特菌",
        "黴菌",
        "污染",
        "異物",
        "滅鼠藥",
    ),

    # ─── Indonesian / Malay (BPOM-ID, KKM-MY) ───
    "id": (
        "penarikan", "tarik kembali",
        "peringatan",
        "makanan", "keracunan makanan",
        "salmonella", "listeria",
        "jamur",
        "kontaminasi",
        "benda asing",
        "racun tikus",
    ),
    "ms": (
        "tarik balik", "panggilan semula",
        "amaran",
        "makanan", "keracunan makanan",
        "salmonella", "listeria",
        "kulat",
        "pencemaran",
        "bendasing",
        "racun tikus",
    ),

    # ─── Thai (Thai FDA) ───
    "th": (
        "เรียกคืน", "เรียกเก็บ",
        "เตือน", "คำเตือน",
        "อาหาร", "อาหารเป็นพิษ",
        "ซาลโมเนลลา", "ลิสทีเรีย",
        "เชื้อรา",
        "การปนเปื้อน",
        "วัตถุแปลกปลอม",
        "ยาเบื่อหนู",
    ),

    # ─── Vietnamese (VFA) ───
    "vi": (
        "thu hồi", "rút khỏi",
        "cảnh báo",
        "thực phẩm", "ngộ độc thực phẩm",
        "salmonella", "listeria",
        "nấm mốc",
        "ô nhiễm",
        "vật thể lạ",
        "thuốc diệt chuột",
    ),

    # ─── Icelandic (MAST) ───
    "is": (
        "innköllun", "innkallað",
        "viðvörun",
        "matvæli", "matareitrun",
        "salmonellosi",
        "listeríusýking",
        "mygla",
        "mengun",
        "aðskotahlut",
        "rottueitur",
    ),
}


# ─────────────────────────────────────────────────────────────────────────
# LAYER 3 — for_languages()
# Returns the union of CORE + every BY_LANGUAGE[lang] tuple, deduplicated
# but preserving insertion order so iteration order is deterministic
# (helps log readability).
# ─────────────────────────────────────────────────────────────────────────
def for_languages(*langs: str) -> Tuple[str, ...]:
    """Return tuple of all keywords applicable to the given languages.

    Always includes CORE. Unknown / empty language codes are ignored.
    The same keyword appearing in CORE and in a BY_LANGUAGE entry is
    de-duplicated. Result order: CORE first, then per-lang in arg order.

    >>> kws = for_languages("fr")
    >>> "listeria" in kws and "rappel" in kws
    True
    >>> "rückruf" in kws
    False
    """
    seen: dict = {}        # str -> None, used as ordered set
    for kw in CORE:
        seen[kw] = None
    for lang in langs:
        if not lang:
            continue
        # tolerate "fr-FR", "zh-Hant", "en_US", uppercase, etc.
        key = lang.replace("_", "-")
        # Try exact match first (preserves zh-Hant), then primary subtag
        bucket = BY_LANGUAGE.get(key) or BY_LANGUAGE.get(key.split("-")[0].lower())
        if not bucket:
            continue
        for kw in bucket:
            seen[kw] = None
    return tuple(seen)


# ─────────────────────────────────────────────────────────────────────────
# Negative filters — items matching ONLY these (and no pathogen) are dropped.
# Kept short and conservative. Anything more aggressive risks false negatives.
# ─────────────────────────────────────────────────────────────────────────
NON_PATHOGEN_REJECTS: Tuple[str, ...] = (
    "undeclared milk", "undeclared egg", "undeclared peanut", "undeclared soy",
    "undeclared wheat", "undeclared gluten", "undeclared nut", "undeclared fish",
    "undeclared shellfish", "undeclared sesame", "undeclared sulphite",
    "allergen labelling", "allergen labeling",
    "labelling error", "labeling error",
    "anomalie d'étiquetage", "etikettierungsfehler", "error de etiquetado",
    "errore di etichettatura",
)


__all__ = ["CORE", "BY_LANGUAGE", "for_languages", "NON_PATHOGEN_REJECTS"]
