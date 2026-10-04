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
    "salmonella", "salmonellen",                    # English/Italian + German plural
    "salmonelle", "salmonella",                     # French/Polish (same)
    "salmonelloza", "salmonelozą",                  # Polish disease form
    "salmonellózis",                                # Hungarian
    "salmonellu",                                   # Icelandic genitive ("vegna salmonellu")
    "salmonellaa", "salmonellan",                   # Finnish partitive/genitive
    "σαλμονέλα", "σαλμονελα",
    # Listeria monocytogenes → Tier 1
    "listeria monocytogenes", "listeria", "listerien",  # + German plural
    "listeriose", "listeriosi",                     # Italian/French disease forms
    "listerioza",                                   # Polish
    "listeriózis",                                  # Hungarian
    "λιστέρια", "λιστερια",
    # STEC / E. coli O157 → Tier 1
    "stec", "e. coli o157", "e.coli o157", "escherichia coli o157",
    "shiga toxin-producing", "shiga-toxin", "shigatoxin",
    "shigatoxinbildende",                           # German (Shiga-toxinbildende E. coli)
    # Bacillus cereus / cereulide → always Tier 1 (locked rule)
    "bacillus cereus", "cereulide", "cereulida",    # ES/PT
    "κερευλίδη", "κερευλιδη",
    # Botulism — high severity
    "clostridium botulinum", "botulinum toxin", "botulism",
    "botulismus",                                   # German / Polish / Hungarian
    "botulisme",                                    # French / Dutch
    "botulinumtoxin",                               # German compound
    "αλλαντίαση", "αλλαντιαση",
    # Other high-severity
    "cronobacter sakazakii", "cronobacter",
    # ── Asian-language fleet (2026-09-30: JP, KR, CN, TH, ID) ───────────────
    # Without these a notice written in the local language can never
    # classify: every row is "unknown: No matching hazard category". Matched
    # after the same NFD/Mn-strip as everything else, so the needles and the
    # haystack degrade identically (voiced kana, Thai tone marks).
    "サルモネラ", "リステリア", "腸管出血性大腸菌", "ボツリヌス", "セレウス",
    "クロノバクター",                                             # Japanese
    "살모넬라", "리스테리아", "장출혈성대장균", "보툴리눔", "세레우스",
    "크로노박터",                                                 # Korean
    "沙门氏菌", "沙门菌", "沙門氏菌", "李斯特菌", "李斯特氏菌",
    "肉毒梭菌", "肉毒杆菌", "肉毒毒素", "蜡样芽孢杆菌", "仙人掌杆菌",
    "克罗诺杆菌", "阪崎肠杆菌", "阪崎腸桿菌",                      # Chinese
    "ซัลโมเนลลา", "ซาลโมเนลลา", "ลิสทีเรีย", "ลิสเทอเรีย", "โบทูลินัม",
    "โบทูลินั่ม",                                                 # Thai
}

PATHOGENS_TIER_2 = {
    "campylobacter", "καμπυλοβακτηρίδιο", "καμπυλοβακτηριδιο",
    "yersinia", "γερσίνια", "γερσινια",
    "shigella", "σιγκέλλα", "σιγκελλα",
    "staphylococcus aureus", "σταφυλόκοκκος",
    "norovirus", "νοροϊός", "νοροιος", "norwalk virus",
    # Hepatitis A & E — foodborne viruses
    "hepatitis a", "ηπατίτιδα α", "ηπατιτιδα α",
    # NOT a bare "hav" / "hev". Audit 2026-08-31. This set is matched with
    # `needle in haystack` (line ~633), so "hav" matched the word "have".
    # Every USDA FSIS notice ends "consumers who HAVe purchased these
    # products", which made every FSIS recall look like Hepatitis A. Three
    # were stamped that way in August. Spaced so it only matches the token.
    "hepatitis a virus", " hav ", "(hav)", "hav virus",
    "epatite a", "virus dell'epatite a", "virus dell epatite a",
    "hepatitis a-virus",                                         # German same
    "hepatitis a", "hepatitis a-virus",                          # Dutch
    "hépatite a", "virus de l'hépatite a", "virus de l hepatite a",  # French
    "wirusowe zapalenie wątroby typu a", "wzw a",                # Polish
    "hepatitis a", "a típusú hepatitis", "hav vírus",            # Hungarian
    "hepatitis e", "ηπατίτιδα ε", "ηπατιτιδα ε",
    # "hev" gets the same treatment for the same reason.
    "hepatitis e virus", " hev ", "(hev)", "hev virus",
    "epatite e", "virus dell'epatite e", "virus dell epatite e",
    "hepatitis e-virus",                                         # German same
    "hépatite e", "virus de l'hépatite e", "virus de l hepatite e",  # French
    "wirusowe zapalenie wątroby typu e", "wzw e",                # Polish
    "hepatitis e", "e típusú hepatitis", "hev vírus",            # Hungarian
    # Rotavirus
    "rotavirus",                                                 # same in all our languages
    "clostridium perfringens",
    "vibrio", "δονακιοειδή", "δονακιοειδη",
    # Generic E. coli without O157 specification → Tier 2 by default
    "e. coli", "e.coli", "escherichia coli", "κολοβακτηρίδιο", "κολοβακτηριδιο",
    # ── Asian-language fleet (2026-09-30) ───────────────────────────────────
    # NOT the bare coliform words 大腸菌 / 大肠菌 / 대장균: "大腸菌群" /
    # "大肠菌群" / "대장균군" are coliform COUNTS, a hygiene indicator on
    # every Japanese and Chinese sampling notice, not a pathogen. Only
    # E. coli proper is listed.
    "黄色ブドウ球菌", "ノロウイルス", "カンピロバクター", "腸炎ビブリオ",
    "ビブリオ", "ウエルシュ菌", "a型肝炎",                          # Japanese
    "황색포도상구균", "노로바이러스", "캠필로박터", "비브리오",
    "장염비브리오", "a형간염",                                     # Korean
    "金黄色葡萄球菌", "金黃色葡萄球菌", "诺如病毒", "諾羅病毒", "空肠弯曲",
    "弯曲杆菌", "大肠埃希氏菌", "大肠杆菌", "副溶血性弧菌", "副溶血弧菌",
    "甲型肝炎", "产气荚膜梭菌",                                     # Chinese
    "สแตฟิโลค็อกคัส", "โนโรไวรัส", "แคมไพโลแบคเตอร์", "อีโคไล",
    "ไวรัสตับอักเสบเอ", "วิบริโอ",                                  # Thai
}

# ACCEPT — mould (2026-09-30). In scope since the operator decision of
# 2026-09-07 (pipeline/_publish_gate.py "MOULD IS ITS OWN HAZARD CLASS" and
# pipeline/_pathogen_scope.py), Tier 2 per scrapers/_models.py. This
# classifier was never told, so every mould notice the fleet found was
# rejected as "unknown: No matching hazard category" — the decision reached
# two of the three places that apply it. Only explicit mould vocabulary: a
# bare "microbial contamination" stays unknown, as it does at the gate.
# NOT Indonesian "jamur" (also "mushroom" — "jamur enoki") and NOT "fungi".
MOULD = {
    "mould", "moulds", "mold", "molds", "mouldy", "moldy",
    "mould contamination", "mold contamination",
    "moisissure", "moisissures", "muffa", "moho", "schimmel", "mögel",
    "pleśń", "plesn", "penész", "hometta",
    "μούχλα", "ευρωτίασ", "fungal growth", "fungal contamination",
    "カビ", "真菌",                                                # Japanese
    "곰팡이",                                                      # Korean
    # NOT bare 霉菌: Chinese sampling notices report 霉菌数 — a mould
    # COUNT against a limit, a hygiene indicator like 菌落总数 — and a bare
    # needle would admit every one. 霉变 / 发霉 are visible mould growth.
    "霉变", "发霉", "發霉",                                         # Chinese
    "เชื้อรา",                                                     # Thai
    "kapang", "cemaran jamur",                                     # Indonesian
}

# ACCEPT — microbial-origin toxins (mycotoxins)
# Greek nouns inflect (αφλατοξίνη → αφλατοξινών gen.pl.), Italian uses 'aflatossina'/
# 'aflatossine'. We include stems where helpful to match all inflected forms.
MICROBIAL_TOXINS = {
    # Aflatoxin
    "aflatoxin", "aflatoxins", "αφλατοξιν",
    "aflatossin",          # Italian stem (aflatossina/aflatossine)
    "aflatoxina", "aflatoxinas",   # Spanish + Portuguese (same form)
    "aflatoksyn",          # Polish stem (aflatoksyna/aflatoksyny)
    "aflatoxiny",          # Czech / Slovak
    "aflatoksiini",        # Finnish
    "aflatoxín",           # Icelandic
    # Ochratoxin
    "ochratoxin", "ωχρατοξιν",
    "ocratossin",          # Italian
    "ocratoxina", "ocratoxinas",   # Spanish + Portuguese
    "ochratoksyn",         # Polish
    "okratoksiini", "okratoksiinit",   # Finnish (full + plural)
    "okratoxín",           # Icelandic
    # Mycotoxin (general — Finnish/Nordic often use generic term)
    "mycotoxin", "mycotoxins", "μυκοτοξιν",
    "micotossin",          # Italian
    "micotoxina",          # Spanish/Portuguese
    "mykotoksyn",          # Polish
    "hometoksiini", "hometoksiinit",   # Finnish ("home" = mold)
    "mykotoksiini",        # Finnish (formal)
    "myglutoxín",          # Icelandic
    # Patulin
    "patulin", "πατουλιν", "patulina",
    "patuliini",           # Finnish
    # Fumonisin
    "fumonisin", "φουμονισιν", "fumonisina", "fumonizyn",  # +Polish
    "fumonisiini",         # Finnish
    # Deoxynivalenol
    "deoxynivalenol", "δεοξυνιβαλενολ",
    "deossinivalenol",     # Italian
    "deoxinivalenol",      # Spanish + Portuguese
    "deoksyniwalenol",     # Polish
    "deoksinivalenoli",    # Finnish
    # Zearalenone
    "zearalenone", "ζεαραλενον", "zearalenona", "zearalenon",  # +DE/PL
    "tsearalenoni",        # Finnish
    # T-2 toxin
    "t-2 toxin", "t2 toxin",
    "tossina t-2", "toxina t-2", "t-2 toxin",
    "t-2-toksiini",        # Finnish
    # Ergot alkaloids
    "ergot alkaloid", "αλκαλοειδη ερυσιβης",
    "alcaloidi della segale cornuta", "alcaloidi ergot",
    "alcaloides del cornezuelo", "alcaloides do ergot",
    "mutterkornalkaloid", "mutterkornalkaloide",   # German
    "alcaloïdes de l'ergot", "alcaloides de ergot",  # French
    "alkaloidy sporyszu",  # Polish
    "anyarozs alkaloidok",  # Hungarian
    "torajyväalkaloidit",  # Finnish
    # Cereulide (Bacillus cereus toxin)
    "cereulide", "cereulida",
}

# ACCEPT — natural plant / fungal toxins
NATURAL_TOXINS = {
    "amanita muscaria", "muscarine", "muscimol", "μουσκαρίνη", "μουσκιμόλη",
    "muscarina", "muscimolo",
    "muscarina", "muscimol",              # Spanish + Portuguese
    "solanine", "σολανίνη",
    "solanina",                          # Italian / Spanish / Portuguese
    "cyanogenic glycoside", "κυανογόνο γλυκοζίδιο",
    "glucoside cianogenico", "glicoside cianogenico",
    "glucósido cianogénico", "glicosídeo cianogênico",
    "tetrodotoxin", "τετροδοτοξίνη",
    "tetrodotossina", "tetrodotoxina",
    "scombroid", "histamine poisoning",
    "sgombroide", "intossicazione da istamina",
    "intoxicación por histamina", "intoxicação por histamina",
    "lectin", "ricin", "ρικίνη",
    "lectina", "ricina",                 # universal Romance form
    "grayanotoxin", "γκραγιανοτοξίνη",
    "grayanotossina", "grayanotoxina",
    # ── Pyrrolizidine alkaloids (added 2026-10-03) ──────────────────────
    #
    # THE ROW THAT FOUND THIS. The Polish gap finder rediscovered GIS's
    # notice "alkaloidy pirolizydynowe w określonej partii herbatki
    # ziołowej z pokrzywy" on 2026-10-02 and refused it with
    # "unknown: No matching hazard category — defer to manual review",
    # because no needle here matched. The recall itself was already
    # PUBLISHED (Herbapol-Lublin, Zielnik Polski nettle tea, 2026-09-10,
    # Tier 3), so the refusal left the register saying the same URL was
    # both published and thrown away — which is what
    # test_a_recall_is_not_both_published_and_rejected caught.
    #
    # The hazard was never in doubt anywhere else: tools/alert_vocab.py
    # has offered "Pyrrolizidine alkaloids" to subscribers since it was
    # written. Only this gate could not see it. PAs are plant toxins that
    # carry over into herbal teas and honey and are capped by EU
    # Regulation 2023/915, so they belong with the natural toxins.
    #
    # "pirolizyd" / "pyrrolizid" / "πυρρολιζιδ" are stems: Polish and
    # Greek inflect the adjective, German compounds it
    # (Pyrrolizidinalkaloide), and a stem matches every form.
    "pyrrolizidine alkaloid", "pyrrolizidine",
    "pyrrolizidin",                      # German / Dutch / Nordic stem
    "alcaloidi pirrolizidinici", "pirrolizidin",          # Italian stem
    "alcaloides de pirrolizidina", "pirrolizidina",       # ES / PT
    "alcaloïdes pyrrolizidiniques",                       # French
    "pirolizyd",                         # Polish stem (pirolizydynowe)
    "πυρρολιζιδ",                        # Greek stem
    "pirrolizidin alkaloidok",           # Hungarian
    "pyrrolitsidiinialkaloidit",         # Finnish
}

# REJECT — allergens (undeclared)
# Multi-lingual: EN / EL / IT / ES / PT / DE / NL / FR / PL / HU.
# The classifier doesn't care which language matched, just which hazard fired.
ALLERGENS = {
    # ─── Milk/dairy ─────────────────────────────────────────────────────────
    "milk", "γάλα", "γαλα", "γαλακτος", "lactose", "λακτόζη", "λακτοζη",
    "latte", "lattosio", "caseina", "siero",
    "leche", "lácteos", "lacteos", "lactosa", "caseína", "caseina",
    "leite", "lacticínios", "lacticinios", "lactose", "caseína",
    "milch", "milcheiweiss", "milcheiweiß", "molke", "kasein",   # German
    "melk", "melkeiwit", "wei",                                    # Dutch
    "lait", "produits laitiers", "lactose", "caséine",            # French
    "mleko", "mleczne", "laktoza", "kazeina",                     # Polish
    "tej", "tejtermék", "laktóz", "tejfehérje", "kazein",         # Hungarian
    "mjölk", "mjolk", "mjölkprotein", "laktos", "kasein",         # Swedish
    "melk", "melkeprotein", "laktose", "kasein",                  # Norwegian
    "mælk", "maelk", "mælkeprotein", "laktose", "kasein",         # Danish
    "maito", "maitotuotteet", "laktoosi", "kaseiini",             # Finnish
    "mjólk", "mjolk", "mjólkurprótein", "laktósi", "kaseín",      # Icelandic
    # ─── Cereals/gluten ─────────────────────────────────────────────────────
    "wheat", "σιτάρι", "σιταρι", "άλευρο σίτου", "αλευρο σιτου",
    "grano", "frumento", "farina di frumento", "orzo", "segale", "farro", "avena",
    "gluten", "γλουτένη", "γλουτενη",
    "glutine",
    "trigo", "cebada", "centeno", "espelta", "avena", "harina de trigo",
    "glúten", "trigo", "cevada", "centeio", "espelta", "aveia",
    "weizen", "weizenmehl", "roggen", "gerste", "hafer", "dinkel",  # German
    "gluten",                                                         # Same in German/Dutch/French
    "tarwe", "tarwemeel", "rogge", "gerst", "haver", "spelt",       # Dutch
    "blé", "froment", "farine de blé", "seigle", "orge", "épeautre", "avoine",  # French
    "pszenica", "mąka pszenna", "żyto", "jęczmień", "owies", "orkisz",  # Polish
    "búza", "búzaliszt", "rozs", "árpa", "zab", "tönköly",         # Hungarian
    "vete", "vetemjöl", "rågmjöl", "korn", "havre", "spelt",       # Swedish
    "hvete", "hvetemel", "rug", "bygg", "havre", "spelt",          # Norwegian
    "hvede", "hvedemel", "rug", "byg", "havre", "spelt",           # Danish
    "vehnä", "vehnäjauho", "ruis", "ohra", "kaura", "speltti",     # Finnish
    "gluteeni",                                                    # Finnish gluten
    "hveiti", "hveitimjöl", "rúgur", "bygg", "hafrar", "spelt",    # Icelandic
    "glúten",                                                      # Icelandic gluten
    # ─── Soy ────────────────────────────────────────────────────────────────
    "soy", "σόγια", "σογια", "soya", "soia", "soja",
    # Same word in German/Dutch/French/Polish/Hungarian: "soja" / "szója" (HU)
    "szója",
    "soja",  # Swedish, Norwegian, Danish use "soya" or "soja"
    "soya",  # NO/DK
    # ─── Peanut ─────────────────────────────────────────────────────────────
    "peanut", "φιστίκι", "φιστικι", "αραχίδα", "αραχιδα",
    "arachide", "arachidi", "noccioline",
    "cacahuete", "cacahuetes", "maní",
    "amendoim", "amendoins",
    "erdnuss", "erdnüsse",                                       # German
    "pinda", "pinda's", "aardnoot",                              # Dutch
    "cacahuète", "cacahuètes", "arachide",                       # French
    "orzeszki ziemne", "fistaszki",                              # Polish
    "földimogyoró",                                              # Hungarian
    "jordnöt", "jordnötter",                                     # Swedish
    "peanøtt", "peanøtter", "jordnøtt", "jordnøtter",            # Norwegian
    "jordnød", "jordnødder",                                     # Danish
    "maapähkinä", "maapähkinät",                                # Finnish
    "jarðhneta", "jarðhnetur",                                  # Icelandic
    # ─── Tree nuts ──────────────────────────────────────────────────────────
    "tree nut", "καρπός με κέλυφος", "καρπος με κελυφος",
    "frutta a guscio", "frutos secos", "frutos de casca",
    "schalenfrucht", "schalenfrüchte", "nüsse",
    "noten", "schaalvruchten",
    "fruits à coque", "noix diverses",
    "orzechy",
    "olajos magvak",
    "almond", "αμύγδαλο", "αμυγδαλο",
    "mandorla", "mandorle", "almendra", "almendras", "amêndoa", "amêndoas",
    "mandel", "mandeln",                                         # German
    "amandel", "amandelen",                                      # Dutch
    "amande", "amandes",                                         # French
    "migdał", "migdały",                                         # Polish
    "mandula",                                                   # Hungarian
    "hazelnut", "φουντούκι", "φουντουκι",
    "nocciola", "nocciole", "avellana", "avellanas", "avelã", "avelãs",
    "haselnuss", "haselnüsse",                                   # German
    "hazelnoot", "hazelnoten",                                   # Dutch
    "noisette", "noisettes",                                     # French
    "orzech laskowy", "orzechy laskowe",                         # Polish
    "mogyoró",                                                   # Hungarian
    "walnut", "καρύδι", "καρυδι",
    "noce", "noci", "nuez", "nueces",
    "walnuss", "walnüsse",                                       # German
    "walnoot", "walnoten",                                       # Dutch
    "noix",                                                      # French (also "fruits à coque" generic)
    "orzech włoski", "orzechy włoskie",                          # Polish
    "dió",                                                       # Hungarian
    "cashew", "κάσιους", "κασιους", "anacardi", "anacardo", "anacardos",
    "kaschu", "kaschunuss", "cashewkern",                        # German
    "cashewnoot",                                                # Dutch
    "noix de cajou",                                             # French
    "nerkowiec",                                                 # Polish
    "kesudió",                                                   # Hungarian
    "pistachio", "φιστίκι αιγίνης", "φιστικι αιγινης",
    "pistacchi", "pistacchio", "pistacho", "pistachos", "pistácio", "pistácios",
    "pistazie", "pistazien",                                     # German
    "pistache", "pistachenoot",                                  # Dutch
    "pistache",                                                  # French
    "pistacja",                                                  # Polish
    "pisztácia",                                                 # Hungarian
    "chestnut", "castagna", "castagne", "castaña", "castanha",
    "edelkastanie", "marone", "esskastanie",                     # German
    "kastanje",                                                  # Dutch
    "châtaigne", "marron",                                       # French
    "kasztan",                                                   # Polish
    "szelídgesztenye",                                           # Hungarian
    # ─── Eggs ───────────────────────────────────────────────────────────────
    "egg", "αυγό", "αυγο", "αυγού",
    "uovo", "uova", "huevo", "huevos", "ovo", "ovos",
    "ei", "eier", "eiweiss", "eiweiß", "hühnerei",               # German
    "ei", "eieren",                                              # Dutch
    "œuf", "œufs", "oeuf", "oeufs",                              # French
    "jajko", "jajka",                                            # Polish
    "tojás", "tojások",                                          # Hungarian
    "ägg",                                                       # Swedish
    "egg",                                                       # Norwegian (same English-like)
    "æg",                                                        # Danish
    "kananmuna", "kananmunat", "muna",                           # Finnish
    "egg",                                                       # Icelandic (same)
    # ─── Fish/shellfish ─────────────────────────────────────────────────────
    "fish", "ψάρι", "ψαρι", "pesce", "pescado", "peixe",
    "fisch", "fische",                                           # German
    "vis", "vissen",                                             # Dutch
    "poisson", "poissons",                                       # French
    "ryba", "ryby",                                              # Polish
    "hal",                                                       # Hungarian
    "fisk",                                                      # Swedish/Norwegian/Danish (same)
    "kala", "kalat", "kalatuotteet",                             # Finnish
    "fiskur",                                                    # Icelandic
    "shellfish", "οστρακοειδή", "οστρακοειδη",
    "crostaceo", "crostacei", "crustáceo", "crustáceos",
    "krebstier", "krebstiere", "schalentier",                    # German
    "schaaldier", "schaaldieren",                                # Dutch
    "crustacé", "crustacés", "fruits de mer",                    # French
    "skorupiaki",                                                # Polish
    "rákfélék",                                                  # Hungarian
    "crustacean", "καρκινοειδή", "καρκινοειδη",
    "mariscos", "moluscos",
    "mollusc", "μαλάκια", "μαλακια", "molluschi",
    "weichtier", "weichtiere",                                   # German
    "weekdier", "weekdieren",                                    # Dutch
    "mollusque", "mollusques",                                   # French
    "mięczaki",                                                  # Polish
    "puhatestűek",                                               # Hungarian
    # ─── Sesame / mustard / sulfites / celery / lupin ───────────────────────
    "sesame", "σουσάμι", "σουσαμι", "sesamo", "sésamo", "ajonjolí", "gergelim",
    "sesam", "sesamsamen",                                       # German + Dutch
    "sésame",                                                    # French
    "sezam",                                                     # Polish
    "szezám", "szezámmag",                                       # Hungarian
    "sulfite", "θειώδη", "θειωδη", "solfiti", "solfito",
    "sulfitos", "sulfito", "sulfuroso",
    "sulfit", "sulfite", "schwefeldioxid",                       # German
    "sulfiet", "sulfieten", "zwaveldioxide",                     # Dutch
    "sulfite", "anhydride sulfureux",                            # French
    "siarczyn", "siarczyny", "dwutlenek siarki",                 # Polish
    "szulfit", "kén-dioxid",                                     # Hungarian
    "mustard", "μουστάρδα", "μουσταρδα", "senape", "mostaza", "mostarda",
    "senf",                                                      # German
    "mosterd",                                                   # Dutch
    "moutarde",                                                  # French
    "musztarda",                                                 # Polish
    "mustár",                                                    # Hungarian
    "celery", "σέλινο", "σελινο", "sedano", "apio", "aipo",
    "sellerie",                                                  # German
    "selderij",                                                  # Dutch
    "céleri",                                                    # French
    "seler",                                                     # Polish
    "zeller",                                                    # Hungarian
    "lupin", "λούπινο", "λουπινο", "lupini", "altramuz", "tremoço", "tremocos",
    "lupine", "lupinen",                                         # German + Dutch (same)
    "lupin",                                                     # French
    "łubin",                                                     # Polish
    "csillagfürt",                                               # Hungarian
    # ─── Allergen qualifiers ────────────────────────────────────────────────
    "allergen", "αλλεργιογόνο", "αλλεργιογονο", "αλλεργιογόνα",
    "allergene", "allergeni",
    "alérgeno", "alergeno", "alérgenos", "alergenos",
    "alergénio", "alergenio", "alergénios", "alergenios",
    "allergen", "allergene",                                     # German (same form)
    "allergeen", "allergenen",                                   # Dutch
    "allergène", "allergènes",                                   # French
    "alergen", "alergeny",                                       # Polish
    "allergén", "allergének",                                    # Hungarian
    "undeclared", "μη δηλωμένο", "μη δηλωμενο", "μη δηλωμένη",
    "non dichiarato", "non dichiarata", "non indicato",
    "no declarado", "no declarada", "no declarados",
    "não declarado", "nao declarado", "não declarada", "nao declarada",
    "nicht deklariert", "nicht angegeben", "nicht ausgewiesen",  # German
    "niet vermeld", "niet aangegeven", "niet gedeclareerd",      # Dutch
    "non déclaré", "non déclarée", "non déclarés", "non indiqué",  # French
    "niezadeklarowany", "niezadeklarowana", "nie zadeklarowany",  # Polish
    "nem jelölt", "nem deklarált", "nincs feltüntetve",          # Hungarian
    "ej deklarerad", "odeklarerad", "ej angiven",                # Swedish
    "ikke deklarert", "ikke oppgitt", "ikke merket",             # Norwegian
    "ikke deklareret", "ikke angivet", "ikke oplyst",            # Danish
    "ei ilmoitettu", "ilmoittamaton", "ei merkitty",             # Finnish
    "ekki tilkynnt", "ekki gefið upp", "ógreint",                # Icelandic
}

# REJECT — synthetic / environmental chemicals & additives
SYNTHETIC_CHEMICALS = {
    # 2026-10-01: bisphenol A migration and delta-9-THC over the acute
    # reference dose (RappelConso 23655, 23592, 23594) matched nothing.
    "bisphenol", "bisphénol", "bisfenol", "bisfenolo",
    "delta-9-thc", "delta-9 thc", "tetrahydrocannabinol",
    "tétrahydrocannabinol", "tetraidrocannabinolo", "tetrahidrocannabinol",
    "coumarin", "κουμαρίνη", "κουμαρινη", "cumarina",
    "kumarin",                                                  # German / Polish / Hungarian
    "coumarine",                                                # French / Dutch
    "pesticide", "φυτοφάρμακο", "φυτοφαρμακο", "pesticida", "pesticidi", "fitofarmaco",
    "plaguicida", "plaguicidas", "fitofarmacêutico", "pesticidas",
    "pestizid", "pestizide", "pflanzenschutzmittel",            # German
    "pesticide", "bestrijdingsmiddel",                          # Dutch (also "pesticide")
    "pesticide", "pesticides",                                  # French (same)
    "pestycyd", "pestycydy",                                    # Polish
    "peszticid", "növényvédő szer",                             # Hungarian
    "chlorpyrifos", "χλωροπυριφός", "clorpirifos", "chlorpyriphos",
    "glyphosate", "γλυφοσάτη", "glifosato", "glyphosat",
    "carbofuran", "carbofurano",
    "additive", "πρόσθετο", "προσθετο", "additivo", "additivi",
    "aditivo", "aditivos",
    "zusatzstoff", "zusatzstoffe",                              # German
    "additief", "additieven",                                   # Dutch
    "additif", "additifs",                                      # French
    "dodatek", "dodatki",                                       # Polish
    "adalékanyag",                                              # Hungarian
    "preservative over limit", "συντηρητικό υπέρβαση", "conservante oltre limite",
    "conservante por encima del límite", "conservante acima do limite",
    "konservierungsmittel über grenzwert",                      # German
    "conservant au-dessus limite",                              # French
    "colorant over limit", "χρωστική υπέρβαση", "colorante oltre limite",
    "colorante por encima del límite",
    "nitrite over limit", "νιτρώδη υπέρβαση", "nitriti oltre limite",
    "nitrato over limit", "νιτρικά υπέρβαση", "nitrati oltre limite",
    "nitritos por encima", "nitratos por encima",
    "nitrit über grenzwert", "nitrat über grenzwert",           # German
    "sudan dye", "sudan i", "sudan ii", "sudan iii", "sudan iv",
    "rhodamine", "ροδαμίνη", "rodamina", "rhodamin",
    "ethylene oxide", "οξείδιο αιθυλενίου", "ossido di etilene",
    "óxido de etileno",
    "ethylenoxid",                                              # German
    "ethyleenoxide",                                            # Dutch
    "oxyde d'éthylène", "oxyde d ethylene",                     # French
    "tlenek etylenu",                                           # Polish
    "etilén-oxid", "etilen-oxid",                               # Hungarian
    "melamine", "μελαμίνη", "melamina", "melamin",
    "acrylamide", "ακρυλαμίδιο", "acrilammide", "acrilamida", "acrylamid",
    "akrylamid",                                                # Polish
    "akrilamid",                                                # Hungarian
    "pah", "polycyclic aromatic", "πολυκυκλικοί αρωματικοί υδρογονάνθρακες",
    "idrocarburi policiclici aromatici",
    "hidrocarburos aromáticos policíclicos", "hidrocarbonetos aromáticos",
    "polyzyklische aromatische kohlenwasserstoffe",             # German
    "hydrocarbures aromatiques polycycliques",                  # French
    "dioxin", "διοξίνη", "diossina", "diossine", "dioxina", "dioxinas",
    "dioxine", "dioxines",
    "pcb", "πολυχλωριωμένα διφαινύλια", "policlorobifenili", "policlorobifenilos",
    "polychlorierte biphenyle",                                 # German
    "polychloorbifenylen",                                      # Dutch
    "polichlorowane bifenyle",                                  # Polish
}

# REJECT — heavy metals
HEAVY_METALS = {
    "cadmium", "κάδμιο", "καδμιο", "cadmio", "cádmio", "cadmium", "kadmium",
    "kadmio", "kadmiou",   # Greek romanized — appears in EFET URL slugs
    "lead", "μόλυβδος", "μολυβδος", "piombo", "plomo", "chumbo",
    "molyvdos", "molyvdou", "molybdos", "molybdou",   # Greek romanized — appears in EFET URL slugs
    "blei",                                                     # German
    "lood",                                                     # Dutch
    "plomb",                                                    # French
    "ołów",                                                     # Polish
    "ólom",                                                     # Hungarian
    "mercury", "υδράργυρος", "υδραργυρος", "mercurio", "mercúrio",
    "udrargyros", "udrargyrou", "ydrargyros", "ydrargyrou",   # Greek romanized (ELOT 743), nom + gen — appears in EFET URL slugs
    "quecksilber",                                              # German
    "kwik",                                                     # Dutch
    "mercure",                                                  # French
    "rtęć",                                                     # Polish
    "higany",                                                   # Hungarian
    "arsenic", "arsenico", "arsénico", "arsénio",
    # NOTE: bare Greek "αρσενικό/αρσενικο" and romanized "arseniko(s/u)"
    # are DELIBERATELY EXCLUDED — they collide with the very common Greek
    # word "αρσενικός/αρσενικού" (= "male"), causing false heavy-metal
    # rejects of real pathogen recalls (run 2026-06-15 rejected two Listeria
    # recalls this way). Greek arsenic-the-metal recalls are vanishingly
    # rare; if one appears, the Latin "arsenic" in the body or the English
    # extraction still catches it. Disambiguation by substring is impossible
    # since arsenic and "male" share the identical Greek root αρσενικ-.
    "arsen",                                                    # German / Polish
    "arseen",                                                   # Dutch
    "arsenic",                                                  # French (same)
    "arzén",                                                    # Hungarian
    "chromium", "χρώμιο", "χρωμιο", "cromo", "crómio", "cromio",
    "chrom",                                                    # German / Polish
    "chroom",                                                   # Dutch
    "chrome",                                                   # French
    "króm",                                                     # Hungarian
    "nickel", "νικέλιο", "νικελιο", "nichel", "níquel", "niquel",
    "nickel",                                                   # German / French / Dutch same
    "nikiel",                                                   # Polish
    "nikkel",                                                   # Hungarian
    "tin", "κασσίτερος", "κασσιτερος", "stagno", "estaño", "estanho",
    "zinn",                                                     # German
    "tin",                                                      # Dutch (same as English)
    "étain",                                                    # French
    "cyna",                                                     # Polish
    # NOTE: Hungarian "ón" (tin) removed — normalizes to "on", which
    # substring/word-matches the ubiquitous English word "on" and caused
    # false heavy-metal rejects on English titles. Use "ónmérgezés"
    # (tin poisoning) if Hungarian tin coverage is needed later.
    "ónmérgezés", "ón szennyeződés",                            # Hungarian tin (safe forms)
    "heavy metal", "βαρύ μέταλλο", "βαρυ μεταλλο", "metallo pesante", "metalli pesanti",
    "metales pesados", "metal pesado", "metais pesados",
    "schwermetall", "schwermetalle",                            # German
    "zware metalen",                                            # Dutch
    "métaux lourds",                                            # French
    "metale ciężkie",                                           # Polish
    "nehézfém", "nehézfémek",                                   # Hungarian
}

# REJECT — foreign matter
FOREIGN_MATTER = {
    "glass", "γυαλί", "γυαλι", "verre", "vetro", "frammenti di vetro",
    "cristal", "vidrio", "fragmentos de vidrio",
    "vidro", "fragmentos de vidro",
    "glas", "glasscherben", "glassplitter",                     # German / Dutch (same word)
    "verre", "fragments de verre",                              # French
    "szkło", "odłamki szkła",                                   # Polish
    "üveg", "üvegdarab", "üvegszilánk",                         # Hungarian
    "glas", "glasbitar", "glasskärva",                          # Swedish
    "glass", "glassbiter", "glassplint",                        # Norwegian
    "glas", "glasstykker", "glasskår",                          # Danish (same form)
    "lasi", "lasinsiruja", "lasinpalat",                        # Finnish
    "lasinpaloja", "lasinsirpaleet", "lasipala",                # Finnish declensions
    "gler", "glerbrot",                                         # Icelandic
    "metal fragment", "θραύσμα μετάλλου", "μέταλλο", "μεταλλο",
    "frammento metallico", "frammenti metallici", "metallo",
    "fragmentos metálicos", "partículas metálicas", "fragmento metálico",
    "metallfremdkörper", "metallsplitter", "metallteile",       # German
    "metaaldeeltjes", "metaalsplinter",                         # Dutch
    "fragments métalliques", "particules métalliques",          # French
    "fragmenty metalu", "odłamki metalu",                       # Polish
    "fémdarab", "fémszilánk",                                   # Hungarian
    "metallbitar", "metalldelar", "metallflisor",               # Swedish
    "metallbiter", "metalldeler",                               # Norwegian
    "metalstykker", "metalspåner",                              # Danish
    "metallinpaloja", "metallisirpaleet",                       # Finnish
    "málmbrot", "málmflísar",                                   # Icelandic
    "plastic", "πλαστικό", "πλαστικο", "plastica", "frammenti di plastica",
    "plástico", "fragmentos de plástico", "plastico",
    "kunststoff", "plastik", "kunststoffteile",                 # German
    "plastic", "kunststof",                                     # Dutch
    # Dutch compounds the material and the particle into one word, and the
    # bare "glas" is four characters so it is word-anchored and cannot
    # reach them (added 2026-10-04 with the rest of the Dutch gap).
    "glasdeeltjes", "glasscherven", "glassplinters",
    "metaaldeeltjes", "kunststofdeeltjes", "plasticdeeltjes",
    "plastique", "fragments de plastique",                      # French
    "tworzywo sztuczne", "plastik",                             # Polish
    "műanyag",                                                  # Hungarian
    "plast", "plastbitar", "plastflisor",                       # Swedish
    "plast", "plastbiter",                                      # Norwegian
    "plast", "plaststykker",                                    # Danish
    "muovi", "muovinpaloja",                                    # Finnish
    "plast", "plastbrot",                                       # Icelandic
    "foreign body", "foreign matter", "ξένο σώμα", "ξενο σωμα",
    "corps étranger", "stone", "πέτρα", "πετρα",
    # ── GENERIC "FOREIGN BODY", IN EVERY FLEET LANGUAGE (added 2026-10-04) ──
    #
    # Until today this concept existed in English, French and Greek only.
    # Every other language in the 43-country fleet could name a specific
    # material (glass, metal, plastic) but could not say "a foreign body
    # was found", which is how most regulators actually word it when the
    # object has not been identified. Those notices fell through every
    # branch to "unknown: No matching hazard category — defer to manual
    # review" and were archived unpublished.
    #
    # The row that exposed it: Italy, 2026-10-02, Franchi Salumi's whole
    # sweet salamella, "possibile presenza di un corpo estraneo", refused by
    # gap_finder/it/rules.py and sitting in Weekly_Rejected while the same
    # recall sat in Pending. Italian is not a special case — 14 of the 18
    # fleet languages tested had the same hole.
    #
    # The equivalents below are the standard regulatory phrasing in each
    # language. They are all six characters or more, so substring matching
    # covers declensions (German "Fremdkörpern", Italian "corpi estranei"
    # is listed separately because the plural changes both words).
    "corpo estraneo", "corpi estranei",                         # Italian
    "cuerpo extraño", "cuerpos extraños",                       # Spanish
    "corpo estranho", "corpos estranhos",                       # Portuguese
    "fremdkörper",                                              # German
    "vreemd voorwerp", "vreemde voorwerpen",                    # Dutch
    "ciało obce", "ciała obce",                                 # Polish
    "idegen test",                                              # Hungarian
    "cizí předmět", "cizí těleso",                              # Czech
    "cizího předmětu", "cizí předměty",                         # Czech, declined
    "cudzie teleso", "cudzieho telesa",                         # Slovak
    "främmande föremål",                                        # Swedish
    "fremmedlegeme",                                            # Danish / Norwegian
    "vieras esine", "vierasesine",                              # Finnish
    "aðskotahlutur",                                            # Icelandic
    "corp străin", "corpuri străine",                           # Romanian
    "strano tijelo", "strano telo",                             # Croatian / Slovene
    "stranog tijela", "stranega telesa",                        # ditto, declined
    "чуждо тяло",                                               # Bulgarian
    "yabancı madde", "yabancı cisim",                           # Turkish
    "ξένου σώματος", "ξένων σωμάτων",                           # Greek, declined
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


def _normalize_keep_accents(text: str) -> str:
    """Lowercase and collapse whitespace, but KEEP diacritics.

    Added 2026-09-26. _normalize strips accents so that a source writing
    "aflatossina" matches "aflatoxin". For terms of four characters or fewer
    that is too blunt: stripping the accent turns a word in one language into
    a completely different, very common word in another.

    Measured on the register the day this was written — ten published rows
    whose ENTIRE Pathogen field is one of these tokens:

        'ble'  x3, all Norway.  French "blé" (wheat) normalises to "ble",
               which is Norwegian for "was" — among the commonest words in
               the language. One of the three is Baza Nordic's sauerkraut
               recall for GLASS PARTICLES, filed as
               "Undeclared allergen — out of scope" with Pathogen "ble".
        'tej'  x1, Poland.  Hungarian "tej" (milk) against Polish "tej
               partii" ("of this batch").
        'lead' x5, Nigeria and Hong Kong.  The English metal against the
               English verb. One is a notice about excessive AFLATOXIN.
        'agg'  x1, Sweden.  Swedish "ägg" — a genuine egg recall, and the
               only true positive of the ten.

    Word boundaries were already in place and did not help: these are
    homographs, not substrings. The repo has met this shape twice before —
    _FM_BOUND exists so "stone" does not match "Blackstone", and
    tests/test_hav_substring_regression.py guards a three-letter
    hepatitis-A abbreviation that once matched the word "have" on every FSIS
    notice. Boundaries fix the substring half; only keeping the accent, or
    anchoring the word, fixes the homograph half.

    (That guard scans for the quoted token on any line not starting with a
    "#" — a docstring counts. Naming it here in quotes is how this paragraph
    first failed the suite, which is a fair demonstration of the point.)
    """
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.lower().strip())


def _normalize_set(keywords: set[str]) -> set[str]:
    return {_normalize(k) for k in keywords}


# Pre-normalize all lexicons once at module load (faster matching)
_PATHOGENS_T1_N = _normalize_set(PATHOGENS_TIER_1)
_PATHOGENS_T2_N = _normalize_set(PATHOGENS_TIER_2)
_MICROBIAL_TOXINS_N = _normalize_set(MICROBIAL_TOXINS)
_MOULD_N = _normalize_set(MOULD)
_NATURAL_TOXINS_N = _normalize_set(NATURAL_TOXINS)
# ── Homograph protection (2026-09-26) ────────────────────────────────────
#
# Short terms that must be matched WITH their diacritics, against text that
# has been lowercased but not accent-stripped. See _normalize_keep_accents
# for the ten rows that made this necessary. Derived rather than hand-listed,
# so a new accented short term in any vocabulary is covered automatically.
#
# 2026-10-04 — TWO DEFECTS IN THIS DERIVATION, both found by the morning pass.
#
#   (a) MOULD was never in the source tuple. The lexicon was added
#       2026-09-07; this block was written 2026-09-26 and listed eight
#       vocabularies by hand, so every accented mould term — Swedish
#       "mögel", Polish "pleśń", Hungarian "penész", Greek "μούχλα" — had
#       NO homograph protection at all. The docstring above promised the
#       opposite: "Derived rather than hand-listed, so a new accented short
#       term in any vocabulary is covered automatically." A hand-written
#       tuple is not a derivation. _CLASSIFIED_LEXICONS below is now the
#       single list `classify` and this block both read, so a lexicon can no
#       longer be in one and missing from the other.
#
#   (b) The cut-off was four characters, and the term that bit is five.
#       Swedish "mögel" flattens to "mogel", which is a substring of the
#       Dutch word "mogelijk" ("possible"). "mogelijke aanwezigheid van"
#       is the standard opening of an NVWA/FAVV notice, so EVERY Dutch and
#       Flemish recall whose text named no pathogen classified as
#       category='mould', tier=2, matched_term='mogel'. Measured on the
#       register of 2026-10-04 no published row carried the bad stamp yet,
#       only because the pathogen branches run before the mould branch and
#       every Dutch row so far named an organism. The rows it was waiting
#       for are Dutch foreign-body and chemical recalls — which the
#       FOREIGN_MATTER gap fixed in the same pass (no fleet language but
#       English, French and Greek could say "foreign body") sent straight
#       into the mould branch. Two gaps, one silent wrong answer.
#
# Terms of five characters keep INFLECTION matching (substring on the
# accented text) because a five-letter stem like "mögel" must still reach
# "mögelangrepp"; four and under stay word-anchored, as the ten rows in
# _normalize_keep_accents require.
#
# A term is skipped when its own lexicon ALSO lists an unaccented spelling
# that flattens to the same string — FOREIGN_MATTER carries both "γυαλί"
# and "γυαλι" on purpose, and demanding the accent would silence the
# variant the author put there deliberately.
_CLASSIFIED_LEXICONS = (
    ALLERGENS, HEAVY_METALS, FOREIGN_MATTER, NATURAL_TOXINS,
    MICROBIAL_TOXINS, SYNTHETIC_CHEMICALS, MOULD,
    PATHOGENS_TIER_1, PATHOGENS_TIER_2,
)

#: Only alphabetic scripts that WRITE WORD BOUNDARIES take part. The whole
#: mechanism exists for one problem — de-accenting a Latin or Greek word
#: turns it into a common word in another language ("blé" -> "ble") — and
#: its remedy is to match the accented form, word-anchored. Japanese, Korean,
#: Chinese and Thai write no spaces between words, so word-anchoring cannot
#: match at all there: `カビ` inside `水カビ様の異物` has a CJK letter on each
#: side and every boundary lookaround fails. Their combining marks are also
#: not diacritics in the relevant sense — NFD splits the dakuten off ビ, so
#: `カビ` flattens to `カヒ` and looks accented to this code while creating no
#: cross-language homograph at all. (Found 2026-10-04 by
#: test_the_classifier_reads_the_fleet_languages::test_accepts on
#: "異物（水カビ様の異物）混入", which stopped matching mould the moment MOULD
#: joined the derivation. Those scripts keep plain substring matching, which
#: is what they need for inflection anyway.)
_BOUNDARY_SCRIPT = re.compile(r"[a-z\u0370-\u03ff\u0400-\u04ff]")

_ACCENT_STRICT: dict[str, str] = {}
_ACCENT_STRICT_SUBSTRING: set[str] = set()
for _src in _CLASSIFIED_LEXICONS:
    _flat_spellings = {_normalize(_x) for _x in _src if _normalize(_x) == _x.lower()}
    for _t in _src:
        _low = _t.lower()
        _flat = _normalize(_t)
        if _flat == _low or len(_flat) > 5:
            continue
        if not _BOUNDARY_SCRIPT.search(_flat):
            continue
        if _flat in _flat_spellings:
            # The lexicon deliberately carries the unaccented spelling too.
            continue
        _ACCENT_STRICT[_flat] = _low
        if len(_flat) == 5:
            _ACCENT_STRICT_SUBSTRING.add(_flat)
del _src, _t, _low, _flat, _flat_spellings

# Terms that carry NO accent to protect them and collide with a common word
# in the same language. Replaced by anchored phrases, exactly as
# scrapers/_pathogen_vocab.py already does for lead ("anchored phrases to
# reduce false positives") — this module simply never got that treatment.
#
#   "lead"  the metal vs the verb. Five rows, including a Hong Kong notice
#           about excessive aflatoxin filed with Pathogen "lead".
#   "tin"   the metal vs the container. A product "canned in a tin" would
#           classify as a heavy-metal recall.
#   "tej"   Hungarian milk vs Polish "tej" ("of this"). Dropped rather than
#           anchored: ALLERGENS already carries tejfehérje and tejtermék, and
#           the cost asymmetry favours missing it. A false allergen match
#           REJECTS a real recall; a missed Hungarian milk allergen merely
#           lets an allergen row through, and the register's own allergen
#           gate refuses it downstream anyway.
_UNANCHORED_HOMOGRAPHS = {"lead", "tin", "tej"}
_ANCHORED_REPLACEMENTS = {
    "lead contamin", "elevated lead", "levels of lead", "lead in product",
    "excess lead", "excessive lead", "high lead",
    "tin contamin", "excess tin", "levels of tin",
}


_ALLERGENS_N = _normalize_set(ALLERGENS)
_SYNTHETIC_CHEMICALS_N = _normalize_set(SYNTHETIC_CHEMICALS)
# The anchored lead/tin phrases replace the bare tokens that
# _UNANCHORED_HOMOGRAPHS now refuses. Added to the NORMALISED set directly so
# the source vocabulary stays a plain word list.
_HEAVY_METALS_N = _normalize_set(HEAVY_METALS) | {
    _normalize(_p) for _p in _ANCHORED_REPLACEMENTS}
_FOREIGN_MATTER_N = _normalize_set(FOREIGN_MATTER)

# Short English foreign-matter nouns that need word-boundary matching even
# though they are ≥5 chars — otherwise they substring-match brand names and
# unrelated words (e.g. 'stone' in 'Blackstone' / 'milestone' / 'limestone'
# / 'Yellowstone'; 'glass' in 'fiberglass' / 'eyeglasses'). Multilingual
# variants for these concepts are separate entries in the lexicon (πέτρα,
# verre, vidrio, etc.) and aren't affected.
_FM_BOUND = {"stone", "glass", "insect", "rubber"}

# Bare material nouns in FOREIGN_MATTER — accepted only with context below.
_FM_BARE_MATERIALS = _normalize_set({
    "glass", "γυαλί", "γυαλι", "verre", "vetro", "cristal", "vidrio", "vidro",
    "glas", "szkło", "üveg", "lasi", "gler", "μέταλλο", "μεταλλο", "metallo",
    "plastic", "πλαστικό", "πλαστικο", "plastica", "plástico", "plastico",
    "kunststoff", "plastik", "kunststof", "plastique", "tworzywo sztuczne",
    "műanyag", "plast", "muovi", "stone", "πέτρα", "πετρα", "insect",
    "έντομο", "εντομο", "rubber", "ελαστικό", "λάστιχο",
})
# Words that say a material is IN the food. Multilingual, stems.
_FM_CONTEXT_N = _normalize_set({
    "fragment", "piece", "splinter", "shard", "particle", "bits of", "bit of",
    "foreign", "sliver", "presence of", "possible presence",
    "frammenti", "frammento", "pezzi", "pezzo", "presenza", "corpo estraneo",
    "morceau", "présence", "corps étranger", "éclat",
    "trozo", "presencia", "cuerpo extraño", "partícula",
    "stück", "teile", "splitter", "fremdkörper", "scherbe",
    # Dutch — NVWA and FAVV word it this way and none of it was here
    # before 2026-10-04, so a Dutch glass recall matched no context word
    # and its bare "glas" was never accepted.
    "aanwezigheid", "deeltjes", "stukjes", "scherven", "splinters",
    "fragmenten", "aangetroffen",
    "odłamk", "kawałk", "darab", "szilánk", "bitar", "biter", "stykker",
    "flisor", "paloja", "sirpale", "brot", "θραύσμ", "κομμάτ", "παρουσία",
    "ξένο σώμα",
})



def _contains_any(haystack: str, needles: set[str],
                  bound_extra: Optional[set[str]] = None,
                  _haystack_accented: str = "") -> Optional[str]:
    """Return first matching needle, or None.

    Short terms (≤4 chars, e.g. 'tin', 'don', 'pcb') use word-boundary matching
    to avoid false positives ('tin' matching 'destino', 'pcb' matching 'pcbs').

    Longer terms (≥5 chars) use substring matching so that stems like
    'αφλατοξιν' match all Greek inflections (αφλατοξίνη, αφλατοξινών, etc.)
    and 'aflatossin' matches Italian inflections (aflatossina, aflatossine).

    `bound_extra` is a set of specific English-noun terms that need
    word-boundary matching even though they are ≥5 chars — e.g. 'stone' must
    not substring-match the brand 'Blackstone'. Pass these per-category at
    the call site (foreign_matter, heavy_metal).
    """
    bound_extra = bound_extra or set()
    # The same text, lowercased but with diacritics intact, for the short
    # accented terms. Built once per call; cheap next to the regex loop.
    haystack_accented = _haystack_accented or ""
    # Pre-compile word-boundary regexes for short terms (cached at module load)
    for needle in needles:
        if not needle:
            continue
        # A term that collides with a common word once de-accented is tested
        # against the ACCENTED text instead, so French "blé" no longer
        # matches Norwegian "ble".
        if needle in _ACCENT_STRICT:
            _acc = _ACCENT_STRICT[needle]
            if not haystack_accented:
                continue
            if needle in _ACCENT_STRICT_SUBSTRING:
                # Five characters: a stem, so substring-match on the ACCENTED
                # text. "mögel" still reaches "mögelangrepp"; Dutch
                # "mogelijke" carries no umlaut and no longer matches.
                if _acc in haystack_accented:
                    return _acc
            elif re.search(r"(?<![^\W\d_])" + re.escape(_acc) + r"(?![^\W\d_])",
                           haystack_accented):
                return _acc
            continue
        # Homographs with no accent to protect them: never match bare.
        if needle in _UNANCHORED_HOMOGRAPHS:
            continue
        if needle in bound_extra:
            # English-noun word-boundary match with optional plural suffix
            # (-s OR -es: 'stone' matches 'stone'/'stones'; 'glass' matches
            # 'glass'/'glasses') but not embedded in other words (Blackstone,
            # milestone, eyeglasses, fiberglass).
            if re.search(r"(?<![a-zα-ω0-9])" + re.escape(needle)
                         + r"(?:es|s)?(?![a-zα-ω0-9])", haystack):
                return needle
        elif len(needle) <= 4:
            # Word-boundary match for short terms
            # \b doesn't work across Unicode word chars consistently, so we use
            # a manual lookahead/lookbehind for non-letter characters.
            if re.search(r"(?<![a-zα-ω0-9])" + re.escape(needle)
                         + r"(?![a-zα-ω0-9])", haystack):
                return needle
        else:
            # Substring match for longer terms (catches inflections)
            if needle in haystack:
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
    blob_acc = _normalize_keep_accents(f"{pathogen} {reason} {product}")

    # Out-of-scope REJECT checks (foreign matter / heavy metals / synthetic
    # chemicals) run on the HAZARD text only — NOT the product/packaging
    # description — so packaging materials ("clear plastic wrapped packages",
    # "glass jar", "plastic tray") can't false-trigger a reject on a real
    # pathogen recall. A genuine foreign-matter recall names the contaminant in
    # its reason/title ("...due to possible plastic contamination"), so it is
    # still caught here. (audit 2026-06-26 — an FSIS Listeria chicken-Caesar-
    # wrap PHA was wrongly rejected as foreign_matter on its "clear plastic
    # wrapped packages" product text.)
    reject_blob = _normalize(f"{pathogen} {reason}")
    reject_blob_acc = _normalize_keep_accents(f"{pathogen} {reason}")

    # ── PRINTED SCOPE (operator decision 2026-09-30) ─────────────────────
    # Foreign matter, heavy metals and synthetic/environmental chemicals were
    # the "UNAMBIGUOUS REJECT path" here (Rule B). The AFTS scope printed on
    # every brief since 2026-07-29 names foreign material and chemical
    # hazards as IN, and the publish gate has always published them; this
    # classifier was the one place still refusing them, so a news-discovered
    # foreign-body recall could never reach the register while a scraped one
    # could. They are now ACCEPTED — after the pathogen/toxin checks below,
    # so a named pathogen still decides the category (glass in a Listeria
    # product is a Listeria recall).
    #
    # Matched on the HAZARD text only (pathogen + reason), never the product
    # description, exactly as before: "glass jar", "plastic tray" and
    # "clear plastic wrapped packages" must not make a recall a foreign-body
    # one (audit 2026-06-26, FSIS Listeria chicken-Caesar wrap).
    _fm = _contains_any(reject_blob, _FOREIGN_MATTER_N, bound_extra=_FM_BOUND,
                        _haystack_accented=reject_blob_acc)
    # A BARE material word ("glass", "plastic", "vetro") was fine as a reject
    # signal and is dangerous as an accept one: "packaged in a glass jar" is
    # not a foreign-body recall. Accept on a bare word only when the hazard
    # text also says something is IN the food (fragment, pieces, foreign...).
    if _fm and _fm in _FM_BARE_MATERIALS and not any(
            t in reject_blob for t in _FM_CONTEXT_N):
        _fm = None
    _hm = _contains_any(reject_blob, _HEAVY_METALS_N,
                        _haystack_accented=reject_blob_acc)
    _sc = _contains_any(reject_blob, _SYNTHETIC_CHEMICALS_N,
                        _haystack_accented=reject_blob_acc)

    # ── ACCEPT path (pathogens & toxins — more specific than allergen mention) ─
    outbreak = detect_outbreak(reason)

    m = _contains_any(blob, _PATHOGENS_T1_N,
                     _haystack_accented=blob_acc)
    if m:
        return Classification(
            verdict="accept", category="pathogen", tier=1,
            matched_term=m,
            rule="Tier-1 pathogen (Salmonella / Listeria / STEC / B. cereus-cereulide / "
                 "C. botulinum / Cronobacter).",
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _PATHOGENS_T2_N,
                     _haystack_accented=blob_acc)
    if m:
        # Outbreak with case counts elevates to Tier 1
        tier = 1 if outbreak else 2
        return Classification(
            verdict="accept", category="pathogen", tier=tier,
            matched_term=m,
            rule=f"Tier-{tier} pathogen" + (" (outbreak case counts elevate to Tier 1)." if outbreak else "."),
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _MICROBIAL_TOXINS_N,
                     _haystack_accented=blob_acc)
    if m:
        return Classification(
            verdict="accept", category="microbial_toxin", tier=2,
            matched_term=m,
            rule="Microbial-origin toxin (mycotoxin) — accepted per Rule B.",
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _NATURAL_TOXINS_N,
                     _haystack_accented=blob_acc)
    if m:
        return Classification(
            verdict="accept", category="natural_toxin", tier=2,
            matched_term=m,
            rule="Natural plant/fungal toxin — accepted per Rule B.",
            outbreak_qualifies=outbreak,
        )

    m = _contains_any(blob, _MOULD_N, _haystack_accented=blob_acc)
    if m:
        return Classification(
            verdict="accept", category="mould", tier=2,
            matched_term=m,
            rule="Mould — microbiological contamination, in scope since "
                 "2026-09-07 (Tier 2).",
            outbreak_qualifies=outbreak,
        )

    if _fm:
        return Classification(
            verdict="accept", category="foreign_matter", tier=3,
            matched_term=_fm,
            rule="Foreign matter — in AFTS scope (printed scope; Rule B "
                 "reject retired 2026-09-30).",
            outbreak_qualifies=False,
        )
    if _hm:
        return Classification(
            verdict="accept", category="heavy_metal", tier=2,
            matched_term=_hm,
            rule="Heavy metal — chemical hazard, in AFTS scope (Rule B "
                 "reject retired 2026-09-30).",
            outbreak_qualifies=False,
        )
    if _sc:
        return Classification(
            verdict="accept", category="synthetic_chemical", tier=3,
            matched_term=_sc,
            rule="Synthetic/environmental chemical — in AFTS scope (Rule B "
                 "reject retired 2026-09-30).",
            outbreak_qualifies=False,
        )

    # ── ALLERGEN REJECT (last resort — only if no pathogen/toxin matched) ──
    m = _contains_any(blob, _ALLERGENS_N,
                     _haystack_accented=blob_acc)
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
