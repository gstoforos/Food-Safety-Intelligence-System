"""English-output policy — one place, deterministic, zero tokens.

THE RULE (operator, 2026-08-02)
===============================
    "everything in English except brand / or product name"

Company, Brand and Product are NAMES. "Scic des Abattoirs du Comminges",
"brie a l'ail", "Χούμους" and "Freshona Bio Beerenmischung" are what the
product is actually called, and translating them would make the row harder to
match against the regulator's own page, not easier. They stay as published.

Every other field is DESCRIPTION and must read in English: Reason, Class,
Pathogen, Country, Region.

WHAT WAS WRONG (audit 2026-08-02)
=================================
157 published rows carried a non-English Reason. Three distinct causes, and
each needs a different fix — which is why this is a module and not a
find-and-replace:

  1. RASFF publishes its notification subject BILINGUALLY, native language and
     English separated by "//", "/", "/////" or ";", in either order:

         "Presencia de Salmonela spp en salchichón procedente de España //
          Presence of Salmonella spp. in cured sausage from Spain;
          risk: serious; category: meat and meat products"

         "Listeria in foie gras from Bulgaria / Listeria en bloc de foie gras
          de pato procedente de Bulgaria; risk: serious; ..."

     Note the second one puts English FIRST. Taking "the part after the
     slash" would have produced Spanish on half the corpus. split_bilingual()
     scores both sides and keeps the English one, preserving the
     "; risk: …; category: …" tail that is part of the house RASFF format.

  2. HALF-TRANSLATED FRENCH. The enricher translated the opening words and
     stopped:

         "Presence of salmonelle dans le produit"
         "Detection of Listeria monocytogenes suite à une analyse"
         "Suspected contamination with Listeria monocytogenes, associée à des
          instructions de cuisson jugées insuffisamment explicites"

     These are the worst kind, because they LOOK translated. They are handled
     by REASON_EN, a verified per-string table: every entry below was read and
     translated individually rather than pattern-substituted, because
     RappelConso motifs carry clinically meaningful distinctions —
     "suspicion de" (suspected) vs "mise en évidence de" (demonstrated) vs
     "présence présumée" (presumed present) are not interchangeable in a food
     safety brief.

  3. WHOLE-SENTENCE GERMAN, SPANISH, ITALIAN from BVL, BLV, AESAN and the
     Ministero della Salute — also in REASON_EN.

WHY A TABLE RATHER THAN A PHRASE ENGINE
---------------------------------------
A phrase engine that turns "présence de" into "presence of" would mangle
"présence présumée" and "absence de test d'étanchéité". These strings are
published to subscribers as the reason a food was recalled; a wrong tense or a
dropped "suspected" is a factual error about a public-health event. Every
translation here is auditable: the French is quoted next to the English.

New source strings that match nothing are NOT guessed at — to_english()
returns None and the caller leaves the row alone and reports it. Silence beats
a machine-translated hazard description.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, Optional, Tuple

# ── Language detection ──────────────────────────────────────────────────────
# Function words only. Content words (pathogen names, food names) are shared
# across languages and would produce false positives on correct English rows.
_MARKERS: Dict[str, frozenset] = {
    "fr": frozenset("""
        à a l au aux d
        de du des la le les un une dans sur et est pour par avec sans
        presence présence détection detection produit produits rappel
        conforme suite raison teneur élevé eleve dépassement présent lié ainsi
        notre nos cette ce ces qui que été être sont fait mise évidence
        analyse échantillon lot lots seuil réglementaire vendu vendus
    """.split()),
    "es": frozenset("""
        por en presencia de la el los las una alerta procedentes debido
        productos retirada consumo alimentaria seguridad española agencia
        nutrición ampliación relativa encima límites permitidos supera
    """.split()),
    "de": frozenset("""
        der die das und von mit für nicht wurde kann rückruf produkt wurden
        nachgewiesen kontamination auf werden ruft zurück einer eines dem den
        aufgrund wird vom bei über
    """.split()),
    "it": frozenset("""
        di il la le dei della per con nel prodotto richiamo presenza dovuto
        lotto sono stato superiore limiti legge proveniente dall
    """.split()),
    "pt": frozenset("""
        de do da dos das para com produto recolha presença devido lote nao não
    """.split()),
    "nl": frozenset("""
        van het de een en met voor niet terugroeping product wordt kan bij
    """.split()),
    "pl": frozenset("""
        w na do nie oraz produkt partia wycofanie ze przez jest przekroczenia
        pochodzącej poziomie
    """.split()),
}

# Audit 2026-08-04: "a" and "à" are now French markers, so an English string
# using the article "a" must earn its keep here or it would be misread.
_ENGLISH_MARKERS = frozenset("""
    a an at by on it its
    the of in from and or with due to detected detection presence recalled
    recall because above level levels limit limits contamination possible
    potential risk category product products batch sold may been has have was
    were is are this that following after not present analysis compliant
    non-compliant testing sample samples exceeded during own-check demonstrated
    suspected withdrawn consumed manufacture supplier
""".split())

_GREEK = re.compile(r"[Ͱ-Ͽἀ-῿]")
_CYRILLIC = re.compile(r"[Ѐ-ӿ]")

_WORD = re.compile(r"[a-zà-ÿąćęłńóśźż']+")


def _words(text) -> frozenset:
    return frozenset(_WORD.findall(str(text or "").lower()))


# Two-letter tokens that are function words in one language and US state
# codes in another. Audit 2026-08-04: an FDA product description ending
# "...Distributed to Asian grocery stores in AZ, CA, FL, HI, IL, KS, LA, MD,
# MO, NC, NH, NV, NY, OK, PA, TX, UT (17 states)" was classified ITALIAN,
# because lowercasing turned IL and LA into the Italian articles "il" and
# "la" — two hits, threshold met. split_bilingual() then treated the comma
# clause as a foreign-language half and truncated the product.
#
# The test is precise rather than a blocklist: a marker is discarded only
# when EVERY occurrence of it in the ORIGINAL text is an all-caps two-letter
# token. "à la" in running text is untouched; "LA" in a state list is not a
# language signal.
_ALLCAPS2 = re.compile(r"\b[A-Z]{2}\b")


def _drop_abbreviation_markers(text: str, hits: frozenset) -> frozenset:
    raw = str(text or "")
    caps = {m.lower() for m in _ALLCAPS2.findall(raw)}
    if not caps:
        return hits
    keep = set()
    for word in hits:
        if len(word) != 2 or word not in caps:
            keep.add(word)
            continue
        # Present as an ordinary lowercase/Capitalised word somewhere too?
        if re.search(r"(?<![A-Za-z])" + re.escape(word) + r"(?![A-Za-z])",
                     raw.replace(word.upper(), "")):
            keep.add(word)
    return frozenset(keep)


def detect_language(text) -> Optional[str]:
    """Return a language code when `text` is confidently NOT English.

    Returns None for English and for anything too short to judge. Deliberately
    conservative: two independent function-word hits are required, so a single
    borrowed word ("saumure", "fiche") never trips it.
    """
    s = str(text or "")
    # A non-Latin script only makes the FIELD non-English when it carries the
    # message. An English sentence that quotes a Greek laboratory name or a
    # Greek product name is still an English sentence — that is the same
    # "names stay as published" principle the whole policy rests on. Require
    # the non-Latin script to be a real share of the letters, not a fragment.
    for rx, code in ((_GREEK, "el"), (_CYRILLIC, "ru")):
        foreign_chars = len(rx.findall(s))
        latin_chars = len(re.findall(r"[A-Za-zÀ-ÿ]", s))
        if foreign_chars >= 8 and foreign_chars >= 0.20 * (latin_chars + foreign_chars):
            return code
    w = _words(s)
    if len(w) < 2:
        return None
    best, score = None, 0
    for lang, markers in _MARKERS.items():
        hit = _drop_abbreviation_markers(s, w & markers)
        n = len(hit)
        if n > score:
            best, score = lang, n
    if score < 2:
        return None
    # A string can legitimately carry a few French words while being English
    # overall — "Fromagerie P. Jacquin & Fils brand Valençay AOP \"fromage de
    # chèvre au lait cru\" recalled due to generic E. coli" is an English
    # sentence quoting a French cheese name. The foreign side has to actually
    # win, not merely appear.
    if len(w & _ENGLISH_MARKERS) > score:
        return None
    return best


def looks_non_english(text) -> bool:
    return detect_language(text) is not None


# ── Bilingual splitting (RASFF) ─────────────────────────────────────────────
# The house RASFF format keeps a metadata tail that is already English and must
# survive the split.
_TAIL = re.compile(r"(;\s*risk:.*)$", re.IGNORECASE | re.DOTALL)
# NOTE the semicolon forms sit LAST and are gated hard below. Audit 2026-08-04:
# ";" is RappelConso's list separator and a normal punctuation mark in a
# product designation, so an ungated semicolon split truncates real names —
#   "merguez; chair farce; farce à tomate; chipolatas; chipolatas aux herbes"
#   "Hellas Meze Golden Smoked Whole Herring, vacuum-packaged; production date"
#   "Salame Nostrano (~800g; lot L6CCTD)"
# would each lose most of the product. Only ONE RASFF row in the corpus uses a
# semicolon as its language boundary, so the rule has to earn it.
_SEPARATORS = ("/////", "////", "///", "//", " / ", "/ ", " /", ";  ", "; ")


def _english_score(part: str) -> int:
    w = _words(part)
    foreign = 0
    for markers in _MARKERS.values():
        foreign = max(foreign, len(w & markers))
    if _GREEK.search(part):
        foreign += 3
    return len(w & _ENGLISH_MARKERS) - foreign


def split_bilingual(text) -> Optional[str]:
    """Return the English half of a bilingual regulator string, or None.

    Order is NOT assumed: RASFF publishes English-first about as often as
    native-first, so both sides are scored and the more English one wins. The
    "; risk: …; category: …" tail is detached before splitting and reattached
    afterwards, because it belongs to the row, not to either language half.
    """
    s = str(text or "").strip()
    if not s:
        return None
    tail = ""
    m = _TAIL.search(s)
    if m:
        tail = m.group(1)
        s = s[:m.start()]
    for sep in _SEPARATORS:
        if sep not in s:
            continue
        parts = [p.strip() for p in s.split(sep) if p.strip()]
        if len(parts) < 2:
            continue
        best = max(parts, key=_english_score)
        if best == s.strip() or looks_non_english(best):
            continue
        # ── A split must be a genuine LANGUAGE boundary ────────────────────
        # Audit 2026-08-04. The rule used to be "the winning half reads as
        # English", which is true of any list whose items are short. Applied
        # to Product — where the writer guard runs it on every write — that
        # silently truncated five real product designations, including a
        # five-item RappelConso charcuterie list and an English FDA name that
        # merely contained a semicolon.
        #
        # A bilingual regulator subject has TWO properties a list does not:
        # the discarded side is itself substantial, and it is identifiably a
        # DIFFERENT language. Requiring both keeps every genuine RASFF split
        # and rejects every list.
        others = [p for p in parts if p != best]
        if not others:
            continue
        longest_other = max(others, key=len)
        if len(best) < 20 or len(longest_other) < 20:
            continue
        if not looks_non_english(longest_other):
            continue
        # Strip separator debris left when the regulator omits the space
        # around the divider ("...z Turcji//exceedance of the MRL...") — the
        # split otherwise leaves a dangling "/" on the kept half.
        best = best.strip().strip("/").strip()
        return (best + tail).strip()
    return None


# ── Verified translations ───────────────────────────────────────────────────
# Key: the published string, normalised by _norm_key(). Value: the English the
# source actually says. Each French original is kept in the key so the pair can
# be re-checked without going back to the workbook.

def _norm_key(text) -> str:
    s = unicodedata.normalize("NFD", str(text or "").strip().lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[‘’‛]", "'", s)
    return re.sub(r"\s+", " ", s).strip(" .;")


_RAW_TRANSLATIONS = {
    # ── RappelConso (FR) — half-translated or untranslated motifs ──────────
    "Presence of salmonelle dans le produit":
        "Presence of Salmonella in the product",
    "Presence of salmonelle spp dans le produit":
        "Presence of Salmonella spp. in the product",
    "Detection of salmonelle dans les produits":
        "Detection of Salmonella in the products",
    "Detection of salmonelle dans le cadre d'autocontrôle":
        "Detection of Salmonella during own-check testing",
    "détection de salmonelle": "Detection of Salmonella",
    "Détection salmonelle suite à analyse":
        "Salmonella detected following analysis",
    "présence de salmonelles": "Presence of Salmonella",
    "présence possible de salmonella": "Possible presence of Salmonella",
    "Suspicion de salmonelle suite à auto contrôle":
        "Suspected Salmonella following own-check testing",
    "Analyse d'autocontrôle avec une détection de Salmonella spp.":
        "Own-check analysis detected Salmonella spp.",
    "Une analyse a mis en évidence la présence de Salmonella spp.":
        "Analysis demonstrated the presence of Salmonella spp.",
    "Un contrôle microbiologique a mis en évidence dans un lot de ce produit "
    "la présence de salmonelle.":
        "Microbiological testing demonstrated the presence of Salmonella in a "
        "batch of this product",
    "L'analyse d'un échantillon de viande marinée au paprika a révélé la "
    "présence de salmonelle.":
        "Analysis of a sample of paprika-marinated meat revealed the presence "
        "of Salmonella",
    "Mise en évidence de Salmonella": "Salmonella demonstrated on testing",
    "Mise en évidence de Listeria monocytogenes":
        "Listeria monocytogenes demonstrated on testing",
    "Mise en évidence de la présence de listéria monocytogenes":
        "Presence of Listeria monocytogenes demonstrated on testing",
    "Mise en évidence de la présence de Listeria monocytogenes":
        "Presence of Listeria monocytogenes demonstrated on testing",
    "présence de listeria monocytogenes": "Presence of Listeria monocytogenes",
    "Presence of Listeria monocytogenes. il ne faut pas consommer le produit":
        "Presence of Listeria monocytogenes — the product must not be consumed",
    "Suspicion de Listeria monocytogenes sur la matière première. il ne faut "
    "pas consommer le produit.":
        "Suspected Listeria monocytogenes in the raw material — the product "
        "must not be consumed",
    "Suspected contamination with Listeria monocytogenes, associée à des "
    "instructions de cuisson jugées insuffisamment explicites pour garantir "
    "la maîtrise":
        "Suspected contamination with Listeria monocytogenes, together with "
        "cooking instructions judged insufficiently explicit to guarantee "
        "control of the hazard",
    "Suspicion de présence de Listeria monocytogenes":
        "Suspected presence of Listeria monocytogenes",
    "Suspicion de présence de Listeria monocytogenes à dlc":
        "Suspected presence of Listeria monocytogenes at the use-by date",
    "Suspicion de présence de Salmonella spp. et Listeria monocytogenes":
        "Suspected presence of Salmonella spp. and Listeria monocytogenes",
    "Info consommateur pour suspicion de listéria monocytogenes":
        "Consumer notice — suspected Listeria monocytogenes",
    "Presence of traces de listéria": "Traces of Listeria detected",
    "Presence of Listeria monocytogenes (agent responsable de la listeriose)":
        "Presence of Listeria monocytogenes (the agent of listeriosis)",
    "Presence of Listeria monocytogenes (agent responsable de la listériose)":
        "Presence of Listeria monocytogenes (the agent of listeriosis)",
    "Presence of Listeria suite à une analyse de laboratoire":
        "Presence of Listeria following laboratory analysis",
    "Presence of Listeria monocytogenes au dela du seuil reglementaire":
        "Listeria monocytogenes above the regulatory threshold",
    "Detection of Listeria monocytogenes suite à une analyse de produit semi "
    "fini":
        "Listeria monocytogenes detected on analysis of a semi-finished product",
    "Detection of la listéria monocytogénès en autocontrôle sur un produit fini":
        "Listeria monocytogenes detected during own-check testing on a "
        "finished product",
    "Detection of Listeria monocytogenes au stade de la mise sur le marché":
        "Listeria monocytogenes detected at the point of placing on the market",
    "Détection ou suspicion de Listeria monocytogenes au stade de la mise sur "
    "le marché":
        "Listeria monocytogenes detected or suspected at the point of placing "
        "on the market",
    "Detection of présence Listeria monocytogenes":
        "Presence of Listeria monocytogenes detected",
    "Detection of listéria monocytogène dans le cadre d'un autocontrôle":
        "Listeria monocytogenes detected during own-check testing",
    "Detection of Listeria monocytogenes sur l'andouille de guémené après "
    "analyses":
        "Listeria monocytogenes detected in the andouille de Guémené following "
        "analysis",
    "Détection Listeria monocytogenèse sur les ponts l'evêque du lot 2610098":
        "Listeria monocytogenes detected in the Pont-l'Évêque cheese from "
        "batch 2610098",
    "Une analyse a mis en évidence la présence de Listeria monocytogenes dans "
    "le produit.":
        "Analysis demonstrated the presence of Listeria monocytogenes in the "
        "product",
    "Possible contamination du taboulé par la listéria":
        "Possible contamination of the tabbouleh with Listeria",
    "Présence Listeria monocytogenes dans une barquette":
        "Presence of Listeria monocytogenes in one tray",
    "Listeria dans le lait de chevre": "Listeria in the goat's milk",
    "Suite à une mise en analyse de nos rillettes de porc vendues entre le "
    "16/06/2026 et le 29/06/2026, une contamination à la Listeria a été "
    "détectée.":
        "Analysis of pork rillettes sold between 16/06/2026 and 29/06/2026 "
        "detected Listeria contamination",
    "Presence of listéria monocytogenes dans 25g - < 10 ufc/ g  et une  aw "
    "0.935;":
        "Listeria monocytogenes present in 25 g at under 10 CFU/g, water "
        "activity 0.935",
    "Presence of Listeria monocytogenes (l.m.)  sur les  6 crêpes goût caramel "
    "beurre salé  fabriquées le 18/06/2026 (lot : 2169 / 62196005), analyses "
    "réalisées":
        "Listeria monocytogenes found on analysis of the 6-pack salted-caramel "
        "crêpes made on 18/06/2026 (batch 2169 / 62196005)",
    "Presence of listéria monocytogenes sur le lot à dlc du 27/06/2026; rappel "
    "et retrait par mesure de précaution pour les lots à dlc du 28/06/2026 et "
    "29/06/2026":
        "Listeria monocytogenes present in the batch with a 27/06/2026 use-by "
        "date; the batches with 28/06/2026 and 29/06/2026 use-by dates are "
        "recalled and withdrawn as a precaution",
    "Rappel du lot 11070 de jambon sec avec os 9 mois en provenance des "
    "salaisons limousines en raison d'un risque de contamination de Listeria "
    "monocytogenes":
        "Recall of batch 11070 of 9-month bone-in dry-cured ham from Salaisons "
        "Limousines because of a risk of Listeria monocytogenes contamination",
    "Rappel du lot 13084 de jambon sec avec os 9 mois en provenance des "
    "salaisons limousines en raison d'un risque de contamination de Listeria "
    "monocytogenes":
        "Recall of batch 13084 of 9-month bone-in dry-cured ham from Salaisons "
        "Limousines because of a risk of Listeria monocytogenes contamination",
    "Notre fournisseur de museaux en saumure : prestige de la sarthe procède "
    "au retrait et rappel des produits de charcuterie suite à une suspicion de "
    "Listeria":
        "Our supplier of brined pork muzzle, Prestige de la Sarthe, is "
        "withdrawing and recalling charcuterie products following a suspicion "
        "of Listeria",
    "Suspected contamination with Listeria monocytogenes d'une matière "
    "première fournie par prestige de la sarthe et qui rentre dans la "
    "fabrication de notre produit":
        "Suspected Listeria monocytogenes contamination of a raw material "
        "supplied by Prestige de la Sarthe and used in the manufacture of this "
        "product",
    # STEC / E. coli
    "présence de escherichia coli stec": "Presence of Shiga toxin-producing "
                                         "Escherichia coli (STEC)",
    "Présence présumée de e.coli stec o26:h11":
        "Presumed presence of E. coli STEC O26:H11",
    "Detection par le fabricant de reblochon farto de thones d'e.coli stec "
    "o26h11":
        "E. coli STEC O26:H11 detected by the manufacturer in Reblochon Farto "
        "de Thônes",
    "Detection of E. coli STEC dans le cadre d'auto-controle":
        "E. coli STEC detected during own-check testing",
    # Chemical / mycotoxin
    "Taux de t-2 toxine et ht-2 toxine supérieures à la valeur réglementaire":
        "T-2 and HT-2 toxin levels above the regulatory limit",
    "La teneur en zéaralénone et la somme de t-2 toxine et de ht-2 toxine":
        "Zearalenone content and the sum of T-2 and HT-2 toxins above limits",
    "Une analyse réalisée par notre fournisseur minoterie suire a révélé un "
    "dépassement de la norme réglementaire en mycotoxines t2-ht2 sur le lot "
    "susmentionné":
        "Analysis by our supplier Minoterie Suire showed the T-2/HT-2 mycotoxin "
        "regulatory limit exceeded in the batch named above",
    "Dépassement du seuil réglementaire en contaminant (aflatoxines)":
        "Regulatory contaminant limit exceeded (aflatoxins)",
    "Retrait de la vente et rappel du produit suite au dépassement du seuil "
    "réglementaire en contaminants (ochratoxines a).":
        "Product withdrawn from sale and recalled after the regulatory "
        "contaminant limit for ochratoxin A was exceeded",
    "La raison de ce rappel est le dépassement de la teneur maximale en "
    "aflatoxine b1. l'aflatoxine b1 est un contaminant génotoxique et "
    "cancérigène   le l":
        "The recall is due to the maximum level for aflatoxin B1 being "
        "exceeded. Aflatoxin B1 is a genotoxic and carcinogenic contaminant",
    "Presence of ochratoxine à un seuil supérieur à la réglementation":
        "Ochratoxin present above the regulatory limit",
    "Présence potentielle d'ochratoxine a au dessus du seuil réglementaire":
        "Potential presence of ochratoxin A above the regulatory limit",
    "non conformité chimique (détection d'ochratoxine).":
        "Chemical non-conformity — ochratoxin detected",
    "Autres contaminants chimiques (présence de mycotoxines)":
        "Other chemical contaminants — mycotoxins present",
    "Résultats d'analyses non conformes en aflatoxines sur un échantillon "
    "prélevé":
        "Non-compliant aflatoxin results on a sample taken",
    "Teneur en cadmium supérieure à la limite autorisée par la réglementation "
    "européenne.":
        "Cadmium content above the limit permitted by EU regulation",
    # Process / other
    "Rappel pour raison sanitaire": "Recall on public-health grounds",
    "Rappel imposé par arrêté préfectoral":
        "Recall ordered by prefectoral decree",
    "Absence de test d'étanchéité et de test de stabilité - barème de "
    "stérilisation non validé":
        "No seal-integrity test and no stability test — the sterilisation "
        "schedule was not validated",
    "Suspicion d'entérotoxine staphylococcique dans le produit.":
        "Suspected staphylococcal enterotoxin in the product",
    "Un contrôle a mis en évidence une teneur insuffisante en sel pouvant "
    "entrainer une mauvaise conservation et un développement microbien et des "
    "défauts":
        "Testing showed an insufficient salt content, which can lead to poor "
        "preservation, microbial growth and defects",
    "Suite à la réception d'un résultat d'analyse non satisfaisant dépassant "
    "un seuil sur un critère de sécurité et déclenchant une situation d'alerte "
    "sanitaire":
        "Following an unsatisfactory analytical result exceeding a food-safety "
        "criterion and triggering a public-health alert",
    "Analyse bactériologique non conforme : présence d'Escherichia coli "
    "(STEC). Vendu chez Intermarché Le Passage. RappelConso fiche 22664.":
        "Non-compliant bacteriological analysis — Escherichia coli (STEC) "
        "present. Sold at Intermarché Le Passage (RappelConso fiche 22664)",
    "RappelConso: chemical non-conformity — presence of mycotoxins in "
    "buckwheat flour (farine de sarrasin). Fiche 22679.":
        "Chemical non-conformity — mycotoxins present in buckwheat flour "
        "(RappelConso fiche 22679)",
    "Présence de Listeria monocytogenes (RappelConso fiche 22634). Raw-milk "
    "farm tomme. Original extraction captured the point-of-sale purchase-"
    "location st":
        "Presence of Listeria monocytogenes (RappelConso fiche 22634) — "
        "raw-milk farmhouse tomme",

    # ── BVL (DE) ──────────────────────────────────────────────────────────
    "Der Großhändler Asia Express Food ruft Enoki-Pilze der Marke Green Box "
    "Limited zurück. Im Rahmen einer Kontrolle wurde das Bakterium Listeria "
    "monocytogenes nachgewiesen, und vom Verzehr wird dringend abgeraten.":
        "Wholesaler Asia Express Food is recalling Green Box Limited brand "
        "enoki mushrooms. Listeria monocytogenes was detected during an "
        "inspection and consumption is strongly advised against",
    "Die Akar GmbH ruft den Brotaufstrich Gourmet Celebi Pistaziencreme im "
    "200g Glas zurück. Der Rückruf erfolgt aufgrund bestehender "
    "Gesundheitsgefahr durch Salmonellen, weshalb vom Verzehr dringend "
    "abgeraten wird.":
        "Akar GmbH is recalling Gourmet Celebi pistachio cream spread in 200 g "
        "jars because of a health risk from Salmonella; consumption is "
        "strongly advised against",
    "Die Orienta Foods GmbH ruft Rotes Chilipulver der Marke Rino (250g und "
    "500g Packungen, MHD E2028/1/1, Charge P2026/1/2) zurück. Bei einer "
    "Lebensmittelkontrolle wurden Aflatoxin-Werte über dem Grenzwert "
    "nachgewiesen.":
        "Orienta Foods GmbH is recalling Rino brand red chilli powder (250 g "
        "and 500 g packs, best before E2028/1/1, batch P2026/1/2). A food "
        "inspection found aflatoxin levels above the legal limit",
    "Die REWE Dortmund ruft vier verschiedene Schinken-Zwiebelmettwurst "
    "Artikel zurück. In einer einzelnen Probe wurden Shigatoxin-bildende "
    "Escherichia coli (STEC) nachgewiesen, weshalb vom Verzehr dringend "
    "abgeraten wird.":
        "REWE Dortmund is recalling four ham-and-onion Mettwurst products. "
        "Shiga toxin-producing Escherichia coli (STEC) was detected in a "
        "single sample; consumption is strongly advised against",

    # ── BLV (CH) ──────────────────────────────────────────────────────────
    "In tiefgekühlten Beeren wurden Noroviren nachgewiesen. Eine "
    "Gesundheitsgefährdung kann nicht ausgeschlossen werden.":
        "Norovirus was detected in frozen berries. A health risk cannot be "
        "ruled out",

    # ── AESAN (ES) ────────────────────────────────────────────────────────
    "Presencia de Salmonella": "Presence of Salmonella",
    "Presencia de Salmonella spp.": "Presence of Salmonella spp.",
    "Ampliación de la alerta alimentaria 33/2026 relativa a Salmonella en "
    "helado. AESAN.":
        "Extension of AESAN food alert 33/2026 concerning Salmonella in "
        "ice cream",

    # ── Ministero della Salute (IT) ───────────────────────────────────────
    "Presenza di Salmonella Typhimurium": "Presence of Salmonella Typhimurium",

    # ── FAVV (BE) ─────────────────────────────────────────────────────────
    "Aflatoxine au-dessus des limites légales dans des noix du Brésil (FAVV "
    "recall 139).":
        "Aflatoxin above legal limits in Brazil nuts (FAVV recall 139)",

    # ── Audit 2026-08-04 ──────────────────────────────────────────────────
    # Reached Recalls untranslated on 2026-08-04. Detection missed it because
    # the only marker it carried was "la" — one hit, below the two-hit
    # threshold — and "contamination" reads as English. "à" is now a marker.
    # Note the source's own spelling: "salmonellle", three l's.
    "Contamination à la salmonellle": "Contamination with Salmonella",
    "Contamination à la salmonelle": "Contamination with Salmonella",
    # Widening the detector to catch the row above surfaced five more that
    # had been invisible for the same reason — one marker each, under the
    # threshold. "inférieur à 10 ufc/g" is the French rendering of a result
    # below the 100 CFU/g regulatory limit for Listeria in ready-to-eat food,
    # and it is kept because it is the substance of the recall notice.
    "Contamination potentielle à la Listeria monocytogenes":
        "Potential contamination with Listeria monocytogenes",
    "Presence of listéria monocytogènes inférieur à 10/g":
        "Listeria monocytogenes present at below 10 CFU/g",
    "Detection of listéria monocytogenes inférieure à 10 ufc/g":
        "Listeria monocytogenes detected at below 10 CFU/g",
    "Presence of Listeria (inférieure à 10)":
        "Listeria present at below 10 CFU/g",
    "Présence d'ochratoxine A": "Presence of ochratoxin A",

    # ── Audit 2026-08-06 (pre-Friday) ─────────────────────────────────────
    # Both reached Recalls on 2026-08-05 and both fall inside the W32 window,
    # so they would have rendered in Friday's weekly report. Same half-
    # translated shape as the rest of the RappelConso set: the enricher did
    # the opening clause and stopped. "dans 10g" is the French analytical
    # convention — Listeria/Salmonella results are reported per 25 g or, as
    # here, per 10 g of sample.
    "Presence of Salmonella spp. dans 10g":
        "Presence of Salmonella spp. in a 10 g sample",
    "Detection of Listeria monocytogenes dans les produits":
        "Detection of Listeria monocytogenes in the products",
    "E. coli STEC dans Crottin de Chavignol (AFSCA/FAVV rappel 133).":
        "E. coli STEC in Crottin de Chavignol (AFSCA/FAVV recall 133)",

    # ── RappelConso (FR), remainder ───────────────────────────────────────
    "Taux d'alcaloïdes d'ergot supérieur au seuil réglementaire.":
        "Ergot alkaloid level above the regulatory limit",

    # ── added 2026-08-28 ────────────────────────────────────────────────
    # Four strings the writer split leaves non-English. Three are simply
    # French/Italian; the first is HALF translated — an English head with the
    # French risk parenthetical still attached, which is why a whole-string
    # language test catches it but a token check on the organism name does
    # not. "agent responsable de la salmonellose" is the standard DGCCRF
    # risk phrasing and adds nothing the Pathogen column does not already
    # carry, so it is dropped rather than translated.
    "Presence of Salmonella spp. (agent responsable de la salmonellose)":
        "Presence of Salmonella spp.",
    "Présence possible de Listeria monocytogenes":
        "Possible presence of Listeria monocytogenes",
    "Présence possible de Listeria":
        "Possible presence of Listeria",
    "Possibile presenza di Listeria monocytogenes":
        "Possible presence of Listeria monocytogenes",
    # Fiche 23052 (2026-08-07). A precautionary recall: the trigger was a
    # non-conforming analysis on an INGREDIENT, not on the finished product,
    # so "concerning an ingredient used in making these products" is kept
    # explicit. The date range is the packing window, not a best-before.
    "Rappel préventif suite à une résultat d'analyse non conforme concernant un ingrédient utilisé dans la fabrication de ces produits.; l'ingrédient a été utilisé dans les préparations emballées entre le 15/07/2026 et le 22/07/2026":
        "Precautionary recall following a non-conforming analysis result on an ingredient used in making these products; the ingredient was used in preparations packed between 15/07/2026 and 22/07/2026",

    # ── HALF-TRANSLATED PATHOGEN NAMES (audit 2026-08-09) ─────────────────
    #
    # 26 published RappelConso Reasons read "Presence of salmonelle" — the
    # verb translated, the ORGANISM left in French. detect_language() cannot
    # see these: it needs two function-word hits and "Presence of salmonelle"
    # has none, so looks_non_english() returns False and the row passes the
    # writer's English-output guard untouched.
    #
    # Same failure shape as the bilingual Product split, one word smaller,
    # and fixed the same way: a verified table, not a rule. There are only
    # SEVEN distinct strings behind those 26 rows, so a transliteration
    # engine would be more machinery than the problem deserves — and it
    # would start guessing at the eighth.
    #
    # NOT added: a generic "salmonelle -> Salmonella" substring rewrite.
    # "Salmonela" appears legitimately inside the Spanish and Romanian halves
    # of RASFF bilingual subjects, where the English half is already there
    # after the "//" and the SPLITTER is the right fix. A substring rewrite
    # would tidy up half of a sentence that should be discarded whole.
    "Presence of salmonelle": "Presence of Salmonella",
    "Detection of salmonelle": "Detection of Salmonella",
    "Salmonelle": "Salmonella",
    "Presence salmonelle": "Presence of Salmonella",
    "Presence of salmonelle enteritidis":
        "Presence of Salmonella Enteritidis",
    "Presence salmonelle s. typhimurium":
        "Presence of Salmonella Typhimurium",
    "Salmonelle entéritidis": "Salmonella Enteritidis",

    # ── AUDIT 2026-08-30 ──────────────────────────────────────────────────
    # The 2026-08-09 table caught "Presence of salmonelle" but not the
    # bare-verb form the same RappelConso field also emits. "Détection …"
    # keeps BOTH the French verb and the French organism, so it was never a
    # near-miss of any existing key and no prefix rule would reach it. Each
    # string below is transcribed from the fiche it was published from.
    "Détection salmonelle": "Detection of Salmonella",
    "Detection salmonelle": "Detection of Salmonella",
    "Détection Listeria monocytogenes": "Detection of Listeria monocytogenes",
    "Detection Listeria monocytogenes": "Detection of Listeria monocytogenes",

    # "ufc/g" is unités formant colonie — the French rendering of CFU/g. The
    # 2026-08-09 entry translated the one long form then in the workbook; two
    # shorter forms have appeared since and neither is a prefix of it.
    "Detection of Listeria monocytogenes (<10ufc/g)":
        "Detection of Listeria monocytogenes (<10 CFU/g)",
    "Detection of Listeria monocytogenes <10 ufc/g":
        "Detection of Listeria monocytogenes at <10 CFU/g",

    # Same half-translation shape, caught by the test above once the 26
    # salmonelle rows stopped masking it: the count was translated, the
    # phrase after it was not. "ufc/g" is the French rendering of CFU/g.
    "Presence of Listeria monocytogenes <10 ufc/g dans une portion de ce lot":
        "Presence of Listeria monocytogenes at <10 CFU/g in one portion of "
        "this lot",

    # The one RASFF bilingual subject the splitter cannot resolve: BOTH
    # halves are non-English (Romanian // Romanian-inflected English), so
    # split_bilingual() correctly refuses to pick a winner rather than
    # guessing. Translated here instead, from the notification's own text.
    "Salmonella spp in care pasare, origine Brazilia // Salmonela spp in chicken meat from Brasil; risk: serious; category: poultry meat and poultry meat products":
        "Salmonella spp. in poultry meat from Brazil; risk: serious; category: poultry meat and poultry meat products",
}

REASON_EN: Dict[str, str] = {_norm_key(k): v for k, v in _RAW_TRANSLATIONS.items()}

# Shared-prefix length required before a truncated published string is matched
# to a table entry. 60 normalised characters is well past the point where two
# different regulator motifs still agree.
_PREFIX_MIN = 60
# …and the ceiling on how much of it is compared, so a motif that had a note
# appended after publication still matches its own translation.
_PREFIX_MAX = 120


def to_english(text) -> Optional[str]:
    """Return an English rendering of `text`, or None if we do not know one.

    Order: exact verified translation, then a bilingual split. Never a guess.
    """
    s = str(text or "").strip()
    if not s:
        return None
    key = _norm_key(s)
    hit = REASON_EN.get(key)
    if hit:
        return hit
    # Published values are truncated at the column width, so the stored string
    # is often a PREFIX of the source string this table was built from (and
    # occasionally the other way round, when a later pass appended a note).
    # Match on the shared prefix, requiring enough of it that two different
    # motifs cannot collide — the shortest distinct RappelConso motif in the
    # corpus is well under this, and every entry here is far longer.
    if len(key) >= _PREFIX_MIN:
        for known, english in REASON_EN.items():
            if len(known) >= _PREFIX_MIN and (known.startswith(key[:_PREFIX_MIN])
                                              or key.startswith(known[:_PREFIX_MIN])):
                # Compare a bounded shared prefix, not the whole overlap: a
                # published value is sometimes the source motif PLUS a note
                # appended by a later pass, so the tails legitimately differ.
                n = min(len(known), len(key), _PREFIX_MAX)
                if known[:n] == key[:n]:
                    return english
    split = split_bilingual(s)
    if split:
        return split
    return None


def englishify_reason(text) -> Tuple[str, bool]:
    """(possibly translated text, changed?) — safe to call on every row.

    Leaves English untouched, leaves unknown non-English untouched (the caller
    reports it), and never invents.
    """
    s = str(text or "")
    if not s.strip():
        return s, False

    # THE TABLE IS CONSULTED FIRST (audit 2026-08-09).
    #
    # This used to read `if not looks_non_english(s): return s, False` — the
    # detector gated the table. That is backwards. looks_non_english() is a
    # STATISTICAL test: it needs two function-word hits before it will call a
    # string foreign, deliberately, so that "brie a l'ail" and a Greek lab
    # name are never mistaken for prose to translate.
    #
    # "Presence of salmonelle" has no French function words at all — the verb
    # was translated and only the ORGANISM was left behind. The detector
    # scored it English, so the table was never consulted, and 26 published
    # rows kept a French pathogen name that the table has an exact, verified
    # entry for.
    #
    # A verified exact-match table entry is stronger evidence than any
    # detector: someone wrote that mapping down deliberately. So it wins.
    # Unknown strings still fall through to the detector, so nothing is
    # guessed and the caller still gets its report.
    out = to_english(s)
    if out and out != s:
        return americanize(out), True
    if not looks_non_english(s):
        a = americanize(s)
        return a, a != s
    out = to_english(s)
    if out and out != s:
        return americanize(out), True
    a = americanize(s)
    return a, a != s


# ──────────────────────────────────────────────────────────────────────
# US spelling — the house language (added 2026-08-14)
# ──────────────────────────────────────────────────────────────────────
# Operator instruction: "mould must be mold to us US english".
#
# WHY THIS IS A WRITE-TIME RULE AND NOT A ONE-OFF DATA EDIT
# ---------------------------------------------------------
# Three FSANZ rows reached Recalls with Pathogen "Mould". They were
# removed on 2026-08-14 as out of scope, but nothing stopped the next
# FSANZ mould recall arriving spelled the same way — Australian and UK
# regulators write British English and always will. Fixing the rows
# without fixing the writer is fixing the symptom.
#
# It also matters for SCOPE, not just style: _publish_gate lists
# "mould" and "mold" as out-of-scope hazard terms, but the pair
# Pathogen "Mould" + Reason "Microbial (Mould) contamination." did not
# resolve to the quality/spoilage class while the US spelling does. So
# the British spelling was letting out-of-scope rows through a gate
# written in US English. Normalising at write time closes that.
#
# TWO GUARDS, BOTH LEARNED THE HARD WAY
# --------------------------------------
# 1. PROPER NOUNS. "Programme" is NOT converted. The register cites
#    "2026 Official Microbiological Criteria for Food Safety Monitoring
#    Programme" (EFET) and "CFS Food Surveillance Programme" (Hong Kong
#    CFS). Those are the official names of named regulatory programmes;
#    renaming them is the same error as writing "US Ministry of
#    Defense". A blanket s/programme/program/ hit all three.
#
# 2. FALSE FRIENDS. "moulded" / "demoulded" are NOT converted here as
#    fungus words — they mean SHAPED IN A MOULD. The register carries
#    "Pork-head brawn (parsleyed) — both moulded and demoulded variants",
#    a charcuterie term rendered from the French "moulé / démoulé". A
#    naive s/mould/mold/ turns a description of how a terrine was formed
#    into an implied fungal contamination on a Product field that the
#    English-output rule exempts anyway. Only the standalone noun and
#    adjective forms are mapped.
import re as _re_us

_US_SPELLINGS = (
    # (compiled pattern, replacement) — word-bounded, case-preserving for
    # a leading capital only, which is all these fields ever use.
    (_re_us.compile(r"\bmould\b", _re_us.I), "mold"),
    (_re_us.compile(r"\bmoulds\b", _re_us.I), "molds"),
    (_re_us.compile(r"\bmouldy\b", _re_us.I), "moldy"),
    (_re_us.compile(r"\bpasteurisation\b", _re_us.I), "pasteurization"),
    (_re_us.compile(r"\bpasteurised\b", _re_us.I), "pasteurized"),
    (_re_us.compile(r"\bsterilised\b", _re_us.I), "sterilized"),
    (_re_us.compile(r"\banalysed\b", _re_us.I), "analyzed"),
    (_re_us.compile(r"\bcolour\b", _re_us.I), "color"),
    (_re_us.compile(r"\bcoloured\b", _re_us.I), "colored"),
    (_re_us.compile(r"\bodour\b", _re_us.I), "odor"),
    (_re_us.compile(r"\bodours\b", _re_us.I), "odors"),
    (_re_us.compile(r"\blabelling\b", _re_us.I), "labeling"),
    (_re_us.compile(r"\bmislabelling\b", _re_us.I), "mislabeling"),
    (_re_us.compile(r"\bfibre\b", _re_us.I), "fiber"),
    (_re_us.compile(r"\bfaecal\b", _re_us.I), "fecal"),
    (_re_us.compile(r"\bdiarrhoea\b", _re_us.I), "diarrhea"),
    (_re_us.compile(r"\boesophag", _re_us.I), "esophag"),
    (_re_us.compile(r"\bhaemolytic\b", _re_us.I), "hemolytic"),
    (_re_us.compile(r"\bhaemorrhagic\b", _re_us.I), "hemorrhagic"),
    # PREFIXED FORMS. \b...\b does not match inside a word, so "pasteurised"
    # leaves "unpasteurised" untouched — and "unpasteurised milk" is the single
    # most common British spelling in raw-milk cheese recalls (FR/UK/IE), the
    # exact rows this register carries most of. "non-pasteurised" and
    # "re-analysed" already worked because the hyphen creates a boundary; the
    # un-/mis-/dis- forms did not. Zero occurrences in the register today, so
    # this is closing the hole before it is used, not repairing damage.
    (_re_us.compile(r"\bunpasteurised\b", _re_us.I), "unpasteurized"),
    (_re_us.compile(r"\bunlabelled\b", _re_us.I), "unlabeled"),
    (_re_us.compile(r"\bmislabelled\b", _re_us.I), "mislabeled"),
    (_re_us.compile(r"\blabelled\b", _re_us.I), "labeled"),
    (_re_us.compile(r"\bdiscoloured\b", _re_us.I), "discolored"),
    (_re_us.compile(r"\bdiscolouration\b", _re_us.I), "discoloration"),
    (_re_us.compile(r"\bunanalysed\b", _re_us.I), "unanalyzed"),
    # NOT here on purpose: programme (proper nouns), moulded/demoulded
    # (shaped, not fungal), litre/flavour/yoghurt (Product and Brand are
    # exempt from the English-output rule and must match the pack).
)


def _match_case(original: str, replacement: str) -> str:
    if original[:1].isupper():
        return replacement[:1].upper() + replacement[1:]
    return replacement


def americanize(text) -> str:
    """British -> US spelling for this register's own analytical prose.

    Call on Reason / Pathogen / Class. Do NOT call on Product or Brand:
    the English-output rule exempts them and the text must match what is
    printed on the pack.
    """
    s = str(text or "")
    if not s:
        return s
    for pat, repl in _US_SPELLINGS:
        s = pat.sub(lambda m, _r=repl: _match_case(m.group(0), _r), s)
    return s
