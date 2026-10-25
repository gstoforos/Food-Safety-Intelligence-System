"""Is a Product value a food name, or only the carton's identifiers?

WHY THIS MODULE EXISTS (2026-10-22)
───────────────────────────────────
pipeline/_publish_gate.py has asked "does Product contain a word?" since
2026-10-20 (_product_is_an_identifier_blockers_2026_10_20). That test
refuses 'mr10ae, mr41ck' and '484565'. It does NOT refuse this, published
2026-10-22 from RappelConso fiche 23905:

    Product  'sachet 1kg: c120261010a106 - sachet 1kg: c120261010a107 -
              sachet 2kg: c120261010b306 - sachet 2kg: c120261010b307'
    heading on the fiche itself: "SUCRE ROUX PURE CANNE"

because "sachet" and "kg" ARE words, so the word test is satisfied by the
packaging vocabulary while the food is still missing. A subscriber reading
that row cannot tell that brown cane sugar was recalled.

The distinction this module draws: a lot line is built from packaging nouns,
units, label keywords and codes, and NOTHING else. Remove that closed
vocabulary and a real product name always leaves something behind
("sucre roux pure canne", "filet de sardine", "bœuf haché"), while a lot
line leaves nothing.

DELIBERATELY CONSERVATIVE. The vocabulary is a fixed list of packaging and
unit words in the languages the register's scrapers actually read, never a
general stop-word list: a food whose whole name is a packaging word would be
mis-flagged, and none exists. False negatives are cheap here (the row keeps
the value it has); a false positive silently replaces a good product name,
so when in doubt this returns False.

MEASURED before use, over the whole live workbook (2,133 Recalls rows,
23 Pending, both review sheets): see
tests/test_product_is_not_only_identifiers.py, which pins the count.
"""
from __future__ import annotations

import re
from typing import Final

__all__ = ["is_identifier_only"]

#: A word, Unicode-aware: at least two letters in a row, any script. One
#: letter is not a word ("x6 330g"), and digits never count.
_A_REAL_WORD: Final = re.compile(r"[^\W\d_]{2,}", re.UNICODE)

#: Packaging nouns, units, and label keywords. These are the words a lot /
#: model / reference line is made of. Lower-cased, compared whole.
#:
#: Kept SHORT on purpose — every entry here is a word this module becomes
#: blind to, so it holds only terms that cannot be a food on their own.
#:
#: THE LABEL PHRASE SPELLED OUT (2026-10-24). Only "date" was listed, so a
#: lot block that writes the date label in full kept a content word and the
#: whole value read as a name. Measured on the 2026-10-23 register: two
#: Tier-1 RappelConso rows were PUBLISHED with a GTIN/DLC block as Product —
#:   fiche 23922  '0207684000000; date limite de consommation; 2026-10-24'
#:   fiche 23914  '3563243012829; 22/10/2026; date limite de consommation;
#:                 2026-10-22; |3563243012621; |3563243014625'
#: because "limite" and "consommation" survived the sweep, exactly as
#: "Sachet" survived the publish gate's word test in the note at the top of
#: this file. The fiche headings say "Blanquette St Jacques" and "Noix de
#: Saint Jacques de la Baie de Saint Brieuc Label Rouge".
#: Added: the regulators' own date-label vocabulary and nothing wider. None
#: of those words can be a food on its own, and the longer forms ("date
#: limite de consommation", "à consommer jusqu'au", "mindestens haltbar
#: bis", "da consumarsi entro", "consumir antes de") are built from them
#: plus words already listed. The German, Dutch, Italian and Iberian label
#: words go in with the French ones because the shape is a SHAPE, not a
#: French one: the same lot block defeats the same sweep in any language
#: whose regulator spells the date label out, and no measured row should
#: have to be published in each of them first. Deliberately NOT added:
#: anything that can stand for a food, and the generic prepositions of
#: those languages beyond the ones the phrase needs. The other direction is
#: measured in tests/test_product_is_not_only_identifiers.A_REAL_NAME,
#: which carries real German, Italian, Spanish and Dutch product names.
#: NOTE: this block is a frozenset built from
#: .split(), so a '#' comment INSIDE the string would be split into tokens
#: and silently add "the", "label", "only"… — keep the prose up here.
_PACKAGING_WORDS: Final = frozenset("""
sachet sachets sac sacs paquet paquets boite boites boîte boîtes barquette
barquettes bocal bocaux bouteille bouteilles carton cartons caisse caisses
pot pots etui etuis étui étuis conserve conserves plaquette plaquettes
filet filets_sac emballage emballages colis lot lots lotto lotti
packung packungen beutel schale becher dose dosen glas karton flasche
zak zakje doos bak pak verpakking
bag bags box boxes pack packs packet packets case cases tray trays jar jars
bottle bottles tub tubs tin tins pouch pouches carton_box sleeve wrapper
unit units piece pieces portion portions
ref ref. reference references référence références art art. articolo
code codes codigo código barcode gtin ean upc sku plu
batch batches charge chargen partia partie partii partita lote lotes
bbd bbe exp expiry expiration
dlc dluo ddm peremption péremption
best before use by sell by
date dates dated jour jours
limite limites consommation consommer conditionne conditionné
durabilite durabilité minimale jusqu à au aux
mindestens haltbar bis mhd verbrauchen
houdbaar houdbaarheid gebruiken minste thm tgt te tot
consumarsi scadenza preferibilmente entro da
caducidad consumir consumo preferente validade antes
no num nr no. num. nro
kg kgs g gr gram grams grammes gramm ml cl l lt litre litres liter liters
oz lb lbs pcs pce stk st szt
x de du des la le les et ou and or of the a an to
""".split())

#: A token that is itself a code: contains a digit, or is a short all-letter
#: block inside something digit-bearing (the "C120261010A106" shape).
_CODE_TOKEN: Final = re.compile(r"^[A-Za-z]{0,4}\d[A-Za-z0-9/.\-]*$")


def _tokens(text: str):
    # Split on everything that is not a letter, a digit or an internal
    # hyphen/slash/dot — those three hold codes together.
    for raw in re.split(r"[^\w/.\-]+", text or ""):
        tok = raw.strip("-/.,;:").lower()
        if tok:
            yield tok


def is_identifier_only(text: str) -> bool:
    """True when `text` names no food — only packaging, units and codes.

    False for an empty value: "nothing here" is a different defect, already
    handled by every caller's own empty check, and returning True would send
    a caller looking for a replacement it does not need.
    """
    s = str(text or "").strip()
    if not s:
        return False
    for tok in _tokens(s):
        if tok in _PACKAGING_WORDS:
            continue
        if _CODE_TOKEN.match(tok):
            continue
        if not _A_REAL_WORD.search(tok):
            continue            # punctuation, single letters, bare digits
        return False            # a content word survived: this is a name
    return True
