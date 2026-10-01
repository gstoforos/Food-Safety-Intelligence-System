"""One spelling per firm and brand across the register.

    "Lidl as the source in France is LIDL and netherlands Lidl.. do you think
     we can make it uniform across the recalls especially for the same source"
                                                        — operator, 2026-10-01

Regulators type firm names however their form allows: RappelConso alone
published "Carrefour le Marché", "Carrefour le Marche" and "CARREFOUR LE
MARCHE" (49 rows), "CARREFOUR FRANCE" / "Carrefour France" (38), "E.LECLERC" /
"E.Leclerc" / "E. Leclerc", and "LIDL" next to the NVWA's "Lidl". Measured on
2026-10-01: 51 names written two or three ways across 1870 rows. The
dashboard's search and the reports' firm counts treat each spelling as a
different firm.

THE RULE
--------
Two values are the SAME NAME when they match after folding case, accents and
punctuation ("E.LECLERC" = "E. Leclerc" = "E.Leclerc"). Every member of such a
group is written in one form, chosen from the forms already in the register,
never invented:

    1. the fewest words in CAPITALS (a shouted name is the form-filler's,
       not the firm's; a short acronym such as GAEC or SAS does not count);
    2. keeps its accents ("Marché" over "Marche");
    3. the form used most often;
    4. alphabetical, so the choice is stable from run to run.

Words that differ are never merged: "Carrefour" and "Carrefour France" stay
two names, "SASU Paturages Comtois" and "Paturages Comtois" stay two names.
That is a different firm or a different legal entity, and the register
records what the regulator named.

"No brand" has one spelling: the house value "Unbranded" (the reviewer prompt
already says so). "Sans marque", "Neutre", "Fabriqué sur place", "Non
communiqué" and a bare "/" all meant that.

RASFF rows are exempt: their Company is the fixed "Origin: X | Notifying: Y"
string and their Brand the notifying country (do not touch the RASFF format).

Applied at the writer choke point, merge_master._write_sheet, on the Recalls
sheet, so every future write converges. Held by tests/test_firm_names_are_uniform.py.
"""
from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from typing import Dict, Iterable, List

FIELDS = ("Company", "Brand")
EXEMPT_SOURCES = ("RASFF (EU)",)

UNBRANDED = "Unbranded"
#: Values that mean "no brand", compared after folding.
_NO_BRAND = {"sans marque", "neutre", "fabrique sur place", "non communique",
             "unbranded", "sans marque sans marque"}
_NO_BRAND_LITERAL = {"/"}


def fold(text) -> str:
    """Case-, accent- and punctuation-blind key."""
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^0-9a-z]+", " ", s)
    return " ".join(s.split())


def _is_all_caps(s: str) -> bool:
    letters = [c for c in s if c.isalpha()]
    return len(letters) >= 2 and all(c.isupper() for c in letters)


def _has_accents(s: str) -> bool:
    return any(unicodedata.combining(c)
               for c in unicodedata.normalize("NFKD", s))


def _shouted_words(s: str) -> int:
    """Words of four or more letters written in capitals. Counted per word,
    not for the whole string: "AKAR GmbH" is not all-caps as a string but
    still shouts its name; "GAEC de la Cascade" keeps a real acronym."""
    return sum(1 for w in re.findall(r"[^\W\d_]+", s)
               if len(w) >= 4 and w.isupper())


def choose(forms: Counter) -> str:
    """The display form for one group of same-name spellings."""
    return sorted(forms, key=lambda f: (_shouted_words(f), not _has_accents(f),
                                        -forms[f], f))[0]


def _exempt(row) -> bool:
    return str(row.get("Source") or "") in EXEMPT_SOURCES


def canonical_map(rows: Iterable[Dict], fields=FIELDS) -> Dict[str, str]:
    """{spelling: canonical spelling} for every spelling that changes."""
    groups: Dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        if _exempt(r):
            continue
        for f in fields:
            v = str(r.get(f) or "").strip()
            if v and fold(v):
                groups[fold(v)][v] += 1
    out = {}
    for forms in groups.values():
        if len(forms) < 2:
            continue
        best = choose(forms)
        for f in forms:
            if f != best:
                out[f] = best
    return out


#: A Company that is not a firm. English, like every published field.
_NO_COMPANY = {"sans marque": UNBRANDED, "neutre": UNBRANDED,
               "non communique": "Not disclosed"}


def unify_firm_names(rows: List[Dict], fields=FIELDS) -> int:
    """Rewrite Company/Brand in place. Returns the number of cells changed."""
    changed = 0
    for r in rows:
        if _exempt(r):
            continue
        b = str(r.get("Brand") or "").strip()
        if "Brand" in fields and b and b != UNBRANDED and (
                b in _NO_BRAND_LITERAL or fold(b) in _NO_BRAND):
            r["Brand"] = UNBRANDED
            changed += 1
        c = str(r.get("Company") or "").strip()
        if "Company" in fields and fold(c) in _NO_COMPANY \
                and c != _NO_COMPANY[fold(c)]:
            r["Company"] = _NO_COMPANY[fold(c)]
            changed += 1
    m = canonical_map(rows, fields)
    if not m:
        return changed
    for r in rows:
        if _exempt(r):
            continue
        for f in fields:
            v = str(r.get(f) or "").strip()
            if v in m:
                r[f] = m[v]
                changed += 1
    return changed
