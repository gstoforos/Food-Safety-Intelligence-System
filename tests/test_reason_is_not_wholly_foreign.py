# -*- coding: utf-8 -*-
"""A published Reason must not be wholly in a foreign language.

WHY THIS EXISTS AND WHY IT DOES NOT JUST CALL detect_language
--------------------------------------------------------------
pipeline/_language.detect_language requires TWO independent function-word
hits before it will call a string non-English. That threshold is deliberate
and documented there: it is what stops an English sentence quoting a French
cheese name ("Fromagerie P. Jacquin & Fils brand Valençay AOP …") from being
rewritten. Loosening it is not the fix.

The cost of the threshold is that SHORT values slip through. On 2026-10-03
five rows were published with a Reason entirely in French:

    Détéction de listéria                          (fiche 23648)
    Présence d'alcaloïdes tropaniques (datura)     (fiche 23624)
    Défaut d'herméticité                           (fiche 23584)
    Conformité bactériologique : insatisfaisante   (fiche 22865)
    Entérotoxine détectée                          (fiche 22809)

Each scores one hit, one short of the threshold, so every language test on
main was green while the register showed French to readers of an English
register. A sixth row (GIS, fiche-less, row 43) carried a wholly POLISH
notice headline for the same reason: the pl marker list holds "partia" and
the notice says "partii", and Polish inflects.

So this test asks a different question, by a different method — diacritic
density against English function-word count — and it is that independence
that makes it worth having. A value fails only when BOTH hold:

  * it carries at least two letters that English essentially does not use,
    and
  * fewer than two common English function words appear in it.

WHAT IS DELIBERATELY NOT ASKED
-------------------------------
ONLY Reason. Not Product — and that exclusion is the whole reason this test
is narrow rather than a general language sweep. Product KEEPS foreign names
by standing policy: brand names, and protected names such as "Pont-l'Évêque
AOP", "Bouchée à la reine", "pâté en croûte". Running this check over
Product flags 35 rows that are all correct, so asking it of Product would
mean either 35 wrong edits or an allowlist longer than the test.

RASFF is excluded on every field. Its Reason is the notification subject as
the Commission published it, frequently bilingual with a "//" separator, and
that format is fixed by standing instruction. Twelve Polish RASFF rows carry
undetected Polish text for exactly that reason, and they are correct.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

try:
    import openpyxl
except ImportError:                                         # pragma: no cover
    openpyxl = None

XLSX = Path(__file__).resolve().parents[1] / "docs" / "data" / "recalls.xlsx"

#: Recalls ONLY — the published register.
#:
#: Weekly_Review is excluded, and this was checked rather than assumed. It
#: holds a SNAPSHOT of each row as it was at the moment it was promoted, so
#: eight of its rows carry the regulator's French while the Recalls copy on
#: the same URL has been translated since — verified row by row on
#: 2026-10-03:
#:
#:   WV "Présence listéria monocytogènes"
#:        → Recalls "Presence of Listeria monocytogenes."
#:   WV "Suspicion de présence de corps étrangers métalliques"
#:        → Recalls "Suspected presence of metallic foreign bodies."
#:   (and six more, all with an English Recalls copy)
#:
#: Rewriting a promotion record to match a later translation would falsify
#: what the row said when it was promoted. The two archive sheets are
#: excluded for the same reason: a rejected row's Reason holds the refusal's
#: own text, which is what an audit trail is for.
SHEETS = ("Recalls",)

#: Source whose Reason is a Commission notification subject, kept verbatim.
VERBATIM_SOURCES = ("RASFF (EU)",)

#: Letters that English essentially does not use. Polish/Czech/Hungarian/
#: Romanian/Nordic diacritics plus the French and Iberian accented vowels.
_FOREIGN_LETTER = re.compile(
    r"[ąćęłńśźżĄĆĘŁŃŚŹŻ"            # Polish
    r"őűŐŰ"                          # Hungarian
    r"ěřůžščťďňĚŘŮŽŠČ"              # Czech / Slovak
    r"șțȘȚăĂ"                        # Romanian
    r"àâçèéêëîïôöùûüÀÂÇÈÉÊËÎÏÔÖÙÛÜ"  # French / Iberian / German
    r"ñÑåäöøæÅÄÖØÆ]"                 # Spanish / Nordic
)

#: Function and hazard words that appear in essentially every English Reason
#: this register writes. Two of them is enough to call a string English even
#: when it quotes an accented name.
_ENGLISH = frozenset("""
a an the of in on for at to and or with without from by as is are was were be
been has have had not no due possible presence detected found above below
after before during product products batch batches lot lots recall recalled
recalling because risk level limit sold may contain contains contamination
exceeding this that its all other than over under per into out off own check
testing analyses laboratory suspected potential exceeds maximum permitted
""".split())

_WORD = re.compile(r"[a-z']+")


def _is_wholly_foreign(value: str) -> bool:
    if len(_FOREIGN_LETTER.findall(value)) < 2:
        return False
    return len(set(_WORD.findall(value.lower())) & _ENGLISH) < 2


def _rows(sheet: str):
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        return []
    values = list(wb[sheet].values)
    if not values:
        return []
    hdr = [str(h) for h in values[0]]
    return [dict(zip(hdr, r)) for r in values[1:] if r]


@pytest.mark.parametrize("sheet", SHEETS)
def test_no_published_reason_is_wholly_foreign(sheet):
    if openpyxl is None or not XLSX.exists():            # pragma: no cover
        pytest.skip("no workbook")
    offenders = []
    for row in _rows(sheet):
        if str(row.get("Source") or "") in VERBATIM_SOURCES:
            continue
        reason = str(row.get("Reason") or "")
        if _is_wholly_foreign(reason):
            offenders.append((str(row.get("Date"))[:10],
                              str(row.get("Source")),
                              reason[:70]))
    assert not offenders, (
        f"{len(offenders)} Reason value(s) in {sheet} are wholly in a "
        f"foreign language:\n  " + "\n  ".join(map(str, offenders[:8])) +
        "\n\nTranslate faithfully and keep the regulator's own wording in "
        "Notes as [original Reason (<lang>): \"…\"]. Do NOT loosen "
        "detect_language to catch these — its two-hit threshold is what "
        "protects English sentences that quote foreign names, and the "
        "undetected values on RASFF rows are correct as published."
    )


def test_the_check_still_catches_what_it_was_written_for():
    """Pinned so the guard cannot be quietly defanged.

    These five strings were published on main on 2026-10-03 and every
    existing language test passed them.
    """
    for bad in ("Détéction de listéria",
                "Présence d'alcaloïdes tropaniques (datura)",
                "Défaut d’herméticité",
                "Conformité bactériologique : insatisfaisante",
                "Entérotoxine détectée",
                "Ostrzeżenie publiczne dotyczące żywności: wykrycie "
                "obecności bakterii Listeria monocytogenes w jednej partii "
                "ćwiartki wędzonej z kurczaka"):
        assert _is_wholly_foreign(bad), f"no longer caught: {bad!r}"


def test_an_english_sentence_quoting_a_foreign_name_passes():
    """The false positives this test must never produce.

    Every string below is a real published Reason or the shape of one. If
    any of them starts failing, the guard has become the thing
    detect_language's two-hit threshold exists to prevent.
    """
    for good in (
        "Presence of Listeria detected following laboratory analyses "
        "(Aqualeha)",
        "Fromagerie P. Jacquin & Fils brand Valençay AOP \"fromage de "
        "chèvre au lait cru\" recalled due to generic E. coli",
        "Recall of batch 11070 of 9-month bone-in dry-cured ham from "
        "Salaisons Limousines because of a risk of Listeria monocytogenes "
        "contamination",
        "Listeria detected at the product's use-by date, on a reference "
        "tray during own-check testing",
        "Exceeds the maximum permitted level of pyrrolizidine alkaloids",
        "Suspected presence of Salmonella",
    ):
        assert not _is_wholly_foreign(good), f"false positive on {good!r}"
