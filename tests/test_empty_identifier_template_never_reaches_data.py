"""The extractor's empty-identifier placeholder is not data. 2026-10-05.

pipeline/extractor.py rule 10 hands the model a FORMAT:

    '<hazard description> (Recall ID <the number printed in the article>)'

When the article prints no reference number the model does not drop the
slot — it fills it with a placeholder, and the register publishes the
placeholder to subscribers:

    "The product contains pyrrolizidine alkaloids (toxic plant substances)
     above the limit. (Recall ID: N/A)"

WHY THE PUBLISH GATE DOES NOT CATCH IT. Rule 2 fires only when the ENTIRE
Reason is a bare identifier ("Recall ID 842632", the prompt's own worked
example). Here the hazard IS described and the template is trailing noise,
so the row passes every rule and publishes.

WHY THIS TEST EXISTS RATHER THAN ANOTHER ONE-OFF REPAIR. On 2026-10-04 three
rows were cleaned by hand in tools/repair_2026_10_04.py and nothing was
added to any writer. The defect recurred the next morning — a Mattilsynet
row scraped 2026-10-05T05:14Z carried it again — and in a spelling
yesterday's one-off regex could not even match, because it had a COLON:
"(Recall ID: N/A)" vs "(Recall ID N/A)". A data fix with no writer-side
guard buys one day.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

pd = pytest.importorskip("pandas")

from pipeline.merge_master import (                          # noqa: E402
    _EMPTY_ID_TEMPLATE,
    strip_empty_identifier_template,
)

#: Sheets that feed reports, emails and the dashboard. `Rejected` is a
#: terminal archive nothing reads, and mass-editing settled audit rows to
#: satisfy a test costs more than it buys — the same exclusion
#: test_a_recall_is_not_both_published_and_rejected makes, for the same
#: reason.
LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")

#: Every spelling seen in the wild, plus the near misses.
LEAKS = (
    "(Recall ID: N/A)", "(Recall ID N/A)", "(Recall ID NA)",
    "(Recall ID: not provided)", "(Recall ID not provided)",
    "(Recall ID: unknown)", "[Recall ID: N/A]", "(recall id: none)",
    "(Recall ID: -)", "(Recall ID: ?)",
    # ── 2026-10-11: two more, both found ALREADY PUBLISHED ──────────────
    # "tbd" was in the alternation and "tba" was not, so NKFH (HU)
    # 2026-10-06 — SZEGA Camembert Kft., STEC in raw-milk Camembert —
    # published "(Recall ID TBA)" to subscribers.
    "(Recall ID TBA)", "(Recall ID: TBA)", "(Recall ID T.B.A.)",
    "(Recall ID: to be advised)", "(Recall ID pending)",
    # And the model does not always fill the slot with a WORD. Ruokavirasto
    # (FI) 2026-10-09, Munax Oy, published "(Recall ID 1234567890)" — a
    # reference number that exists nowhere, which is worse than an admitted
    # blank because it looks checkable.
    "(Recall ID 1234567890)", "(Recall ID: 0123456789)",
    "(Recall ID 000000)", "(Recall ID: 11111)", "(Recall ID 987654321)",
    # ── 2026-10-26: two more, both found ALREADY PUBLISHED ──────────────
    # The negation moved to the FRONT of the phrase. The alternation was
    # anchored on "recall id" coming first, so it read
    # "(Recall ID not provided)" and walked straight past the same
    # sentence written the other way round. NFSA (EG) 2026-10-11 —
    # Yasensvit Ltd shell eggs, Salmonella Enteritidis — published
    # "(No Recall ID provided)" to subscribers.
    "(No Recall ID provided)", "(No Recall ID given)",
    "(no recall id specified)", "[No Recall ID available]",
    "(Recall ID provided)", "(Recall ID)", "(Recall ID: )",
    # And the model prefixes its invented digits with the agency's own
    # short name, so the digits stop being the whole captured group and
    # the digit-run guard never sees them. Same class as the Ruokavirasto
    # "1234567890" row, found published twice over:
    #   AGES (AT) 2026-10-16  Lidl Milbona High Protein Pudding, B. cereus
    #   MAST (IS) 2026-10-13  Lifa ehf. Timur berry, Salmonella
    "(Recall ID AGES-12345)", "(Recall ID MAST-123456)",
    "(Recall ID: NFSA-987654321)", "(Recall ID GIS 11111)",
)

#: Digit runs that are NOT placeholders and must survive. A guard that eats
#: a real reference destroys the row's citation, which is what rule 10 is
#: for. 842632 is the leaked prompt example and a genuine-looking id; it
#: stays, exactly as the KEEPERS below require.
REAL_DIGIT_IDS = (
    "(Recall ID 842632)", "(Recall ID: 23743)", "(Recall ID 12660)",
    "(Recall ID 2026)", "(Recall ID: 878744)",
)

#: A REAL reference number must survive untouched — that is what rule 10
#: is for, and stripping it would destroy the row's citation.
KEEPERS = (
    "Salmonella in sesame paste (Recall ID 842632)",
    "Listeria monocytogenes (Recall ID FSA-PRIN-47-2026)",
    "Lead above the limit (Recall ID H-0700-2026)",
    # ── pinned 2026-10-26, when the guard learned the <AGENCY>-<digits>
    # shape. These three are the near misses that must NOT be eaten.
    #
    # GIS-2026-08-19-001 is load-bearing: the whole of
    # tests/test_a_dated_recall_id_agrees_with_date.py reads that
    # identifier as the regulator's own, and the Date on that row was
    # corrected from 2026-10-12 to 2026-08-19 because of it. Strip it and
    # the correction loses its evidence.
    "Presence of Listeria monocytogenes (Recall ID GIS-2026-08-19-001)",
    # L260/26 is Eurospin's LOT code, which the ministry's own recall form
    # spells "…Portamivia L260_26" in this row's URL. Mislabelled as a
    # recall id by the extractor, but it is a real reference read off a
    # real page, so the guard leaves it to an operator, not a regex.
    "Presence of Listeria monocytogenes (Recall ID L260/26)",
    "Public health alert (Recall ID PHA-10-2026)",
)


@pytest.mark.parametrize("leak", LEAKS)
def test_every_known_spelling_of_the_placeholder_is_matched(leak):
    body = "Pyrrolizidine alkaloids above the limit. " + leak
    assert _EMPTY_ID_TEMPLATE.search(body), (
        f"{leak!r} is the extractor's empty-identifier template and is not "
        f"matched. It will be published to subscribers verbatim.")
    rows = [{"Reason": body}]
    assert strip_empty_identifier_template(rows) == 1
    assert rows[0]["Reason"] == "Pyrrolizidine alkaloids above the limit."


@pytest.mark.parametrize("real", REAL_DIGIT_IDS)
def test_a_plausible_digit_reference_is_never_stripped(real):
    """Only an UNMISTAKABLE placeholder run goes: one repeated digit, or a
    strict ascending/descending sequence of four or more. Everything else a
    regulator could actually have issued is left alone."""
    body = "Listeria monocytogenes detected. " + real
    rows = [{"Reason": body}]
    assert strip_empty_identifier_template(rows) == 0, (
        f"{real!r} looks like a real regulator reference and was stripped. "
        f"The row loses its citation.")
    assert rows[0]["Reason"] == body


@pytest.mark.parametrize("keeper", KEEPERS)
def test_a_real_reference_number_is_never_stripped(keeper):
    rows = [{"Reason": keeper}]
    assert strip_empty_identifier_template(rows) == 0, (
        f"{keeper!r} carries a REAL regulator reference. Rule 10 exists to "
        f"capture it and this guard must not remove it.")
    assert rows[0]["Reason"] == keeper


def test_the_writer_choke_point_calls_the_strip():
    """So the data fix cannot be undone by a refactor of _write_sheet."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    i = src.find("def _write_sheet")
    assert i > 0, "_write_sheet not found"
    body = src[i:src.find("\ndef ", i + 100)]
    assert "strip_empty_identifier_template" in body, (
        "_write_sheet no longer strips the extractor's empty-identifier "
        "template. That is the writer choke point; without it the "
        "placeholder reaches Recalls again, as it did on 2026-10-05.")


def test_the_promoter_calls_it_too():
    """promote_gate_passing does NOT go through _write_sheet (2026-10-04)."""
    src = (ROOT / "pipeline" / "promote_gate_passing.py").read_text(
        encoding="utf-8")
    assert "strip_empty_identifier_template" in src, (
        "pipeline/promote_gate_passing.py appends rows to Recalls directly "
        "and skips every guard in _write_sheet. It must call the strip "
        "itself, exactly as it calls apply_label_aliases.")


def test_no_live_row_carries_the_placeholder():
    if not XLSX.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    x = pd.ExcelFile(XLSX)
    bad = []
    for sheet in x.sheet_names:
        if sheet not in LIVE_SHEETS:
            continue
        df = pd.read_excel(x, sheet)
        if "Reason" not in df.columns:
            continue
        for _, r in df.iterrows():
            v = str(r.get("Reason") or "")
            if _EMPTY_ID_TEMPLATE.search(v):
                bad.append((sheet, str(r.get("URL"))[:70], v[-70:]))
    assert not bad, (
        f"{len(bad)} live row(s) carry the extractor's empty-identifier "
        f"template in Reason and would publish it verbatim:\n  " +
        "\n  ".join(map(str, bad[:5])))
