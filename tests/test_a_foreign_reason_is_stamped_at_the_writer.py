# -*- coding: utf-8 -*-
"""A log line is not a hand-off.

The morning fix pass translates the rows that carry a "[needs cleanup: …]"
stamp (operator 2026-10-01). Before 2026-10-21 the stamps were applied by
exactly two writers — pipeline/recall_confirm_agent.py (Reason, Product,
Company/Brand) and pipeline/promote_gate_passing.py (Product and
Company/Brand only) — and NOT by merge_master._write_sheet, the choke point
every write to Recalls goes through. _write_sheet counted non-English
Reasons into a log warning and wrote the row unmarked.

MEASURED, main at 0ae22b9c on 2026-10-21. Four RappelConso rows published
2026-10-20/21 with French text:

    fiche 23874  Reason  "RESULTAT ANALYSE NON CONFORME"
    fiche 23873  Reason  "Contamination à l'histamine"
    fiche 23872  Reason  "Contamination du produit à l'histamine"
    fiche 23876  Reason  "Détection de Listeria monocytogènes suite à une
                          analyse produit"      Product "boudin à la viande"

Not one carried a Reason stamp. Fiche 23876 reached Recalls through the
hourly Pending->Recalls merge with no provenance note of any kind, so
neither stamping writer ever saw it. Three of the four held
tests/test_language_policy.py, tests/test_reason_is_not_wholly_foreign.py
and tests/test_product_and_firm_names_are_english.py RED — CI catching the
French AFTER it reached the dashboard and the subscriber mail.

The stamp STAMPS. It must not block (there is no model at the writer, the
verified REASON_EN table is a deliberate human act, and a gate that held
the hourly merge for a French motif would stop the register) and it must
not translate.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import merge_master as mm  # noqa: E402

REASON_STAMP = "[needs cleanup: Reason not in English]"
PRODUCT_STAMP = "[needs cleanup: Product not in English]"
FIRM_STAMP = "[needs cleanup: Company/Brand not in English]"


def _write(rows, schema=None):
    """Run the real _write_sheet and hand back the rows it mutated."""
    from openpyxl import Workbook

    schema = schema or ["Date", "Source", "Company", "Brand", "Product",
                        "Pathogen", "Reason", "Class", "Country", "Region",
                        "Tier", "Outbreak", "URL", "Notes"]
    wb = Workbook()
    mm._write_sheet(wb, "Recalls", schema, rows)
    return rows


def _row(**over):
    row = {
        "Date": "2026-10-20",
        "Source": "RappelConso (FR)",
        "Company": "Poiscaille",
        "Brand": "Maison de la Bottarga",
        "Product": "Seafood charcuterie — tuna sausage",
        "Pathogen": "Histamine / scombrotoxin",
        "Reason": "Histamine contamination",
        "Class": "Recall",
        "Country": "France",
        "Region": "Europe",
        "Tier": 1,
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23873/interne",
        "Notes": "",
    }
    row.update(over)
    return row


# ── Reason ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("reason", [
    "RESULTAT ANALYSE NON CONFORME",
    "Contamination du produit à l'histamine",
    "Détection de Listeria monocytogènes suite à une analyse produit",
])
def test_a_foreign_reason_is_stamped(reason: str) -> None:
    rows = _write([_row(Reason=reason)])
    assert REASON_STAMP in str(rows[0]["Notes"]), rows[0]["Notes"]


def test_the_reason_itself_is_not_touched() -> None:
    """Stamp, never translate: the regulator's wording survives verbatim."""
    reason = "RESULTAT ANALYSE NON CONFORME"
    rows = _write([_row(Reason=reason)])
    assert rows[0]["Reason"] == reason


@pytest.mark.parametrize("reason", [
    "Presence of Salmonella",
    "Presence of Listeria monocytogenes",
    "Detected in the products, posing a health risk",
    # An English sentence quoting a foreign name keeps the two-hit
    # threshold's protection — this is what the policy tests warn against
    # loosening, and the stamp must not undo it.
    "Presence of Listeria monocytogenes in Tomme de Savoie",
])
def test_an_english_reason_is_not_stamped(reason: str) -> None:
    rows = _write([_row(Reason=reason)])
    assert REASON_STAMP not in str(rows[0]["Notes"]), rows[0]["Notes"]


def test_the_stamp_is_idempotent() -> None:
    """A row promoted by promote_gate_passing and then rewritten here
    carries one stamp, not two."""
    row = _row(Reason="RESULTAT ANALYSE NON CONFORME",
               Notes="poiscaille " + REASON_STAMP)
    rows = _write([row])
    assert str(rows[0]["Notes"]).count(REASON_STAMP) == 1, rows[0]["Notes"]


def test_the_writer_never_blocks_on_language() -> None:
    """Three French rows go in, three rows come out. The register does not
    stop because a motif is in French."""
    rows = _write([
        _row(Reason="RESULTAT ANALYSE NON CONFORME",
             URL="https://rappel.conso.gouv.fr/fiche-rappel/23874/interne"),
        _row(Reason="Contamination du produit à l'histamine",
             URL="https://rappel.conso.gouv.fr/fiche-rappel/23872/interne"),
        _row(Reason="Presence of Salmonella",
             URL="https://rappel.conso.gouv.fr/fiche-rappel/23859/interne"),
    ])
    assert len(rows) == 3


# ── Product and the firm fields ────────────────────────────────────────────

def test_a_foreign_product_is_stamped_at_the_writer() -> None:
    """fiche 23876 — product_needs_english('boudin à la viande') has always
    been True; nothing called it on this path."""
    rows = _write([_row(Product="boudin à la viande",
                        Reason="Presence of Listeria monocytogenes")])
    assert PRODUCT_STAMP in str(rows[0]["Notes"]), rows[0]["Notes"]


def test_an_english_product_is_not_stamped() -> None:
    rows = _write([_row(Product="Rainbow trout fillets, 125 g",
                        Reason="Presence of Listeria monocytogenes")])
    assert PRODUCT_STAMP not in str(rows[0]["Notes"]), rows[0]["Notes"]


def test_a_non_latin_firm_name_is_stamped() -> None:
    rows = _write([_row(Company="株式会社ヤマダ", Brand="ヤマダ",
                        Product="Grated cheese",
                        Reason="Presence of Listeria monocytogenes",
                        Source="CAA (JP)", Country="Japan", Region="Asia")])
    assert FIRM_STAMP in str(rows[0]["Notes"]), rows[0]["Notes"]


def test_a_latin_script_firm_name_is_exempt() -> None:
    """The operator rule exempts brand and product NAMES in Latin script —
    'Poiscaille' and 'Maison de la Bottarga' are names, not descriptions."""
    rows = _write([_row(Reason="Presence of Listeria monocytogenes")])
    assert FIRM_STAMP not in str(rows[0]["Notes"]), rows[0]["Notes"]


# ── the sibling writer ─────────────────────────────────────────────────────

def test_the_offline_promoter_also_stamps_reason() -> None:
    """pipeline/promote_gate_passing.py promoted three of the four French
    rows and stamped only Product. Reason is in its list now; the confirm
    agent has had it all along."""
    src = (ROOT / "pipeline" / "promote_gate_passing.py").read_text(
        encoding="utf-8")
    assert REASON_STAMP in src, (
        "promote_gate_passing must stamp a non-English Reason — the morning "
        "fix pass reads what is stamped")
    assert "looks_non_english" in src
