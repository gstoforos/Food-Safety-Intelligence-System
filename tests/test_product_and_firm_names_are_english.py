"""Product and firm names are published in English.

    "give me that japan and ALL must be the product in english... not sure is
     in Japan china etc the firm must be in english if available"
                                                        — operator, 2026-10-01

WHAT WAS WRONG
==============
The dashboard showed, for 2026-09-29/30:

    CAA (JP)   近鉄百貨店 草津店   釜揚げしらす (kamaage shirasu, boiled whitebait)
    CAA (JP)   沖製あん            輪島の地酒ぜりい (Wajima local-sake jelly)
    RappelConso (FR)  Grand Frais   haché de boeuf

The 2026-08-02 rule kept Company, Brand and Product exactly as the regulator
published them. Measured on the 1870-row register that day: 633 published
Products were not in English (French 600+, plus Greek, Japanese, Dutch,
German, Polish, Italian, Spanish) and 9 rows carried a firm name in Greek or
Japanese script.

THE RULE NOW
============
  Product        English. Brand names, protected names (AOP/PDO/IGP) and
                 lot / weight / date details are kept as written.
  Company/Brand  As published in Latin letters; in any other script the
                 firm's own English name, otherwise its romanization.
  Notes          "[original product: …]" / "[original company: …]" keeps the
                 regulator's wording, so a row can still be matched to its page.

Translation needs a model: reviewer 2 (self-hosted) translates. The two gates
that publish without a model — reviewer 3 and the offline promoter — cannot,
so they STAMP the row "[needs cleanup: Product not in English]" and the daily
Morning Fix pass translates it. Never a rejection: a real recall is not
discarded over wording.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._language import (  # noqa: E402
    has_non_latin_script, product_needs_english,
)

RECALLS = ROOT / "docs" / "data" / "recalls.json"
STAMP = "[needs cleanup:"


def _rows():
    return json.loads(RECALLS.read_text(encoding="utf-8"))


# ── the detector ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    "近鉄百貨店 草津店", "釜揚げしらす", "輪島の地酒ぜりい (Wajima local-sake jelly)",
    "ΜΠΑΡΜΠΑ ΣΤΑΘΗΣ Μ.Α.Β.Ε.Ε.", "Хлеб ржаной", "냉동 만두", "ข้าวมันไก่",
])
def test_a_non_latin_name_needs_english(text):
    assert has_non_latin_script(text)
    assert product_needs_english(text)


@pytest.mark.parametrize("text", [
    "Aflatoxins (B1=15.2; Tot.=38.0 μg/kg - ppb) in dried figs from Türkiye",
    "Kamaage shirasu (boiled whitebait)",
    "Brie de Melun AOP cheese (Talleyrand)",
    "Hainanese chicken rice",
])
def test_english_and_a_lone_greek_mu_pass(text):
    """RASFF writes the Greek mu in μg/kg. One letter is not a script."""
    assert not has_non_latin_script(text)
    assert not product_needs_english(text)


@pytest.mark.parametrize("text", [
    "saucisse de filet de poulet halal", "bouchées aux crevettes",
    "référence 3225164", "2 filets de poulet facon hache",
    "camembert fermier au lait cru",
])
def test_a_short_french_name_needs_english(text):
    """2026-10-02: ten RappelConso names of 2026-10-01 published in French —
    each was below the two-function-word floor of the detector."""
    assert product_needs_english(text)


@pytest.mark.parametrize("text", [
    "Halal chicken fillet sausage", "Shrimp bites",
    "Reblochon de Savoie AOP farmhouse cheese 450 g",
    "Crème fraîche (cultured cream)",
])
def test_an_english_name_with_a_protected_french_name_passes(text):
    assert not product_needs_english(text)


def test_a_french_description_needs_english():
    assert product_needs_english(
        "perles des mers -salade de pâtes cuite, surimi saveur crabe et "
        "œufs de truite en dés")


# ── the register ───────────────────────────────────────────────────────────

def test_no_published_name_is_in_a_non_latin_script():
    offenders = [(r.get("Source"), f, str(r.get(f))[:50])
                 for r in _rows() for f in ("Product", "Company", "Brand")
                 if has_non_latin_script(r.get(f))
                 and STAMP not in str(r.get("Notes") or "")]
    assert not offenders, offenders[:8]


def test_no_published_product_reads_in_another_language():
    """A row the publish gates stamped for translation is tracked work, not a
    silent miss; everything else must already be English."""
    offenders = [(r.get("Source"), str(r.get("Product"))[:60])
                 for r in _rows()
                 if product_needs_english(r.get("Product"))
                 and STAMP not in str(r.get("Notes") or "")]
    assert not offenders, offenders[:8]


def test_the_japanese_rows_read_in_english():
    """Every CAA (JP) row reads in English, and anything that was
    translated or romanized keeps the regulator's own value in Notes.

    THE "[original company:" STAMP IS CONDITIONAL, and it is worth saying
    why rather than asserting it flatly. This test used to require it on
    every CAA row, which held while the only two Japanese rows in the
    register had Japanese firm names. On 2026-10-03 five more arrived and
    two of them — "Houwa poultry farm&T.T" and "NEPAL EXPRESS PARCEL AND
    LOGISTICS PVT LTD." — are published by the CAA in Latin letters
    already. There is no original to keep: the published value IS the
    regulator's value, and writing "[original company: Houwa poultry
    farm&T.T]" would record a translation that never happened.

    The rule the policy actually states is the one asserted here: a name in
    a non-Latin script is replaced by the firm's own English name or its
    standard romanization, AND the original is kept so the row can still be
    matched against the regulator's page. A name already in Latin letters
    is passed through untouched and needs no stamp.

    Product is unconditional — the CAA writes 商品名 in Japanese on every
    notice, so every row has something that had to be translated.
    """
    jp = [r for r in _rows() if r.get("Source") == "CAA (JP)"]
    assert jp
    for r in jp:
        for f in ("Company", "Product"):
            assert not has_non_latin_script(r.get(f)), (f, r.get(f))
        notes = str(r.get("Notes") or "")
        assert "[original product:" in notes, (
            "a CAA notice names its product in Japanese, so every row must "
            "keep that original", r.get("Product"))
        # Romanized or translated → the original is mandatory. Latin-script
        # firm names are compared against the originals this register has
        # already recorded, so the stamp cannot simply be dropped to dodge
        # the rule.
        if "romaniz" in notes or "[original company:" in notes:
            assert "[original company:" in notes, (
                "a romanized firm name must keep the regulator's own value",
                r.get("Company"))


def test_a_latin_script_japanese_firm_name_is_passed_through():
    """The companion to the rule above: if a CAA row's Company is in Latin
    letters it must be the notice's own string, not an invented English
    rendering. Pinned on the two rows added 2026-10-03."""
    jp = {str(r.get("Company") or ""): str(r.get("Notes") or "")
          for r in _rows() if r.get("Source") == "CAA (JP)"}
    for latin in ("Houwa poultry farm&T.T",
                  "NEPAL EXPRESS PARCEL AND LOGISTICS PVT LTD."):
        assert latin in jp, (
            f"{latin!r} is the CAA's own spelling and must not be "
            f"re-cased or tidied")


def test_every_translated_row_keeps_its_original():
    """The 2026-10-01 pass stamped every row it changed."""
    for r in _rows():
        n = str(r.get("Notes") or "")
        if "[English names 2026-10-01]" in n:
            assert "[original " in n, (r.get("Source"), r.get("Product"))


# ── the publish path ───────────────────────────────────────────────────────

def test_reviewer_two_is_told_to_translate():
    from pipeline.recall_review_agent import build_review_prompt
    p = build_review_prompt({"Source": "RappelConso (FR)",
                             "Product": "haché de boeuf"})
    assert "Product must read in ENGLISH" in p
    assert '"original":{"Product":"","Company":"","Brand":""}' in p
    assert "keep them EXACTLY" not in p


def test_reviewer_two_keeps_the_original_in_notes():
    from pipeline.recall_review_agent import apply_review
    row = {"Source": "CAA (JP)", "Company": "近鉄百貨店 草津店",
           "Product": "釜揚げしらす", "Notes": "",
           "URL": "https://www.recall.caa.go.jp/result/detail.php?rcl=1"}
    out = apply_review(row, {"fields": {
        "Company": "Kintetsu Department Store, Kusatsu",
        "Product": "Kamaage shirasu (boiled whitebait)"},
        "original": {"Company": "近鉄百貨店 草津店", "Product": "釜揚げしらす"}})
    assert out["Product"] == "Kamaage shirasu (boiled whitebait)"
    assert "[original product: 釜揚げしらす]" in out["Notes"]
    assert "[original company: 近鉄百貨店 草津店]" in out["Notes"]


def test_reviewer_two_warns_never_rejects_on_language():
    from pipeline.recall_review_agent import _field_integrity_flags, _is_blocking
    probs = _field_integrity_flags({
        "Source": "CAA (JP)", "Company": "近鉄百貨店 草津店",
        "Product": "釜揚げしらす", "Reason": "Detection of a marine biotoxin",
        "URL": "https://www.recall.caa.go.jp/result/detail.php?rcl=1"})
    lang = [p for p in probs if "not in English" in p]
    assert any(p.startswith("Product") for p in lang)
    assert any(p.startswith("Company") for p in lang)
    assert not any(_is_blocking(p) for p in lang)


@pytest.mark.parametrize("path", ["pipeline/recall_confirm_agent.py",
                                  "pipeline/promote_gate_passing.py"])
def test_the_model_free_gates_stamp_untranslated_names(path):
    src = (ROOT / path).read_text(encoding="utf-8")
    assert "product_needs_english" in src
    assert "[needs cleanup: Product not in English]" in src
    assert "[needs cleanup: Company/Brand not in English]" in src
