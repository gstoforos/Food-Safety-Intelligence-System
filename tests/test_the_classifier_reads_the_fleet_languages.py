"""The gap-finder classifier reads the languages the fleet searches in.

2026-09-30. The fleet runs Japan, Korea, Taiwan, Hong Kong, Thailand and
Indonesia, and pipeline/gap_finder/rules.py knew only Latin and Greek hazard
words — a notice in the local language could only ever classify "unknown".
Mould was the same shape of gap in every language: in scope since the
operator decision of 2026-09-07 at the publish gate and in _pathogen_scope,
and never told to this classifier.

The negatives matter as much as the positives: coliform and total-count
indicators, a Chinese mould COUNT, "jamur" (mushroom), Moldova, molasses.
"""
from __future__ import annotations

import pytest

from pipeline.gap_finder.rules import classify

ACCEPT = [
    ("サルモネラ属菌が検出", "pathogen"),
    ("リステリア・モノサイトゲネス", "pathogen"),
    ("腸管出血性大腸菌O157", "pathogen"),
    ("黄色ブドウ球菌", "pathogen"),
    ("異物（水カビ様の異物）混入", "mould"),
    ("살모넬라균 검출", "pathogen"),
    ("리스테리아 모노사이토제네스", "pathogen"),
    ("곰팡이 검출", "mould"),
    ("检出沙门氏菌", "pathogen"),
    ("金黄色葡萄球菌不合格", "pathogen"),
    ("產品發霉", "mould"),
    ("ตรวจพบเชื้อซัลโมเนลลา", "pathogen"),
    ("พบเชื้อรา", "mould"),
    ("cemaran kapang", "mould"),
    ("mould contamination", "mould"),
    ("Presence of mold", "mould"),
    ("moisissures visibles", "mould"),
]

NOT_ACCEPT = [
    "大腸菌群陽性",            # coliform count, JP
    "大肠菌群超标",            # coliform count, CN
    "대장균군 기준 초과",       # coliform count, KR
    "菌落总数超标",            # total plate count
    "霉菌数不符合标准",         # mould COUNT against a limit, CN
    "jamur enoki",            # Indonesian: mushroom
    "Moldova walnuts",
    "molasses cookies",
    "microbial contamination",  # vague: stays unknown, as at the gate
]


@pytest.mark.parametrize("text,category", ACCEPT)
def test_accepts(text, category):
    c = classify(reason=text)
    assert (c.verdict, c.category) == ("accept", category), (text, c)


@pytest.mark.parametrize("text", NOT_ACCEPT)
def test_does_not_accept(text):
    assert classify(reason=text).verdict != "accept", text


def test_mould_is_tier_2_like_everywhere_else():
    assert classify(reason="visible mould").tier == 2


def test_a_pathogen_still_beats_mould():
    assert classify(reason="Salmonella and mould").category == "pathogen"


def test_foreign_matter_is_in_scope_since_2026_09_30():
    """Rule B retired on the operator's decision ("printed scope
    everywhere"). A bare material word still does not make a recall a
    foreign-body one — "packaged in a glass jar" is not a hazard."""
    assert classify(reason="glass fragments").category == "foreign_matter"
    assert classify(reason="packaged in a glass jar").verdict != "accept"
    assert classify(reason="Listeria; glass fragments").category == "pathogen"
