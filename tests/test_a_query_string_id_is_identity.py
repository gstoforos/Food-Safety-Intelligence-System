"""The recall id in a query string IS the recall (2026-10-01).

A verified Japanese CAA recall (rcl=00000035897) was skipped as "already
approved" because every CAA detail URL normalised to the same key. Korea
(seq=) and Thailand (name=) had the same shape.
"""
import pytest

from pipeline.merge_master import _dedup_key, _normalize_url_for_dedup

PAIRS = [
    ("https://www.recall.caa.go.jp/result/detail.php?rcl=00000035897&screenkbn=01",
     "https://www.recall.caa.go.jp/result/detail.php?rcl=00000035898&screenkbn=01"),
    ("https://www.mfds.go.kr/eng/brd/m_61/view.do?seq=101",
     "https://www.mfds.go.kr/eng/brd/m_61/view.do?seq=102"),
    ("https://food.fda.moph.go.th/media.php?name=09_2026_a.pdf",
     "https://food.fda.moph.go.th/media.php?name=09_2026_b.pdf"),
]


@pytest.mark.parametrize("a,b", PAIRS)
def test_two_recalls_are_two_keys(a, b):
    assert _normalize_url_for_dedup(a) != _normalize_url_for_dedup(b)
    assert _dedup_key({"URL": a}) != _dedup_key({"URL": b})


def test_tracking_params_are_still_stripped():
    a = "https://www.recall.caa.go.jp/result/detail.php?rcl=1&screenkbn=01&utm_source=x"
    b = "https://www.recall.caa.go.jp/result/detail.php?rcl=1"
    assert _normalize_url_for_dedup(a) == _normalize_url_for_dedup(b)


def test_seq_elsewhere_is_not_an_identity():
    assert _normalize_url_for_dedup("https://example.org/a?seq=1") == \
        _normalize_url_for_dedup("https://example.org/a?seq=2")
