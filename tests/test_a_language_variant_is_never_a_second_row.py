"""One notice served in seven languages is one recall. 2026-10-28.

THE BUG
-------
``merge_master._normalize_url_for_dedup`` keys a row on its URL.
A regulator that serves the same notice once per language — the language as
a path segment — therefore mints one row per language, and each copy takes
its own independent trip through the reviewers and comes back with its own
verdict on the same recall.

It had been found once before, on admin.ch (2026-09-07: one federal release
at /de/, /fr/ and /it/, the German copy published beside the French one).
The fix was host-scoped, so the next agency with the same URL shape was not
covered. Measured on the workbook of 2026-10-28, AESAN (ES) was that agency:

    alert 2026_72  Listeria in cooked mussels   PUBLISHED at the bare
                   address, 2 more copies in Pending, 3 in Rejected
    alert 2026_75  Salmonella in sesame tahini  6 copies, all rejected,
                   nothing published — the recall was lost
    alert 2026_74  Salmonella in egg white      2 copies, both blocked
    alert 2026_73  glass in ricotta sauce       1 copy, rejected

THIS TEST IS THE SHAPE THAT GENERALISES. It does not name a host. It takes
every URL the register holds, strips a leading two-letter language segment,
and fails when two URLs that differ ONLY by that segment land on different
dedup keys. The next agency to serve its notices per language is caught by
the data itself, before a second verdict can be filed on one recall.

WHAT IS NOT A FAILURE. Two URLs that differ by more than the language
segment are different URLs and are expected to key differently. A host
whose first path segment merely looks like a language code but is content
("eu" for a European section, say) would show up here, and the fix then is
to narrow the host rule in _normalize_url_for_dedup, not to loosen this
test: the assertion below prints both URLs so the difference is visible.
"""
from __future__ import annotations

import collections
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

pd = pytest.importorskip("pandas")

from pipeline.merge_master import _normalize_url_for_dedup  # noqa: E402

#: host/<lang>/rest  ->  (host, rest)
_LANG_SEGMENT = re.compile(r"^([a-z0-9.-]+)/([a-z]{2})/(.+)$")

#: Language codes only. A two-letter segment outside this set is treated as
#: content, so a path like host/id/12345 is never read as Indonesian.
_LANGUAGES = {
    "ar", "bg", "ca", "cs", "da", "de", "el", "en", "es", "et", "eu", "fi",
    "fr", "ga", "gl", "hr", "hu", "is", "it", "lt", "lv", "mt", "nl", "no",
    "pl", "pt", "ro", "sk", "sl", "sv", "tr", "va",
}


def _all_urls():
    if not XLSX.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    x = pd.ExcelFile(XLSX)
    out = []
    for sheet in x.sheet_names:
        df = pd.read_excel(x, sheet)
        if "URL" not in df.columns:
            continue
        for v in df["URL"]:
            u = str(v).strip()
            if u and u.lower() != "nan":
                out.append(u)
    return out


def _language_blind_key(key: str) -> str:
    m = _LANG_SEGMENT.match(key)
    if m and m.group(2) in _LANGUAGES:
        return m.group(1) + "/" + m.group(3)
    return key


def test_one_notice_per_language_is_not_several_recalls():
    by_blind = collections.defaultdict(set)
    for u in _all_urls():
        key = str(_normalize_url_for_dedup(u))
        if not key:
            continue
        by_blind[_language_blind_key(key)].add(key)

    bad = {b: sorted(keys) for b, keys in by_blind.items() if len(keys) > 1}

    assert not bad, (
        f"{len(bad)} notice(s) in the register are keyed as SEVERAL recalls "
        f"because the only difference between their URLs is a language path "
        f"segment. Each copy is reviewed separately and can be published and "
        f"rejected at the same time:\n" +
        "\n".join(f"  {b}\n" + "\n".join(f"      {k}" for k in ks)
                  for b, ks in list(bad.items())[:5]) +
        "\n\nStrip the language segment for that host in "
        "merge_master._normalize_url_for_dedup, the way admin.ch and "
        "aesan.gob.es already are, and add the host to INTENTIONAL in "
        "tests/test_a_query_param_recall_id_is_never_dropped.py.")


@pytest.mark.parametrize("variant", [
    "https://www.aesan.gob.es/en/alertas/2026_75",
    "https://www.aesan.gob.es/ca/alertas/2026_75",
    "https://www.aesan.gob.es/eu/alertas/2026_75",
    "https://www.aesan.gob.es/gl/alertas/2026_75",
    "https://www.aesan.gob.es/va/alertas/2026_75",
    "https://www.aesan.gob.es/fr/alertas/2026_75",
    "https://aesan.gob.es/es/alertas/2026_75",
])
def test_every_aesan_language_copy_keys_to_the_spanish_address(variant):
    """The regression that lost AESAN 2026_75, pinned per language."""
    assert (_normalize_url_for_dedup(variant)
            == _normalize_url_for_dedup("https://www.aesan.gob.es/alertas/2026_75")
            == "aesan.gob.es/alertas/2026_75")


def test_an_aesan_content_path_is_not_mistaken_for_a_language():
    """'alertas', 'actualidad' and 'aecosan' must survive untouched."""
    for path in ("alertas/2026_75",
                 "actualidad/actualidad-noticias/some-news-item",
                 "aecosan/web/seguridad_alimentaria/alertas_alimentarias"):
        got = _normalize_url_for_dedup("https://www.aesan.gob.es/" + path)
        assert got == "aesan.gob.es/" + path, got


def test_a_two_letter_segment_on_another_host_is_left_alone():
    """The AESAN rule is host-scoped: nothing else may be rewritten by it."""
    for url in ("https://example.gov/en/alerts/123",
                "https://recalls-rappels.canada.ca/en/alert-recall/some-slug"):
        assert "/en/" in _normalize_url_for_dedup(url)
