"""A recall id in the query string is identity, not tracking. 2026-10-05.

THE BUG, THREE TIMES
--------------------
``merge_master._normalize_url_for_dedup`` strips query parameters so that
``...?utm_source=news`` is not a different recall from ``...``. It keeps an
allow-list of parameters that ARE identity. The allow-list has now been
wrong three times, each time on a host that files its recalls by number
under one path:

  2026-07-26  fda.gov   ?search_api_fulltext=H-0700-2026   3 recalls -> 1 key
  2026-09-18  api.fda.gov ?search=recall_number:"H-…"      8 rows   -> 1 key
  2026-10-01  recall.caa.go.jp ?rcl=…, mfds.go.kr ?seq=…,
              fda.moph.go.th ?name=…                       every recall
                                                            after the first
                                                            dropped silently
  2026-10-05  fsis.usda.gov ?search=015-2026               3 recalls -> 1 key
              beaconbio.org ?reportid=…&eventid=…

Each was found only after a verified recall had been dropped as a
"duplicate" of an unrelated one. The allow-list is the wrong shape of
defence — it can only ever know the hosts somebody has already been burned
by.

THIS TEST IS THE RIGHT SHAPE. It does not enumerate hosts. It takes every
URL the register actually holds, pushes it through the real function, and
fails when two DISTINCT urls collapse to one dedup key. Whatever host it
happens on, the next one is caught by the data itself.

WHAT IS NOT A COLLISION. http/https, a leading www., a trailing slash and
letter case are exactly what the function exists to collapse, so URLs that
differ only in those are expected to share a key. So are the deliberate
collapses the function documents: admin.ch serves one federal release at
/de/, /fr/ and /it/ under one message id, and FSANZ republishes an amended
alert at an "updated-DDMMYY-" slug. Those are listed by hand below, each
with the comment in the function that explains it, so that widening this
is a decision somebody makes on purpose.
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

#: Collapses the function performs ON PURPOSE, each explained in its own
#: comment inside _normalize_url_for_dedup. Matched against the dedup KEY.
INTENTIONAL = (
    # One Swiss federal release, served per language and per department host.
    re.compile(r"^admin\.ch/newnsb/"),
    # FSANZ republishes an amended alert at a new "updated-DDMMYY-" slug.
    re.compile(r"^foodstandards\.gov\.au/food-recalls/recall-alert/"),
    # 2026-10-06. data.food.gov.uk/food-alerts/id.html is the FSA linked-data
    # API's own CATALOG page, not a recall notice, and "?__htmlView=" is that
    # API's HTML-view toggle — an empty-valued presentation parameter, never a
    # recall identifier. The register holds three rows on it, all the same
    # scrape of that one catalog page (archived Rejected 2026-08-26 by the
    # operator as not_a_notice, plus a later re-scrape in Weekly_Rejected).
    # Collapsing them to one key is correct and loses nothing: a real FSA
    # alert lives at /food-alerts/id/<number>, which carries its identifier
    # in the PATH and so cannot reach this key at all.
    #
    # NOT a precedent for query strings generally — the four incidents in the
    # docstring above were all identifiers. This one is listed because the
    # differing part is provably not one.
    re.compile(r"^data\.food\.gov\.uk/food-alerts/id\.html$"),
)


def _trivial(urls):
    """True when the URLs differ only in protocol, www., case or trailing /."""
    canon = set()
    for u in urls:
        s = u.strip().lower().rstrip("/")
        s = re.sub(r"^https?://", "", s)
        s = re.sub(r"^www\.", "", s)
        canon.add(s)
    return len(canon) == 1


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
                out.append((sheet, u))
    return out


def test_no_two_distinct_urls_collapse_to_one_dedup_key():
    by_key = collections.defaultdict(set)
    for sheet, u in _all_urls():
        by_key[str(_normalize_url_for_dedup(u))].add(u)

    bad = []
    for key, urls in by_key.items():
        if len(urls) < 2 or _trivial(urls):
            continue
        if any(p.match(key) for p in INTENTIONAL):
            continue
        bad.append((key, sorted(urls)))

    assert not bad, (
        f"{len(bad)} dedup key(s) carry MORE THAN ONE distinct recall URL. "
        f"Every recall after the first on such a key is silently dropped as "
        f"a duplicate of the others:\n" +
        "\n".join(f"  {k}\n" + "\n".join(f"      {u}" for u in v)
                  for k, v in bad[:5]) +
        "\n\nIf the differing part is a recall identifier, add it to the "
        "keeper list in merge_master._normalize_url_for_dedup (host-scoped). "
        "If it is genuinely presentation-only, add the key shape to "
        "INTENTIONAL in this test with the reason.")


@pytest.mark.parametrize("a,b", [
    # The 2026-10-05 pair, pinned so the keeper cannot be removed again.
    ("https://www.fsis.usda.gov/recalls-alerts?search=015-2026",
     "https://www.fsis.usda.gov/recalls-alerts?search=019-020-2026"),
    # The 2026-09-18 pair that taught us the shape.
    ('https://api.fda.gov/food/enforcement.json?search=recall_number:"H-0700-2026"',
     'https://api.fda.gov/food/enforcement.json?search=recall_number:"H-0699-2026"'),
    # 2026-10-01.
    ("https://www.recall.caa.go.jp/result/detail.php?rcl=00000035905&screenkbn=01",
     "https://www.recall.caa.go.jp/result/detail.php?rcl=00000035897&screenkbn=01"),
])
def test_two_recalls_on_one_endpoint_keep_two_keys(a, b):
    assert _normalize_url_for_dedup(a) != _normalize_url_for_dedup(b), (
        f"{a} and {b} are two different recalls and must not share a dedup "
        f"key — the recall id lives in the query string on this host.")


def test_tracking_parameters_are_still_stripped():
    """The keeper list must not become 'keep everything'."""
    base = "https://example.gov/recall/abc"
    for junk in ("?utm_source=news", "?utm_campaign=x&utm_medium=y",
                 "?fbclid=123", "?oc=5"):
        assert _normalize_url_for_dedup(base + junk) == \
            _normalize_url_for_dedup(base), (
            f"{junk} is tracking, not identity, and must still be stripped")
