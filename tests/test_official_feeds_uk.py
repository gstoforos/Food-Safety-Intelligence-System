"""UK / FSA food-alerts fetcher — offline behavioural tests.

WHY THIS FILE EXISTS
====================
Between 2026-07-08 and 2026-08-18 the UK collector was broken FOUR times
in a row, and not one of the breakages was caught before it ran in
production:

  1. `_sort=-created`  silently ignored by the API. Ran green every day,
                       returned fifty 2018 alerts every day, and no FSA row
                       entered the register for a month.
  2. `_pageSize=250`   400 Client Error. Took UK *and* Scotland down
                       (scotland.py delegates into uk.fetch).
  3. `_page=<n>`       also 400, and inert on this API anyway.
  4. `date` / `since`  NameError at the first line of the slicing loop —
                       the module did not even import `datetime`, so
                       fetch() could never have run once.

Breakage 4 is the important one for this file. It was not a subtle
protocol misunderstanding: the function was unrunnable, and it shipped
because *nothing in the repo has ever called uk.fetch()*. The suite had
484 passing tests and zero coverage of the fetchers, so "484 tests pass"
was true and meaningless at the same time.

These tests call the real fetch() against a fake `get_json`, so the code
path executes end to end offline. Any NameError, signature drift or
slicing-logic regression fails here instead of at 02:10 UTC in Actions.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.official_feeds.sources import uk as uk_src  # noqa: E402

PAGE_CAP = 50  # the server-side clamp uk.fetch is written against


def _alert(n: int, created: date, countries=("GB-ENG",), notation=None):
    """One FSA food-alerts item in the shape the live API returns."""
    note = notation or f"FSA-PRIN-{n:03d}-2026"
    return {
        "@id": f"https://data.food.gov.uk/food-alerts/id/{note}",
        "notation": note,
        "type": ["https://data.food.gov.uk/codes/food-alerts/def/Alert",
                 "https://data.food.gov.uk/codes/food-alerts/def/PRIN"],
        "title": f"Recall of product {n}",
        "created": f"{created.isoformat()}T09:00:00Z",
        "alertAuthor": "Example Foods Ltd",
        "alertURL": f"https://alerts.food.gov.uk/news-alerts/alert/{note.lower()}",
        "country": [{"@id": f"http://data.food.gov.uk/codes/food-alerts/country/{c}"}
                    for c in countries],
        "productDetails": [{"productName": f"Product {n}"}],
        "problem": [{"pathogenRisk": {"label": "Listeria monocytogenes"}}],
    }


class FakeAPI:
    """Stands in for get_json. Honours min-created / max-created and
    reproduces the server's hard clamp to 50 items per response."""

    def __init__(self, corpus):
        self.corpus = corpus
        self.calls = []          # [(min, max, pageSize)]

    def __call__(self, url, params=None, **kw):
        params = params or {}
        lo = params.get("min-created")
        hi = params.get("max-created")
        size = int(params.get("_pageSize", PAGE_CAP))
        assert "_page" not in params, "_page is inert on this API and 400s"
        assert size <= PAGE_CAP, f"_pageSize={size} would 400"
        self.calls.append((lo, hi, size))
        hits = [i for i in self.corpus
                if (lo is None or i["created"][:10] >= lo)
                and (hi is None or i["created"][:10] <= hi)]
        return {"items": hits[:min(size, PAGE_CAP)]}


@pytest.fixture
def patch_api(monkeypatch):
    def _install(corpus):
        api = FakeAPI(corpus)
        monkeypatch.setattr(uk_src, "get_json", api)
        return api
    return _install


# ── 1. The regression that broke 2026-08-18 ─────────────────────────────

def test_fetch_runs_at_all(patch_api):
    """NameError guard. `date` was never imported and `since` was never
    assigned, so fetch() raised on line 212 before touching the network.
    This is the whole point of the file: call the function."""
    today = date.today()
    api = patch_api([_alert(1, today - timedelta(days=3)),
                     _alert(2, today - timedelta(days=10))])
    recs = uk_src.fetch()
    assert len(recs) == 2
    assert {r.source_id for r in recs} == {"FSA-PRIN-001-2026",
                                           "FSA-PRIN-002-2026"}
    assert api.calls, "fetch() never issued a request"


def test_lookback_window_is_sent_and_is_a_real_date(patch_api):
    """`min-created` must be lookback_days before today, not a literal or
    a leftover string. The July 2026 loss was a window that drifted."""
    today = date.today()
    api = patch_api([_alert(1, today - timedelta(days=2))])
    uk_src.fetch(lookback_days=30)
    lo, hi, _ = api.calls[0]
    assert lo == (today - timedelta(days=30)).isoformat()
    assert hi == today.isoformat()


# ── 2. Date slicing (the replacement for pagination) ────────────────────

def test_full_page_is_split_until_nothing_is_truncated(patch_api):
    """A slice returning a full 50 is ambiguous — it may be truncated —
    so it must be halved and re-fetched. 120 alerts across 90 days cannot
    be retrieved in one clamped request; all 120 must still come back,
    each exactly once."""
    today = date.today()
    corpus = [_alert(n, today - timedelta(days=n % 90)) for n in range(120)]
    api = patch_api(corpus)

    recs = uk_src.fetch(lookback_days=90)

    ids = [r.source_id for r in recs]
    assert len(ids) == len(set(ids)), "overlapping slices produced duplicates"
    assert len(ids) == 120, f"expected all 120 alerts, got {len(ids)}"
    assert len(api.calls) > 1, "never split — truncation would go unnoticed"


def test_page_size_never_exceeds_the_server_clamp(patch_api):
    """_pageSize=250 was a 400. FakeAPI asserts on it; this test pins the
    contract even when the caller passes a larger limit."""
    today = date.today()
    api = patch_api([_alert(1, today - timedelta(days=1))])
    uk_src.fetch(limit=250)
    assert all(size <= PAGE_CAP for _, _, size in api.calls)


def test_request_backstop_is_honoured(patch_api):
    """max_pages bounds the slice queue so a pathological split cannot
    hammer the FSA. It must warn, not loop."""
    today = date.today()
    corpus = [_alert(n, today - timedelta(days=n % 90)) for n in range(300)]
    api = patch_api(corpus)
    uk_src.fetch(lookback_days=90, max_pages=5)
    assert len(api.calls) <= 5


# ── 3. The loudness guards (green-while-broken is the enemy) ────────────

def test_empty_window_raises(patch_api):
    """The FSA publishes several alerts a week. Zero over a 90-day window
    means the query stopped working — it must not return [] quietly."""
    patch_api([])
    with pytest.raises(RuntimeError, match="returned 0 items"):
        uk_src.fetch()


def test_ignored_date_filter_raises(monkeypatch):
    """The July 2026 failure mode: the API serves its oldest page and the
    scraper reports success. Detected from the DATA, not the HTTP status —
    a 200 proved nothing for a month."""
    class IgnoresDates(FakeAPI):
        def __call__(self, url, params=None, **kw):
            self.calls.append((params.get("min-created"),
                               params.get("max-created"),
                               int(params.get("_pageSize", PAGE_CAP))))
            return {"items": self.corpus}      # date filter ignored

    api = IgnoresDates([_alert(n, date(2018, 3, 1)) for n in range(3)])
    monkeypatch.setattr(uk_src, "get_json", api)
    with pytest.raises(RuntimeError, match="ignored min-created"):
        uk_src.fetch()


# ── 4. Scotland delegation (scotland.py calls straight into here) ───────

def test_scotland_only_alerts_are_excluded_from_uk(patch_api):
    today = date.today()
    patch_api([
        _alert(1, today - timedelta(days=1), countries=("GB-ENG",)),
        _alert(2, today - timedelta(days=1), countries=("GB-SCT",)),
    ])
    recs = uk_src.fetch()
    assert [r.source_id for r in recs] == ["FSA-PRIN-001-2026"]
    assert recs[0].authority == "FSA"


def test_scotland_mode_returns_only_scotland(patch_api):
    today = date.today()
    patch_api([
        _alert(1, today - timedelta(days=1), countries=("GB-ENG",)),
        _alert(2, today - timedelta(days=1), countries=("GB-SCT",)),
    ])
    recs = uk_src.fetch(include_scotland=True)
    assert [r.source_id for r in recs] == ["FSA-PRIN-002-2026"]
    assert recs[0].authority == "FSS"
    assert recs[0].country_name == "Scotland"


def test_scotland_module_delegates_without_error(monkeypatch):
    """scotland.py is a thin wrapper into uk.fetch — breakages 2, 3 and 4
    all took Scotland down as collateral. Exercise that path too."""
    from pipeline.official_feeds.sources import scotland as sct
    today = date.today()
    api = FakeAPI([_alert(1, today - timedelta(days=1), countries=("GB-SCT",))])
    monkeypatch.setattr(uk_src, "get_json", api)
    recs = sct.fetch()
    assert [r.country_name for r in recs] == ["Scotland"]


# ── 5. The URL rule (metadata endpoint is not a notice) ─────────────────

def test_alerturl_is_used_not_the_json_id(patch_api):
    """@id is the machine-readable record. Eight published rows once
    carried it as their citation."""
    today = date.today()
    patch_api([_alert(1, today - timedelta(days=1))])
    rec = uk_src.fetch()[0]
    assert rec.url == ("https://alerts.food.gov.uk/news-alerts/alert/"
                       "fsa-prin-001-2026")
    assert "data.food.gov.uk" not in rec.url


def test_missing_alerturl_does_not_fall_back_to_the_id(patch_api):
    today = date.today()
    item = _alert(1, today - timedelta(days=1))
    item.pop("alertURL")
    patch_api([item])
    rec = uk_src.fetch()[0]
    assert rec.url == "", "the @id metadata endpoint must never be cited"
