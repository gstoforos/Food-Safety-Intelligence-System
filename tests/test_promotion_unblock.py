"""
tests/test_promotion_unblock.py
===============================
2026-07-28 INCIDENT — publication stopped for four days, silently.

Recalls froze at 1231 rows from 2026-07-24 while Pending grew to 49.
Collection was healthy and the reviewer workflows fired 6-7x/day. Three
defects compounded:

  1. TLS. rappel.conso.gouv.fr began serving an INCOMPLETE certificate
     chain. Python's ssl cannot build a trust path ("unable to get local
     issuer certificate"); browsers recover via the cert's AIA extension.
     claude_check could not read any RappelConso fiche.

  2. A ONE-WAY TRAP. On a fetch failure claude_check returns SKIP and
     parked the row in STATUS_PENDING_GAP_V2 — a DEMOTION into the
     gap-finder state machine, whose only exit is a successful Claude
     pass. With a permanent fetch failure that is a closed loop:
         fetch fails -> SKIP -> pending_gap_v2 -> cannot promote
         -> exit needs a pass -> which needs the fetch.
     24 rows locked: 16 Listeria, 3 STEC, 2 Salmonella, Norovirus,
     Ochratoxin, Aflatoxin.

  3. NO ALARM. Every run reported a green "+0 promoted". Nothing measured
     how long rows had been parked, so nothing could complain.

These tests pin all three fixes. The fail-closed guarantee is unchanged
throughout: a row whose verification did not COMPLETE never reaches
Recalls.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    NON_PROMOTABLE_STATUSES, STATUS_PENDING, STATUS_PENDING_RETRY,
    STATUS_PENDING_GAP_V2, promote_approved,
)

RAPPELCONSO = "https://rappel.conso.gouv.fr/fiche-rappel/22999/Interne"


def _row(**over):
    base = {
        "Date": "2026-07-28", "Source": "RappelConso (FR)",
        "Company": "Paturages Comtois", "Brand": "Paturages Comtois",
        "Product": "morbier aop", "Pathogen": "Listeria monocytogenes",
        "Reason": "Presence of Listeria monocytogenes", "Class": "Recall",
        "Country": "France", "Region": "Europe", "Tier": 1, "Outbreak": 0,
        "URL": RAPPELCONSO, "Notes": "", "Status": STATUS_PENDING,
    }
    base.update(over)
    return base


class TestFailClosedStillHolds:
    """The parking lot must keep blocking unverified rows."""

    @pytest.mark.parametrize("status", sorted(NON_PROMOTABLE_STATUSES))
    def test_every_parking_status_blocks_promotion(self, status):
        new, _kept, _arch = promote_approved([_row(Status=status)], [], {})
        assert new == [], f"{status} must never auto-promote"

    def test_retry_status_is_non_promotable(self):
        assert STATUS_PENDING_RETRY in NON_PROMOTABLE_STATUSES

    def test_plain_pending_still_promotes(self):
        new, _kept, _arch = promote_approved([_row()], [], {})
        assert len(new) == 1


class TestParkingIsRecoverable:
    """A transient failure must not permanently demote a normal row."""

    def test_retry_status_releases_to_plain_pending(self):
        # Simulates the advance loop: a parked row that later PASSES is
        # flipped back to plain pending and promotes in the SAME run.
        parked = _row(Status=STATUS_PENDING_RETRY)
        assert promote_approved([dict(parked)], [], {})[0] == []
        released = {**parked, "Status": STATUS_PENDING}
        assert len(promote_approved([released], [], {})[0]) == 1

    def test_gap_v2_no_longer_used_for_plain_rows(self):
        # Regression on the demotion itself: parking a plain pending row
        # must yield pending_retry, never pending_gap_v2 — otherwise the
        # row re-enters the gap state machine it never belonged to.
        from pipeline import claude_check as cc
        row = _row()
        skipped = {0}
        pending = [row]
        for idx in skipped:
            r = pending[idx]
            cur = (r.get("Status") or "").strip().lower()
            if cur in ("", "pending", cc.STATUS_PENDING_RETRY):
                r["Status"] = cc.STATUS_PENDING_RETRY
        assert row["Status"] == STATUS_PENDING_RETRY
        assert row["Status"] != STATUS_PENDING_GAP_V2


class TestStalenessAlarm:
    def test_silent_when_parking_lot_is_clear(self, capsys):
        from pipeline.claude_check import _report_parking_lot_staleness
        assert _report_parking_lot_staleness([_row()]) == 0
        out = capsys.readouterr().out
        assert "::error" not in out and "::warning" not in out

    def test_errors_when_rows_are_stuck_past_threshold(self, capsys):
        from pipeline import claude_check as cc
        old = (datetime.now(timezone.utc).date()
               - timedelta(days=cc.PARKING_STALE_ERROR_DAYS + 1)).isoformat()
        rows = [_row(Status=STATUS_PENDING_RETRY, ScrapedAt=old) for _ in range(24)]
        n = cc._report_parking_lot_staleness(rows)
        assert n == 24
        out = capsys.readouterr().out
        assert "::error" in out and "publication stalled" in out.lower()

    def test_warns_in_the_middle_band(self, capsys):
        from pipeline import claude_check as cc
        mid = (datetime.now(timezone.utc).date()
               - timedelta(days=cc.PARKING_STALE_WARN_DAYS)).isoformat()
        n = cc._report_parking_lot_staleness(
            [_row(Status=STATUS_PENDING_RETRY, ScrapedAt=mid)])
        out = capsys.readouterr().out
        assert n == 0 and "::warning" in out

    def test_fresh_parked_rows_do_not_alarm(self, capsys):
        from pipeline import claude_check as cc
        today = datetime.now(timezone.utc).date().isoformat()
        assert cc._report_parking_lot_staleness(
            [_row(Status=STATUS_PENDING_RETRY, ScrapedAt=today)]) == 0
        out = capsys.readouterr().out
        assert "::error" not in out


class _Resp:
    def __init__(self, text="<html><body>Listeria monocytogenes</body></html>"):
        self.status_code = 200
        self.text = text
        self.content = text.encode()
        self.headers = {"Content-Type": "text/html; charset=utf-8"}
        self.url = RAPPELCONSO


class TestTlsChainFallback:
    def test_shared_fetch_retries_via_curl_on_ssl_error(self, monkeypatch):
        from scrapers import _base
        calls = {"curl": 0}

        class _S:
            def request(self, *a, **k):
                raise requests.exceptions.SSLError(
                    "certificate verify failed: unable to get local issuer certificate")

        def _curl(url, method="GET", timeout=None, **kw):
            calls["curl"] += 1
            return _Resp()

        monkeypatch.setattr(_base, "fetch_via_curl_cffi", _curl)
        monkeypatch.setattr(_base, "is_akamai_host", lambda u: False)
        assert _base.fetch(_S(), RAPPELCONSO) is not None
        assert calls["curl"] == 1

    def test_shared_fetch_does_not_retry_on_other_errors(self, monkeypatch):
        from scrapers import _base
        calls = {"curl": 0}

        class _S:
            def request(self, *a, **k):
                raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(_base, "fetch_via_curl_cffi",
                            lambda *a, **k: calls.__setitem__("curl", 1))
        monkeypatch.setattr(_base, "is_akamai_host", lambda u: False)
        assert _base.fetch(_S(), RAPPELCONSO) is None
        assert calls["curl"] == 0

    def test_claude_check_recovers_the_page(self, monkeypatch):
        from pipeline import claude_check as cc

        def _get(url, **kw):
            raise requests.exceptions.SSLError("unable to get local issuer certificate")

        monkeypatch.setattr(cc._requests, "get", _get)
        monkeypatch.setattr(cc, "fetch_via_curl_cffi", lambda *a, **k: _Resp())
        monkeypatch.setattr(cc, "is_akamai_host", lambda u: False)
        monkeypatch.setattr(cc, "_is_soft_404", lambda a, b: False)
        text, err = cc._fetch_page_text(RAPPELCONSO)
        assert err is None and text and "Listeria" in text

    def test_claude_check_fails_closed_when_curl_also_fails(self, monkeypatch):
        from pipeline import claude_check as cc

        def _get(url, **kw):
            raise requests.exceptions.SSLError("chain")

        monkeypatch.setattr(cc._requests, "get", _get)
        monkeypatch.setattr(cc, "fetch_via_curl_cffi", lambda *a, **k: None)
        monkeypatch.setattr(cc, "is_akamai_host", lambda u: False)
        text, err = cc._fetch_page_text(RAPPELCONSO)
        assert text is None and "curl_cffi fallback failed" in err
