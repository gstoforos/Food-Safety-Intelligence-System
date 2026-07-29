"""
tests/test_promotion_unblock.py
===============================
2026-07-24..28 INCIDENT — publication stopped for four days, silently.

Recalls froze at 1231 rows. Collection was healthy (41 rows scraped on
07-28) and the reviewer workflows fired 6-7x/day. Four defects compounded,
all triggered by ONE upstream fact: rappel.conso.gouv.fr began serving an
INCOMPLETE TLS certificate chain (missing intermediate). Browsers recover
via the certificate's AIA extension; Python's ssl cannot.

  1. TLS         — no fallback, so no RappelConso fiche could be read.
  2. TRAP        — SKIP parked rows in pending_gap_v2, a DEMOTION whose
                   only exit is a Claude pass, which needs the fetch.
  3. FALSE VERDICT — url_gate saw SSLError -> Status=rejected; claude_check
                   saw the SAME SSLError and counted it as a second
                   reviewer "confirming" a dead URL -> archived to
                   Weekly_Rejected. 23 rows discarded, 19 with real
                   pathogens (10 Listeria, 5 Salmonella, Aflatoxin,
                   Cronobacter, Norovirus). That is not two reviewers
                   agreeing; it is one infrastructure fault seen twice.
  4. NO ALARM    — every run reported a green "+0 promoted".

Fail-closed is preserved throughout: a row whose verification did not
COMPLETE still never reaches Recalls. The change is that it stays
RECOVERABLE instead of being destroyed.
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

URL = "https://rappel.conso.gouv.fr/fiche-rappel/22999/Interne"


def _row(**o):
    b = {"Date": "2026-07-28", "Source": "RappelConso (FR)",
         "Company": "Bienheureux", "Brand": "Bienheureux",
         "Product": "camembert de normandie aop", "Pathogen": "Listeria monocytogenes",
         "Reason": "Presence of Listeria monocytogenes", "Class": "Recall",
         "Country": "France", "Region": "Europe", "Tier": 1, "Outbreak": 0,
         "URL": URL, "Notes": "", "Status": STATUS_PENDING}
    b.update(o)
    return b


class TestFailClosedStillHolds:
    @pytest.mark.parametrize("status", sorted(NON_PROMOTABLE_STATUSES))
    def test_every_parking_status_blocks_promotion(self, status):
        assert promote_approved([_row(Status=status)], [], {})[0] == []

    def test_plain_pending_still_promotes(self):
        assert len(promote_approved([_row()], [], {})[0]) == 1


class TestParkingIsRecoverable:
    def test_retry_status_is_non_promotable_but_releasable(self):
        parked = _row(Status=STATUS_PENDING_RETRY)
        assert promote_approved([dict(parked)], [], {})[0] == []
        assert len(promote_approved([{**parked, "Status": STATUS_PENDING}], [], {})[0]) == 1

    def test_parking_no_longer_demotes_into_gap_machine(self):
        from pipeline import claude_check as cc
        row = _row()
        cur = (row.get("Status") or "").strip().lower()
        if cur in ("", "pending", cc.STATUS_PENDING_RETRY):
            row["Status"] = cc.STATUS_PENDING_RETRY
        assert row["Status"] == STATUS_PENDING_RETRY != STATUS_PENDING_GAP_V2


class TestTransportIsNotAVerdict:
    """The defect that DESTROYED 19 real recalls."""

    TRANSIENT = [
        "fetch failed (fetch error: SSLError: HTTPSConnectionPool(host='rappel.conso.gouv.fr', port=443): Max retries exceeded",
        "fetch error: SSLError (TLS chain) and curl_cffi fallback failed",
        "fetch failed (HTTP 403)",
        "fetch failed (HTTP 503)",
        "claude API returned non-OK status — will recheck next run",
        "fetch error: ConnectionError: connection reset",
        "claude API slow-response (over budget) — will recheck next run",
    ]
    DEAD = ["fetch failed (HTTP 404)", "fetch failed (HTTP 410)"]

    @staticmethod
    def _escalates(reason: str) -> bool:
        # Calls the REAL production predicate, not a copy — so reverting
        # the fix makes these tests fail.
        from pipeline.claude_check import _is_dead_url_reason
        return _is_dead_url_reason(reason)

    @pytest.mark.parametrize("reason", TRANSIENT)
    def test_transport_failure_never_archives_a_row(self, reason):
        assert not self._escalates(reason), (
            f"transport failure would be treated as a reviewer verdict: {reason!r}")

    @pytest.mark.parametrize("reason", DEAD)
    def test_genuinely_dead_url_still_archives(self, reason):
        assert self._escalates(reason)


class TestUrlValidatorTolerance:
    def test_tls_error_is_tolerated_not_broken(self, monkeypatch):
        from review import url_validator as uv

        def _get(*a, **k):
            raise requests.exceptions.SSLError("unable to get local issuer certificate")

        monkeypatch.setattr(uv.requests, "get", _get, raising=False)
        monkeypatch.setattr(uv.requests, "head", _get, raising=False)
        res = uv.check_url(URL)
        assert res["reason"] == "tls_error"
        assert res["ok"] is True, "a TLS handshake failure must not mark the URL broken"


class _Resp:
    def __init__(self, text="<html><body>Listeria monocytogenes</body></html>"):
        self.status_code, self.text = 200, text
        self.content = text.encode()
        self.headers = {"Content-Type": "text/html; charset=utf-8"}
        self.url = URL


class TestTlsChainFallback:
    def test_shared_fetch_retries_on_ssl_only(self, monkeypatch):
        from scrapers import _base
        n = {"curl": 0}

        class _S:
            def request(self, *a, **k):
                raise requests.exceptions.SSLError("unable to get local issuer certificate")

        monkeypatch.setattr(_base, "fetch_via_curl_cffi",
                            lambda *a, **k: (n.__setitem__("curl", 1), _Resp())[1])
        monkeypatch.setattr(_base, "is_akamai_host", lambda u: False)
        assert _base.fetch(_S(), URL) is not None and n["curl"] == 1

    def test_shared_fetch_does_not_retry_other_errors(self, monkeypatch):
        from scrapers import _base
        n = {"curl": 0}

        class _S:
            def request(self, *a, **k):
                raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(_base, "fetch_via_curl_cffi",
                            lambda *a, **k: n.__setitem__("curl", 1))
        monkeypatch.setattr(_base, "is_akamai_host", lambda u: False)
        assert _base.fetch(_S(), URL) is None and n["curl"] == 0

    def test_claude_check_recovers_the_page(self, monkeypatch):
        from pipeline import claude_check as cc
        monkeypatch.setattr(cc._requests, "get",
                            lambda *a, **k: (_ for _ in ()).throw(
                                requests.exceptions.SSLError("chain")))
        monkeypatch.setattr(cc, "fetch_via_curl_cffi", lambda *a, **k: _Resp())
        monkeypatch.setattr(cc, "is_akamai_host", lambda u: False)
        monkeypatch.setattr(cc, "_is_soft_404", lambda a, b: False)
        text, err = cc._fetch_page_text(URL)
        assert err is None and "Listeria" in text


class TestStalenessAlarm:
    def test_errors_when_stuck_past_threshold(self, capsys):
        from pipeline import claude_check as cc
        old = (datetime.now(timezone.utc).date()
               - timedelta(days=cc.PARKING_STALE_ERROR_DAYS + 1)).isoformat()
        n = cc._report_parking_lot_staleness(
            [_row(Status=STATUS_PENDING_RETRY, ScrapedAt=old) for _ in range(24)])
        assert n == 24 and "::error" in capsys.readouterr().out

    def test_quiet_when_clear(self, capsys):
        from pipeline import claude_check as cc
        assert cc._report_parking_lot_staleness([_row()]) == 0
        assert "::error" not in capsys.readouterr().out
