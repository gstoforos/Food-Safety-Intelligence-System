# -*- coding: utf-8 -*-
"""A 200 that is not the record is not a live URL.

rappel.conso.gouv.fr answers HTTP 200 for a fiche that does not exist,
serving its generic listing page instead of a 404. Every check in
review/url_validator.py was status-based, so a withdrawn fiche read as
alive.

THE ROW THAT FOUND IT. Recalls carried the PINTON Sardinella pâté recall
(Clostridium botulinum, fiche 24039, published 06/11/2026) at

    https://rappel.conso.gouv.fr/fiche-rappel/24039/interne

The regulator withdrew that fiche and re-published the same recall — same
firm, same product, same Motif word for word — as fiche 24086 on
13/11/2026. The url_guardian checks every four hours and saw HTTP 200
every time. The dead URL stood in the published register for eight days
and was found by reading the page, not by the pipeline.

The repair is deliberately narrow: ONE host, ONE measured title, and a
soft 404 is never verified_dead, so the guardian flags it for a human the
way it flags a generic URL and never blanks a URL on a content match.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from review.url_validator import (  # noqa: E402
    _SOFT_404_TITLES, body_is_soft_404, check_url, host_serves_soft_404,
)

FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/24039/interne"
LIVE = "https://rappel.conso.gouv.fr/fiche-rappel/24086/interne"
STANDIN = ("<html><head><title>Rappel Consommateur - Accueil</title></head>"
           "<body>Produits rappelés dans toutes les catégories</body></html>")
REAL = ("<html><head><title>Pâté de Sardinelle PINTON en 180 g PINTON"
        "</title></head><body>Motif du rappel : Gonflement de certaines "
        "boites lié à un problème au sertissage.</body></html>")


def test_the_host_that_found_this_is_registered():
    assert "rappel.conso.gouv.fr" in _SOFT_404_TITLES
    assert host_serves_soft_404(FICHE)
    assert host_serves_soft_404("https://www.rappel.conso.gouv.fr/x")


def test_an_unregistered_host_is_never_content_checked():
    """A wrong content match deletes a live URL, so only measured hosts."""
    for url in ("https://www.fsis.usda.gov/recalls-alerts/whatever",
                "https://www.lebensmittelwarnung.de/x.html",
                "https://webgate.ec.europa.eu/rasff-window/screen/notification/1"):
        assert not host_serves_soft_404(url)
        assert not body_is_soft_404(url, STANDIN)


def test_the_standin_page_is_a_soft_404():
    assert body_is_soft_404(FICHE, STANDIN)


def test_the_real_fiche_is_not_a_soft_404():
    assert not body_is_soft_404(LIVE, REAL)


@pytest.mark.parametrize("body", ["", "   ", None])
def test_an_empty_body_decides_nothing(body):
    assert not body_is_soft_404(FICHE, body)


def test_a_bare_title_string_works_too():
    """`body` may be the whole HTML or just the title the caller read."""
    assert body_is_soft_404(FICHE, "Rappel Consommateur - Accueil")


class _Resp:
    def __init__(self, status, text=""):
        self.status_code = status
        self.text = text

    def close(self):
        pass


def _patch(monkeypatch, body):
    import review.url_validator as v
    monkeypatch.setattr(v.requests, "head",
                        lambda *a, **k: _Resp(200), raising=False)
    monkeypatch.setattr(v.requests, "get",
                        lambda *a, **k: _Resp(200, body), raising=False)


def test_check_url_reports_a_soft_404_as_generic_not_ok(monkeypatch):
    _patch(monkeypatch, STANDIN)
    out = check_url(FICHE)
    assert out["reason"] == "generic"
    assert out["ok"] is False
    assert out["generic"] is True
    # NEVER blanked on a content match — a human decides.
    assert out["verified_dead"] is False


def test_check_url_leaves_a_real_fiche_alone(monkeypatch):
    _patch(monkeypatch, REAL)
    out = check_url(LIVE)
    assert out["reason"] == "ok"
    assert out["ok"] is True


def test_a_body_that_cannot_be_read_does_not_condemn_the_url(monkeypatch):
    """A failed GET must leave the 200 standing, not invent a death."""
    import review.url_validator as v

    def _boom(*a, **k):
        raise RuntimeError("network")

    monkeypatch.setattr(v.requests, "head",
                        lambda *a, **k: _Resp(200), raising=False)
    monkeypatch.setattr(v.requests, "get", _boom, raising=False)
    out = check_url(FICHE)
    assert out["reason"] == "ok"
    assert out["verified_dead"] is False
