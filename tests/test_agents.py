"""The curator must refuse everything that got through before.

Each test below is a real incident from 2026-08. If one of them ever passes
by accident, the agent loop has become the thing it was built to prevent.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.agents._contract import (
    Evidence, Proposal, read_proposals, write_proposals,
)
from pipeline.agents import curator as C


def _p(action="enrich", url="https://example.org/x", changes=None,
       kinds=("regulator_page",), conf=0.9):
    return Proposal(
        action=action, target={"url": url}, changes=changes or {"Reason": "x"},
        reason="test", confidence=conf, agent="test",
        evidence=[Evidence(kind=k, url=url, quote="q") for k in kinds])


# ── the contract refuses before anything touches the network ─────────────

def test_a_proposal_with_no_evidence_is_structurally_invalid():
    p = _p(kinds=())
    assert "no evidence at all" in p.structural_problems()


def test_model_inference_alone_cannot_move_a_row():
    p = _p(kinds=("model_inference",))
    probs = " ".join(p.structural_problems())
    assert "cannot move a row" in probs
    assert C.apply_one(p, apply=False, offline=True).applied is False


def test_model_inference_may_still_raise_a_flag():
    p = Proposal(action="flag", target={"news_url": "https://n"},
                 reason="r", confidence=0.5, agent="t",
                 evidence=[Evidence(kind="model_inference", quote="q")])
    assert p.structural_problems() == []


def test_promote_without_a_url_is_refused():
    p = Proposal(action="promote", target={}, reason="r", confidence=0.9,
                 agent="t", evidence=[Evidence(kind="regulator_page", quote="q")])
    assert any("no target URL" in x for x in p.structural_problems())


def test_an_unknown_action_is_refused():
    assert any("unknown action" in x for x in _p(action="delete").structural_problems())


def test_proposal_id_is_stable_and_content_addressed():
    a = _p(changes={"Reason": "same"})
    b = _p(changes={"Reason": "same"})
    c = _p(changes={"Reason": "different"})
    assert a.proposal_id == b.proposal_id != c.proposal_id


# ── the checks themselves ────────────────────────────────────────────────

def test_protected_fields_are_unreachable_by_an_agent():
    for field in ("Tier", "Outbreak", "report_week", "DateAdded"):
        assert field not in C.WRITABLE, (
            f"{field} drives published statistics and must not be agent-writable")


def test_scope_check_rejects_an_empty_pathogen():
    assert C.check_scope({"Pathogen": "", "Product": "p"})


def test_scope_check_rejects_pet_food():
    bad = C.check_scope({"Pathogen": "Salmonella",
                         "Product": "Turkey Pate Wet Food for Dogs",
                         "Company": "Fromm Family Foods", "Reason": "Salmonella"})
    assert any("pet food" in x for x in bad), bad


def test_language_check_catches_the_rows_that_slipped_through():
    for reason in ("Présence possible de Listeria monocytogenes",
                   "Possibile presenza di Listeria monocytogenes"):
        assert C.check_language({"Reason": reason, "Company": "X"}), reason


def test_language_check_allows_a_non_english_product_name():
    """Brand and product names are exempt — that is the operator rule."""
    assert not C.check_language(
        {"Reason": "Presence of Listeria monocytogenes",
         "Company": "Vitafrais", "Product": "Brie au lait pasteurisé"})


def test_url_content_match_rejects_a_page_about_something_else(monkeypatch):
    """The SHEIN plush toy incident, reproduced.

    Row says Brie / Listeria; the fiche it cites is a soft-toy recall. The
    URL resolves and the gate is clean — only content matching catches it.
    """
    class R:
        status_code = 200
        text = ("<html><body>Peluche de 12 pouces style ane, marque SHEIN. "
                "Risque d'etouffement pour les jeunes enfants.</body></html>")

    monkeypatch.setattr(C, "__name__", C.__name__)
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: R())
    bad = C.check_url_resolves_and_matches(
        "https://rappel.conso.gouv.fr/fiche-rappel/22230/Interne",
        {"Pathogen": "Listeria monocytogenes", "Company": "VITAFRAIS",
         "Brand": "Osé Bio", "Product": "Brie au lait pasteurisé"})
    assert bad and "describes something other than this row" in bad[0]


def test_url_content_match_accepts_the_matching_page(monkeypatch):
    class R:
        status_code = 200
        text = ("<html>Roti de boeuf cuit rappele par CARREFOUR — presence de "
                "Listeria monocytogenes</html>")
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: R())
    assert C.check_url_resolves_and_matches(
        "https://rappel.conso.gouv.fr/fiche-rappel/23343/Interne",
        {"Pathogen": "Listeria monocytogenes", "Company": "CARREFOUR FRANCE",
         "Product": "Roti de boeuf cuit"}) == []


def test_a_dead_url_is_refused_not_warned(monkeypatch):
    class R:
        status_code = 404
        text = ""
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: R())
    bad = C.check_url_resolves_and_matches("https://x/fsa-prn-40-2026",
                                           {"Pathogen": "Salmonella"})
    assert bad and "404" in bad[0]


# ── round trip ───────────────────────────────────────────────────────────

def test_proposals_round_trip(tmp_path: Path):
    ps = [_p(), _p(action="flag", kinds=("news_article",))]
    f = write_proposals(ps, "test", out_dir=tmp_path)
    back = read_proposals(f)
    assert [x.proposal_id for x in back] == [x.proposal_id for x in ps]
    assert back[0].evidence[0].kind == "regulator_page"


def test_a_future_schema_is_refused(tmp_path: Path):
    f = tmp_path / "p.json"
    f.write_text(json.dumps({"schema": 99, "proposals": []}))
    with pytest.raises(SystemExit):
        read_proposals(f)


def test_link_event_needs_hard_evidence():
    p = Proposal(action="link_event", target={"url": "https://x"},
                 changes={"candidate_event": "https://n"}, reason="r",
                 confidence=0.7, agent="t",
                 evidence=[Evidence(kind="news_article", url="https://n", quote="q")])
    # structurally it is refused for soft-evidence-only
    assert any("cannot move a row" in x for x in p.structural_problems())
