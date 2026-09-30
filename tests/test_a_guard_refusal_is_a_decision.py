"""A url-guard refusal is a DECISION, not an outage (2026-09-30).

The livelock this pins
----------------------
On 2026-09-29 every reviewer-1 run exited 3. Searx returned "results": [] for
every query, so the model answered "No official regulator URL found" for four
RappelConso rows, and the url-guard correctly refused each rejection — the
authority URL was on the row. Reviewer 1 counted each refusal as "retry",
saw retry == rows, printed NO REVIEW PERFORMED and returned 3 ABOVE the
write-back. The refusal tags it had already put into row["Notes"] were thrown
away, so refusal_count() never saw a prior refusal, `_prior >= 1` was never
true, and the 2026-09-25 escalation — built to stop exactly this repetition —
could never fire.

Three things are held here, each as a sweep rather than a spot check:

1. Behaviour: a run whose only decisions are guard refusals WRITES BACK, and
   the second run escalates. A run in which nothing was decided still exits 3
   and writes nothing.
2. Shape, across every reviewer source on disk (pipeline/*.py AND every
   workflow heredoc): no guard-refusal branch increments a retry counter.
3. Shape, across every workflow: no Searx health check passes on the mere
   presence of a "results" key — it must count results.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
WF = ROOT / ".github" / "workflows"

OFFICIAL = "https://rappel.conso.gouv.fr/fiche-rappel/{}/interne"


# ─────────────────────────────────────────────────────────────────────────
# 1. behaviour
# ─────────────────────────────────────────────────────────────────────────

def _workbook(path: Path, rows):
    from pipeline.merge_master import save_xlsx_with_pending
    save_xlsx_with_pending([], rows, path)


def _pending(path: Path):
    from pipeline import recall_url_agent as a
    return {r["URL"]: r for r in a._load_sheet(path, "Pending")}


def _row(n: int, status: str = "pending_gap"):
    return {"Date": "2026-09-28", "Company": f"Co {n}",
            "Product": f"Fromage {n}", "Pathogen": "Listeria monocytogenes",
            "Reason": "Listeria", "Country": "France",
            "Source": "RappelConso", "URL": OFFICIAL.format(23640 + n),
            "Status": status, "Notes": ""}


@pytest.fixture
def agent(monkeypatch):
    from pipeline import recall_url_agent as a
    monkeypatch.setattr(a, "_provenance_ok", lambda row, url: [])
    return a


def _run(a, monkeypatch, xlsx: Path, verdict):
    monkeypatch.setattr(a, "review_url", verdict)
    monkeypatch.setattr(sys, "argv", ["recall_url_agent", "--xlsx", str(xlsx),
                                      "--commit", "true"])
    return a.main()


def _searx_empty(row):
    return {"decision": "reject", "official_url": row["URL"],
            "reason": "No official regulator URL found",
            "pathogen_if_found": "", "identity_matches": False}


def _infra(row):
    return {"decision": "retry", "official_url": row["URL"],
            "reason": "INFRA: no llama response (retry)",
            "pathogen_if_found": "", "identity_matches": False}


def test_all_refused_writes_back_and_the_second_run_escalates(
        agent, monkeypatch, tmp_path):
    xlsx = tmp_path / "recalls.xlsx"
    _workbook(xlsx, [_row(1), _row(2), _row(3), _row(4)])

    # Run 1: every rejection refused. Must NOT be "no review performed".
    assert _run(agent, monkeypatch, xlsx, _searx_empty) == 0
    after1 = _pending(xlsx)
    assert len(after1) == 4, "a refused row must stay in Pending"
    for r in after1.values():
        assert agent.refusal_count(r["Notes"]) == 1, (
            "the refusal tag must reach disk — it is the only memory the "
            "escalation has")
        assert str(r["Status"]).lower() == "pending_gap", (
            "a first refusal does not move the row")

    # Run 2: the same answer again. The escalation must now fire.
    assert _run(agent, monkeypatch, xlsx, _searx_empty) == 0
    after2 = _pending(xlsx)
    for r in after2.values():
        assert str(r["Status"]).lower() == "pending_gap_v1", (
            "second refusal on a self-evidently official URL escalates one "
            "step through next_status — this is what the livelock blocked")
        assert "refused AND escalated" in r["Notes"]


def test_nothing_decided_still_exits_3_and_writes_nothing(
        agent, monkeypatch, tmp_path):
    xlsx = tmp_path / "recalls.xlsx"
    _workbook(xlsx, [_row(1), _row(2)])
    before = xlsx.read_bytes()
    assert _run(agent, monkeypatch, xlsx, _infra) == 3
    assert xlsx.read_bytes() == before, "a true outage must write nothing"


def test_a_mixed_run_counts_refusals_as_decisions(agent, monkeypatch,
                                                  tmp_path):
    xlsx = tmp_path / "recalls.xlsx"
    _workbook(xlsx, [_row(1), _row(2)])
    calls = iter([_infra, _searx_empty])
    assert _run(agent, monkeypatch, xlsx,
                lambda row: next(calls)(row)) == 0
    notes = [r["Notes"] for r in _pending(xlsx).values()]
    assert sum(agent.refusal_count(n) for n in notes) == 1


# ─────────────────────────────────────────────────────────────────────────
# 2. shape sweep — every reviewer source, including the heredocs that run
# ─────────────────────────────────────────────────────────────────────────

_CALL = re.compile(r"_ref\s*=\s*_?reject_refusal\(")


def _reviewer_sources():
    """Every file that CALLS the guard from a reviewer loop (not the module
    that defines it)."""
    out = []
    for p in sorted((ROOT / "pipeline").glob("*.py")):
        t = p.read_text(encoding="utf-8")
        if _CALL.search(t):
            out.append((p.relative_to(ROOT).as_posix(), t))
    for p in sorted(WF.glob("*.yml")):
        t = p.read_text(encoding="utf-8")
        if _CALL.search(t):
            out.append((p.relative_to(ROOT).as_posix(), t))
    return out


def _refusal_branches(text: str):
    """Each `if _ref:` block that follows a reject_refusal() call, up to the
    next line at the same or shallower indentation."""
    lines = text.split("\n")
    for i, l in enumerate(lines):
        if not _CALL.search(l):
            continue
        for j in range(i + 1, min(i + 6, len(lines))):
            m = re.match(r"(\s*)if _ref:", lines[j])
            if m:
                ind = len(m.group(1))
                k = j + 1
                while k < len(lines) and (not lines[k].strip() or
                                          len(lines[k]) - len(lines[k].lstrip())
                                          > ind):
                    k += 1
                yield "\n".join(lines[j:k])
                break


SOURCES = _reviewer_sources()


def test_the_sweep_found_the_reviewers():
    names = {n for n, _ in SOURCES}
    assert "pipeline/recall_url_agent.py" in names
    assert ".github/workflows/recall-url-agent.yml" in names
    assert len(SOURCES) >= 3


@pytest.mark.parametrize("name,text", SOURCES, ids=[n for n, _ in SOURCES])
def test_no_guard_refusal_is_counted_as_retry(name, text):
    branches = list(_refusal_branches(text))
    assert branches, f"{name}: calls reject_refusal but no `if _ref:` found"
    for b in branches:
        assert not re.search(r"""counts\[["']retry["']\]\s*=|"""
                             r"""results\[["']retry["']\]\.append""", b), (
            f"{name}: a url-guard refusal is counted as retry. A refusal is "
            f"a decision; counting it as retry is what made reviewer 1 exit 3 "
            f"above its write-back and lose the refusal notes (2026-09-29).")


# ─────────────────────────────────────────────────────────────────────────
# 3. Searx: reachable is not useful — sweep every workflow
# ─────────────────────────────────────────────────────────────────────────

_SEARX_WFS = sorted(p for p in WF.glob("*.yml")
                    if re.search(r'curl[^\n]*(\\\n[^\n]*)*\$SEARX_URL',
                                 p.read_text(encoding="utf-8")))


def test_the_searx_sweep_found_the_health_checks():
    names = {p.name for p in _SEARX_WFS}
    assert {"recall-url-agent.yml", "recall-review-agent.yml"} <= names


@pytest.mark.parametrize("wf", _SEARX_WFS, ids=lambda p: p.name)
def test_searx_health_check_counts_results(wf):
    t = wf.read_text(encoding="utf-8")
    assert "grep -q '\"results\"'" not in t, (
        f"{wf.name}: '\"results\": []' contains the string \"results\" — an "
        f"empty Searx passes this check")
    assert not re.search(r'\$SEARX_URL[^\n]*\|\s*head -c', t), (
        f"{wf.name}: piping any answer to head is a reachability check, not "
        f"a usefulness check")
    assert 'len(json.load(sys.stdin).get("results")' in t, (
        f"{wf.name}: the Searx health check must count results")
