"""No Gemini anywhere (operator ruling 2026-09-30).

    "gemini must not be anywhere my agents have replaced them"

Review, URL resolution, gap-finding and HTML extraction run on our own model
(Qwen on the VPS): recall_url_agent, recall_review_agent,
recall_confirm_agent, the gap_finder_fleet, and scrapers/_base._call_llama.

Before this, Gemini was still live in five places nobody was looking at:
the scrapers fell back to it whenever the VPS was down; the enrichment pass
sent incomplete rows to it; the synthesis writer used it as a second
backend; url-resurrect (dispatched daily by FsisScheduler.gs) was Gemini-
grounded; and gap-row-gating-chain / perplexity-gap-finder still ran
url_gate_gemini. Twelve live workflows passed GEMINI_* keys.

The Gemini-only modules are listed in RETIRED below. They are to be deleted
in the GitHub UI; until they are, nothing may import or run them, and this
test passes both before and after the deletion.

Mentions that describe history (why a guard exists, how an old Gemini
defect looked, parsing tags old rows still carry) are not calls and are
left alone. What is banned is anything that can reach Google's model.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows"

# Gemini-only code. DELETE these in the GitHub UI.
RETIRED_PY = {
    "enrichment/gemini_client.py",
    "gap_finder_gemini.py",
    "url_gate_gemini.py",
    "pipeline/gap_finder_gemini.py",
    "pipeline/gemini_check.py",
    "pipeline/gemini_client.py",
    "pipeline/sunday_gemini_qa.py",
    "pipeline/url_gate_gemini.py",
    "pipeline/url_gate_claude.py",       # only delegated to url_gate_gemini
    "pipeline/url_resurrect.py",         # Gemini-grounded URL repair
    "pipeline/resolve_dead_urls.py",     # Gemini-grounded URL repair
    "pipeline/fix_scraper_urls.py",      # Gemini-grounded URL repair
    "pipeline/gap_finder_cascade.py",    # Gemini free -> paid -> ...
    "review/gemini_reviewer.py",
    "tools/simulate_validators.py",      # simulated url_gate_gemini
    "reports/gap_finder_tavily.py",      # stale copy with a Gemini extractor
}
RETIRED_WF = {
    "gemini-check.yml", "gemini-gap-finder.yml", "gemini-sunday-qa.yml",
    "gemini-url-gate.yml", "gap-finder-cascade.yml", "gap-row-gating-chain.yml",
    "claude-gap-finder.yml", "openai-gap-finder.yml", "perplexity-gap-finder.yml",
    "url-resurrect.yml", "resolve-dead-urls.yml",
}
RETIRED_MODULES = sorted({p[:-3].replace("/", ".") for p in RETIRED_PY}
                         | {"gap_finder_gemini", "url_gate_gemini"})

# Anything that can reach Google's model.
CALLS = re.compile(
    r"from\s+google\s+import\s+genai|google\.genai|google\.generativeai|"
    r"import\s+google\.generativeai|generativelanguage\.googleapis|genai\.Client")


def _live_py():
    for p in ROOT.rglob("*.py"):
        rel = p.relative_to(ROOT).as_posix()
        if rel.startswith(("tests/", "docs/data/", ".git/")) or "/_attic/" in rel:
            continue
        if rel in RETIRED_PY:
            continue
        yield rel, p.read_text(encoding="utf-8", errors="replace")


def _live_workflows():
    for p in sorted(WF.glob("*.yml")):
        if p.name in RETIRED_WF:
            continue
        yield p.name, p.read_text(encoding="utf-8")


def _code_lines(text):
    return [l for l in text.splitlines() if not l.lstrip().startswith("#")]


def test_no_live_code_can_reach_gemini():
    hits = [f"{rel}: {m.group(0)}" for rel, src in _live_py()
            for line in _code_lines(src) for m in [CALLS.search(line)] if m]
    assert not hits, hits


def test_nothing_imports_a_retired_module():
    pat = re.compile(r"^\s*(?:from|import)\s+(%s)\b|^\s*from\s+(pipeline|enrichment|review|tools|reports)"
                     r"\s+import\s+[^#\n]*\b(%s)\b" % (
                         "|".join(re.escape(m) for m in RETIRED_MODULES),
                         "|".join(re.escape(m.split(".")[-1]) for m in RETIRED_MODULES)))
    hits = [f"{rel}: {l.strip()}" for rel, src in _live_py()
            for l in src.splitlines() if pat.search(l)]
    assert not hits, hits


def test_no_live_workflow_runs_a_retired_module():
    names = "|".join(re.escape(m) for m in RETIRED_MODULES)
    paths = "|".join(re.escape(p) for p in RETIRED_PY)
    pat = re.compile(r"python3?\s+-m\s+(%s)\b|(%s)" % (names, paths))
    hits = [f"{n}: {l.strip()}" for n, t in _live_workflows()
            for l in _code_lines(t) if pat.search(l)]
    assert not hits, hits


def test_no_workflow_passes_a_gemini_key_or_installs_its_sdk():
    pat = re.compile(r"secrets\.GEMINI|vars\.GEMINI|^\s*GEMINI_[A-Z0-9_]*\s*:|"
                     r"google-genai|google-generativeai", re.M)
    hits = [f"{p.name}: {l.strip()}" for p in sorted(WF.glob("*.yml"))
            for l in _code_lines(p.read_text(encoding="utf-8")) if pat.search(l)]
    assert not hits, hits


@pytest.mark.parametrize("name", sorted(RETIRED_WF))
def test_a_retired_workflow_that_still_exists_does_nothing(name):
    p = WF / name
    if not p.exists():
        return
    body = "\n".join(_code_lines(p.read_text(encoding="utf-8")))
    assert "retired:" in body and "python" not in body, name


def test_requirements_carry_no_gemini_sdk():
    req = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert not re.search(r"^\s*google-(genai|generativeai)\b", req, re.M)


def test_scraper_extraction_never_falls_back_to_gemini(monkeypatch):
    import scrapers._base as b

    def boom(*a, **k):
        raise RuntimeError("VPS down")

    called = []
    monkeypatch.setattr(b, "_call_llama", boom)
    monkeypatch.setattr(b, "_call_gemini", lambda *a, **k: called.append(1) or "[]")
    monkeypatch.setattr(b, "_openai_api_keys", lambda: [])
    with pytest.raises(RuntimeError):
        b._call_llm("p", "<html></html>")
    assert not called, "a VPS outage must not hand the page to Gemini"


def test_the_retired_helper_never_contacts_google():
    import scrapers._base as b
    with pytest.raises(RuntimeError, match="retired"):
        b._call_gemini("p", "<html></html>")


def test_the_scraper_class_is_not_named_for_gemini():
    import scrapers._base as b
    assert hasattr(b, "GenericLLMScraper")
    assert not hasattr(b, "GenericGeminiScraper")


def test_the_synthesis_writer_has_no_gemini_backend():
    from pipeline.synthesis_writer import _SYNTHESIS_BACKENDS
    assert "gemini" not in [n for n, _ in _SYNTHESIS_BACKENDS]


def test_enrichment_does_not_import_a_model_client():
    src = (ROOT / "enrichment" / "enrich_rows.py").read_text(encoding="utf-8")
    assert "gemini_client" not in "\n".join(_code_lines(src))
