# -*- coding: utf-8 -*-
"""The alert vocabulary contract.

alerts.html offers a subscriber a word. The Apps Script decides whether a
register row matches it. These tests hold the two ends together:

  * every word offered is a word the matcher has an explicit token list for
  * every word offered is PRODUCIBLE — the pipeline can actually emit a row
    that matches it, either because the label is in PATHOGEN_RULES / _TIERS or
    because rows with that hazard exist today
  * no word the 2026-09-14 audit retired creeps back in
  * the generated files are in sync with tools/alert_vocab.py
  * the 13 Sep defect stays fixed: a "Clostridium botulinum" rule never
    matches Clostridium perfringens
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import alert_vocab as V  # noqa: E402

GS = os.path.join(ROOT, "tools", "apps_script", "AftsAlerts.gs")
GS_BASE = os.path.join(ROOT, "tools", "apps_script", "AftsAlerts.base.gs")
HTML = os.path.join(ROOT, "docs", "alerts.html")
RECALLS = os.path.join(ROOT, "docs", "data", "recalls.json")

# The merged file is the whole Apps Script project, so evaluating it under node
# needs the Apps Script globals to exist. None of them is called by the
# vocabulary self-test; they only have to be defined.
_GAS_STUBS = (
    "global.SpreadsheetApp={};global.UrlFetchApp={};global.Utilities={};"
    "global.ContentService={};global.HtmlService={};global.ScriptApp={};"
)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rows():
    with open(RECALLS, encoding="utf-8") as fh:
        return json.load(fh)


def _js_array_from(path, name):
    """Pull a flat string array out of a .gs / .html file by variable name."""
    src = open(path, encoding="utf-8").read()
    m = re.search(r"(?:var|const)\s+%s\s*=\s*\[(.*?)\n\];" % re.escape(name),
                  src, re.S)
    assert m, "%s not found in %s" % (name, os.path.basename(path))
    return re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1))


def _js_map_keys_from(path, name):
    src = open(path, encoding="utf-8").read()
    m = re.search(r"var\s+%s\s*=\s*\{(.*?)\n\};" % re.escape(name), src, re.S)
    assert m, "%s not found in %s" % (name, os.path.basename(path))
    return re.findall(r'^\s{2}"((?:[^"\\]|\\.)*)":', m.group(1), re.M)


# ---------------------------------------------------------------------------
# the generated files are current
# ---------------------------------------------------------------------------

def test_generated_files_are_in_sync():
    r = subprocess.run(
        [sys.executable, os.path.join("tools", "gen_alert_vocab.py"), "--check"],
        cwd=ROOT, capture_output=True, text=True)
    assert r.returncode == 0, (
        "alerts.html / AftsAlerts.gs are stale.\n" + r.stdout + r.stderr)


def test_merged_gs_keeps_every_function_from_the_deployed_base():
    """The generated file is the deployed script PLUS the engine, never minus.

    Losing a helper here is not a test failure in production — it is an alert
    mailer that throws on its next scan. The base file is the operator's
    2026-07-25 script verbatim; only two of its functions are replaced.
    """
    base = open(GS_BASE, encoding="utf-8").read()
    full = open(GS, encoding="utf-8").read()
    fn = lambda src: set(re.findall(r"^function\s+(\w+)", src, re.M))
    const = lambda src: set(re.findall(r"^const\s+(\w+)", src, re.M))
    missing = sorted(fn(base) - fn(full))
    assert not missing, "merge dropped function(s): %s" % missing
    missing_c = sorted(const(base) - const(full))
    assert not missing_c, "merge dropped const(s): %s" % missing_c
    for name in ("recallMatchesCriterion_", "sendAlertMatchEmail_"):
        n = len(re.findall(r"^function\s+%s\b" % name, full, re.M))
        assert n == 1, "%s defined %d times — Apps Script picks one at random" % (name, n)


def test_merged_gs_has_no_first_token_truncation():
    """The exact shape of the 13 Sep bug, in CODE (comments may quote it)."""
    full = open(GS, encoding="utf-8").read()
    code = "\n".join(l for l in full.splitlines()
                     if not l.lstrip().startswith(("*", "//", "/*")))
    for bug in ("v.split(/[\\s/()]/)[0]", "v.split(/[\\s—()-]/)[0]"):
        assert bug not in code, "first-token truncation is back: %s" % bug


def test_html_and_gs_offer_the_same_words():
    assert _js_array_from(HTML, "PATHOGENS") == _js_map_keys_from(GS, "PATHOGEN_VOCAB")
    assert _js_array_from(HTML, "PRODUCTS") == _js_map_keys_from(GS, "PRODUCT_VOCAB")
    assert _js_array_from(HTML, "COUNTRIES") == _js_array_from(GS, "COUNTRY_LIST")


def test_html_words_match_the_python_source_of_truth():
    assert _js_array_from(HTML, "PATHOGENS") == list(V.PATHOGEN)
    assert _js_array_from(HTML, "PRODUCTS") == list(V.PRODUCT)
    assert _js_array_from(HTML, "COUNTRIES") == list(V.COUNTRY)


# ---------------------------------------------------------------------------
# every offered word is producible
# ---------------------------------------------------------------------------

def _normaliser_labels():
    from scrapers import _models as m
    labels = set(m._TIERS)
    rules = m.PATHOGEN_RULES
    labels |= {k for k, _ in rules} if isinstance(rules, list) else set(rules)
    return labels


def test_every_pathogen_term_is_producible(rows):
    """A term may be offered only if the pipeline can emit a row matching it.

    Two provenances are allowed:
      rules — a normalizePathogen() label exists, so the organism is collected
              even if no notice has named it yet (Yersinia, Shigella, Brucella)
      rows  — free-text hazards the review agents write; at least one row today
    """
    labels = _normaliser_labels()
    orphans = []
    for term in V.PATHOGEN:
        by_rules = any(V.matches({"Pathogen": lab}, "pathogen", term)
                       for lab in labels)
        by_rows = any(V.matches(r, "pathogen", term) for r in rows)
        if not (by_rules or by_rows):
            orphans.append(term)
    assert not orphans, (
        "offered pathogen words that no register row can ever match: %s" % orphans)


def test_every_product_category_matches_something(rows):
    dead = [t for t in V.PRODUCT
            if not any(V.matches(r, "product", t) for r in rows)]
    assert not dead, "product categories that match no row: %s" % dead


def test_every_offered_country_is_a_register_country(rows):
    seen = {(r.get("Country") or "").strip() for r in rows}
    seen.discard("")
    dead = [c for c in V.COUNTRY
            if not any(V.matches({"Country": s}, "country", c) for s in seen)]
    assert not dead, "offered countries with no rows: %s" % dead


def test_every_register_country_is_offered(rows):
    """A new source country must show up here, not as a filter that never fires."""
    ignore = {"Unknown", "unknown origin", ""}
    missing = sorted({
        (r.get("Country") or "").strip() for r in rows
    } - ignore - {
        s for s in {(r.get("Country") or "").strip() for r in rows}
        if any(V.matches({"Country": s}, "country", c) for c in V.COUNTRY)
    })
    assert not missing, (
        "register countries no offered word reaches: %s\n"
        "add them to COUNTRY in tools/alert_vocab.py, then "
        "python3 tools/gen_alert_vocab.py --write" % missing)


def test_every_row_matches_at_least_one_pathogen_term(rows):
    """No published row may be invisible to every pathogen alert."""
    blind = [r for r in rows
             if not any(V.matches(r, "pathogen", t) for t in V.PATHOGEN)]
    assert not blind, "rows no pathogen alert can reach: %s" % (
        sorted({r.get("Pathogen") for r in blind})[:10],)


# ---------------------------------------------------------------------------
# retired words stay retired
# ---------------------------------------------------------------------------

def test_retired_words_are_not_offered():
    offered = {t.lower() for t in V.PATHOGEN} | {t.lower() for t in V.PRODUCT}
    back = [w for w in list(V.RETIRED_PATHOGEN) + list(V.RETIRED_PRODUCT)
            if w.lower() in offered]
    assert not back, "retired words are being offered again: %s" % back


def test_retired_words_carry_a_reason():
    for w, why in list(V.RETIRED_PATHOGEN.items()) + list(V.RETIRED_PRODUCT.items()):
        assert why.strip(), "no reason recorded for retiring %r" % w


def test_undeclared_allergen_stays_out(rows):
    """The publish gate blocks allergen-only notices, so the word can't fire."""
    assert "Undeclared allergen" not in V.PATHOGEN
    assert "Undeclared allergen" in V.RETIRED_PATHOGEN


def test_legacy_rules_still_resolve():
    for old, new in V.PATHOGEN_LEGACY.items():
        assert new in V.PATHOGEN, "legacy %r points at missing term %r" % (old, new)


def test_legacy_ochratoxin_a_still_fires(rows):
    """An existing subscriber stored 'Ochratoxin A'. It must keep working."""
    n_old = sum(1 for r in rows if V.matches(r, "pathogen", "Ochratoxin A"))
    n_new = sum(1 for r in rows if V.matches(r, "pathogen", "Ochratoxin"))
    assert n_old == n_new > 0


# ---------------------------------------------------------------------------
# the 13 Sep defect
# ---------------------------------------------------------------------------

BUG_CASES = [
    ("pathogen", "Clostridium botulinum", {"Pathogen": "Clostridium perfringens"}, False),
    ("pathogen", "Clostridium botulinum", {"Pathogen": "C. botulinum"}, True),
    ("pathogen", "Clostridium botulinum", {"Pathogen": "Clostridium botulinum"}, True),
    ("pathogen", "Clostridium perfringens", {"Pathogen": "Clostridium perfringens"}, True),
    ("pathogen", "Listeria", {"Pathogen": "Listeria monocytogenes"}, True),
    ("pathogen", "Salmonella", {"Pathogen": "Salmonella Enteritidis"}, True),
    ("pathogen", "E. coli / STEC",
     {"Pathogen": "Shiga toxin-producing E. coli (STEC)"}, True),
    ("pathogen", "E. coli / STEC",
     {"Pathogen": "Coliform / total bacterial count"}, False),
    ("pathogen", "Bacillus cereus / cereulide",
     {"Pathogen": "Cereulide (B. cereus toxin)"}, True),
    ("pathogen", "Ochratoxin", {"Pathogen": "Ochratoxin A"}, True),
    ("pathogen", "Aflatoxin", {"Pathogen": "Aflatoxin B1 + Ochratoxin A"}, True),
    ("pathogen", "Mold / spoilage", {"Pathogen": "Mold"}, True),
    ("pathogen", "Foreign material / physical hazard",
     {"Pathogen": "Foreign material (glass)"}, True),
    ("pathogen", "Heavy metals", {"Pathogen": "Cadmium (heavy metal)"}, True),
    ("pathogen", "T-2 / HT-2 toxin",
     {"Pathogen": "Mycotoxins (T-2 and HT-2 toxin)"}, True),
    ("pathogen", "Listeria", {"Pathogen": ""}, False),
    ("country", "Czechia", {"Country": "Czechia"}, True),
    ("country", "Czechia", {"Country": "Czech Republic"}, True),
    ("country", "Korea, South", {"Country": "Korea, South"}, True),
    ("country", "Turkey", {"Country": "Turkey"}, True),
    ("country", "EU-wide / multi-country", {"Country": "Multiple EU/EEA + UK"}, True),
    ("country", "France", {"Country": "Belgium / France"}, True),
    ("product", "Dairy — cheese", {"Product": "Tomme de Burdignes", "Reason": ""}, True),
    ("product", "Dairy — cheese",
     {"Product": "lardons fumés pxm 200g", "Reason": ""}, False),
    ("product", "Meat — pork", {"Product": "lardons fumés pxm 200g", "Reason": ""}, True),
    ("product", "Dried fruit / nuts",
     {"Product": "Ochratoxin A in organic dried figs from Turkey", "Reason": ""}, True),
    ("product", "Dried fruit / nuts",
     {"Product": "best before date 12/2026", "Reason": ""}, False),
    ("brand", "Ferrarini", {"Brand": "Ferrarini", "Company": ""}, True),
    ("brand", "Ferrarini", {"Brand": "", "Company": "Ferrarini S.p.A."}, True),
    ("brand", "Ferrarini", {"Brand": "Ferrari", "Company": ""}, False),
]


@pytest.mark.parametrize("cat,term,row,want", BUG_CASES)
def test_matcher_cases(cat, term, row, want):
    assert V.matches(row, cat, term) is want


def test_no_rule_is_ever_split_into_its_first_word():
    """The exact shape of the old bug: rule truncated to its leading token."""
    for term in V.PATHOGEN:
        head = re.split(r"[\s/()]+", term.lower())[0]
        if head == term.lower() or len(head) < 4:
            continue
        # A row that is ONLY the leading token must not match the full term,
        # unless one of the term's own tokens legitimately reaches it —
        # "Mycotoxins (other)" holds the token "mycotoxin", so a bare
        # "mycotoxins" row is a true match, not a truncation.
        if any(re.search(V.token_pattern(t), head, re.I) for t in V.PATHOGEN[term]):
            continue
        assert not V.matches({"Pathogen": head}, "pathogen", term), (
            "rule %r still matches a row that is only %r" % (term, head))


# ---------------------------------------------------------------------------
# the JavaScript agrees with the Python
# ---------------------------------------------------------------------------

def test_apps_script_self_test_passes():
    node = None
    for cand in ("node", "nodejs"):
        try:
            subprocess.run([cand, "--version"], capture_output=True, check=True)
            node = cand
            break
        except (OSError, subprocess.CalledProcessError):
            continue
    if node is None:
        pytest.skip("node is not available")

    harness = (
        "const fs=require('fs');"
        "global.Logger={log:function(m){console.log(String(m));}};"
        "global.MailApp={sendEmail:function(){}};"
        + _GAS_STUBS +
        "eval(fs.readFileSync(%r,'utf8'));"
        "process.exit(test_fsisAlertVocab()?0:1);" % GS
    )
    r = subprocess.run([node, "-e", harness], capture_output=True, text=True)
    assert r.returncode == 0, (
        "AftsAlertVocab.gs self-test failed:\n" + r.stdout + r.stderr)
    assert "All alert-vocabulary cases pass." in r.stdout


def test_javascript_and_python_agree_on_every_case():
    node = None
    for cand in ("node", "nodejs"):
        try:
            subprocess.run([cand, "--version"], capture_output=True, check=True)
            node = cand
            break
        except (OSError, subprocess.CalledProcessError):
            continue
    if node is None:
        pytest.skip("node is not available")

    cases = [{"cat": c, "term": t, "row": r} for c, t, r, _ in BUG_CASES]
    harness = (
        "const fs=require('fs');"
        "global.Logger={log:function(){}};"
        "global.MailApp={sendEmail:function(){}};"
        + _GAS_STUBS +
        "eval(fs.readFileSync(%r,'utf8'));"
        "const cs=%s;"
        "console.log(JSON.stringify(cs.map(function(c){"
        "return recallMatchesCriterion_(c.row,c.cat,c.term);})));"
        % (GS, json.dumps(cases))
    )
    r = subprocess.run([node, "-e", harness], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    got_js = json.loads(r.stdout.strip().splitlines()[-1])
    got_py = [V.matches(c["row"], c["cat"], c["term"]) for c in cases]
    disagree = [(cases[i], got_py[i], got_js[i])
                for i in range(len(cases)) if got_py[i] != got_js[i]]
    assert not disagree, "Python and Apps Script disagree: %s" % disagree
