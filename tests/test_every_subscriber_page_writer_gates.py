# -*- coding: utf-8 -*-
"""Every writer of a subscriber page gates it. Morning fix pass 2026-11-18.

tests/test_subscriber_pages_are_gated.py checks the FILES in docs/. That is
necessary and not sufficient: the committed files can be gated while an
ungated writer exists, because whichever writer runs last decides what is on
disk. On 2026-11-18 all seven docs/daily pages on main carried the tag — and
three writers of subscriber HTML did not add it:

  * pipeline/build_weekly_updates_check.py — the Wednesday weekly-updates
    check, whose ONLY job is to rebuild stale weekly reports. It wrote
    `report_path.write_text(html)` while the Friday path it documents itself
    as reusing (docs/build_weekly_report_afts.refresh_stale_weeks) writes
    `_gate_html(html)`. Found live: this pass ran the check for 2026-11-18,
    W46 was rebuilt 77 -> 78 recalls, and the page came back ungated.
  * pipeline/merge_master.py — rebuilds docs/daily/<date>.html on the HOURLY
    Pending->Recalls merge. Any brief it rebuilt after the 08:00 daily search
    stayed ungated until the next morning.
  * pipeline/backfill_recent_briefs.py — same, for the backfill window.

Two writers of the same files did gate: pipeline/daily_recall_search.py and
tools/rebase_and_verify.py. One shape, five instances, two of them already
correct — so this file pins the rule at the SOURCE, where the file-level
test cannot see it.

The check is deliberately crude: a module that writes a path under
docs/daily/ or a docs/<year>-W<nn>.html must mention add_gate (or the
_gate_html wrapper around it) somewhere. A crude source check that cannot be
satisfied by accident is worth more here than an exact AST match on five
different call shapes.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: Modules that write a subscriber HTML page, and what they write.
WRITERS = {
    "pipeline/build_weekly_updates_check.py": "the rebuilt weekly report",
    "pipeline/merge_master.py": "docs/daily/<date>.html on the hourly merge",
    "pipeline/backfill_recent_briefs.py": "docs/daily/<date>.html (backfill)",
    "pipeline/daily_recall_search.py": "docs/daily/<date>.html (08:00 search)",
    "tools/rebase_and_verify.py": "docs/daily/<date>.html on a rebase",
    "docs/build_weekly_report_afts.py": "the Friday weekly report",
    "docs/build_monthly_report_afts.py": "the monthly report and -all",
}

#: An assignment that names a SUBSCRIBER page path. Narrow on purpose:
#: docs/data/, docs/reports/, docs/marketing/, docs/social/ and docs/audits/
#: are public by design (see PUBLIC_DIRS in
#: tests/test_subscriber_pages_are_gated.py).
_PATH_ASSIGN = re.compile(
    r"""^\s*(?P<var>[A-Za-z_][A-Za-z_0-9]*)\s*=\s*.*?
        (DAILY_DIR
         |["']daily["']
         |daily[\\/]
         |-W\{
         |W%02d
        )""", re.VERBOSE)

_WRITE = re.compile(r"^\s*(?P<var>[A-Za-z_][A-Za-z_0-9]*)\.write_text\(")


@pytest.mark.parametrize("rel,what", sorted(WRITERS.items()))
def test_a_subscriber_page_writer_mentions_the_gate(rel, what):
    p = ROOT / rel
    if not p.exists():                                    # pragma: no cover
        pytest.skip(f"{rel} not present")
    src = p.read_text(encoding="utf-8", errors="replace")
    assert "add_gate" in src or "_gate_html" in src, (
        f"{rel} writes {what} but never mentions add_gate(). A subscriber "
        f"page written without <script src=\"/fsis-gate.js\"> in <head> "
        f"opens unblurred for anyone with the link. "
        f"tests/test_subscriber_pages_are_gated.py cannot catch this: it "
        f"reads the FILES, and whichever writer runs last decides what is "
        f"on disk — on 2026-11-18 every file was gated while three writers "
        f"were not.")


def _ungated_writes(src: str) -> list[tuple[int, str]]:
    """(line no, line) for every write of a subscriber page path that is
    not passed through the gate.

    A path variable is tracked from its assignment, because the write is
    usually on the next line: that is exactly how the 2026-11-18 defect hid
    from a line-local check (`html_path = DAILY_DIR / f"{iso}.html"` on one
    line, `html_path.write_text(html)` on the next).
    """
    lines = src.splitlines()
    tracked: set[str] = set()
    out: list[tuple[int, str]] = []
    for i, line in enumerate(lines, 1):
        m = _PATH_ASSIGN.match(line)
        if m:
            tracked.add(m.group("var"))
        w = _WRITE.match(line)
        if not w or w.group("var") not in tracked:
            continue
        if "json.dumps" in line or ".json" in line:
            continue                      # JSON to the same variable name
        if "add_gate(" in line or "_gate_html(" in line:
            continue
        # The value may have been gated a few lines earlier, as the monthly
        # builder does (`html = _add_gate(html)`). COMMENTS DO NOT COUNT:
        # the first version of this check accepted the ungated write in
        # pipeline/backfill_recent_briefs.py because the comment above it
        # explained the defect and contained the string "add_gate(".
        window = [w2.split("#", 1)[0] for w2 in lines[max(0, i - 7):i - 1]
                  if not w2.lstrip().startswith("#")]
        if any("add_gate(" in w2 or "_gate_html(" in w2 for w2 in window):
            continue
        out.append((i, line.strip()[:100]))
    return out


@pytest.mark.parametrize("rel,what", sorted(WRITERS.items()))
def test_no_writer_writes_a_subscriber_page_without_gating_it(rel, what):
    p = ROOT / rel
    if not p.exists():                                    # pragma: no cover
        pytest.skip(f"{rel} not present")
    bad = _ungated_writes(p.read_text(encoding="utf-8", errors="replace"))
    assert not bad, (
        f"{rel} writes {what} without passing the HTML through add_gate():\n  "
        + "\n  ".join(f"line {n}: {t}" for n, t in bad) +
        "\n\nadd_gate() is idempotent (pipeline/_gate.py), so wrapping the "
        "value is always safe. If the path is genuinely public, move it "
        "under one of the PUBLIC_DIRS in "
        "tests/test_subscriber_pages_are_gated.py instead of skipping the "
        "gate.")


def test_the_check_would_have_caught_the_2026_11_18_defect():
    """The defect, in its original two-line shape, must be detected."""
    defect = (
        'def apply():\n'
        '    for iso in window:\n'
        '        html = render_daily_html(iso, rows, 5)\n'
        '        html_path = DAILY_DIR / f"{iso}.html"\n'
        '        html_path.write_text(html, encoding="utf-8")\n')
    assert _ungated_writes(defect), (
        "the source check no longer detects the ungated write it was "
        "written for — a path assigned from DAILY_DIR on one line and "
        "written on the next. Do not relax it.")
    fixed = defect.replace("write_text(html,",
                           "write_text(add_gate(html),")
    assert not _ungated_writes(fixed), (
        "the gated form is reported as ungated — the check is now a false "
        "alarm and would be deleted by the next person to see it.")
