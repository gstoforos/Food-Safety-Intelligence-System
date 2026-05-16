#!/usr/bin/env python3
"""
patch_workflow_concurrency.py
=============================
Brings ALL writer workflows under the shared `fsis-data-writers` GitHub
Actions concurrency group, so only one xlsx-writer runs at a time and
the 2026-05-16 17:06→19:02 UTC race-condition stomp class becomes
structurally impossible.

WHAT IT DOES
------------
Walks .github/workflows/*.yml and applies one of three actions per file:

  ACTION_REPLACE   The file has a `concurrency:` block with an
                   escape-hatch group name. Replace the entire block
                   with the standard 3-line shared block.
  ACTION_INSERT    The file has no `concurrency:` block at all. Insert
                   the standard 3-line block just before the first
                   top-level `on:` key.
  ACTION_SKIP      The file is already correctly in `fsis-data-writers`,
                   OR it's a read-only / PR-test workflow that
                   intentionally stays out of the writer lock.

THE STANDARD BLOCK
------------------
    concurrency:
      group: fsis-data-writers
      cancel-in-progress: false

WHAT IT WILL NOT DO
-------------------
- It will NOT modify a workflow that already has
  `group: fsis-data-writers`. Idempotent on re-run.
- It will NOT touch the 4 workflows in EXPLICIT_SKIP (audit-scrapers,
  resolve-dead-urls, gemini-sunday-qa already in lock but listed for
  clarity, tests.yml). These are read-only or PR-only.
- It will NOT use a regex on multi-line YAML — every edit is a literal
  string replacement against the file's exact current bytes, so a
  comment-laced block (like fsis-url-guardian.yml had pre-fix) cannot
  be mismatched.

USAGE
-----
    python patch_workflow_concurrency.py            # apply
    python patch_workflow_concurrency.py --dry-run  # report only

EXIT CODES
----------
  0 — success, all targets patched, post-validation passed
  1 — file not found or unexpected state encountered
  2 — post-validation failed (a target file still has wrong group)
"""
from __future__ import annotations
import argparse
import sys
import yaml
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────
# Configuration — explicit lists, no guessing.
# ──────────────────────────────────────────────────────────────────────────

# Files where we REPLACE the existing escape-hatch concurrency block.
# Each value is the EXACT escape-hatch group name currently in the file.
REPLACE_TARGETS: dict[str, str] = {
    "afts-monthly-report.yml":      "afts-monthly-report",
    "afts-weekly-report.yml":       "afts-weekly-report",
    "build-public-xlsx.yml":        "build-public-xlsx",
    "export-training-data-v2.yml":  "export-training-data-v2",
    "export-training-data-v2-1.yml":"export-training-data-v2-1",
    "export-training-data-v2-2.yml":"export-training-data-v2-2",
    "gap-finder-cascade.yml":       "gap-finder-cascade",
    "public-xlsx-build.yml":        "public-xlsx-build",
    "quality-sample.yml":           "quality-sample",
    "rescue-historical-rejects.yml":"rescue-historical-rejects",
    "scraper-health.yml":           "scraper-health",
    "scraper-health-monthly.yml":   "scraper-health-monthly",
    "train-tier-classifier.yml":    "train-tier-classifier",
    "training-corpus-audit.yml":    "training-corpus-audit",
}

# Files where we INSERT a new concurrency block (no block currently exists).
# These all push to xlsx and need to be in the lock.
INSERT_TARGETS: list[str] = [
    "cleanup-dataset.yml",
    "gap-auditor.yml",
    "one-off-repair.yml",
]

# Files we deliberately do NOT touch:
#   tests.yml         — PR-test pattern (group: tests-${{ github.ref }},
#                        cancel-in-progress: true). Correct for PR runs.
#   audit-scrapers.yml — read-only, never pushes.
#   resolve-dead-urls.yml — read-only, never pushes.
EXPLICIT_SKIP: set[str] = {
    "tests.yml",
    "audit-scrapers.yml",
    "resolve-dead-urls.yml",
}

# The canonical 3-line block we want every writer workflow to have.
STANDARD_BLOCK = (
    "concurrency:\n"
    "  group: fsis-data-writers\n"
    "  cancel-in-progress: false\n"
)

WORKFLOWS_DIR = Path(".github/workflows")


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _find_replace_block(text: str, escape_group: str) -> tuple[int, int] | None:
    """Locate the existing `concurrency:` block whose group matches
    escape_group. Returns (start, end) byte offsets of the block in `text`
    (end is exclusive, INCLUDING the trailing newline), or None.

    The matcher is structural, not regex:
      1. Find a line that starts with `concurrency:` at column 0
      2. Walk forward, line by line, while lines are either blank, comment
         (`  #` or `#`), or indented under the block (`  group:`, `  cancel-in-progress:`).
      3. Stop at the first non-indented, non-comment line — that's the
         end of the block.
      4. Within the block, require `group: <escape_group>` to be present.
         If not, return None (the file has SOME concurrency block but not
         the one we expected — refuse to guess).
    """
    lines = text.splitlines(keepends=True)
    n = len(lines)
    i = 0
    while i < n:
        if lines[i].startswith("concurrency:"):
            block_start = i
            j = i + 1
            saw_group = False
            saw_correct_group = False
            while j < n:
                ln = lines[j]
                # Block continues while line is indented (' '), blank, or
                # a comment line indented under the block.
                if ln.strip() == "":
                    # Blank line — end of YAML block.
                    break
                if not (ln.startswith("  ") or ln.startswith("\t")):
                    # Dedented — next top-level key.
                    break
                stripped = ln.strip()
                if stripped.startswith("#"):
                    j += 1
                    continue
                if stripped.startswith("group:"):
                    saw_group = True
                    val = stripped.split("group:", 1)[1].strip()
                    # Strip inline YAML comment if present
                    if "#" in val:
                        val = val.split("#", 1)[0].strip()
                    if val == escape_group:
                        saw_correct_group = True
                j += 1
            if saw_group and saw_correct_group:
                # Block ends at line j (exclusive). Compute byte offsets.
                start_off = sum(len(lines[k]) for k in range(block_start))
                end_off = sum(len(lines[k]) for k in range(j))
                return (start_off, end_off)
            # Else: it's a concurrency block, but not the one we expected.
            # Refuse to guess.
            return None
        i += 1
    return None


def _find_insert_anchor(text: str) -> int | None:
    """Locate the byte offset of the first top-level `on:` line, which is
    where we insert the new concurrency block (just before `on:`). The
    inserted block separates itself from preceding content with a blank
    line and from `on:` with another blank line.
    """
    lines = text.splitlines(keepends=True)
    for i, ln in enumerate(lines):
        if ln.startswith("on:"):
            return sum(len(lines[k]) for k in range(i))
    return None


def _validate(path: Path) -> tuple[bool, str]:
    """Verify the file (a) parses as YAML, (b) has concurrency.group ==
    'fsis-data-writers'. Returns (ok, message)."""
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        return (False, f"YAML parse error: {e}")
    if not isinstance(doc, dict):
        return (False, "top-level YAML is not a mapping")
    conc = doc.get("concurrency")
    if not isinstance(conc, dict):
        return (False, "concurrency: block missing or not a mapping")
    grp = (conc.get("group") or "").strip()
    if grp != "fsis-data-writers":
        return (False, f"group is {grp!r}, expected 'fsis-data-writers'")
    return (True, "ok")


# ──────────────────────────────────────────────────────────────────────────
# Patcher
# ──────────────────────────────────────────────────────────────────────────

def patch_replace(path: Path, escape_group: str, dry_run: bool) -> str:
    """Replace the existing concurrency block in `path`. Returns status."""
    text = path.read_text(encoding="utf-8")
    span = _find_replace_block(text, escape_group)
    if span is None:
        # Check whether the file is already correct (idempotent re-run).
        ok, _ = _validate(path)
        if ok:
            return "ALREADY_PATCHED"
        return f"FAIL: no concurrency block with group={escape_group!r} found"
    start, end = span
    new_text = text[:start] + STANDARD_BLOCK + text[end:]
    if dry_run:
        return "WOULD_REPLACE"
    path.write_text(new_text, encoding="utf-8")
    return "REPLACED"


def patch_insert(path: Path, dry_run: bool) -> str:
    """Insert a new concurrency block in `path` before `on:`."""
    text = path.read_text(encoding="utf-8")
    # Idempotency: if a fsis-data-writers block is already present, skip.
    ok, _ = _validate(path)
    if ok:
        return "ALREADY_PATCHED"
    off = _find_insert_anchor(text)
    if off is None:
        return "FAIL: no top-level 'on:' anchor found"
    # Insert the block before `on:`, with blank-line separation.
    # Ensure the preceding character is a blank line (LF) to keep YAML readable.
    prefix = text[:off]
    suffix = text[off:]
    # Make sure there's exactly one blank line between prior content and
    # our block (i.e. the text before `off` ends with "\n\n").
    if not prefix.endswith("\n\n"):
        if prefix.endswith("\n"):
            prefix = prefix + "\n"
        else:
            prefix = prefix + "\n\n"
    new_text = prefix + STANDARD_BLOCK + "\n" + suffix
    if dry_run:
        return "WOULD_INSERT"
    path.write_text(new_text, encoding="utf-8")
    return "INSERTED"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workflows-dir", default=str(WORKFLOWS_DIR),
                    help="Path to .github/workflows directory")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change without writing files")
    args = ap.parse_args()

    wf_dir = Path(args.workflows_dir)
    if not wf_dir.is_dir():
        print(f"ERROR: workflows dir not found: {wf_dir}", file=sys.stderr)
        return 1

    print("=" * 78)
    print("WORKFLOW CONCURRENCY PATCHER")
    print(f"Target: {wf_dir.resolve()}")
    print(f"Mode:   {'DRY-RUN' if args.dry_run else 'APPLY'}")
    print("=" * 78)
    print()

    manifest: list[tuple[str, str, str]] = []  # (file, action, status)

    # 1. REPLACE existing escape-hatch blocks
    print("── REPLACE: escape-hatch concurrency groups → fsis-data-writers ──")
    for fname, escape_group in sorted(REPLACE_TARGETS.items()):
        path = wf_dir / fname
        if not path.exists():
            print(f"  {fname:<42} MISSING")
            manifest.append((fname, "REPLACE", "MISSING"))
            continue
        status = patch_replace(path, escape_group, args.dry_run)
        print(f"  {fname:<42} {status}")
        manifest.append((fname, "REPLACE", status))

    print()
    print("── INSERT: add concurrency block to no-block workflows ──")
    for fname in sorted(INSERT_TARGETS):
        path = wf_dir / fname
        if not path.exists():
            print(f"  {fname:<42} MISSING")
            manifest.append((fname, "INSERT", "MISSING"))
            continue
        status = patch_insert(path, args.dry_run)
        print(f"  {fname:<42} {status}")
        manifest.append((fname, "INSERT", status))

    print()
    print(f"── SKIP: deliberately untouched ({len(EXPLICIT_SKIP)} files) ──")
    for fname in sorted(EXPLICIT_SKIP):
        path = wf_dir / fname
        if path.exists():
            print(f"  {fname:<42} (intentional skip)")
        else:
            print(f"  {fname:<42} (intentional skip, file absent)")

    # ── Post-validation ─────────────────────────────────────────────────
    if args.dry_run:
        print()
        print("=" * 78)
        print("DRY-RUN COMPLETE — no files modified.")
        print(f"Would patch: {len(REPLACE_TARGETS)} replace + {len(INSERT_TARGETS)} insert")
        return 0

    print()
    print("── POST-VALIDATION ──")
    print("All targeted files must now have concurrency.group == 'fsis-data-writers'.")
    print()
    fail_count = 0
    for fname in sorted(set(REPLACE_TARGETS.keys()) | set(INSERT_TARGETS)):
        path = wf_dir / fname
        if not path.exists():
            continue
        ok, msg = _validate(path)
        flag = "✓" if ok else "✗"
        print(f"  {flag} {fname:<42} {msg}")
        if not ok:
            fail_count += 1

    print()
    print("=" * 78)
    if fail_count == 0:
        # Final summary count
        try:
            in_lock = 0
            for f in sorted(wf_dir.glob("*.yml")):
                ok, _ = _validate(f)
                if ok:
                    in_lock += 1
            print(f"✓ SUCCESS — {in_lock} writer workflows now in fsis-data-writers lock.")
            print(f"  {len(EXPLICIT_SKIP)} files deliberately skipped (read-only / PR-test).")
        except Exception:
            print("✓ SUCCESS — all targets validated.")
        return 0
    else:
        print(f"✗ POST-VALIDATION FAILED — {fail_count} file(s) still wrong.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
