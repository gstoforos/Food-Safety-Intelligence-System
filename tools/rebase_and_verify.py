# -*- coding: utf-8 -*-
"""Rebase a fix pass onto the CURRENT main, rebuild what it touches, verify.

    python -m tools.rebase_and_verify \
        --code 2-CODE-...-base-<sha>.zip  [--data 1-DATA-...-base-<sha>.zip] \
        --message "remove-rows: ..."      [--out DIR] [--no-promote]

WHY THIS EXISTS (operator 2026-10-04: "do we have reduced mistakes")
--------------------------------------------------------------------
Every fix pass on 2026-10-04 was built on a main that had moved by the time
it was uploaded — 7 bot commits in 6 hours. The morning fix's data zip was
based on d16ae98d; uploading its recalls.xlsx would have erased the Pending
rows three gap finders wrote afterwards, and it left the 10-01 daily brief
still listing a drug recall it had removed. Each of those was caught by hand.
This script does the hand work, every time, in the same order:

  1. Run in a clone that is EXACTLY origin/main (fetched). Refuses otherwise.
  2. CODE: copies pipeline/ tests/ tools/ .github/ files from the zip, but
     only where main has not touched the file since the zip's base sha.
     A file both sides changed is a conflict — refused, never overwritten.
  3. DATA: the zip's recalls.xlsx is NEVER copied. The register is rebuilt
     by re-running the pass's own repair script(s) (tools/repair_*.py in the
     code zip) and the offline promoter against main's CURRENT workbook.
     Derived files (recalls.json, public xlsx, sources.json) are
     regenerated. Every other docs/ file — pages, report JSON, a monthly or
     weekly report the pass built because a workflow failed — is copied
     only if main has not changed it since the base; step 5 then rebuilds
     whatever the register change touches on top of it.
  4. Diffs Recalls before/after by URL: added, removed, changed.
  5. Rebuilds every report those rows touch:
       daily briefs (docs/daily/<date>.html that exist, never today's) and
       daily-index.json; each affected week's HTML (ascending, so the open
       week is built last) and weekly-index.json; the Sunday review slice
       (weekly-review-latest.json). The weekly latest-pointer is never
       moved by this script: if a build moves it, the old one is restored.
       Closed MONTHS are reported, not rebuilt — the day-10 updates check
       owns them (a rebuild relabels the report "UPDATED" and rewrites the
       mailer summary).
  6. Sweeps every daily/weekly HTML for a removed URL still on the page.
  7. Runs the FULL suite on a fresh clone of the same main sha, with the
     changes committed under --message. A test that fails ONLY with the
     change refuses the run; a test that also fails on plain main is listed
     as PRE-EXISTING ON MAIN in the report (not hidden, not blocking). "remove-rows" is required in the
     message when rows were removed (test_register_never_shrinks).
  8. Re-checks origin/main. If it moved during the run: refuses to zip.
  9. Writes 1-DATA-docs-base-<sha>-UPLOAD-NOW.zip and
     2-CODE-pipeline-tests-tools-base-<sha>.zip plus REBASE-REPORT.md.

Exit 0 only when every step passed. Nothing is pushed.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]

XLSX = "docs/data/recalls.xlsx"
# Rebuilt from the register, never copied from a zip.
DERIVED = {
    XLSX, "docs/data/recalls.json", "docs/data/afts-recalls-public.xlsx",
    "docs/data/sources.json",
}
# Rebuilt by step 5, never copied from a zip.
REPORT_RE = re.compile(
    r"^docs/(?:daily/\d{4}-\d{2}-\d{2}\.html|\d{4}-W\d{2}\.html|daily-index\.json"
    r"|data/weekly-(?:index|summary-latest|review-latest|rejected-latest)\.json)$")
# Closed-month outputs: owned by the day-10 updates check.
MONTHLY_RE = re.compile(
    r"^docs/(?:\d{4}-M\d{2}(?:-all)?\.html|data/monthly-[\w-]+\.json|marketing/.*)$")
CODE_ROOTS = ("pipeline/", "tests/", "tools/", ".github/", "scrapers/")
SKIP_RE = re.compile(r"(?:^|/)(?:__pycache__/|\.pytest_cache/)|\.pyc$|^READ-ME-FIRST\.md$")
REGIONS_RE = re.compile(r"(\d+) regions? scanned")
REMOVAL_WORDS = ("remove-rows", "delete-rows", "reject-batch",
                 "deliberate row removal", "approved deletion")


class Refused(RuntimeError):
    """A step that must stop the run, with the reason the operator reads."""


# ── pure helpers (tested in tests/test_rebase_and_verify.py) ────────────────

def base_sha_from_name(name: str) -> str:
    """'2-CODE-2026-10-04-base-d16ae98d.zip' -> 'd16ae98d'."""
    m = re.search(r"base-([0-9a-f]{7,40})", Path(name).name)
    if not m:
        raise Refused(f"cannot read a base sha from {name!r} (expected '...base-<sha>...')")
    return m.group(1)


def classify(path: str) -> str:
    """Where a file from a zip goes: skip | derived | report | monthly | code | docs | other."""
    if SKIP_RE.search(path):
        return "skip"
    if path in DERIVED:
        return "derived"
    if REPORT_RE.match(path):
        return "report"
    if MONTHLY_RE.match(path):
        return "monthly"
    if path.startswith(CODE_ROOTS):
        return "code"
    if path.startswith("docs/"):
        return "docs"
    return "other"


def strip_wrapper(names: Iterable[str]) -> Dict[str, str]:
    """Map zip member -> repo path. Accepts zips with or without one top folder."""
    files = [n for n in names if not n.endswith("/")]
    tops = {n.split("/", 1)[0] for n in files if "/" in n}
    known = {"docs", "pipeline", "tests", "tools", ".github", "scrapers"}
    out = {}
    for n in files:
        p = n
        if len(tops) == 1 and not (tops & known) and "/" in n:
            p = n.split("/", 1)[1]
        out[n] = p
    return out


def diff_rows(before: Dict[str, dict], after: Dict[str, dict],
              fields: Iterable[str] = ("Date", "Source", "Company", "Product", "Pathogen",
                                       "Tier", "Outbreak", "Country", "Region", "Class")
              ) -> Tuple[List[dict], List[dict], List[Tuple[dict, dict, List[str]]]]:
    """Recalls before/after keyed by URL -> (added, removed, changed)."""
    added = [after[u] for u in after.keys() - before.keys()]
    removed = [before[u] for u in before.keys() - after.keys()]
    changed = []
    for u in before.keys() & after.keys():
        diff = [f for f in fields if str(before[u].get(f, "")) != str(after[u].get(f, ""))]
        if diff:
            changed.append((before[u], after[u], diff))
    return added, removed, changed


def affected_dates(added, removed, changed) -> Set[str]:
    ds = {str(r.get("Date", ""))[:10] for r in list(added) + list(removed)}
    for b, a, _ in changed:
        ds |= {str(b.get("Date", ""))[:10], str(a.get("Date", ""))[:10]}
    return {d for d in ds if re.match(r"^\d{4}-\d{2}-\d{2}$", d)}


def iso_week_file(d: str) -> str:
    y, w, _ = date.fromisoformat(d).isocalendar()
    return f"{y}-W{w:02d}.html"


def parse_porcelain_z(out: str) -> List[str]:
    """Paths from `git status --porcelain -z` (renames give the new path)."""
    files, parts, i = [], out.split("\0"), 0
    while i < len(parts):
        entry = parts[i]
        i += 1
        if len(entry) < 4:
            continue
        code, path = entry[:2], entry[3:]
        if code[0] in "RC":
            i += 1                      # the next field is the ORIGINAL path
        if not SKIP_RE.search(path):
            files.append(path)
    return sorted(files)


def deleted_in_porcelain_z(out: str) -> Set[str]:
    """Paths `git status --porcelain -z` reports as deleted (X or Y == 'D')."""
    gone, parts, i = set(), out.split("\0"), 0
    while i < len(parts):
        entry = parts[i]
        i += 1
        if len(entry) < 4:
            continue
        code, path = entry[:2], entry[3:]
        if code[0] in "RC":
            i += 1
        if "D" in code:
            gone.add(path)
    return gone


def failed_test_ids(pytest_output: str) -> List[str]:
    """Test ids from pytest's 'FAILED <id> - <message>' summary lines."""
    ids = []
    for line in pytest_output.splitlines():
        if line.startswith(("FAILED ", "ERROR ")):
            tid = line.split(" ", 1)[1].split(" - ", 1)[0].strip()
            if tid and tid not in ids:
                ids.append(tid)
    return ids


def needs_removal_word(n_removed: int, message: str) -> bool:
    """True when the message is missing the word the shrink guard needs."""
    return n_removed > 0 and not any(w in message.lower() for w in REMOVAL_WORDS)


# ── shell helpers ───────────────────────────────────────────────────────────

def sh(args: List[str], cwd: Path, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(args, cwd=cwd, text=True, capture_output=capture)
    if check and r.returncode != 0:
        raise Refused(f"`{' '.join(args)}` failed ({r.returncode}):\n{(r.stdout or '')[-1500:]}"
                      f"{(r.stderr or '')[-1500:]}")
    return (r.stdout or "").strip()


def git(repo: Path, *a: str, check: bool = True) -> str:
    return sh(["git", *a], repo, check=check)


def py(repo: Path, *a: str, check: bool = True) -> str:
    return sh([sys.executable, *a], repo, check=check)


def remote_main(repo: Path) -> str:
    out = git(repo, "ls-remote", "origin", "refs/heads/main")
    if not out:
        raise Refused("origin has no refs/heads/main")
    return out.split()[0]


def load_recalls(repo: Path) -> Dict[str, dict]:
    sys.path.insert(0, str(repo))
    import importlib
    m = importlib.import_module("pipeline.merge_master")
    rows = m._load_sheet(repo / XLSX, "Recalls", m.RECALLS_SCHEMA)
    return {str(r.get("URL", "")).strip(): dict(r) for r in rows if str(r.get("URL", "")).strip()}


# ── the run ─────────────────────────────────────────────────────────────────

class Run:
    def __init__(self, repo: Path, code: Optional[Path], data: Optional[Path],
                 message: str, out: Path, promote: bool):
        self.repo, self.code, self.data = repo, code, data
        self.message, self.out, self.promote = message, out, promote
        self.log: List[str] = []
        self.main = ""
        self.touched: Set[str] = set()
        self.preexisting: List[str] = []

    def say(self, line: str = "") -> None:
        print(line, flush=True)
        self.log.append(line)

    # 1
    def check_clean_main(self) -> None:
        git(self.repo, "fetch", "-q", "origin")
        self.main = remote_main(self.repo)
        head = git(self.repo, "rev-parse", "HEAD")
        if head != self.main:
            raise Refused(f"clone is at {head[:8]}, origin/main is {self.main[:8]} — "
                          f"run: git checkout -q --detach {self.main[:8]}")
        dirty = git(self.repo, "status", "--porcelain", "--untracked-files=no")
        if dirty:
            raise Refused("clone has local changes — start from a clean clone of main:\n" + dirty)
        self.say(f"main = {self.main[:8]}")

    def _base_known(self, base: str) -> None:
        if git(self.repo, "cat-file", "-t", base, check=False) != "commit":
            raise Refused(f"base {base} is not in this clone's history")
        if subprocess.run(["git", "merge-base", "--is-ancestor", base, self.main],
                          cwd=self.repo).returncode != 0:
            raise Refused(f"base {base} is not an ancestor of main {self.main[:8]}")

    def _changed_on_main(self, base: str, path: str) -> bool:
        return bool(git(self.repo, "diff", "--name-only", base, self.main, "--", path))

    # 2 + 3 (copy part)
    def apply_zip(self, z: Path, kinds: Set[str]) -> List[str]:
        base = base_sha_from_name(z.name)
        self._base_known(base)
        copied, conflicts, ignored = [], [], []
        with zipfile.ZipFile(z) as zf:
            for member, path in strip_wrapper(zf.namelist()).items():
                k = classify(path)
                if k not in kinds:
                    if k == "derived":
                        ignored.append(f"{path} (regenerated from the register)")
                    elif k != "skip":
                        ignored.append(f"{path} ({k}: not expected in this zip)")
                    continue
                if self._changed_on_main(base, path):
                    # identical content is not a conflict
                    cur = self.repo / path
                    if cur.exists() and cur.read_bytes() == zf.read(member):
                        continue
                    conflicts.append(path)
                    continue
                dest = self.repo / path
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(zf.read(member))
                copied.append(path)
        if not copied and not conflicts:
            self.say(f"{z.name}: every file is already on main — nothing to apply")
        # A report or page main rewrote since the base is main's to keep: the
        # rebuild in step 5 redoes it if the register change touches it.
        soft = [c for c in conflicts if classify(c) in ("report", "monthly")]
        conflicts = [c for c in conflicts if c not in soft]
        for c in soft:
            self.say(f"  kept main's newer {c} (rebuilt below if touched)")
        if conflicts:
            raise Refused(f"{z.name}: main changed these files since {base[:8]} and so did "
                          f"the zip — merge by hand:\n  " + "\n  ".join(conflicts))
        self.say(f"{z.name}: base {base[:8]}, {len(copied)} file(s) applied")
        for i in ignored:
            self.say(f"  not copied: {i}")
        self.touched |= set(copied)
        return copied

    # 3 (rebuild the register)
    def rebuild_register(self) -> None:
        repairs = sorted(p for p in self.touched if re.match(r"^tools/repair_[\w-]+\.py$", p))
        has_xlsx = False
        if self.data:
            with zipfile.ZipFile(self.data) as zf:
                has_xlsx = any(p == XLSX for p in strip_wrapper(zf.namelist()).values())
        if has_xlsx and not repairs and self.touched:
            raise Refused("the data zip carries recalls.xlsx but the code zip has no "
                          "tools/repair_*.py to re-run — the workbook cannot be rebased "
                          "safely. Ship the repair script with the pass.")
        for r in repairs:
            out = py(self.repo, r).splitlines()
            self.say(f"repair: {r} -> {out[-1] if out else '(no output)'}")
        if self.promote and repairs:
            out = py(self.repo, "-m", "pipeline.promote_gate_passing", "--apply")
            self.say("promote: " + (out.splitlines()[-2] if len(out.splitlines()) > 1 else out))
        if repairs or self.data:
            py(self.repo, "-m", "tools.monitored_sources")
            py(self.repo, "-m", "pipeline.export_public_xlsx")

    # 5
    def rebuild_reports(self, added, removed, changed) -> List[str]:
        sys.path.insert(0, str(self.repo))
        from pipeline.daily_recall_search import (render_daily_html, load_recalls_for_date,
                                                  update_daily_index)
        from pipeline._gate import add_gate
        try:
            from zoneinfo import ZoneInfo
            today = datetime.now(ZoneInfo("Europe/Athens")).date().isoformat()
        except Exception:                                   # noqa: BLE001
            today = date.today().isoformat()
        notes = []
        dates = sorted(affected_dates(added, removed, changed))
        for d in dates:
            f = self.repo / "docs" / "daily" / f"{d}.html"
            if d >= today or not f.exists():
                continue
            m = REGIONS_RE.search(f.read_text(encoding="utf-8", errors="replace"))
            regions = int(m.group(1)) if m else 0
            dd = date.fromisoformat(d)
            rows = load_recalls_for_date(self.repo / XLSX, dd)
            f.write_text(add_gate(render_daily_html(dd, rows, regions)), encoding="utf-8")
            update_daily_index(dd, rows)
            notes.append(f"daily brief {d}: {len(rows)} row(s), {regions} regions")
        pointer = self.repo / "docs" / "data" / "weekly-summary-latest.json"
        before_ptr = pointer.read_bytes() if pointer.exists() else None
        before_name = json.loads(before_ptr).get("filename") if before_ptr else None
        weeks = sorted({iso_week_file(d) for d in dates})
        keep = before_ptr          # newest pointer content that still names the same week
        for wf in weeks:
            if not (self.repo / "docs" / wf).exists():
                notes.append(f"{wf}: no published page — not built")
                continue
            anchor = next(d for d in dates if iso_week_file(d) == wf)
            py(self.repo, "docs/build_weekly_report_afts.py", "--week-end", anchor,
               "--xlsx", XLSX, "--no-refresh")
            notes.append(f"{wf}: rebuilt")
            if before_ptr is not None and pointer.exists():
                cur = pointer.read_bytes()
                if json.loads(cur).get("filename") == before_name:
                    keep = cur     # same week, refreshed numbers: keep them
        if before_ptr is not None:
            now_name = json.loads(pointer.read_bytes()).get("filename")
            if now_name != before_name:
                notes.append(f"weekly pointer: a build moved it {before_name} -> {now_name}; "
                             f"put back to {before_name}")
            pointer.write_bytes(keep)
        months = sorted({d[:7] for d in dates if d[:7] < today[:7]})
        for mth in months:
            net = sum(1 for r in added if str(r.get("Date", ""))[:7] == mth) - \
                  sum(1 for r in removed if str(r.get("Date", ""))[:7] == mth)
            notes.append(f"month {mth}: net {net:+d} row(s) — NOT rebuilt (day-10 updates check)")
        if not (added or removed or changed):
            return notes
        from pipeline.weekly_review_capture import export_week_slice, review_day_for
        export_week_slice(self.repo / XLSX, self.repo / "docs/data/weekly-review-latest.json",
                          week_end=review_day_for().isoformat())
        notes.append(f"weekly-review-latest.json re-exported (week_end {review_day_for()})")
        return notes

    # 6
    def sweep(self, removed) -> List[str]:
        urls = [str(r.get("URL", "")).strip() for r in removed if str(r.get("URL", "")).strip()]
        left = []
        pages = list((self.repo / "docs" / "daily").glob("*.html")) + \
            list((self.repo / "docs").glob("*-W*.html"))
        for p in pages:
            t = p.read_text(encoding="utf-8", errors="replace")
            for u in urls:
                if u in t or u.replace("&", "&amp;") in t:
                    left.append(f"{p.relative_to(self.repo)} still shows {u[-70:]}")
        return left

    # 7
    def full_suite(self, changed_files: List[str]) -> str:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td) / "t"
            sh(["git", "clone", "-q", "--shared", str(self.repo), str(t)], Path(td))
            git(t, "checkout", "-q", "--detach", self.main)
            for f in changed_files:
                src, dst = self.repo / f, t / f
                if src.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                elif dst.exists():
                    dst.unlink()
            git(t, "add", "-A")
            sh(["git", "-c", "user.email=verify@local", "-c", "user.name=verify",
                "commit", "-q", "-m", self.message], t)
            r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                                "tests"], cwd=t, text=True, capture_output=True)
            out = r.stdout or ""
            tail = "\n".join(out.strip().splitlines()[-6:])
            if r.returncode == 0:
                return tail.splitlines()[-1]
            failed = failed_test_ids(out)
            if not failed:
                raise Refused("full suite FAILED on a fresh clone of main + changes:\n" + tail)
            # Which of these were already failing on main WITHOUT the change?
            # (2026-10-05: main was red from bot data written overnight; a
            # code-only fix must not be blocked by it, and must not hide it.)
            b = Path(td) / "b"
            sh(["git", "clone", "-q", "--shared", str(self.repo), str(b)], Path(td))
            git(b, "checkout", "-q", "--detach", self.main)
            rb = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                                 *failed], cwd=b, text=True, capture_output=True)
            already = set(failed_test_ids(rb.stdout or ""))
            new = [f for f in failed if f not in already]
            if new:
                raise Refused("full suite: these FAIL because of the change (they pass on main):\n  "
                              + "\n  ".join(new) + "\n" + tail)
            self.say("PRE-EXISTING ON MAIN — failing without this change too, NOT fixed by it:")
            for f in failed:
                self.say(f"  {f}")
            self.preexisting = failed
            return tail.splitlines()[-1] + f"  ({len(failed)} pre-existing on main)"

    def changed_files(self) -> List[str]:
        # -z: NUL-separated, never trimmed. The first version read
        # `git status --porcelain` through sh(), whose .strip() ate the
        # leading space of " M path" on the FIRST line, so line[3:] cut the
        # first character off the first modified file — which then never
        # reached the test clone or the zip (found 2026-10-05: the pet-food
        # fix's own code file was dropped and its tests failed).
        # The morning pass of 2026-10-05 found the same bug independently and
        # added the second guard below: whatever the parse yields must be a
        # real file (or a deletion), or the run refuses instead of dropping.
        r = subprocess.run(["git", "status", "--porcelain", "-z", "--untracked-files=all"],
                           cwd=self.repo, capture_output=True, text=True)
        if r.returncode != 0:
            raise Refused(f"`git status --porcelain` failed ({r.returncode}): "
                          f"{(r.stderr or '')[-500:]}")
        files = parse_porcelain_z(r.stdout or "")
        deleted = deleted_in_porcelain_z(r.stdout or "")
        missing = [f for f in files if f not in deleted and not (self.repo / f).exists()]
        if missing:
            raise Refused("the changed-file list names paths that do not exist — the "
                          "porcelain parse is wrong and files would be dropped from the "
                          f"zips in silence: {missing[:5]}")
        return files

    # 9
    def write_zips(self, files: List[str]) -> List[Path]:
        self.out.mkdir(parents=True, exist_ok=True)
        sha = self.main[:7]
        d = self.out / f"1-DATA-docs-base-{sha}-UPLOAD-NOW.zip"
        c = self.out / f"2-CODE-pipeline-tests-tools-base-{sha}.zip"
        made = []
        for z, sel in ((d, [f for f in files if f.startswith("docs/")]),
                       (c, [f for f in files if not f.startswith("docs/")])):
            if z.exists():
                z.unlink()
            if not sel:
                continue
            with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in sel:
                    if (self.repo / f).exists():
                        zf.write(self.repo / f, f)
            made.append(z)
        return made

    def go(self) -> int:
        self.check_clean_main()
        before = load_recalls(self.repo)
        if self.code:
            self.apply_zip(self.code, {"code", "other"})
        if self.data:
            self.apply_zip(self.data, {"docs", "report", "monthly"})
        self.rebuild_register()
        # fresh import: the register changed on disk
        for k in [k for k in sys.modules if k.startswith("pipeline")]:
            del sys.modules[k]
        after = load_recalls(self.repo)
        added, removed, changed = diff_rows(before, after)
        self.say(f"Recalls {len(before)} -> {len(after)}: +{len(added)} -{len(removed)} "
                 f"~{len(changed)}")
        for r in sorted(added, key=lambda r: str(r.get("Date"))):
            self.say(f"  + {r.get('Date')} {r.get('Source')} | {str(r.get('Company'))[:40]} | "
                     f"{r.get('Pathogen')}")
        for r in sorted(removed, key=lambda r: str(r.get("Date"))):
            self.say(f"  - {r.get('Date')} {r.get('Source')} | {str(r.get('Company'))[:40]} | "
                     f"{r.get('Pathogen')}")
        for b, a, f in changed:
            self.say(f"  ~ {a.get('Date')} {str(a.get('Company'))[:40]}: "
                     + ", ".join(f"{x} {b.get(x)!r}->{a.get(x)!r}" for x in f))
        if needs_removal_word(len(removed), self.message):
            raise Refused(f"{len(removed)} row(s) removed — the commit message must contain "
                          f"'remove-rows'")
        for n in self.rebuild_reports(added, removed, changed):
            self.say("report: " + n)
        left = self.sweep(removed)
        if left:
            raise Refused("removed rows still on published pages:\n  " + "\n  ".join(left))
        files = self.changed_files()
        self.say(f"{len(files)} changed file(s)")
        self.say("suite: " + self.full_suite(files))
        now = remote_main(self.repo)
        if now != self.main:
            raise Refused(f"main moved during the run ({self.main[:8]} -> {now[:8]}) — "
                          f"re-run on a fresh clone of the new main")
        for z in self.write_zips(files):
            self.say(f"zip: {z}")
        self.say(f"commit message: {self.message}")
        (self.out / "REBASE-REPORT.md").write_text(
            "# Rebase and verify\n\n```\n" + "\n".join(self.log) + "\n```\n", encoding="utf-8")
        return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--code", type=Path)
    ap.add_argument("--data", type=Path)
    ap.add_argument("--message", required=True, help="the commit message George will use")
    ap.add_argument("--out", type=Path, default=Path("/mnt/user-data/outputs"))
    ap.add_argument("--repo", type=Path, default=ROOT)
    ap.add_argument("--no-promote", action="store_true",
                    help="do not run the offline promoter after the repair script")
    a = ap.parse_args(argv)
    if not a.code and not a.data:
        ap.error("give --code and/or --data")
    try:
        return Run(a.repo.resolve(), a.code, a.data, a.message, a.out, not a.no_promote).go()
    except Refused as e:
        print(f"\nREFUSED — nothing zipped.\n{e}", file=sys.stderr)
        return 2


if __name__ == "__main__":                                   # pragma: no cover
    raise SystemExit(main())
