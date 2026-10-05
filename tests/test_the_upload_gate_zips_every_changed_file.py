"""The gate must not drop a file it just rebuilt. 2026-10-05.

``tools/rebase_and_verify.py`` is the last thing between a morning pass and
the register. On 2026-10-05 it reported "14 changed file(s)", printed
"report: 2026-W39.html: rebuilt", and then shipped 13 files. The missing
one was ``docs/2026-W39.html`` — the page it had just rebuilt. The data zip
carried a weekly-index saying week 39 held 68 recalls while the page on the
site still said 67.

THE CAUSE. ``sh()`` returns ``stdout.strip()``. git's porcelain v1 format
is ``XY<space>path``, and for a worktree-modified file ``XY`` is ``" M"`` —
so every such line begins with a space, and ``.strip()`` removes it from
the FIRST line of the output only. ``changed_files`` then sliced
``line[3:]``:

    " M docs/2026-W39.html"  ->  "M docs/2026-W39.html"  ->  "ocs/2026-W39.html"

``"ocs/…"`` does not start with ``"docs/"``, so the file went into the CODE
zip's selection, where ``(repo / f).exists()`` was False and it was dropped
without a word. Whichever path sorts first in git's output is the casualty;
on a run with no page rebuilt that is
``docs/data/afts-recalls-public.xlsx``.

A silent drop here is worse than a refusal. The tool exists so that a pass
cannot ship a partial set, and this made it ship one while reporting
success.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

rv = pytest.importorskip("tools.rebase_and_verify")


def _repo(tmp_path):
    r = tmp_path / "repo"
    (r / "docs" / "data").mkdir(parents=True)
    (r / "pipeline").mkdir()
    for f, body in (("docs/2026-W39.html", "<html>old</html>"),
                    ("docs/data/recalls.json", "[]"),
                    ("pipeline/merge_master.py", "x = 1\n")):
        (r / f).write_text(body, encoding="utf-8")
    env = dict(**{"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                  "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})
    import os
    env = {**os.environ, **env}
    subprocess.run(["git", "init", "-q"], cwd=r, check=True, env=env)
    subprocess.run(["git", "add", "-A"], cwd=r, check=True, env=env)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=r, check=True, env=env)
    return r


def _run(repo, out):
    return rv.Run(repo, None, None, "msg", out, False)


def test_a_modified_file_that_sorts_first_is_not_truncated(tmp_path):
    """The exact 2026-10-05 shape: docs/2026-W39.html sorts before docs/data/."""
    repo = _repo(tmp_path)
    (repo / "docs" / "2026-W39.html").write_text("<html>new</html>",
                                                 encoding="utf-8")
    files = _run(repo, tmp_path / "out").changed_files()
    assert "docs/2026-W39.html" in files, (
        f"the first modified path came back truncated: {files}. "
        f"git porcelain writes ' M path' and sh() strips the leading space "
        f"off the first line, so a fixed line[3:] slice eats the path's "
        f"first character.")


def test_every_changed_file_lands_in_exactly_one_zip(tmp_path):
    repo = _repo(tmp_path)
    (repo / "docs" / "2026-W39.html").write_text("<html>new</html>",
                                                 encoding="utf-8")
    (repo / "docs" / "data" / "recalls.json").write_text("[1]",
                                                         encoding="utf-8")
    (repo / "pipeline" / "merge_master.py").write_text("x = 2\n",
                                                       encoding="utf-8")
    (repo / "tools").mkdir()
    (repo / "tools" / "repair_2026_10_05.py").write_text("# new\n",
                                                         encoding="utf-8")
    out = tmp_path / "out"
    run = _run(repo, out)
    run.main = "0" * 40
    files = run.changed_files()
    import zipfile
    shipped = set()
    for z in run.write_zips(files):
        shipped |= set(zipfile.ZipFile(z).namelist())
    assert shipped == set(files), (
        f"the gate reported {len(files)} changed file(s) and shipped "
        f"{len(shipped)}. Missing: {sorted(set(files) - shipped)}. A file "
        f"the tool rebuilt and then did not ship is how a partial upload "
        f"happens while the run reports success.")


def test_a_path_that_does_not_exist_refuses_rather_than_dropping(tmp_path):
    """The guard that would have caught the original bug loudly."""
    repo = _repo(tmp_path)
    (repo / "docs" / "2026-W39.html").write_text("<html>new</html>",
                                                 encoding="utf-8")
    run = _run(repo, tmp_path / "out")
    real = subprocess.run
    import tools.rebase_and_verify as mod

    class _Fake:
        returncode = 0
        stdout = "M ocs/2026-W39.html\n"      # the truncated shape
        stderr = ""

    mod.subprocess.run = lambda *a, **k: _Fake()
    try:
        with pytest.raises(mod.Refused):
            run.changed_files()
    finally:
        mod.subprocess.run = real


def test_changed_files_does_not_slice_stripped_output():
    """Pinned in source: the strip is what made the slice wrong."""
    src = (ROOT / "tools" / "rebase_and_verify.py").read_text(encoding="utf-8")
    i = src.index("def changed_files")
    body = src[i:src.index("\n    # 9", i)]
    assert "git(self.repo" not in body, (
        "changed_files must not read git status through sh()/git(), which "
        "returns stdout.strip() and eats the leading status space of the "
        "first line.")
