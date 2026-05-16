"""
Commit updated data files to GitHub — with PRE-EMPTIVE merge + retry-merge.

==============================================================================
WHAT THIS FIXES — RACE STOMPS (audit 2026-05-16)
==============================================================================
Pre-fix, the merge logic only fired AFTER a push conflict. A workflow that
pushed without conflict but had loaded xlsx from a stale checkout would
silently overwrite remote rows that had been committed between its load
and its save. This was the documented cause of the 17:06→19:02 UTC
claude-check stomp on 2026-05-16, when 6 promoted Recalls rows were lost
to a clean (non-conflicting) push from a workflow that had loaded the
xlsx pre-17:06.

==============================================================================
ARCHITECTURE — TWO LAYERS OF SAFETY
==============================================================================
LAYER 1 (NEW) — Pre-emptive fetch+merge before the first push
    git fetch origin
    If remote has commits we don't: pull --ff-only after stashing our
    staged changes. If pull altered xlsx, row-union-merge our staged
    version against the new remote (via pipeline.xlsx_merge) BEFORE we
    push. This catches the load-modify-save staleness path that the
    on-conflict retry missed.

LAYER 2 (EXISTING) — Conflict-driven retry merge
    If push fails despite layer 1 (another writer landed between our
    layer-1 sync and our push), we reset-hard, pull, merge, and retry.

The two layers are belt-and-suspenders. Concurrency groups in the
workflow .yml files are layer 0 (the killshot — they prevent overlap in
the first place). All three together make the row-loss class impossible.

Binary files like recalls.xlsx cannot be rebased, so both layers rely
on the row-union logic in pipeline.xlsx_merge — never on git's own
3-way merge. xlsx_merge.py has a canary assertion that fires if any
sheet shrinks during merge; this module respects that signal and
aborts the push rather than ship data loss.
"""
from __future__ import annotations
import os
import shutil
import subprocess
import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)

MAX_PUSH_ATTEMPTS = 3

# Cumulative shared-state files that MUST be row-union-merged when they
# exist on both local + remote, never overwritten. See xlsx_merge.py for
# the per-sheet merge rules.
XLSX_REL = "docs/data/recalls.xlsx"
JSON_REL = "docs/data/recalls.json"


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a git command, logging failures."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        log.debug("cmd=%s rc=%d stderr=%s", " ".join(cmd),
                  result.returncode, result.stderr.strip())
    return result


def _row_union_merge_xlsx(cwd: str, ours_snapshot: Path) -> bool:
    """Row-union our snapshot with the freshly-pulled remote xlsx, write
    back to the remote location in the working tree. xlsx_merge enforces
    a canary (merged >= max(remote, ours) per sheet) and raises on
    shrink — we treat that as a hard fail.

    Returns True on success, False on failure (caller decides fallback)."""
    try:
        from pipeline.xlsx_merge import merge_xlsx_with_remote
        remote_xlsx = Path(cwd) / XLSX_REL
        counts = merge_xlsx_with_remote(
            remote_path=remote_xlsx,
            ours_path=ours_snapshot,
            out_path=remote_xlsx,
        )
        log.info("xlsx row-union merge: recalls remote=%d ours=%d merged=%d; "
                 "pending remote=%d ours=%d merged=%d",
                 counts.get("recalls_remote", -1),
                 counts.get("recalls_ours", -1),
                 counts.get("recalls_merged", -1),
                 counts.get("pending_remote", -1),
                 counts.get("pending_ours", -1),
                 counts.get("pending_merged", -1))
        return True
    except AssertionError as ae:
        # Canary tripped — merge would have shrunk a sheet. Fail loud.
        log.error("xlsx merge CANARY tripped — refusing to write merged "
                  "file that would lose rows: %s", ae)
        return False
    except Exception as me:
        log.error("xlsx merge failed: %s", me)
        return False


def _regenerate_json_mirror(cwd: str) -> bool:
    """Regenerate docs/data/recalls.json from the (just-merged) xlsx so
    the mirror matches. Idempotent."""
    try:
        from pipeline.merge_master import mirror_json_from_xlsx
        mirror_json_from_xlsx(Path(cwd) / XLSX_REL, Path(cwd) / JSON_REL)
        return True
    except Exception as je:
        log.warning("json mirror regenerate failed: %s", je)
        return False


def _preemptive_sync(cwd: str, files: list[str]) -> None:
    """LAYER 1: before the first commit, fetch from remote and reconcile
    if we're behind. If our staged xlsx exists and remote also moved the
    xlsx forward, row-union-merge BEFORE the push so the push is
    fast-forward.

    Behaviour:
      - If we're up to date: noop.
      - If remote has new commits AND we have staged changes: stash, pull,
        row-merge xlsx against new remote, regenerate json mirror, re-stage.
      - If ff-only pull is rejected (local has diverged): noop; let the
        post-push retry merge handle it.

    Idempotent and safe to call before every commit. Errors are caught
    and logged at the call site; this never raises."""
    branch = (_run(["git", "-C", cwd, "branch", "--show-current"])
              .stdout.strip() or "main")

    _run(["git", "-C", cwd, "fetch", "origin", branch])

    # How many commits behind are we?
    behind = _run(["git", "-C", cwd, "rev-list", "--count",
                   f"HEAD..origin/{branch}"])
    n_behind = int(behind.stdout.strip() or "0") if behind.returncode == 0 else 0
    if n_behind == 0:
        return  # already up to date — no reconciliation needed

    log.info("Pre-emptive sync: local is %d commit(s) behind origin/%s — "
             "reconciling before first push", n_behind, branch)

    # Save our currently-staged xlsx + json (and any other tracked file)
    # BEFORE pulling, so we can merge them against the new remote.
    tmp = Path(cwd) / ".presync-tmp"
    tmp.mkdir(exist_ok=True)
    saved: dict[str, Path] = {}
    for rel in files:
        src = Path(cwd) / rel
        if src.exists():
            dst = tmp / rel.replace("/", "__")
            shutil.copy2(src, dst)
            saved[rel] = dst

    # Stash staged changes; pull; restore.
    stash = _run(["git", "-C", cwd, "stash", "push", "--include-untracked",
                  "-m", "presync-stash"])
    stashed = stash.returncode == 0 and "No local changes" not in stash.stdout

    pull = _run(["git", "-C", cwd, "pull", "--ff-only", "origin", branch])
    if pull.returncode != 0:
        # ff-only rejected — local has diverged. Restore stash and let
        # the post-push retry handle the conflict.
        log.info("Pre-emptive sync: ff-only pull rejected (local diverged) "
                 "— deferring to post-push retry")
        if stashed:
            _run(["git", "-C", cwd, "stash", "pop"])
        shutil.rmtree(tmp, ignore_errors=True)
        return

    # Pull succeeded. Now reconcile our staged content with the new remote.
    if stashed:
        pop = _run(["git", "-C", cwd, "stash", "pop"])
        if pop.returncode != 0:
            # Stash pop reported a conflict — xlsx is the most likely
            # culprit. Row-union-merge our snapshot against the just-
            # pulled remote, then restore the other files from our
            # snapshots (keeping ours for non-cumulative paths).
            log.info("Pre-emptive sync: stash pop conflict — row-merging "
                     "xlsx, keeping ours for other files")
            if XLSX_REL in saved and (Path(cwd) / XLSX_REL).exists():
                _row_union_merge_xlsx(cwd, saved[XLSX_REL])
                _regenerate_json_mirror(cwd)
            for rel, snap in saved.items():
                if rel in (XLSX_REL, JSON_REL):
                    continue
                dest = Path(cwd) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(snap, dest)
            # Drop the conflicted stash; we've reconciled manually.
            _run(["git", "-C", cwd, "stash", "drop"])
            # Re-stage everything.
            for rel in files:
                _run(["git", "-C", cwd, "add", "--", rel])
    else:
        # No stash existed (changes were just on disk, not in index).
        # Our files are still in working tree post-pull. If xlsx changed
        # on remote AND we have a snapshot, row-union-merge.
        if XLSX_REL in saved and (Path(cwd) / XLSX_REL).exists():
            _row_union_merge_xlsx(cwd, saved[XLSX_REL])
            _regenerate_json_mirror(cwd)
            for rel in files:
                _run(["git", "-C", cwd, "add", "--", rel])

    shutil.rmtree(tmp, ignore_errors=True)


def git_commit_and_push(repo_dir: Path, files: list[str], message: str) -> bool:
    """Stage, commit, push files with PRE-EMPTIVE sync + retry-merge.
    Returns True on success.
    """
    try:
        cwd = str(repo_dir)

        # Configure committer (CI-friendly)
        _run(["git", "-C", cwd, "config", "user.email",
              os.getenv("GIT_USER_EMAIL", "fsis-bot@advfood.tech")])
        _run(["git", "-C", cwd, "config", "user.name",
              os.getenv("GIT_USER_NAME", "FSIS Bot")])

        # Stage
        for f in files:
            _run(["git", "-C", cwd, "add", f])

        # Check if there are changes to commit
        result = _run(["git", "-C", cwd, "diff", "--cached", "--quiet"])
        if result.returncode == 0:
            log.info("No changes to commit")
            return True

        # ── LAYER 1: pre-emptive sync ───────────────────────────────────
        # Reconcile with remote BEFORE we commit. If remote has moved on,
        # row-union-merge our staged xlsx with the new remote so the push
        # is fast-forward and no rows can be lost.
        try:
            _preemptive_sync(cwd, files)
        except Exception as e:
            log.warning("Pre-emptive sync raised — continuing to commit: %s", e)

        # Commit
        _run(["git", "-C", cwd, "commit", "-m", message])

        # Push with retry
        push_cmd = _build_push_cmd(cwd)

        for attempt in range(1, MAX_PUSH_ATTEMPTS + 1):
            result = _run(push_cmd)
            if result.returncode == 0:
                log.info("Pushed (attempt %d): %s", attempt, message)
                return True

            log.warning("Push attempt %d failed: %s",
                        attempt, result.stderr.strip())
            if attempt == MAX_PUSH_ATTEMPTS:
                break

            # ── LAYER 2: Binary-safe retry merge ────────────────────────
            # git rebase can't handle binary xlsx, so instead we:
            #   1. Identify which files we changed AND classify them
            #      as modifications/additions vs deletions
            #   2. Save physical copies of modifications/additions
            #   3. Hard-reset to pre-commit state + pull remote
            #      (this restores deleted files from HEAD~1)
            #   4. Restore our modifications on top of the updated remote;
            #      re-apply our deletions by unlinking from working tree
            #   5. Re-stage everything (modifications AND deletions) and commit
            #
            # Audit 2026-05-14: pre-fix, this block used `--name-only` and
            # filtered out non-existent paths with `if src.exists()`, which
            # silently dropped every deletion. After reset --hard HEAD~1 +
            # pull, the deleted files reappeared from HEAD~1 and were
            # never re-staged for the retry commit. Net effect: when push
            # failed due to a concurrent-workflow conflict (common in
            # this pipeline), the retry commit included our modifications
            # but NOT our deletions. Orphan HTMLs accumulated in
            # docs/daily/ for weeks until disk hygiene was investigated.

            # 1. Collect our changed file paths + status (M/A/D/R/C)
            changed = _run(["git", "-C", cwd, "diff",
                            "HEAD~1", "--name-status"])
            modifications: list[str] = []  # M or A — file exists post-commit
            deletions: list[str] = []       # D — file removed post-commit
            for ln in changed.stdout.splitlines():
                parts = ln.split("\t")
                if len(parts) < 2:
                    continue
                status = parts[0].strip()
                if not status:
                    continue
                if status.startswith("D"):
                    deletions.append(parts[1].strip())
                elif status.startswith("R") or status.startswith("C"):
                    # Rename / copy: parts = [status, old_path, new_path].
                    # Old path is gone (treat as deletion); new path is
                    # present (treat as modification/addition).
                    if len(parts) >= 3:
                        deletions.append(parts[1].strip())
                        modifications.append(parts[2].strip())
                    else:
                        # Malformed line — best-effort fall-through
                        modifications.append(parts[1].strip())
                else:
                    # M, A, T (typechange), U (unmerged) — file should
                    # exist in working tree
                    modifications.append(parts[1].strip())

            # 2. Save copies of modifications/additions to a temp dir.
            #    Deletions don't need saving (file is already gone).
            tmp = Path(cwd) / ".push-retry-tmp"
            tmp.mkdir(exist_ok=True)
            saved: list[tuple[str, Path]] = []
            for rel in modifications:
                src = Path(cwd) / rel
                if src.exists():
                    dst = tmp / rel.replace("/", "__")
                    shutil.copy2(src, dst)
                    saved.append((rel, dst))
                else:
                    # Modification claimed by `git diff` but file is gone
                    # from working tree — odd state, log it and skip.
                    log.warning("Push retry: expected modification at %s "
                                "but file is missing — skipping save", rel)

            # 3. Undo our commit, pull remote
            branch = _run(["git", "-C", cwd, "branch",
                           "--show-current"]).stdout.strip() or "main"
            _run(["git", "-C", cwd, "reset", "--hard", "HEAD~1"])
            _run(["git", "-C", cwd, "pull", "--ff-only",
                  "origin", branch])

            # 4. Restore our files on top of the updated remote.
            #
            # CRITICAL: for cumulative shared state (recalls.xlsx and the
            # mirror recalls.json), we MUST NOT blindly copy our local
            # over the freshly-pulled remote — that destroys rows the
            # remote-winner just added. Instead, merge xlsx by union of
            # rows (URL primary, fallback to Date+Company+Pathogen) and
            # regenerate the json mirror from the merged xlsx.
            saved_map = dict(saved)

            if XLSX_REL in saved_map and (Path(cwd) / XLSX_REL).exists():
                if _row_union_merge_xlsx(cwd, saved_map[XLSX_REL]):
                    saved = [(rel, dst) for rel, dst in saved if rel != XLSX_REL]
                    # CRITICAL FIX 2026-05-14 evening: explicitly stage the
                    # merged xlsx. Removing it from `saved` is correct so
                    # step 4b's "copy ours over working tree" loop doesn't
                    # clobber the merged file with our raw saved version.
                    # BUT step 5's `git add` loop only iterates `saved` —
                    # so without an explicit add here the merged xlsx
                    # sits modified-but-unstaged on disk, and the retry
                    # commit silently drops it.
                    _run(["git", "-C", cwd, "add", "--", XLSX_REL])
                    if JSON_REL in saved_map:
                        if _regenerate_json_mirror(cwd):
                            saved = [(rel, dst) for rel, dst in saved
                                     if rel != JSON_REL]
                            _run(["git", "-C", cwd, "add", "--", JSON_REL])
                else:
                    # AUDIT 2026-05-16: when the canary trips (a row-loss
                    # would happen if we merged), we ABORT rather than
                    # fall back to the old ours-wins overwrite. Losing a
                    # push attempt is recoverable; losing rows is not.
                    log.error("xlsx merge failed/canary-tripped — refusing "
                              "to fall back to ours-wins overwrite. "
                              "Aborting push to avoid data loss.")
                    shutil.rmtree(tmp, ignore_errors=True)
                    return False

            # For non-cumulative files (logs, configs, html outputs) the
            # original ours-wins copy is acceptable.
            for rel, dst in saved:
                dest = Path(cwd) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dst, dest)
            shutil.rmtree(tmp, ignore_errors=True)

            # Re-apply our deletions. After `git reset --hard HEAD~1` +
            # `git pull`, any files we deleted in our original commit
            # are back in the working tree (either restored from HEAD~1
            # or pulled from remote). To preserve our intent, we unlink
            # them again. If a deletion path is not in the working tree
            # post-pull, it means remote also deleted it (or never had
            # it) — either way, nothing to do.
            redeleted = 0
            for rel in deletions:
                dest = Path(cwd) / rel
                try:
                    if dest.exists():
                        dest.unlink()
                        redeleted += 1
                except Exception as e:
                    log.warning("Push retry: could not re-delete %s: %s",
                                rel, e)
            if deletions:
                log.info("Push retry: re-applied %d/%d deletions",
                         redeleted, len(deletions))

            # 5. Re-stage and commit. For modifications: file is on
            # disk, `git add` stages the modification. For deletions:
            # file is missing from working tree, `git add` stages the
            # deletion (verified empirically — `git add -- <missing>`
            # on a tracked path stages the deletion, exit 0).
            for rel, _ in saved:
                _run(["git", "-C", cwd, "add", "--", rel])
            for rel in deletions:
                _run(["git", "-C", cwd, "add", "--", rel])
            check = _run(["git", "-C", cwd, "diff",
                          "--cached", "--quiet"])
            if check.returncode != 0:
                retry_msg = f"{message} (retry {attempt})"
                _run(["git", "-C", cwd, "commit", "-m", retry_msg])

            time.sleep(attempt * 3)

        log.error("git push failed after %d attempts", MAX_PUSH_ATTEMPTS)
        return False

    except Exception as e:
        log.error("commit_and_push failed: %s", e)
        return False


def _build_push_cmd(cwd: str) -> list[str]:
    """Build the git push command, injecting token if available."""
    token = os.getenv("GH_TOKEN", "")
    if token:
        remote = _run(["git", "-C", cwd, "remote", "get-url", "origin"])
        url = remote.stdout.strip()
        if url.startswith("https://github.com/"):
            authed = url.replace(
                "https://github.com/",
                f"https://x-access-token:{token}@github.com/",
            )
            return ["git", "-C", cwd, "push", authed, "HEAD"]
    return ["git", "-C", cwd, "push"]
