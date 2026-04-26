"""
Commit updated data files to GitHub — with retry for push conflicts.

Binary files like recalls.xlsx cannot be rebased, so the retry strategy is:
  1. Try git push
  2. On failure: save our changed files, pull remote, restore ours, re-commit
  3. Retry up to MAX_PUSH_ATTEMPTS times

This handles the common "non-fast-forward" error when two workflows in
the fsis-data-writers concurrency group finish close together.
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


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a git command, logging failures."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        log.debug("cmd=%s rc=%d stderr=%s", " ".join(cmd),
                  result.returncode, result.stderr.strip())
    return result


def git_commit_and_push(repo_dir: Path, files: list[str], message: str) -> bool:
    """Stage, commit, push files with retry. Returns True on success."""
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

            # ── Binary-safe retry ──────────────────────────────────────
            # git rebase can't handle binary xlsx, so instead we:
            #   1. Identify which files we changed
            #   2. Save physical copies
            #   3. Hard-reset to pre-commit state + pull remote
            #   4. Restore our files on top of the updated remote
            #   5. Re-stage + commit

            # 1. Collect our changed file paths
            changed = _run(["git", "-C", cwd, "diff",
                            "HEAD~1", "--name-only"])
            our_files = [
                ln.strip() for ln in changed.stdout.splitlines()
                if ln.strip()
            ]

            # 2. Save copies to a temp dir
            tmp = Path(cwd) / ".push-retry-tmp"
            tmp.mkdir(exist_ok=True)
            saved: list[tuple[str, Path]] = []
            for rel in our_files:
                src = Path(cwd) / rel
                if src.exists():
                    dst = tmp / rel.replace("/", "__")
                    shutil.copy2(src, dst)
                    saved.append((rel, dst))

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
            xlsx_rel = "docs/data/recalls.xlsx"
            json_rel = "docs/data/recalls.json"
            saved_map = dict(saved)

            if xlsx_rel in saved_map and (Path(cwd) / xlsx_rel).exists():
                try:
                    from pipeline.xlsx_merge import merge_xlsx_with_remote
                    from pipeline.merge_master import mirror_json_from_xlsx
                    remote_xlsx = Path(cwd) / xlsx_rel
                    counts = merge_xlsx_with_remote(
                        remote_path=remote_xlsx,
                        ours_path=saved_map[xlsx_rel],
                        out_path=remote_xlsx,
                    )
                    log.info("Push retry: merged xlsx instead of overwriting "
                             "(remote=%d, ours=%d, merged=%d recalls)",
                             counts.get("recalls_remote", -1),
                             counts.get("recalls_ours", -1),
                             counts.get("recalls_merged", -1))
                    saved = [(rel, dst) for rel, dst in saved if rel != xlsx_rel]
                    if json_rel in saved_map:
                        try:
                            mirror_json_from_xlsx(remote_xlsx,
                                                   Path(cwd) / json_rel)
                            saved = [(rel, dst) for rel, dst in saved
                                     if rel != json_rel]
                        except Exception as je:
                            log.warning("json mirror regenerate failed: %s", je)
                except Exception as me:
                    log.error("xlsx merge failed (%s) — falling back to "
                              "the legacy ours-wins overwrite. This may "
                              "cause data loss.", me)

            # For non-cumulative files (logs, configs, html outputs) the
            # original ours-wins copy is acceptable.
            for rel, dst in saved:
                dest = Path(cwd) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dst, dest)
            shutil.rmtree(tmp, ignore_errors=True)

            # 5. Re-stage and commit
            for rel, _ in saved:
                _run(["git", "-C", cwd, "add", rel])
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
