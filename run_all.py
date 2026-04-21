"""
Commit updated data files to GitHub.
- In GitHub Actions: relies on actions/checkout + actions/upload + workflow git push
- Locally: uses subprocess git add/commit/push (requires GH_TOKEN or local git creds)
"""
from __future__ import annotations
import os
import subprocess
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def git_commit_and_push(repo_dir: Path, files: list[str], message: str) -> bool:
    """Stage, commit, push files. Returns True on success."""
    try:
        # Configure committer (CI-friendly)
        subprocess.run(["git", "-C", str(repo_dir), "config", "user.email",
                        os.getenv("GIT_USER_EMAIL", "fsis-bot@advfood.tech")], check=True)
        subprocess.run(["git", "-C", str(repo_dir), "config", "user.name",
                        os.getenv("GIT_USER_NAME", "FSIS Bot")], check=True)

        # Stage
        for f in files:
            subprocess.run(["git", "-C", str(repo_dir), "add", f], check=True)

        # Check if there are changes to commit
        result = subprocess.run(
            ["git", "-C", str(repo_dir), "diff", "--cached", "--quiet"],
            capture_output=True,
        )
        if result.returncode == 0:
            log.info("No changes to commit")
            return True

        # Commit
        subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", message], check=True)

        # Push (token in URL or via credential helper)
        token = os.getenv("GH_TOKEN", "")
        if token:
            # Get current remote URL, inject token
            remote = subprocess.run(
                ["git", "-C", str(repo_dir), "remote", "get-url", "origin"],
                capture_output=True, text=True, check=True,
            ).stdout.strip()
            if remote.startswith("https://github.com/"):
                authed = remote.replace("https://github.com/", f"https://x-access-token:{token}@github.com/")
                subprocess.run(["git", "-C", str(repo_dir), "push", authed, "HEAD"], check=True)
            else:
                subprocess.run(["git", "-C", str(repo_dir), "push"], check=True)
        else:
            subprocess.run(["git", "-C", str(repo_dir), "push"], check=True)

        log.info("Pushed: %s", message)
        return True
    except subprocess.CalledProcessError as e:
        log.error("git command failed: %s", e)
        return False
    except Exception as e:
        log.error("commit_and_push failed: %s", e)
        return False
