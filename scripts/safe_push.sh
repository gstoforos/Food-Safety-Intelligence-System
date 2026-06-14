#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════
#  AFTS — race-safe xlsx commit + push
# ════════════════════════════════════════════════════════════════════════
#  Many data-writer workflows push to main concurrently (gap-finders, NA /
#  Asia / LATAM / Oceania collectors, news-feed, merge-master). A bare
#  `git push` loses to whoever pushed first:
#       ! [rejected] main -> main (fetch first)
#  and the whole run's work is discarded. recalls.xlsx is a BINARY file and
#  cannot be git-merged, so on rejection we row-union our rows back in via
#  pipeline.xlsx_merge (which has a no-shrink canary so it never silently
#  drops rows), then retry. Up to 5 attempts with backoff.
#
#  USAGE:
#    scripts/safe_push.sh "<commit message>" <path1> [path2 ...]
#
#  EXAMPLE:
#    scripts/safe_push.sh "Greek gap finder: $(date -u +%F) auto-update" \
#        docs/data/recalls.xlsx docs/data/gap_finder_gr/
#
#  The FIRST path MUST be docs/data/recalls.xlsx (the file that needs
#  row-union merge on conflict). Extra paths (per-country jsonl dirs) are
#  committed alongside but are not row-merged — on conflict they take the
#  remote's version after rebase, which is correct (they're regenerated
#  each run).
#
#  EXIT CODES:
#    0  pushed successfully
#    1  push failed after 5 attempts, or xlsx_merge tripped its canary
# ════════════════════════════════════════════════════════════════════════
set -euo pipefail

MSG="${1:?commit message required}"
shift
PATHS=("$@")
if [ "${#PATHS[@]}" -eq 0 ]; then
  echo "safe_push: no paths given" >&2
  exit 1
fi

XLSX="docs/data/recalls.xlsx"

git config user.name  "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

git add "${PATHS[@]}" || true
if git diff --cached --quiet; then
  echo "No changes to commit."
  exit 0
fi
git commit -m "$MSG"

PUSHED=0
for attempt in 1 2 3 4 5; do
  if git push; then
    echo "✓ pushed on attempt $attempt"
    PUSHED=1
    break
  fi
  echo "push attempt $attempt rejected — syncing remote and row-merging"

  # Preserve our xlsx, drop our commit, fast-forward to origin.
  cp "$XLSX" /tmp/ours_recalls.xlsx
  git reset --hard HEAD~1
  git pull --rebase --autostash origin main \
    || git pull --no-rebase origin main

  # Row-union OUR rows back into the freshly-pulled xlsx.
  # CLI is positional: <remote> <ours> <out>.
  set +e
  python -m pipeline.xlsx_merge "$XLSX" /tmp/ours_recalls.xlsx "$XLSX"
  MERGE_RC=$?
  set -e
  if [ "$MERGE_RC" -ne 0 ]; then
    echo "  ✗ xlsx_merge failed (rc=$MERGE_RC) — aborting to avoid data loss" >&2
    exit 1
  fi
  echo "  row-union merge ok"

  git add "${PATHS[@]}" || true
  git commit -m "$MSG (merge attempt $attempt)"
  sleep $((attempt * 2))
done

if [ "$PUSHED" -ne 1 ]; then
  echo "✗ push failed after 5 attempts" >&2
  exit 1
fi
