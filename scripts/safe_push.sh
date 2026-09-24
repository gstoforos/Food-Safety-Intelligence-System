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

# ── Stage only the paths that exist ─────────────────────────────────────
#
# WHY THIS FUNCTION EXISTS (2026-09-23). It used to be one line:
#
#     git add "${PATHS[@]}" || true
#
# git add is ATOMIC across pathspecs. If ANY pathspec matches nothing it
# aborts with "fatal: pathspec ... did not match any files" and stages
# NOTHING — not even the paths that were fine. The "|| true" swallowed
# that, `git diff --cached --quiet` then saw an empty index, and the
# script printed "No changes to commit." and exited 0.
#
# So a single wrong path argument silently discarded a whole run's work
# and left the workflow GREEN.
#
# That is not hypothetical. Three gap finders were passing a REGION-named
# directory that the pipeline never creates — CountryConfig.data_dir is
# f"docs/data/gap_finder_{code}", so a region name cannot ever exist:
#
#     scandinavian_gap_finder.yml   docs/data/gap_finder_nordic/
#     east_eu_gap_finder.yml        docs/data/gap_finder_easteu/
#     central_eu_gap_finder.yml     docs/data/gap_finder_centraleu/
#
# while africa_gap_finder.yml, doing the same multi-country job, listed
# its countries properly (gap_finder_za/ gap_finder_ng/) and worked.
#
# On 2026-09-23 those three ran, found recalls, wrote them into
# recalls.xlsx, and committed nothing: Sweden 10 rejected rows, Norway 2
# Pending + 13 rejected, Moldova 7, Czechia 5, Croatia 1, Iceland 2 — and
# ZERO rows dated 2026-09-23 in the register from any of them. Norway's
# four accepted Listeria recalls included. Every run since the paths were
# introduced did the same, daily, with a green tick.
#
# Missing paths are now skipped with a warning instead of poisoning the
# whole add, and a genuine git failure is no longer swallowed.
stage_existing() {
  local -a present=()
  local p
  for p in "${PATHS[@]}"; do
    if [ -e "${p%/}" ]; then
      present+=("$p")
    else
      echo "safe_push: WARNING — path does not exist, skipping: $p" >&2
      echo "safe_push:   (if this is a gap_finder_<cc>/ dir, the country may" >&2
      echo "safe_push:    not have written one yet — that is fine. If it is a" >&2
      echo "safe_push:    REGION name like gap_finder_nordic/, it is a bug:" >&2
      echo "safe_push:    the pipeline only ever writes per-country dirs.)" >&2
    fi
  done
  if [ "${#present[@]}" -eq 0 ]; then
    echo "safe_push: ✗ none of the given paths exist — nothing can be committed" >&2
    printf 'safe_push:     %s\n' "${PATHS[@]}" >&2
    exit 1
  fi
  git add "${present[@]}"
}

# ── Canary: work done but nothing staged ────────────────────────────────
#
# The failure above was invisible because "nothing staged" and "nothing
# changed" print the same message. They are not the same thing. If the
# xlsx is dirty in the working tree but the index is empty, the run
# produced work that is about to be thrown away — fail loudly instead.
assert_not_silently_dropping_work() {
  if ! git diff --cached --quiet; then
    return 0                      # something is staged, all good
  fi
  if [ -n "$(git status --porcelain -- "$XLSX")" ]; then
    echo "safe_push: ✗ $XLSX HAS CHANGED but nothing is staged." >&2
    echo "safe_push:   This run did work that is about to be discarded." >&2
    echo "safe_push:   Check the paths passed to safe_push.sh:" >&2
    printf 'safe_push:     %s\n' "${PATHS[@]}" >&2
    exit 1
  fi
  echo "No changes to commit."
  exit 0
}


stage_existing
assert_not_silently_dropping_work
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

  stage_existing
  git commit -m "$MSG (merge attempt $attempt)"
  sleep $((attempt * 2))
done

if [ "$PUSHED" -ne 1 ]; then
  echo "✗ push failed after 5 attempts" >&2
  exit 1
fi
