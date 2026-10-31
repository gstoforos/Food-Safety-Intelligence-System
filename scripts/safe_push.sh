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
#  committed alongside; on a push rejection they are carried over the
#  rebase by preserve_our_non_xlsx_work / restore_our_non_xlsx_work below.
#
#  CORRECTION (morning fix 2026-10-30). These lines used to say the extra
#  paths "take the remote's version after rebase, which is correct
#  (they're regenerated each run)". BOTH HALVES WERE WRONG.
#
#  They did not take the remote's version by design — `git reset --hard
#  HEAD~1` on the retry path reverted the working tree for every file in
#  our commit, and only the xlsx had been copied aside. And run_log.jsonl
#  is NOT regenerated each run: it is APPEND-ONLY, one record per country
#  per exit, and it is what tests/test_no_country_goes_dark.py and
#  tools/fleet_shard.catchup_codes read to decide whether a country has
#  gone dark.
#
#  MEASURED on 2026-10-30: all 170 "(merge attempt N)" commits in the
#  preceding 30 days carried docs/data/recalls.xlsx and NOTHING ELSE. Not
#  one carried a single jsonl file. The Nordic and Central EU gap finders
#  retry every single day, so their per-country run logs had effectively
#  stopped being committed at all.
#
#  What that cost, concretely: the fleet ran 'us' on 2026-10-29 inside a
#  shard of 9, the run wrote its record, the retry threw the record away,
#  and tests/test_no_country_goes_dark.py went red with "'us' last ran
#  2026-10-14, 15 days ago" for a country that was running fine. Worse,
#  catchup_codes reads the same log, so 'us' was force-added to the
#  catch-up every day from 2026-10-24 on, burning one of the three daily
#  catch-up slots that genuinely stale countries needed.
#
#  The xlsx was never the only output worth keeping.
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


# ── Carry the run's NON-xlsx work over the retry rebase ─────────────────
#
# See the CORRECTION in the header. `git reset --hard HEAD~1` below reverts
# the working tree for every path in our commit. The xlsx survives because
# it is copied aside and row-unioned; everything else used to be lost.
#
# preserve_* runs while our commit is still HEAD and squirrels away our
# version of each non-xlsx path (plus the list of paths our commit
# DELETED, so a deletion is not resurrected by the rebase).
# restore_* puts them back on top of the freshly pulled tree.
OURS_DIR=""

preserve_our_non_xlsx_work() {
  OURS_DIR="$(mktemp -d)"
  : > "$OURS_DIR/keep.z"
  : > "$OURS_DIR/gone.z"
  local path status
  while IFS= read -r -d '' status && IFS= read -r -d '' path; do
    [ "$path" = "$XLSX" ] && continue
    case "$status" in
      D*)
        printf '%s\0' "$path" >> "$OURS_DIR/gone.z"
        ;;
      *)
        [ -e "$path" ] || continue
        mkdir -p "$OURS_DIR/tree/$(dirname "$path")"
        cp -p "$path" "$OURS_DIR/tree/$path"
        printf '%s\0' "$path" >> "$OURS_DIR/keep.z"
        ;;
    esac
  done < <(git diff -z --name-status --no-renames HEAD~1 HEAD)
}

# A run_log.jsonl is append-only, and on a retry BOTH sides may hold a
# record the other does not: ours from this run, the remote's from a
# concurrent writer that won the race. Taking either side whole loses a
# country's record, which is exactly the thing the log exists to prove.
# So union the lines — remote first, ours after, exact duplicates
# collapsed, order preserved. Same spirit as the xlsx row-union.
union_jsonl_lines() {
  local remote="$1" ours="$2" out="$3"
  awk '!seen[$0]++' "$remote" "$ours" > "$out"
}

restore_our_non_xlsx_work() {
  [ -n "$OURS_DIR" ] || return 0
  local path
  while IFS= read -r -d '' path; do
    mkdir -p "$(dirname "$path")"
    if [ "$(basename "$path")" = "run_log.jsonl" ] && [ -f "$path" ]; then
      union_jsonl_lines "$path" "$OURS_DIR/tree/$path" "$path.union"
      mv "$path.union" "$path"
      echo "safe_push:   run_log union: $path ($(wc -l < "$path") records)"
    else
      cp -p "$OURS_DIR/tree/$path" "$path"
    fi
  done < "$OURS_DIR/keep.z"
  while IFS= read -r -d '' path; do
    rm -f "$path"
  done < "$OURS_DIR/gone.z"
  rm -rf "$OURS_DIR"
  OURS_DIR=""
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

  # Preserve our work, drop our commit, fast-forward to origin.
  # BOTH calls matter: the xlsx is row-unioned below, and everything else
  # the run produced is carried over by restore_our_non_xlsx_work. Dropping
  # either one silently discards a run's output — see the header.
  cp "$XLSX" /tmp/ours_recalls.xlsx
  preserve_our_non_xlsx_work
  git reset --hard HEAD~1
  git pull --rebase --autostash origin main \
    || git pull --no-rebase origin main
  restore_our_non_xlsx_work

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
