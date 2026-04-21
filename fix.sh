#!/usr/bin/env bash
# ============================================================================
# AFTS FSIS — Full repo repair, v2
# ============================================================================
# Runs from the repo root. Makes every fix in the correct order so nothing
# gets destroyed before it's been copied to its canonical home.
#
# Order matters:
#   1. Repair docs/ builder paths FIRST (before deleting root originals)
#   2. Restore requirements.txt
#   3. Drop gap_finder_claude.py into pipeline/
#   4. Delete all scrambled/duplicate junk
#
# Assumes both of these files are present in the same directory as this
# script (or paths adjusted):
#   ./gap_finder_claude.py          (the Python module I gave you)
# ============================================================================
set -euo pipefail

if [ ! -d ".git" ]; then
  echo "ERROR: run this from the repo root (no .git here)."; exit 1
fi

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ============================================================================
# 1. SWAP: restore canonical build scripts at docs/
# ============================================================================
echo ">> 1/5 Restoring docs/ build scripts to their correct content"

# The real Python monthly builder (accepts --month-end, outputs M<MM>.html)
# is currently saved as docs/build_weekly_report_afts.py (wrong filename).
# The real weekly builder is at reports/build_weekly_report_afts.py.
# We copy both to their canonical docs/ paths in one atomic pass.

if head -3 docs/build_weekly_report_afts.py | grep -q "Monthly Report Generator"; then
  echo "   docs/build_weekly_report_afts.py is actually the MONTHLY builder."
  echo "   Moving it to docs/build_monthly_report_afts.py"
  cp docs/build_weekly_report_afts.py docs/build_monthly_report_afts.py
else
  echo "   SKIP: docs/build_weekly_report_afts.py is not the monthly builder — inspect manually."
fi

if [ -f "reports/build_weekly_report_afts.py" ] && \
   head -3 reports/build_weekly_report_afts.py | grep -q "Weekly Report Generator"; then
  echo "   Copying reports/build_weekly_report_afts.py → docs/build_weekly_report_afts.py (real weekly)"
  cp reports/build_weekly_report_afts.py docs/build_weekly_report_afts.py
else
  echo "   SKIP: reports/build_weekly_report_afts.py not found or not the real weekly — inspect manually."
fi

echo ""
echo "   Verification after swap:"
echo "     docs/build_monthly_report_afts.py  -> $(head -3 docs/build_monthly_report_afts.py | grep -oE 'Monthly Report Generator|Weekly Report Generator|<!DOCTYPE' | head -1)"
echo "     docs/build_weekly_report_afts.py   -> $(head -3 docs/build_weekly_report_afts.py | grep -oE 'Monthly Report Generator|Weekly Report Generator|<!DOCTYPE' | head -1)"

# ============================================================================
# 2. RESTORE requirements.txt
# ============================================================================
echo ""
echo ">> 2/5 Restoring requirements.txt (currently 0 bytes)"

cat > requirements.txt <<'EOF'
# AFTS FSIS — pinned versions for reproducibility.
# If you hit install friction on any of these, loosen the pin.
requests>=2.31,<3
openpyxl>=3.1,<4
feedparser>=6.0,<7
weasyprint==63.1
EOF
echo "   Wrote $(wc -c < requirements.txt) bytes to requirements.txt"

# ============================================================================
# 3. INSTALL gap_finder_claude.py
# ============================================================================
echo ""
echo ">> 3/5 Installing pipeline/gap_finder_claude.py"

if [ -f "$THIS_DIR/gap_finder_claude.py" ]; then
  cp "$THIS_DIR/gap_finder_claude.py" pipeline/gap_finder_claude.py
  echo "   Installed pipeline/gap_finder_claude.py ($(wc -c < pipeline/gap_finder_claude.py) bytes)"
else
  echo "   SKIP: gap_finder_claude.py not found alongside this script."
  echo "         Download from Claude and place next to fix.sh, then re-run."
fi

# ============================================================================
# 4. DELETE scrambled / duplicate junk
# ============================================================================
echo ""
echo ">> 4/5 Deleting scrambled / duplicate files"

# 4a. 63 misnamed agency scrapers at root (canonical in scrapers/*/)
for f in aesan.py afsca.py ages.py anmat_ar.py ansvsa.py anvisa_br.py \
         arcsa_ec.py asae.py audit_scraper_urls.py bfsa.py blv_ch.py bvl.py \
         cfia.py cfs_hk.py cofepris_mx.py digesa_pe.py efet.py \
         fda.py fda_gh.py fda_ph.py fodevarestyrelsen.py food_poison_journal.py \
         food_safety_news.py fsa_uk.py fsai.py fssai_in.py gis.py hah.py \
         invima_co.py isp_cl.py kebs_ke.py kkm_my.py livsmedelsverket.py \
         mapaq.py mast.py mattilsynet.py mfds_kr.py mhlw_jp.py moccae_ae.py \
         moh_il.py mpi_nz.py nafdac_ng.py ncc_za.py nebih.py nfsa_eg.py \
         nvwa.py onssa_ma.py pvd.py rappelconso.py rasff.py ruokavirasto.py \
         salute_it.py samr_cn.py sfda_sa.py svps.py szpi.py \
         test_usda_fsis_scraper.py tfda_tw.py tgthb_tr.py thaifda.py \
         uvhvvr.py vfa_vn.py vmvt.py news.py outbreak_news_today.py; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4a] Root scrapers deleted"

# 4b. Scrambled Python files at root (HTML or duplicate-of-subdir content)
# NOTE: we deliberately KEEP root build_monthly_report_afts.py and
# build_weekly_report_afts.py out of this list — they may be the fallback
# if docs/ swap failed. Delete them manually once you've confirmed docs/ works.
for f in __init__.py _base.py _models.py _news_base.py \
         claude_client.py cleanup_dataset.py commit_github.py dashboard.py \
         enrich_rows.py gap_finder_claude.py merge_master.py monthly.py \
         monthly_models.py monthly_stats.py openai_client.py \
         pathogen_italic.py process_authority.py run_all.py \
         url_gate_claude.py url_guardian.py url_validator.py yearly.py; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4b] Root scrambled pipeline-duplicates deleted"

# 4c. 9 rename-artifact files inside scrapers/
for f in "scrapers/__init__ (1).py" "scrapers/__init__ (2).py" \
         "scrapers/__init__ (3).py" "scrapers/__init__ (4).py" \
         "scrapers/__init__ (5).py" "scrapers/__init__ (6).py" \
         "scrapers/__init__ (9).py" "scrapers/ages (11).py" \
         "scrapers/fda (12).py"; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4c] scrapers/ numbered duplicates deleted"

# 4d. HTML/PDF duplicates at root (canonical in docs/)
for f in 2026-M03.html 2026-M03-all.html 2026-W15.html 2026-W15.pdf \
         2026-W16.html 2026-W17.html alerts.html hub.html index.html; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4d] Root HTML/PDF duplicates deleted"

# 4e. JSON duplicates at root (canonical in docs/data/)
for f in monthly-index.json monthly-summary-latest.json \
         monthly-summary-2026-M03.json weekly-index.json \
         weekly-summary-latest.json pdf-urls.json; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4e] Root JSON duplicates deleted"

# 4f. Workflow duplicates at root (canonical in .github/workflows/)
for f in afts-monthly-report.yml afts-weekly-report.yml \
         claude-url-gate.yml daily-scrape.yml fsis-url-guardian.yml \
         morning-critical-scrape.yml news-feed.yml openai-gap-finder.yml; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4f] Root workflow duplicates deleted"

# 4g. Stale data/ dir (canonical is docs/data/)
if [ -d "data" ]; then
  for f in data/index.html data/recalls.json data/recalls.xlsx \
           data/pdf-urls.json data/weekly-summary-latest.json \
           data/monthly-summary-2026-M03.json; do
    [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
  done
  rmdir data 2>/dev/null || true
fi
echo "   [4g] Stale data/ dir cleaned"

# 4h. Scrambled files inside docs/ that shouldn't be there
#     (these are Python code misfiled into the static-site directory,
#      OR HTML renamed to .py; every one has canonical twin elsewhere)
for f in docs/build_monthly_report_afts.py.bak \
         docs/monthly_models.py docs/monthly_stats.py \
         docs/pathogen_italic.py docs/process_authority.py; do
  [ -f "$f" ] && git rm -f "$f" >/dev/null 2>&1 || true
done
echo "   [4h] docs/ stray .py files cleaned"

# ============================================================================
# 5. STATUS REPORT
# ============================================================================
echo ""
echo ">> 5/5 Final status check"
echo ""

check() {
  local label="$1" path="$2" expect="$3"
  if [ ! -e "$path" ]; then
    echo "   ❌ $label: MISSING ($path)"
  elif [ ! -s "$path" ]; then
    echo "   ❌ $label: EMPTY ($path)"
  elif [ -n "$expect" ] && ! head -3 "$path" 2>/dev/null | grep -q "$expect"; then
    echo "   ⚠️  $label: exists but doesn't match '$expect' ($path)"
  else
    echo "   ✅ $label"
  fi
}

check "scrapers/news.py (fetch+purge)"             "scrapers/news.py"                            ""
check "requirements.txt"                           "requirements.txt"                            ""
check "pipeline/gap_finder_claude.py"              "pipeline/gap_finder_claude.py"               ""
check "pipeline/gap_finder_openai.py"              "pipeline/gap_finder_openai.py"               ""
check "pipeline/merge_master.py"                   "pipeline/merge_master.py"                    ""
check "pipeline/purge_old_news.py"                 "pipeline/purge_old_news.py"                  ""
check "pipeline/url_guardian.py"                   "pipeline/url_guardian.py"                    ""
check "pipeline/build_missing_weekly_reports.py"   "pipeline/build_missing_weekly_reports.py"    ""
check "pipeline/build_missing_monthly_reports.py"  "pipeline/build_missing_monthly_reports.py"   ""
check "docs/build_monthly_report_afts.py (Python)" "docs/build_monthly_report_afts.py"           "Monthly Report Generator"
check "docs/build_weekly_report_afts.py (Python)"  "docs/build_weekly_report_afts.py"            "Weekly Report Generator"
check "docs/data/recalls.xlsx"                     "docs/data/recalls.xlsx"                      ""
check "docs/data/monthly-index.json"               "docs/data/monthly-index.json"                ""
check "docs/data/weekly-summary-latest.json"       "docs/data/weekly-summary-latest.json"        ""

echo ""
echo "================================================================"
echo " Staged. Review 'git status', then:"
echo "   git add -A"
echo "   git commit -m 'chore: repair scrambled repo (v2 cleanup)'"
echo "   git push"
echo ""
echo " If verification above showed any ❌ or ⚠️, stop and investigate"
echo " BEFORE committing. Rollback with: git checkout HEAD ."
echo "================================================================"
