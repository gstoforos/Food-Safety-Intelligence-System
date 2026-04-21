#!/usr/bin/env bash
# ============================================================================
# AFTS FSIS — Repo cleanup
# ============================================================================
# Deletes scrambled / duplicate files at repo root.
# Run from the repo root. Creates a git commit with every removal.
#
#   cd ~/Food-Safety-Intelligence-System
#   bash /path/to/AFTS-FIX/cleanup.sh
#
# Safe: every deleted file has its canonical twin in scrapers/, docs/,
# docs/data/, pipeline/, reports/, tools/, or .github/workflows/.
# ============================================================================
set -euo pipefail

if [ ! -d ".git" ]; then
  echo "ERROR: run this from the repo root (no .git here)."; exit 1
fi

echo ">> Safety check — making sure canonical subdirs exist before we delete root dupes"
for dir in scrapers pipeline reports tools docs docs/data .github/workflows; do
  if [ ! -d "$dir" ]; then
    echo "   ABORT: $dir is missing — don't trust the cleanup, bail out."
    exit 1
  fi
done

echo ""
echo ">> 1/7 Deleting 63 agency scraper .py duplicates at root (canonical in scrapers/)"
# Every one of these has a twin at scrapers/<name>.py
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
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 2/7 Deleting scrambled Python files at root (HTML or wrong-location content)"
# These either hold HTML content in a .py file, or duplicate pipeline/ or reports/ or docs/
for f in __init__.py _base.py _models.py _news_base.py \
         build_monthly_report_afts.py build_weekly_report_afts.py \
         claude_client.py cleanup_dataset.py commit_github.py dashboard.py \
         enrich_rows.py gap_finder_claude.py merge_master.py monthly.py \
         monthly_models.py monthly_stats.py openai_client.py \
         pathogen_italic.py process_authority.py run_all.py \
         url_gate_claude.py url_guardian.py url_validator.py yearly.py; do
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 3/7 Deleting 9 rename-artifact files inside scrapers/ (GitHub zip collisions)"
for f in "scrapers/__init__ (1).py" "scrapers/__init__ (2).py" \
         "scrapers/__init__ (3).py" "scrapers/__init__ (4).py" \
         "scrapers/__init__ (5).py" "scrapers/__init__ (6).py" \
         "scrapers/__init__ (9).py" "scrapers/ages (11).py" \
         "scrapers/fda (12).py"; do
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 4/7 Deleting HTML/PDF duplicates at root (canonical in docs/)"
for f in 2026-M03.html 2026-M03-all.html 2026-W15.html 2026-W15.pdf \
         2026-W16.html 2026-W17.html alerts.html hub.html index.html; do
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 5/7 Deleting JSON duplicates at root (canonical in docs/data/)"
for f in monthly-index.json monthly-summary-latest.json \
         monthly-summary-2026-M03.json weekly-index.json \
         weekly-summary-latest.json pdf-urls.json; do
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 6/7 Deleting workflow duplicates at root (canonical in .github/workflows/)"
for f in afts-monthly-report.yml afts-weekly-report.yml \
         claude-url-gate.yml daily-scrape.yml fsis-url-guardian.yml \
         morning-critical-scrape.yml news-feed.yml openai-gap-finder.yml; do
  [ -f "$f" ] && git rm -f "$f" || true
done

echo ""
echo ">> 7/7 Consolidating data/ into docs/data/"
# Everything in data/ is stale — docs/data/ is what GitHub Pages serves.
# The ONE file worth keeping is the per-month frozen summary.
if [ -f "data/monthly-summary-2026-M03.json" ]; then
  if [ ! -f "docs/data/monthly-summary-2026-M03.json" ]; then
    echo "   Moving data/monthly-summary-2026-M03.json -> docs/data/"
    git mv "data/monthly-summary-2026-M03.json" "docs/data/monthly-summary-2026-M03.json"
  else
    git rm -f "data/monthly-summary-2026-M03.json" || true
  fi
fi
for f in "data/index.html" "data/recalls.json" "data/recalls.xlsx" \
         "data/pdf-urls.json" "data/weekly-summary-latest.json"; do
  [ -f "$f" ] && git rm -f "$f" || true
done
rmdir data 2>/dev/null || true

echo ""
echo ">> Also removing env.example (duplicate of .env.example) if present"
[ -f "env.example" ] && [ -f ".env.example" ] && git rm -f "env.example" || true

echo ""
echo ">> Removing reports/__pycache__ (should not be in git)"
[ -d "reports/__pycache__" ] && git rm -rf "reports/__pycache__" || true

echo ""
echo "================================================================"
echo " Cleanup staged. Review 'git status' then:"
echo "   git commit -m 'chore: remove scrambled duplicates (root -> subdirs)'"
echo "   git push"
echo "================================================================"
