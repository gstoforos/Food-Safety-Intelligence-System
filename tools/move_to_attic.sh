#!/bin/sh
# Move the never-executed scraper files out of the live tree.
#
# WHY
# ---
# pipeline/run_all.discover_scrapers() walks exactly nine packages:
#
#     north_america  europe_eu  europe_non_eu  eu_wide  asia
#     oceania  africa  latam  middle_east
#
# A file sitting directly under scrapers/ is never imported and has never
# run. 71 of them had accumulated by the 2026-09-14 audit, and 54 were
# MISNAMED on top of that — scrapers/nebih.py defined GIS (PL),
# scrapers/fsai.py defined BVL (DE), scrapers/gis.py defined EFET (GR).
# That is the trap: a fix aimed at "the GIS scraper" by filename lands in
# scrapers/gis.py, changes EFET, and the Polish scraper stays broken. It
# is how GIS (PL) returned zero rows for eight months.
#
# Four more are live but misfiled into the wrong region — ten-line stubs
# shadowing a maintained scraper of the same agency, where which one wins
# depends on import order. Those are moved as DUP-<package>-<name>.py so
# the collision stays readable in the attic.
#
# VERIFIED by running it (2026-09-16, in a throwaway worktree of main):
#
#   * No module under pipeline/, tools/, tests/, docs/ or any of the nine
#     live packages imports any file listed here.
#   * discover_scrapers() reports 73 before and 69 after.
#
# READ THAT SECOND LINE. The move is NOT a no-op, and the four it removes
# are the point of it. Each was a 10-line stub that shadowed a maintained
# scraper for the same agency, and both copies were being discovered:
#
#   europe_eu/mast.py            10 lines  <- shadowed europe_non_eu/mast.py         44
#   europe_eu/mattilsynet.py     10        <- shadowed europe_non_eu/mattilsynet.py  37
#   north_america/anmat_ar.py    10        <- shadowed latam/anmat_ar.py             38
#   north_america/digesa_pe.py   10        <- shadowed latam/digesa_pe.py            38
#
# Which of the pair produced the day's rows depended on import order. After
# the move each agency has exactly one scraper, it is the maintained one,
# and it is filed in the right region — Iceland and Norway are not EU, and
# Argentina and Peru are not North America. The only duplicate left is
# FDA (4), which is four deliberately separate FDA surfaces.
#
# The other 71 files have never run at all, so for those the move really is
# a no-op.
#
# NOT MOVED, deliberately:
#   scrapers/news.py, scrapers/food_safety_news.py — no AGENCY class; the
#     live news path is the scrapers/news_feeds/ package, and these two
#     want reading before anyone assumes they are dead.
#   scrapers/test_usda_fsis_scraper.py — a test file living under
#     scrapers/. It belongs in tests/, which is a separate decision.
#
# HOW
# ---
#     sh tools/move_to_attic.sh          # from the repo root
#     python3 -m pytest tests/test_scraper_registry.py -q
#
# The two xfail-marked tests in tests/test_scraper_registry.py turn XPASS
# once this has run. That is the signal to delete their @_ATTIC_PENDING
# markers.
#
# Reversible: every move is a git mv, so `git checkout -- .` before you
# commit, or `git revert` after.

set -e

if [ ! -d scrapers ] || [ ! -d pipeline ]; then
    echo "run this from the repo root" >&2
    exit 1
fi

mkdir -p scrapers/_attic
cat > scrapers/_attic/__init__.py <<'EOF'
"""Scrapers that never ran.

Not a package the discoverer walks — the leading underscore keeps it out
of pipeline/run_all.discover_scrapers(), which is also why it must never
be renamed to look like a region. Kept rather than deleted because some
of these files contain parsing work worth salvaging when the agency they
were MEANT to cover is next touched. Check the AGENCY constant inside,
never the filename: 54 of these are misnamed.
"""
EOF

git mv "scrapers/europe_eu/mast.py" "scrapers/_attic/DUP-europe_eu-mast.py"
git mv "scrapers/europe_eu/mattilsynet.py" "scrapers/_attic/DUP-europe_eu-mattilsynet.py"
git mv "scrapers/north_america/anmat_ar.py" "scrapers/_attic/DUP-north_america-anmat_ar.py"
git mv "scrapers/north_america/digesa_pe.py" "scrapers/_attic/DUP-north_america-digesa_pe.py"
git mv "scrapers/aesan.py" "scrapers/_attic/aesan.py"
git mv "scrapers/ages.py" "scrapers/_attic/ages.py"
git mv "scrapers/ansvsa.py" "scrapers/_attic/ansvsa.py"
git mv "scrapers/anvisa_br.py" "scrapers/_attic/anvisa_br.py"
git mv "scrapers/arcsa_ec.py" "scrapers/_attic/arcsa_ec.py"
git mv "scrapers/asae.py" "scrapers/_attic/asae.py"
git mv "scrapers/audit_scraper_urls.py" "scrapers/_attic/audit_scraper_urls.py"
git mv "scrapers/bfsa.py" "scrapers/_attic/bfsa.py"
git mv "scrapers/blv_ch.py" "scrapers/_attic/blv_ch.py"
git mv "scrapers/bpom_id.py" "scrapers/_attic/bpom_id.py"
git mv "scrapers/bvl.py" "scrapers/_attic/bvl.py"
git mv "scrapers/bvl_rss.py" "scrapers/_attic/bvl_rss.py"
git mv "scrapers/cfia.py" "scrapers/_attic/cfia.py"
git mv "scrapers/cfs_hk.py" "scrapers/_attic/cfs_hk.py"
git mv "scrapers/comesa.py" "scrapers/_attic/comesa.py"
git mv "scrapers/digesa_pe.py" "scrapers/_attic/digesa_pe.py"
git mv "scrapers/efet.py" "scrapers/_attic/efet.py"
git mv "scrapers/efsa_rss.py" "scrapers/_attic/efsa_rss.py"
git mv "scrapers/fda.py" "scrapers/_attic/fda.py"
git mv "scrapers/fda_gh.py" "scrapers/_attic/fda_gh.py"
git mv "scrapers/fodevarestyrelsen.py" "scrapers/_attic/fodevarestyrelsen.py"
git mv "scrapers/fodevarestyrelsen_rss.py" "scrapers/_attic/fodevarestyrelsen_rss.py"
git mv "scrapers/food_poison_journal.py" "scrapers/_attic/food_poison_journal.py"
git mv "scrapers/fsa_uk.py" "scrapers/_attic/fsa_uk.py"
git mv "scrapers/fsai.py" "scrapers/_attic/fsai.py"
git mv "scrapers/fsai_rss.py" "scrapers/_attic/fsai_rss.py"
git mv "scrapers/fsanz.py" "scrapers/_attic/fsanz.py"
git mv "scrapers/fsanz_rss.py" "scrapers/_attic/fsanz_rss.py"
git mv "scrapers/fss_scotland_rss.py" "scrapers/_attic/fss_scotland_rss.py"
git mv "scrapers/fssai_in.py" "scrapers/_attic/fssai_in.py"
git mv "scrapers/gis.py" "scrapers/_attic/gis.py"
git mv "scrapers/hah.py" "scrapers/_attic/hah.py"
git mv "scrapers/invima_co.py" "scrapers/_attic/invima_co.py"
git mv "scrapers/isp_cl.py" "scrapers/_attic/isp_cl.py"
git mv "scrapers/kebs_ke.py" "scrapers/_attic/kebs_ke.py"
git mv "scrapers/kkm_my.py" "scrapers/_attic/kkm_my.py"
git mv "scrapers/livsmedelsverket.py" "scrapers/_attic/livsmedelsverket.py"
git mv "scrapers/livsmedelsverket_rss.py" "scrapers/_attic/livsmedelsverket_rss.py"
git mv "scrapers/mapaq.py" "scrapers/_attic/mapaq.py"
git mv "scrapers/mattilsynet.py" "scrapers/_attic/mattilsynet.py"
git mv "scrapers/mfds_kr.py" "scrapers/_attic/mfds_kr.py"
git mv "scrapers/mhlw_jp.py" "scrapers/_attic/mhlw_jp.py"
git mv "scrapers/moccae_ae.py" "scrapers/_attic/moccae_ae.py"
git mv "scrapers/moh_il.py" "scrapers/_attic/moh_il.py"
git mv "scrapers/moph_qa.py" "scrapers/_attic/moph_qa.py"
git mv "scrapers/mpi_nz.py" "scrapers/_attic/mpi_nz.py"
git mv "scrapers/msp_uy.py" "scrapers/_attic/msp_uy.py"
git mv "scrapers/nafdac_ng.py" "scrapers/_attic/nafdac_ng.py"
git mv "scrapers/nebih.py" "scrapers/_attic/nebih.py"
git mv "scrapers/nfsa_eg.py" "scrapers/_attic/nfsa_eg.py"
git mv "scrapers/nvwa.py" "scrapers/_attic/nvwa.py"
git mv "scrapers/onssa_ma.py" "scrapers/_attic/onssa_ma.py"
git mv "scrapers/outbreak_news_today.py" "scrapers/_attic/outbreak_news_today.py"
git mv "scrapers/pvd.py" "scrapers/_attic/pvd.py"
git mv "scrapers/rappelconso.py" "scrapers/_attic/rappelconso.py"
git mv "scrapers/rasff.py" "scrapers/_attic/rasff.py"
git mv "scrapers/ruokavirasto.py" "scrapers/_attic/ruokavirasto.py"
git mv "scrapers/salute_it.py" "scrapers/_attic/salute_it.py"
git mv "scrapers/samr_cn.py" "scrapers/_attic/samr_cn.py"
git mv "scrapers/sfa_sg.py" "scrapers/_attic/sfa_sg.py"
git mv "scrapers/sfda_sa.py" "scrapers/_attic/sfda_sa.py"
git mv "scrapers/svps.py" "scrapers/_attic/svps.py"
git mv "scrapers/tfda_tw.py" "scrapers/_attic/tfda_tw.py"
git mv "scrapers/tgthb_tr.py" "scrapers/_attic/tgthb_tr.py"
git mv "scrapers/thaifda.py" "scrapers/_attic/thaifda.py"
git mv "scrapers/usda_fsis.py" "scrapers/_attic/usda_fsis.py"
git mv "scrapers/usda_fsis_rss.py" "scrapers/_attic/usda_fsis_rss.py"
git mv "scrapers/uvhvvr.py" "scrapers/_attic/uvhvvr.py"
git mv "scrapers/vfa_vn.py" "scrapers/_attic/vfa_vn.py"
git mv "scrapers/vmvt.py" "scrapers/_attic/vmvt.py"
git mv "scrapers/vta.py" "scrapers/_attic/vta.py"

echo "moved 75 files into scrapers/_attic/"
echo "now run: python3 -m pytest tests/test_scraper_registry.py -q"
