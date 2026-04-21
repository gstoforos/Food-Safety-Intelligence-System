#!/usr/bin/env bash
# ============================================================================
# AFTS FSIS — Scraper reconstruction
# ============================================================================
# Run AFTER fix.sh. Does two things:
#
#  1. Fills the 7 empty scrapers at scrapers/*.py by copying from their
#     real regional equivalents. Keeps them only as compatibility stubs —
#     they're not imported by run_all.py (it only scans regional subdirs)
#     but anything still importing the flat path won't break.
#
#  2. Builds scrapers/news_feeds/ — the directory pipeline/run_all.py
#     tries to import but which doesn't exist. Its contents are
#     currently misfiled inside scrapers/ under the wrong filenames:
#
#       scrapers/usda_fsis.py  (234 lines)  → actually BaseNewsScraper
#       scrapers/fsanz.py      (22 lines)   → actually FoodPoisonJournal
#       scrapers/mpi_nz.py     (24 lines)   → actually OutbreakNewsToday
#
#     We move them into scrapers/news_feeds/ under their real names,
#     then restore the correct usda_fsis/fsanz/mpi_nz scrapers from the
#     regional subdirs.
#
#  3. Writes a new scrapers/news_feeds/food_safety_news.py from scratch
#     (nothing in the zip had this scraper's content).
# ============================================================================
set -euo pipefail

if [ ! -d ".git" ]; then
  echo "ERROR: run this from the repo root (no .git here)."; exit 1
fi

# ---------------------------------------------------------------------------
# 1. FILL EMPTY SCRAPERS — copy from regional equivalents
# ---------------------------------------------------------------------------
echo ">> 1/3 Filling empty flat scrapers/*.py from their regional equivalents"

declare -A FILLS=(
  ["scrapers/arcsa_ec.py"]="scrapers/latam/arcsa_ec.py"
  ["scrapers/cfia.py"]="scrapers/north_america/cfia.py"
  ["scrapers/comesa.py"]="scrapers/africa/comesa.py"
  ["scrapers/mhlw_jp.py"]="scrapers/asia/mhlw_jp.py"
  ["scrapers/rappelconso.py"]="scrapers/europe_eu/rappelconso.py"
  ["scrapers/vta.py"]="scrapers/europe_eu/vta.py"
)

for dest in "${!FILLS[@]}"; do
  src="${FILLS[$dest]}"
  if [ ! -f "$src" ]; then
    echo "   ⚠️  Source missing: $src — skipping $dest"
    continue
  fi
  if [ -s "$dest" ]; then
    echo "   (already filled) $dest — skipping"
    continue
  fi
  cp "$src" "$dest"
  echo "   ✅ $dest  ←  $src  ($(wc -c < $dest)B)"
done

# ---------------------------------------------------------------------------
# 2. BUILD scrapers/news_feeds/ — pipeline/run_all.py imports this dir
# ---------------------------------------------------------------------------
echo ""
echo ">> 2/3 Building scrapers/news_feeds/ from misfiled content"

mkdir -p scrapers/news_feeds

# Package marker
touch scrapers/news_feeds/__init__.py

# The BaseNewsScraper class is currently at scrapers/usda_fsis.py (234 lines).
# Move it to scrapers/news_feeds/_news_base.py.
if [ -s "scrapers/usda_fsis.py" ] && \
   head -10 scrapers/usda_fsis.py | grep -q "Base class for RSS/Atom"; then
  cp scrapers/usda_fsis.py scrapers/news_feeds/_news_base.py
  echo "   ✅ scrapers/news_feeds/_news_base.py  ←  scrapers/usda_fsis.py  (BaseNewsScraper)"
fi

# FoodPoisonJournal is at scrapers/fsanz.py (22 lines)
if [ -s "scrapers/fsanz.py" ] && \
   head -5 scrapers/fsanz.py | grep -q "Food Poison Journal"; then
  cp scrapers/fsanz.py scrapers/news_feeds/food_poison_journal.py
  echo "   ✅ scrapers/news_feeds/food_poison_journal.py  ←  scrapers/fsanz.py"
fi

# OutbreakNewsToday is at scrapers/mpi_nz.py (24 lines)
if [ -s "scrapers/mpi_nz.py" ] && \
   head -5 scrapers/mpi_nz.py | grep -q "Outbreak News Today"; then
  cp scrapers/mpi_nz.py scrapers/news_feeds/outbreak_news_today.py
  echo "   ✅ scrapers/news_feeds/outbreak_news_today.py  ←  scrapers/mpi_nz.py"
fi

# Restore the REAL usda_fsis / fsanz / mpi_nz scrapers from regional subdirs
# (Now that we've moved their hijacked content into news_feeds/.)
if [ -f "scrapers/north_america/usda_fsis.py" ]; then
  cp scrapers/north_america/usda_fsis.py scrapers/usda_fsis.py
  echo "   ✅ scrapers/usda_fsis.py restored from scrapers/north_america/"
fi

if [ -f "scrapers/oceania/fsanz.py" ]; then
  cp scrapers/oceania/fsanz.py scrapers/fsanz.py
  echo "   ✅ scrapers/fsanz.py restored from scrapers/oceania/"
fi

if [ -f "scrapers/oceania/mpi_nz.py" ]; then
  cp scrapers/oceania/mpi_nz.py scrapers/mpi_nz.py
  echo "   ✅ scrapers/mpi_nz.py restored from scrapers/oceania/"
fi

# ---------------------------------------------------------------------------
# 3. WRITE food_safety_news.py — nothing in the zip had its content
# ---------------------------------------------------------------------------
echo ""
echo ">> 3/3 Writing scrapers/news_feeds/food_safety_news.py (new — no source to copy from)"

cat > scrapers/news_feeds/food_safety_news.py <<'EOF'
"""
Food Safety News — RSS feed scraper.

https://www.foodsafetynews.com covers foodborne-illness outbreaks, recalls,
regulatory enforcement, and litigation in the food industry. Published by
Marler Clark, it is one of the most comprehensive US-focused food-safety
news outlets.

Feed: https://www.foodsafetynews.com/feed/
Update frequency: daily
Coverage: Primarily USA with regular international coverage
"""
from __future__ import annotations
from scrapers.news_feeds._news_base import BaseNewsScraper


class FoodSafetyNewsScraper(BaseNewsScraper):
    SOURCE_NAME = "Food Safety News"
    FEED_URLS = [
        "https://www.foodsafetynews.com/feed/",
    ]
    # Food Safety News is food-specific by editorial mission, so we don't
    # need the strict pathogen filter — any item that mentions a pathogen
    # OR matches the broad food-safety context keywords is kept.
    PATHOGEN_STRICT = False
EOF
echo "   ✅ scrapers/news_feeds/food_safety_news.py  ($(wc -c < scrapers/news_feeds/food_safety_news.py)B)"

# ---------------------------------------------------------------------------
# Also: scrapers/food_safety_news.py at root is still 0-byte.
# Fill it with the same content as the new news_feeds one (for
# compatibility with anything still expecting the flat path).
# ---------------------------------------------------------------------------
if [ -f "scrapers/food_safety_news.py" ] && [ ! -s "scrapers/food_safety_news.py" ]; then
  cp scrapers/news_feeds/food_safety_news.py scrapers/food_safety_news.py
  echo "   ✅ scrapers/food_safety_news.py filled (compat stub)"
fi

# ---------------------------------------------------------------------------
# Final verification
# ---------------------------------------------------------------------------
echo ""
echo ">> Final verification: compile check on every touched file"
echo ""
fail=0
for f in scrapers/arcsa_ec.py scrapers/cfia.py scrapers/comesa.py \
         scrapers/mhlw_jp.py scrapers/rappelconso.py scrapers/vta.py \
         scrapers/usda_fsis.py scrapers/fsanz.py scrapers/mpi_nz.py \
         scrapers/food_safety_news.py \
         scrapers/news_feeds/__init__.py \
         scrapers/news_feeds/_news_base.py \
         scrapers/news_feeds/food_poison_journal.py \
         scrapers/news_feeds/outbreak_news_today.py \
         scrapers/news_feeds/food_safety_news.py; do
  if [ ! -e "$f" ]; then
    echo "   ❌ MISSING: $f"
    fail=1
  elif [[ "$f" == *"/__init__.py" ]] && [ ! -s "$f" ]; then
    # Empty __init__.py is a legal package marker.
    echo "   ✅ $f (empty package marker — legal)"
  elif [ ! -s "$f" ]; then
    echo "   ❌ EMPTY: $f"
    fail=1
  elif python3 -m py_compile "$f" 2>/dev/null; then
    echo "   ✅ $f"
  else
    echo "   ⚠️  $f compiles with issues"
    fail=1
  fi
done

echo ""
echo "================================================================"
if [ $fail -eq 0 ]; then
  echo " All reconstructions compile. Review and commit:"
  echo "   git add -A"
  echo "   git commit -m 'fix: reconstruct empty scrapers + build news_feeds/ dir'"
  echo "   git push"
else
  echo " Some files have issues — review output above before committing."
fi
echo "================================================================"
