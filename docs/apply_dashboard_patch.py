"""
apply_dashboard_patch.py
========================
One-off patch script that injects a "Daily" tab into docs/index.html so the
dashboard shows yesterday's daily-recall-brief card between News and Weekly.

Usage (run locally after cloning the repo):
    python apply_dashboard_patch.py

Idempotent: detects whether the patch is already applied and exits cleanly.
Zero dependencies beyond Python stdlib.

What it does:
  1. Adds a Daily tab button (after News, before Weekly)
  2. Adds <div id="panel-daily"> block
  3. Extends switchTab(t) with a 'daily' branch + lazy loadDaily()
  4. Adds loadDaily() that fetches daily-index.json and renders cards
  5. CSS additions reuse existing .report-card classes so no style edits
     needed

Rollback:
  - Back up docs/index.html before running (script creates .bak automatically)
  - Delete the patched file, restore docs/index.html.bak
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INDEX = ROOT / "docs" / "index.html"
BACKUP = ROOT / "docs" / "index.html.bak"

PATCH_MARKER = "<!-- DAILY_PATCH_V1 -->"


# ---------------------------------------------------------------------------
# Patch fragments
# ---------------------------------------------------------------------------

TAB_BUTTON = (
    '<button class="tab" id="tab-daily" onclick="switchTab(\'daily\')">'
    '📅 Daily <span id="daily-count" style="opacity:.55;font-weight:400">'
    '</span></button>'
)

PANEL = f"""
{PATCH_MARKER}
<div id="panel-daily" style="display:none">
<div class="wrap">
<div class="sec">Daily Recall Briefs — OpenAI-verified yesterday sweep</div>
<div id="daily-list"><p class="loading">Loading daily-index.json…</p></div>
</div>
</div>
"""


SWITCHTAB_ADDITION = """  document.getElementById('panel-daily').style.display=(t==='daily'?'block':'none');"""

LOADDAILY_HOOK = (
    "  if(t==='daily'&&!window._daily_loaded){loadDaily();window._daily_loaded=true;}"
)


LOADDAILY_FN = """
// ===== DAILY — OpenAI-verified yesterday sweep =====
async function loadDaily(){
  const list=document.getElementById('daily-list');
  try{
    const r=await fetch('daily-index.json?_='+Date.now(),{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    const data=await r.json();
    const entries=(data.entries||[]).slice(0,60);
    document.getElementById('daily-count').textContent=' · '+entries.length;
    if(!entries.length){
      list.innerHTML='<p class="loading">No daily briefs yet. First brief '+
        'will appear tomorrow 10:00 Athens.</p>';
      return;
    }
    list.innerHTML=entries.map(e=>{
      const d=new Date(e.date+'T00:00:00');
      const nice=d.toLocaleDateString('en-GB',{weekday:'short',day:'numeric',
                                               month:'short',year:'numeric'});
      const regions=Object.keys(e.by_region||{}).sort();
      const regionLabel=regions.length?regions.join(' · '):'no recalls';
      return `
      <div class="report-card" onclick="window.open('${e.url}','_blank')">
        <div class="report-week">DAILY · ${e.date}</div>
        <div class="report-title">${nice}</div>
        <div class="report-stats">
          <span><strong>${e.total||0}</strong> recalls</span>
          <span class="stat-tier1"><strong>${e.tier1||0}</strong> Tier-1</span>
          <span class="stat-outbreak"><strong>${e.outbreak||0}</strong> outbreak</span>
          <span style="color:var(--dim);font-size:10px">${regionLabel}</span>
        </div>
        <div class="report-actions">
          <a class="report-btn primary" href="${e.url}" target="_blank"
             rel="noopener" onclick="event.stopPropagation()">Open brief →</a>
        </div>
      </div>`;
    }).join('');
  }catch(err){
    list.innerHTML=`<p class="loading">Failed to load daily-index.json: ${err.message}</p>`;
  }
}
"""


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def apply() -> int:
    if not INDEX.exists():
        print(f"ERROR: {INDEX} not found. Run this script from the repo root.")
        return 1

    html = INDEX.read_text(encoding="utf-8")

    if PATCH_MARKER in html:
        print("Patch already applied (found DAILY_PATCH_V1 marker). Exiting.")
        return 0

    # Backup
    BACKUP.write_text(html, encoding="utf-8")
    print(f"Backup saved: {BACKUP}")

    # --- 1. Insert tab button AFTER the News tab, BEFORE Weekly ---
    news_tab_re = re.compile(
        r'(<button class="tab" id="tab-news"[^>]*>[^<]*'
        r'<span id="news-count"[^>]*></span></button>)',
        re.S,
    )
    m = news_tab_re.search(html)
    if not m:
        print("ERROR: could not locate News tab button. Aborting.")
        BACKUP.unlink(missing_ok=True)
        return 2
    html = html[:m.end()] + "\n" + TAB_BUTTON + html[m.end():]
    print("✓ Added Daily tab button after News")

    # --- 2. Insert <div id="panel-daily"> AFTER panel-news, BEFORE panel-reports ---
    panel_news_end_re = re.compile(
        r'(<div id="panel-news" style="display:none">.*?)'
        r'(\n<div id="panel-reports")',
        re.S,
    )
    m = panel_news_end_re.search(html)
    if not m:
        print("ERROR: could not locate end of panel-news. Aborting.")
        INDEX.write_text(BACKUP.read_text(encoding="utf-8"), encoding="utf-8")
        return 3
    # Find the </div> that closes panel-news. Simpler: insert right before panel-reports line.
    panel_reports_line_re = re.compile(r'(\n<div id="panel-reports" style="display:none">)')
    html = panel_reports_line_re.sub(PANEL + r'\1', html, count=1)
    print("✓ Added Daily panel before Weekly panel")

    # --- 3. Extend switchTab(t) ---
    # Insert the new display toggle line right after the panel-monthly line
    switchtab_re = re.compile(
        r"(document\.getElementById\('panel-monthly'\)\.style\.display="
        r"\(t==='monthly'\?'block':'none'\);)"
    )
    html = switchtab_re.sub(r"\1\n" + SWITCHTAB_ADDITION, html, count=1)
    print("✓ Extended switchTab() with daily panel toggle")

    # Insert lazy-load hook — right after the monthly lazy-load hook
    monthly_hook_re = re.compile(
        r"(if\(t==='monthly'&&!window\._monthly_loaded\)"
        r"\{loadMonthlyReports\(\);window\._monthly_loaded=true;\})"
    )
    html = monthly_hook_re.sub(r"\1\n" + LOADDAILY_HOOK, html, count=1)
    print("✓ Added loadDaily() lazy-load trigger in switchTab()")

    # --- 4. Add loadDaily() function near the other load functions ---
    # Inject before the "===== DOWNLOADS" comment which is the next major section
    downloads_marker_re = re.compile(r"// ===== DOWNLOADS")
    html = downloads_marker_re.sub(LOADDAILY_FN + "\n// ===== DOWNLOADS",
                                   html, count=1)
    print("✓ Added loadDaily() function")

    INDEX.write_text(html, encoding="utf-8")
    print(f"\nPatch applied to {INDEX}")
    print(f"Backup at {BACKUP} — delete once verified.")
    print("\nTo test locally:")
    print("  python -m http.server --directory docs 8000")
    print("  open http://localhost:8000")
    return 0


if __name__ == "__main__":
    sys.exit(apply())
