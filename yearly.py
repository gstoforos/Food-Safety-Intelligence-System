<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Pathogen Intelligence Briefing &middot; Week 16, 2026</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root {
  --black:#0a0e1a; --orange:#E8601A;
  --ink:#111827; --body:#1f2937; --muted:#6b7280; --dim:#9ca3af;
  --bg:#ffffff; --s1:#f9fafb; --s2:#f3f4f6; --brd:#e5e7eb;
  --red:#dc2626; --amber:#f59e0b; --violet:#9333ea; --green:#059669;
}
* { box-sizing:border-box; }
html, body { margin:0; padding:0; background:var(--bg); }
body {
  font-family:'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color:var(--body); font-size:14px; line-height:1.65;
  max-width:1180px; margin:0 auto; padding:0 40px 60px;
}
a { color:var(--orange); text-decoration:none; }
a:hover { text-decoration:underline; }

.masthead {
  border-top:6px solid var(--black);
  padding:28px 0 22px;
  display:flex; justify-content:space-between; align-items:flex-start;
  border-bottom:1px solid var(--brd);
  margin-bottom:32px;
}
.brand-block .brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:24px;
  color:var(--black); letter-spacing:-0.01em; text-transform:uppercase;
  line-height:1.1;
}
.brand-block .brand em { color:var(--orange); font-style:normal; font-weight:800; }
.brand-block .tagline {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:600;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.14em;
  margin-top:8px;
}
.mast-right { text-align:right; }
.report-label {
  display:inline-block; background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  padding:5px 11px; letter-spacing:0.12em; text-transform:uppercase;
  margin-bottom:10px;
}
.report-meta {
  font-family:'DM Mono', monospace; font-size:11px;
  color:var(--muted); line-height:1.8;
}
.report-meta strong { color:var(--ink); font-weight:700; }

.r-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:38px;
  color:var(--black); letter-spacing:-0.02em; line-height:1.15;
  margin:2px 0 10px;
}
.r-title .accent { color:var(--orange); }
.r-kicker {
  font-family:'Syne', sans-serif; font-weight:800; font-size:13px;
  color:var(--black); letter-spacing:0.08em; text-transform:uppercase;
  margin:8px 0 6px;
}
.r-kicker-dot { color:var(--orange); font-style:normal; margin:0 2px; }
.r-sub {
  color:var(--muted); font-size:14px; margin-bottom:16px;
}
.r-sub strong { color:var(--ink); font-weight:600; }
.r-authority {
  display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  padding:10px 14px; background:var(--s1); border-left:3px solid var(--orange);
  font-family:'DM Mono', monospace; font-size:11px; color:var(--ink);
  margin-bottom:30px;
}
.auth-label {
  font-size:9px; font-weight:700; color:var(--orange);
  text-transform:uppercase; letter-spacing:0.14em;
  border-right:1px solid var(--brd); padding-right:10px;
}

.kpi-strip {
  display:grid; grid-template-columns:repeat(4, 1fr);
  gap:1px; background:var(--brd); border:1px solid var(--brd);
  margin-bottom:32px;
}
.kpi { background:#fff; padding:22px 20px; }
.kpi-label {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.1em;
  margin-bottom:8px;
}
.kpi-value {
  font-family:'Syne', sans-serif; font-weight:800; font-size:42px;
  color:var(--black); line-height:1; letter-spacing:-0.02em;
}
.kpi-value.red { color:var(--red); }
.kpi-value.violet { color:var(--violet); }
.kpi-value.orange { color:var(--orange); font-size:20px; line-height:1.2; }
.kpi-delta {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  margin-top:10px; letter-spacing:0.04em;
}
.kpi-top { font-size:11px; color:var(--muted); margin-top:10px; font-style:italic; }

.sec-head {
  display:flex; align-items:baseline; gap:14px;
  margin:40px 0 16px;
}
.sec-num {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.12em;
}
.sec-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--black); letter-spacing:-0.01em;
}
.sec-rule { flex:1; height:1px; background:var(--brd); }
.sec-caption { color:var(--muted); font-size:13px; margin:-4px 0 14px; }
.sec-caption em { color:var(--ink); font-style:italic; }

.analysis {
  background:var(--s1);
  padding:26px 30px; margin-bottom:10px;
}
.analysis p { margin:0 0 14px; font-size:14.5px; line-height:1.75; }
.analysis p:last-child { margin-bottom:0; }
/* Process Authority Note — fires when Clostridium / low-acid / aseptic /
   ROP-MAP / UHT-HTST / hot-fill triggers match in the window. Rendered
   as a white card with a top rule so it stands apart from the grey
   analysis panel without introducing any coloured vertical bars. */
.analysis p.pa-note {
  margin:18px -30px 0 -30px; padding:16px 30px;
  background:#fff; border-top:1px solid var(--brd);
  font-size:14px; line-height:1.7;
}
.analysis p.pa-note .pa-label {
  display:inline; font-family:'DM Mono',ui-monospace,monospace;
  font-weight:700; letter-spacing:0.06em; text-transform:uppercase;
  color:var(--red); font-size:11px; margin-right:6px;
}

table.data {
  width:100%; border-collapse:collapse; margin:0 0 10px;
  background:#fff; border:1px solid var(--brd);
  font-size:13px;
}
table.data th {
  background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  text-transform:uppercase; letter-spacing:0.1em;
  padding:12px 12px; text-align:left; border-bottom:2px solid var(--orange);
}
table.data td {
  padding:14px 12px; border-bottom:1px solid var(--brd);
  vertical-align:top;
}
table.data tr:last-child td { border-bottom:none; }
table.data tr:nth-child(even) td { background:#fafbfc; }
table.data td.num {
  font-family:'DM Mono', monospace; font-weight:600; text-align:right;
  white-space:nowrap;
}
table.data td.empty {
  text-align:center; color:var(--muted); padding:28px; font-style:italic;
}

/* Top 5 column sizing - keeps table within A4 and desktop viewport */
table.top5 { table-layout:fixed; width:100%; }
table.top5 th:nth-child(1), table.top5 td:nth-child(1) { width:5%;  }  /* # */
table.top5 th:nth-child(2), table.top5 td:nth-child(2) { width:9%;  }  /* Date */
table.top5 th:nth-child(3), table.top5 td:nth-child(3) { width:19%; }  /* Pathogen */
table.top5 th:nth-child(4), table.top5 td:nth-child(4) { width:18%; }  /* Company */
table.top5 th:nth-child(5), table.top5 td:nth-child(5) { width:30%; }  /* Product */
table.top5 th:nth-child(6), table.top5 td:nth-child(6) { width:19%; }  /* Jurisdiction+Source */
table.top5 td { word-wrap:break-word; overflow-wrap:break-word; }

.rank-num {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--orange); text-align:center;
}
.date-cell {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
}
.path-dot {
  display:inline-block; width:9px; height:9px; border-radius:50%;
  margin-right:7px; vertical-align:middle;
}
.path-name { font-weight:600; color:var(--ink); }
.co-cell strong { color:var(--black); font-weight:700; display:block; }
.brand-sub { font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }
.prod-cell { color:var(--body); }
.juris-country { font-weight:600; color:var(--ink); }
.src-sub {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); margin-top:3px;
}
.juris-link { margin-top:6px; }
.chip-tier1 {
  display:inline-block; background:var(--red); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-tier2 {
  display:inline-block; background:var(--amber); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-outbreak {
  display:inline-block; background:var(--violet); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:4px; letter-spacing:0.06em;
}
.src-link {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.02em;
}
.src-na { color:var(--dim); font-family:'DM Mono', monospace; font-size:10px; font-style:italic; }

.dist-grid {
  display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:10px;
}
.dist-grid h3 {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  text-transform:uppercase; letter-spacing:0.1em; margin:0 0 10px;
}
.bar-track {
  width:100%; height:8px; background:var(--s2);
  border-radius:1px; overflow:hidden;
}
.bar-fill { height:100%; }

.cta-box {
  margin:40px 0 30px;
  padding:26px 30px;
  background:var(--black); color:#fff;
  display:flex; justify-content:space-between; align-items:center;
  flex-wrap:wrap; gap:18px;
}
.cta-text { flex:1; min-width:280px; }
.cta-text h3 {
  font-family:'Syne', sans-serif; font-weight:800; font-size:20px;
  margin:0 0 6px; color:#fff; letter-spacing:-0.01em;
}
.cta-text p { margin:0; color:#d1d5db; font-size:13px; }
.cta-btn {
  background:var(--orange); color:#fff; font-family:'DM Mono', monospace;
  font-size:11px; font-weight:700; padding:14px 22px;
  text-transform:uppercase; letter-spacing:0.1em;
  border:none; cursor:pointer; white-space:nowrap;
}
.cta-btn:hover { background:#d35416; text-decoration:none; color:#fff; }

.meth {
  background:var(--s1); border:1px solid var(--brd);
  padding:22px 26px; margin-bottom:24px; font-size:13px;
  color:var(--body);
}
.meth strong { color:var(--black); }
.meth p { margin:0 0 10px; }
.meth p:last-child { margin-bottom:0; }

.footer {
  margin-top:50px; padding-top:26px; border-top:2px solid var(--black);
  display:flex; justify-content:space-between; align-items:flex-start;
  flex-wrap:wrap; gap:20px; font-size:12px;
}
.foot-brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:15px;
  color:var(--black); text-transform:uppercase; letter-spacing:0.02em;
}
.foot-brand em { color:var(--orange); font-style:normal; }
.foot-meta {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); line-height:1.8; margin-top:6px;
}
.foot-legal {
  font-size:11px; color:var(--muted); max-width:440px;
  text-align:right; line-height:1.6;
}

@media print {
  /* Running footer on every printed page: process-authority attribution
     anchors the AFTS differentiator visually throughout the document. */
  @page {
    size: A4;
    margin: 14mm 14mm 18mm 14mm;
    @bottom-left {
      content: "AFTS · Process Validation Intelligence · under AFTS process authority";
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
    @bottom-right {
      content: "Page " counter(page) " / " counter(pages);
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
  }
  body { max-width:none; padding:0; margin:0; font-size:11px; }
  .cta-box { display:none; }

  /* Lock print-mode layout: even if the browser's print page is narrow,
     these must not collapse into mobile responsive layouts. */
  .masthead { flex-direction:row !important; }
  .mast-right { text-align:right !important; }
  .kpi-strip { grid-template-columns:repeat(4, 1fr) !important; }
  .dist-grid { display:block !important; grid-template-columns:1fr !important; gap:0 !important; }
  .dist-grid > div { width:100% !important; display:block !important; }
  .dist-grid > div:nth-child(2) { page-break-before:always !important; break-before:page !important; margin-top:0 !important; }

  /* Page 1 compression: tighten the above-the-fold so the first Intelligence
     Analysis paragraph opens on page 1 rather than orphaning the heading. */
  .masthead { border-top-width:4px; padding:18px 0 12px; margin-bottom:22px; }
  .brand-block .brand { font-size:18px; }
  .brand-block .tagline { font-size:10px; margin-top:5px; letter-spacing:0.12em; }
  .report-label { font-size:9px; padding:4px 10px; margin-bottom:8px; }
  .report-meta { font-size:10px; line-height:1.7; }
  .r-kicker { font-size:12px; margin:6px 0 5px; letter-spacing:0.07em; }
  .r-title { font-size:26px; margin:2px 0 8px; }
  .r-sub { font-size:13px; margin-bottom:12px; line-height:1.55; }
  .r-authority { padding:9px 12px; font-size:11px; margin-bottom:22px; }
  .auth-label { font-size:8px; padding-right:9px; }
  .kpi-strip { margin-bottom:24px; }
  .kpi { padding:16px 14px; }
  .kpi-label { font-size:9px; margin-bottom:6px; }
  .kpi-value { font-size:28px; }
  .kpi-value.orange { font-size:18px; }
  .kpi-delta { font-size:9px; margin-top:7px; }
  .kpi-top { font-size:10px; margin-top:7px; }
  .sec-head { margin:28px 0 12px; page-break-after:avoid; break-after:avoid; }
  .sec-num { font-size:10px; }
  .sec-title { font-size:20px; white-space:nowrap; }
  .analysis { padding:22px 26px; }
  .analysis p { font-size:13px; margin:0 0 12px; line-height:1.7; }

  table.data th { background:var(--black) !important; color:#fff !important; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
  /* Prevent any table row from splitting across a page break */
  table.data tr { page-break-inside:avoid; break-inside:avoid; }
  /* Top-5 print tightening - fit all 6 columns on A4 */
  table.top5 { font-size:9px; page-break-inside:avoid; }
  table.top5 th { padding:6px 5px; font-size:8px; }
  table.top5 td { padding:6px 5px; line-height:1.35; }
  table.top5 tr { page-break-inside:avoid; }
  table.top5 .rank-num { font-size:14px; }
  table.top5 .path-name { font-size:9px; }
  table.top5 .date-cell { font-size:8px; }
  table.top5 .prod-cell { font-size:9px; line-height:1.35; }
  table.top5 .co-cell strong { font-size:9px; }
  table.top5 .juris-country { font-size:9px; }
  table.top5 .brand-sub, table.top5 .src-sub { font-size:8px; margin-top:1px; }
  table.top5 .chip-tier1, table.top5 .chip-tier2, table.top5 .chip-outbreak { font-size:7px; padding:1px 3px; margin-left:3px; }
  table.top5 .src-link { font-size:8px; }
  table.top5 .juris-link { margin-top:3px; }
  /* Tighten the caption above the Top 5 so more room for rows */
  .sec-caption { font-size:10px; margin:-2px 0 8px; }
  /* Force section boundaries on page breaks for clean 4-page distribution:
     P1 = masthead + KPI + § 01 Analysis
     P2 = § 02 Top 5
     P3 = § 03 Distribution
     P4 = § 04 Methodology + Footer */
  section.page-break, div.page-break { page-break-before:always; }
  .sec-head.break-before { page-break-before:always; break-before:page; }

  /* Footer: switch from flex to a clean vertical stack for print.
     WeasyPrint and some browser print engines overlap the two halves
     when flex wraps at narrow widths - block layout avoids it entirely.
     page-break-inside: avoid keeps brand block + disclaimer together on one page. */
  .footer {
    display:block !important;
    margin-top:26px;
    page-break-inside:avoid;
    break-inside:avoid;
  }
  .footer > div { display:block !important; width:auto !important; }
  .footer > div:first-child { margin-bottom:12px; }
  .foot-legal {
    text-align:left !important;
    max-width:none !important;
    padding-top:10px;
    border-top:1px solid var(--brd);
  }
  /* Keep the methodology section with its adjacent section intact */
  .meth { page-break-inside:avoid; break-inside:avoid; }
}

@media screen and (max-width:900px) {
  body { padding:0 20px 40px; }
  .kpi-strip { grid-template-columns:repeat(2,1fr); }
  .dist-grid { grid-template-columns:1fr; }
  .masthead { flex-direction:column; gap:16px; }
  .mast-right { text-align:left; }
  .r-title { font-size:28px; }
}

/* Mobile Top-5: switch from a 6-column table to stacked cards.
   On phones, a horizontal table would either scroll sideways (bad UX) or
   compress columns into unreadable widths. Instead, each row becomes a
   card with labeled fields - all data visible, no horizontal scroll. */
@media screen and (max-width:700px) {
  table.top5, table.top5 thead, table.top5 tbody, table.top5 tr, table.top5 td {
    display:block; width:auto !important;
  }
  /* Kill all fixed column widths - they would make card-mode cells unreadably narrow */
  table.top5 th:nth-child(1), table.top5 td:nth-child(1),
  table.top5 th:nth-child(2), table.top5 td:nth-child(2),
  table.top5 th:nth-child(3), table.top5 td:nth-child(3),
  table.top5 th:nth-child(4), table.top5 td:nth-child(4),
  table.top5 th:nth-child(5), table.top5 td:nth-child(5),
  table.top5 th:nth-child(6), table.top5 td:nth-child(6) {
    width:auto !important;
  }
  table.top5 { border:none; table-layout:auto !important; }
  table.top5 thead { display:none; }
  table.top5 tr {
    border:1px solid var(--brd); border-left:4px solid var(--orange);
    background:#fff; margin-bottom:12px; padding:8px 4px;
    position:relative;
  }
  table.top5 tr:nth-child(even) td { background:transparent; }
  table.top5 td {
    border:none !important; padding:7px 14px 7px 108px !important;
    position:relative; min-height:28px;
    word-wrap:normal; overflow-wrap:normal;
  }
  table.top5 td::before {
    content:attr(data-label);
    position:absolute; left:14px; top:7px; width:88px;
    font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
    color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
  }
  /* Rank number sits in top-right corner as an orange badge */
  table.top5 .rank-num {
    position:absolute; top:8px; right:14px; padding:0 !important;
    font-size:28px; min-height:0; text-align:right;
  }
  table.top5 .rank-num::before { display:none; }
  table.top5 .date-cell { font-size:11px; }
  table.top5 .path-name { font-size:13px; }
  table.top5 .co-cell strong { font-size:13px; }
  table.top5 .prod-cell { line-height:1.45; font-size:13px; }
  table.top5 .juris-country { font-size:13px; }
  table.top5 .juris-link { margin-top:6px; }
}

@media screen and (max-width:480px) {
  body { padding:0 14px 30px; }
  .kpi-strip { grid-template-columns:1fr 1fr; }
  .kpi { padding:16px 14px; }
  .kpi-value { font-size:28px; }
  .r-title { font-size:24px; }
  .analysis { padding:18px 20px; }
  .analysis p { font-size:13px; }
}
</style>
</head>
<body>

<header class="masthead">
  <div class="brand-block">
    <div class="brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="tagline">Food Safety Intelligence System &middot; Weekly Briefing</div>
  </div>
  <div class="mast-right">
    <div class="report-label">Subscribers Edition</div>
    <div class="report-meta">
      <strong>ISSUE</strong> &middot; Week 16, 2026<br>
      <strong>PERIOD</strong> &middot; 11 Apr &ndash; 17 Apr 2026<br>
      <strong>PUBLISHED</strong> &middot; 17 Apr 2026 &middot; 09:00 UTC
    </div>
  </div>
</header>

<div class="r-kicker">AFTS <span class="r-kicker-dot">&middot;</span> Process Validation Intelligence</div>
<h1 class="r-title">Pathogen Surveillance <span class="accent">&middot;</span> Week 16</h1>
<p class="r-sub">
  AI-powered analysis of <strong>12</strong> regulatory recall actions across
  <strong>6</strong> jurisdictions, aggregated from 66 primary sources
  monitored continuously by the AFTS intelligence platform.
</p>
<div class="r-authority">
  <span class="auth-label">Process Authority</span>
  Food Process Engineering &middot; Thermal Processing &middot; Regulatory Compliance
</div>

<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value">12</div>
    <div class="kpi-delta" style="color:#059669">&#9660; -55 (-82%) vs prior week</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">7</div>
    <div class="kpi-delta" style="color:var(--muted)">Immediate public-health risk</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Active Outbreaks</div>
    <div class="kpi-value violet">0</div>
    <div class="kpi-delta" style="color:var(--muted)">Confirmed cluster events</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange">Listeria monocytogenes</div>
    <div class="kpi-top">5 cases &middot; 42% of total</div>
  </div>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 01</span>
  <h2 class="sec-title">Intelligence Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="analysis">
  <p>This week produced 12 pathogen-related recall incidents across the AFTS monitoring network, with 7 classified as Tier-1 and 0 confirmed outbreak event(s). Listeria monocytogenes dominated the surveillance window, accounting for 5 of 12 incidents (42%). The elevated Tier-1 ratio indicates sustained regulatory pressure and should be read by food manufacturers as a signal of tightening enforcement.</p>
  <p>Listeria monocytogenes at this concentration points to post-process recontamination in ready-to-eat deli, dairy, and cooked-meat lines rather than thermal underprocess. The likely failure modes are Zone 1 environmental harbourage, sanitation SOP drift, and post-lethality recontamination. The relevant frameworks for review are the environmental monitoring programme under 21 CFR 117 and the thermal-lethality validation applicable to the product class (21 CFR 113 / 114 where applicable), supported by qualified process authority oversight.</p>
  <p>Regulatory activity this week spanned multiple jurisdictions (RASFF, FDA, CFIA, FSA, and national authorities), signalling continued inspection intensity. AFTS recommends that food manufacturers use this briefing as a prompt to re-verify the single highest-leverage control for their commodity this week and to confirm documentation packages are ready for rapid regulatory response.</p>
  <p class="pa-note"><span class="pa-label">Process Authority Note:</span> This window contains 2 incident(s) implicating Clostridium or botulinum toxin, with Liquid Blenz Corp (United States) cited for Clostridium botulinum. Any shelf-stable low-acid canned food (LACF, 21 CFR 113) or acidified food (21 CFR 114) in the affected category must be reviewed to confirm that the product has achieved the established scheduled thermal process, that production is conducted under proper GMPs and the applicable regulatory framework, and that this has been validated under the guidance of a qualified process authority in order to control Clostridium botulinum spores. The most common compliance gap behind recalls of this profile is an unfiled or outdated scheduled process (FDA Form 2541 / 2541e), a process deviation resolved without qualified process authority review, or a formulation change — pH, a_w, salt, preservative — introduced without re-evaluation. A Tier-1 / Class-I classification on a low-acid product reliably triggers a 21 CFR 108 process filing audit and FDA Form 483 citations on the subsequent inspection, with analogous EU / CFIA enforcement steps.</p>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 02</span>
  <h2 class="sec-title">Top 5 Critical Threats</h2>
  <span class="sec-rule"></span>
</div>
<p class="sec-caption">
  Ranked by pathogen severity (<em>C. botulinum</em> &rarr; <em>Listeria</em> &rarr; STEC &rarr; <em>Salmonella</em>), outbreak status, and tier classification.
  Each row links to the originating regulatory notice.
</p>
<table class="data top5">
  <thead>
    <tr>
      <th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td class="rank-num" data-label="#">1</td>
      <td class="date-cell" data-label="Date">13 Apr 2026</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:#9333ea"></span>
        <span class="path-name">Clostridium botulinum</span>
        <span class="chip-tier1">T1</span>
      </td>
      <td class="co-cell" data-label="Company"><strong>Marché Rio</strong><div class="brand-sub">—</div></td>
      <td class="prod-cell" data-label="Product">Épices haïtiennes / Haitian spices — plastic container, refrigerated</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">Canada</div>
        <div class="src-sub">MAPAQ QC</div>
        <div class="juris-link"><a class="src-link" href="https://www.quebec.ca/nouvelles/actualites/details/mise-en-garde-a-la-population-innocuite-non-assuree-depices-haitiennes-vendues-par-lentreprise-marche-rio-69749" target="_blank" rel="noopener">View source &rarr;</a></div>
      </td>
    </tr>
    <tr>
      <td class="rank-num" data-label="#">2</td>
      <td class="date-cell" data-label="Date">15 Apr 2026</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:#9333ea"></span>
        <span class="path-name">Clostridium botulinum</span>
        <span class="chip-tier1">T1</span>
      </td>
      <td class="co-cell" data-label="Company"><strong>Liquid Blenz Corp</strong><div class="brand-sub">Good Brain Tonic</div></td>
      <td class="prod-cell" data-label="Product">Good Brain Tonic 16 oz &amp; 32 oz (All codes)</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">United States</div>
        <div class="src-sub">FDA</div>
        <div class="juris-link"><a class="src-link" href="https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/liquid-blenz-corp-recalls-product-due-possible-health-risk" target="_blank" rel="noopener">View source &rarr;</a></div>
      </td>
    </tr>
    <tr>
      <td class="rank-num" data-label="#">3</td>
      <td class="date-cell" data-label="Date">11 Apr 2026</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:#dc2626"></span>
        <span class="path-name">Listeria monocytogenes</span>
        <span class="chip-tier1">T1</span>
      </td>
      <td class="co-cell" data-label="Company"><strong>Les délices de la cabraire</strong><div class="brand-sub">Les délices de la cabraire</div></td>
      <td class="prod-cell" data-label="Product">fromage de brebis fermier</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">France</div>
        <div class="src-sub">RappelConso (FR)</div>
        <div class="juris-link"><a class="src-link" href="https://rappel.conso.gouv.fr/fiche-rappel/22010/Interne" target="_blank" rel="noopener">View source &rarr;</a></div>
      </td>
    </tr>
    <tr>
      <td class="rank-num" data-label="#">4</td>
      <td class="date-cell" data-label="Date">14 Apr 2026</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:#dc2626"></span>
        <span class="path-name">Listeria monocytogenes</span>
        <span class="chip-tier1">T1</span>
      </td>
      <td class="co-cell" data-label="Company"><strong>7-Eleven Canada</strong><div class="brand-sub">7-Eleven</div></td>
      <td class="prod-cell" data-label="Product">12 sandwich/sub/wrap products</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">Canada</div>
        <div class="src-sub">CFIA</div>
        <div class="juris-link"><a class="src-link" href="https://recalls-rappels.canada.ca/en/alert-recall/7-eleven-brand-sandwiches-subs-and-wraps-recalled-due-listeria-monocytogenes" target="_blank" rel="noopener">View source &rarr;</a></div>
      </td>
    </tr>
    <tr>
      <td class="rank-num" data-label="#">5</td>
      <td class="date-cell" data-label="Date">14 Apr 2026</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:#dc2626"></span>
        <span class="path-name">Listeria monocytogenes</span>
        <span class="chip-tier1">T1</span>
      </td>
      <td class="co-cell" data-label="Company"><strong>NOTRE JARDIN</strong><div class="brand-sub">NOTRE JARDIN</div></td>
      <td class="prod-cell" data-label="Product">SALADE COMPOSEE 400G</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">France</div>
        <div class="src-sub">RappelConso (FR)</div>
        <div class="juris-link"><a class="src-link" href="https://rappel.conso.gouv.fr/fiche-rappel/22021/Interne" target="_blank" rel="noopener">View source &rarr;</a></div>
      </td>
    </tr>
  </tbody>
</table>

<div class="sec-head">
  <span class="sec-num">&sect; 03</span>
  <h2 class="sec-title">Distribution Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="dist-grid">
  <div>
    <h3>Pathogen Profile</h3>
    <table class="data">
      <thead>
        <tr><th>Pathogen</th><th class="num">Cases</th><th class="num">%</th><th>Share</th></tr>
      </thead>
      <tbody>
        
        <tr>
          <td><span class="path-dot" style="background:#dc2626"></span>Listeria monocytogenes</td>
          <td class="num">5</td>
          <td class="num">42%</td>
          <td><div class="bar-track"><div class="bar-fill" style="width:42%;background:#dc2626"></div></div></td>
        </tr>
        <tr>
          <td><span class="path-dot" style="background:#dc2626"></span>Salmonella spp.</td>
          <td class="num">4</td>
          <td class="num">33%</td>
          <td><div class="bar-track"><div class="bar-fill" style="width:33%;background:#dc2626"></div></div></td>
        </tr>
        <tr>
          <td><span class="path-dot" style="background:#9333ea"></span>Clostridium botulinum</td>
          <td class="num">2</td>
          <td class="num">17%</td>
          <td><div class="bar-track"><div class="bar-fill" style="width:17%;background:#9333ea"></div></div></td>
        </tr>
        <tr>
          <td><span class="path-dot" style="background:#dc2626"></span>E. coli</td>
          <td class="num">1</td>
          <td class="num">8%</td>
          <td><div class="bar-track"><div class="bar-fill" style="width:8%;background:#dc2626"></div></div></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div>
    <h3>Geographic &middot; Regulatory</h3>
    <table class="data">
      <thead>
        <tr><th>Country</th><th>Authority</th><th class="num">Cases</th><th class="num">%</th></tr>
      </thead>
      <tbody>
        
        <tr>
          <td>France</td>
          <td>RappelConso / DGCCRF</td>
          <td class="num">4</td>
          <td class="num">33%</td>
        </tr>
        <tr>
          <td>Germany</td>
          <td>BVL</td>
          <td class="num">3</td>
          <td class="num">25%</td>
        </tr>
        <tr>
          <td>Canada</td>
          <td>CFIA</td>
          <td class="num">2</td>
          <td class="num">17%</td>
        </tr>
        <tr>
          <td>United States</td>
          <td>FDA / USDA FSIS</td>
          <td class="num">1</td>
          <td class="num">8%</td>
        </tr>
        <tr>
          <td>New Zealand</td>
          <td>MPI / FSANZ</td>
          <td class="num">1</td>
          <td class="num">8%</td>
        </tr>
        <tr>
          <td>Italy</td>
          <td>Ministero della Salute</td>
          <td class="num">1</td>
          <td class="num">8%</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<div class="cta-box">
  <div class="cta-text">
    <h3>Live Dashboard &middot; Full Dataset Access</h3>
    <p>Filter by pathogen, country, tier, and source. Download the accumulative XLSX dataset. Set custom alerts.</p>
  </div>
  <a class="cta-btn" href="https://www.advfood.tech/food-safety-intelligence" target="_blank" rel="noopener">Access Portal &rarr;</a>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 04</span>
  <h2 class="sec-title">Methodology &amp; Sources</h2>
  <span class="sec-rule"></span>
</div>
<div class="meth">
  <p>
    <strong>Process authority.</strong> Analytical frameworks, severity rubrics, pathogen
    classification, and the engineering interpretation of each recall are developed under the
    process authority of AFTS, drawing on in-house expertise in food process engineering,
    thermal processing, and regulatory compliance. Every view is grounded in validated
    process engineering: thermal processing (21 CFR 113/114), pasteurisation (PMO), aseptic
    and UHT, hold-tube and F-value lethality, and HACCP. This is what the AFTS platform brings
    that pure data feeds do not &mdash; data under engineering authority.
  </p>
  <p>
    <strong>Data &amp; AI pipeline.</strong> The system aggregates regulatory recall notices from
    66 primary sources across 60+ countries (FDA, USDA FSIS, RASFF, FSA, FSANZ, CFIA, RappelConso,
    BVL, AESAN, EFET, and national authorities) and processes each record through Gemini
    (extraction), OpenAI GPT (normalisation), and Claude (Tier-1 validation). Records are
    de-duplicated and harmonised into the accumulative dataset.
  </p>
  <p>
    <strong>This briefing.</strong> Statistical analysis filters the accumulative dataset to the
    reporting week (11 Apr &ndash; 17 Apr 2026).
    AI-generated narrative is produced against AFTS process-authority prompts and edited for
    publication. Figures and pathogen names are preserved verbatim from source data.
  </p>
</div>

<footer class="footer">
  <div>
    <div class="foot-brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="foot-meta">
      Food Process Engineering &middot; Thermal Processing &middot; Regulatory Compliance<br>
      advfood.tech &middot; info@advfood.tech &middot; Athens, Greece<br>
      &copy; 2026 Advanced Food Tech Solutions
    </div>
  </div>
  <div class="foot-legal">
    This briefing is provided for informational purposes only and does not constitute regulatory, legal,
    or medical advice. Subscribers should verify recall status with the originating regulatory authority
    before taking action. Next issue: Friday, 24 Apr 2026.
  </div>
</footer>

</body>
</html>