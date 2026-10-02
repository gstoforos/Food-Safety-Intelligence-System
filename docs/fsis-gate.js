/* fsis-gate.js — AFTS FSIS subscriber sign-in (2026-10-01)
 * ===========================================================================
 * Loaded in <head> of every subscriber page: the dashboard, weekly and
 * monthly reports, Live Signals, daily briefs, alerts. NOT on the preview
 * (index-promo.html), the hub or the marketing PDFs.
 *
 *   - ONLY THE DASHBOARD (index) asks for sign-in. Every other page opens
 *     straight away; there this file only cuts the address back.
 *   - The dashboard is blurred and covered by the sign-in box until the Google
 *     script (FsisAccess.gs, through Router.gs) says this session is valid.
 *   - Sign in = first name + last name + email + access token from the
 *     welcome email. "Remember me" keeps them on the device and signs in by
 *     itself next time (never after another device took over — that would
 *     ping-pong between two devices).
 *   - One device at a time: a sign-in on another device ends this one; the
 *     check runs on load and every 5 minutes.
 *   - The address bar is cut back to the site root as soon as the page opens,
 *     so a copied link is https://fsis.advfood.tech/ (the sign-in), never the
 *     report itself.
 *
 * Nothing secret is in this file. The token lives in the subscriber's email
 * and the Subscribers sheet; this file only carries a session id the script
 * issued, which is useless once that subscriber signs in elsewhere.
 * ===========================================================================
 */
(function () {
  'use strict';

  var GATE_URL = 'https://script.google.com/macros/s/AKfycbwA2UeM1KtmUOcI6T2dwT-6Ox2DOPZUJtWvePaMU8wgrkCcOrEw9kVq9BtWpZ0NQSQ/exec';
  // The dashboard's SHEET button opens GATE_URL?action=fsis_sheet&sid=… (2026-10-02).
  window.FSIS_GATE_URL = GATE_URL;
  var KEY = 'fsis-session';
  var RECHECK_MS = 5 * 60 * 1000;
  var GRACE_MS = 12 * 3600 * 1000;   // service unreachable: keep a session checked in the last 12 h
  var LINKS = {
    preview:   'https://www.advfood.tech/fsispreview',
    subscribe: 'https://www.advfood.tech/fsis-plans',
    support:   'info@advfood.tech'
  };

  var doc = document, root = doc.documentElement;

  /* ONLY THE DASHBOARD (the index) ASKS FOR SIGN-IN (operator 2026-10-01).
     Reports, Live Signals and daily briefs open without it; on those this
     file only cuts the address back (step 2). */
  var IS_INDEX = /^\/(index\.html?)?$/i.test(location.pathname);

  /* ── 1. Blur immediately, before anything paints ─────────────────────── */
  if (IS_INDEX) root.classList.add('fsis-locked');
  var css = doc.createElement('style');
  css.id = 'fsis-gate-css';
  css.textContent =
    'html.fsis-locked body>*:not(#fsis-gate){filter:blur(9px) grayscale(.4);pointer-events:none;user-select:none;-webkit-user-select:none}' +
    'html.fsis-locked,html.fsis-locked body{overflow:hidden!important}' +
    '#fsis-gate{position:fixed;inset:0;z-index:2147483000;display:flex;align-items:flex-start;justify-content:center;overflow-y:auto;' +
      'background:rgba(15,15,15,.45);padding:28px 16px;font-family:Inter,-apple-system,"Segoe UI",Roboto,sans-serif}' +
    '#fsis-gate .g-card{width:100%;max-width:560px;background:#fff;color:#111;border:1.5px solid #111;border-radius:4px;' +
      'box-shadow:0 18px 50px rgba(0,0,0,.35)}' +
    '#fsis-gate .g-top{background:#111;color:#fff;padding:14px 22px;display:flex;justify-content:space-between;align-items:center;gap:10px;' +
      'font:500 10.5px/1.3 "DM Mono",ui-monospace,monospace;letter-spacing:.16em;text-transform:uppercase}' +
    '#fsis-gate .g-top span{color:#a8a8a8}' +
    '#fsis-gate .g-body{padding:22px 22px 18px}' +
    '#fsis-gate h2{margin:0 0 8px;font:800 24px/1.15 Inter,-apple-system,sans-serif;color:#111;letter-spacing:-.02em}' +
    '#fsis-gate p{margin:0 0 12px;font-size:14px;line-height:1.55;color:#3a3a3a}' +
    '#fsis-gate .g-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 14px}' +
    '#fsis-gate .g-full{grid-column:1/-1}' +
    '#fsis-gate label{display:block;font:500 10px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:#6b6b6b;margin:12px 0 6px}' +
    '#fsis-gate input{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:3px;border:1px solid #c9c9c9;background:#f6f6f4;' +
      'color:#111;font-size:14px;outline:none}' +
    '#fsis-gate input:focus{border-color:#111;background:#fff;box-shadow:0 0 0 3px rgba(0,0,0,.08)}' +
    '#fsis-gate .g-rem{display:flex;align-items:center;gap:8px;margin:14px 0 0;font:400 13px/1.3 Inter,-apple-system,sans-serif;letter-spacing:0;text-transform:none;color:#3a3a3a;cursor:pointer}' +
    '#fsis-gate .g-rem input{width:16px;height:16px;margin:0;padding:0;accent-color:#111;cursor:pointer}' +
    '#fsis-gate input.tok{font-family:"DM Mono",ui-monospace,monospace;letter-spacing:.08em;text-transform:uppercase}' +
    '#fsis-gate button{width:100%;margin-top:16px;padding:13px;border:1.5px solid #111;border-radius:3px;background:#111;color:#fff;' +
      'font:600 12px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.14em;text-transform:uppercase;cursor:pointer}' +
    '#fsis-gate button:hover{background:#333}#fsis-gate button[disabled]{opacity:.6;cursor:wait}' +
    '#fsis-gate .g-msg{min-height:18px;margin-top:10px;font-size:13px;line-height:1.5;color:#3a3a3a}' +
    '#fsis-gate .g-msg.err{color:#111;font-weight:600;border-left:3px solid #111;padding-left:8px}' +
    '#fsis-gate .g-msg.ok{color:#111;border-left:3px solid #8a8a8a;padding-left:8px}' +
    '#fsis-gate .g-info{margin-top:14px;padding:14px 16px;background:#f2f2f0;border:1px solid #e2e2de;font-size:12.8px;line-height:1.6;color:#3a3a3a}' +
    '#fsis-gate .g-info b{color:#111}' +
    '#fsis-gate .g-info ul{margin:6px 0 0;padding-left:16px}#fsis-gate .g-info li{margin:2px 0}' +
    '#fsis-gate .g-links{display:flex;flex-wrap:wrap;justify-content:space-between;gap:8px;padding:12px 22px;border-top:1px solid #e2e2de;' +
      'background:#fafaf8;font-size:12.5px}' +
    '#fsis-gate .g-links a{color:#111;text-decoration:none;border-bottom:1px solid #b5b5b5;cursor:pointer}#fsis-gate .g-links a:hover{border-color:#111}' +
    '#fsis-gate .g-wait{padding:22px;font:500 11px/1.4 "DM Mono",ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:#3a3a3a}' +
    '@media (max-width:520px){#fsis-gate .g-grid{grid-template-columns:1fr}#fsis-gate{padding:12px 10px}#fsis-gate h2{font-size:21px}}';
  (doc.head || root).appendChild(css);

  /* ── 2. Session id handed over in the link (#fsis=…), then the URL is cut
          back to the site root. A <base> keeps the page's own relative links
          (data files, images, other reports) pointing where they did. ───── */
  var handed = '';
  try {
    var m = /[#&]fsis=([A-Za-z0-9_]+)/.exec(location.hash || '');
    if (m) handed = m[1];
  } catch (e) {}
  var ORIGINAL = location.href.split('#')[0];
  var ORIGINAL_HASH = (location.hash || '').replace(/[#&]?fsis=[A-Za-z0-9_]+/, '');
  try {
    if (window.top === window && location.pathname !== '/' && history.replaceState) {
      if (!doc.querySelector('base')) {
        var b = doc.createElement('base');
        b.href = ORIGINAL;
        (doc.head || root).insertBefore(b, (doc.head || root).firstChild);
      }
      history.replaceState(null, '', '/');
    } else if (handed && history.replaceState) {
      history.replaceState(null, '', location.pathname + location.search);
    }
  } catch (e) {}

  /* In-page anchors (#section) would follow the <base> and reload the page:
     scroll to them instead. */
  doc.addEventListener('click', function (ev) {
    var a = ev.target && ev.target.closest ? ev.target.closest('a[href^="#"]') : null;
    if (!a) return;
    var id = a.getAttribute('href').slice(1);
    var t = id && (doc.getElementById(id) || doc.getElementsByName(id)[0]);
    if (t) { ev.preventDefault(); t.scrollIntoView({ behavior: 'smooth' }); }
  }, true);

  if (!IS_INDEX) return;   // reports & briefs: address cut back, nothing else

  /* ── storage (may be blocked: everything works without it) ──────────── */
  function load() { try { return JSON.parse(localStorage.getItem(KEY) || 'null'); } catch (e) { return null; } }
  function save(s) { try { localStorage.setItem(KEY, JSON.stringify(s)); } catch (e) {} }
  function clear() { try { localStorage.removeItem(KEY); } catch (e) {} }

  /* "Remember me": name, email and token kept on this device, so the box is
     filled in and signs in by itself next time. Unticked = all of it wiped. */
  var RKEY = 'fsis-remember';
  function loadRem() { try { return JSON.parse(localStorage.getItem(RKEY) || 'null'); } catch (e) { return null; } }
  function saveRem(r) { try { localStorage.setItem(RKEY, JSON.stringify(r)); } catch (e) {} }
  function clearRem() { try { localStorage.removeItem(RKEY); } catch (e) {} }

  /* Same browser = same device, whether the page is inside Wix or in its own
     tab, so the two do not lock each other out. */
  function deviceKey() {
    var s = [navigator.userAgent, navigator.language, (screen.width + 'x' + screen.height + 'x' + screen.colorDepth),
             (Intl.DateTimeFormat().resolvedOptions().timeZone || ''), navigator.platform || ''].join('|');
    var h = 2166136261;
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
    return 'd' + h.toString(36);
  }

  /* ── JSONP: Apps Script answers cross-origin GET as a script ─────────── */
  var seq = 0;
  function call(params, done) {
    var name = '__fsisCb' + (++seq) + '_' + Date.now();
    var el = doc.createElement('script');
    var finished = false;
    var timer = setTimeout(function () { finish({ ok: false, code: 'NETWORK' }); }, 15000);
    function finish(res) {
      if (finished) return;
      finished = true; clearTimeout(timer);
      try { delete window[name]; } catch (e) { window[name] = undefined; }
      if (el.parentNode) el.parentNode.removeChild(el);
      done(res || { ok: false, code: 'NETWORK' });
    }
    window[name] = finish;
    var q = ['callback=' + name];
    for (var k in params) if (params.hasOwnProperty(k)) q.push(k + '=' + encodeURIComponent(params[k]));
    el.src = GATE_URL + '?' + q.join('&');
    el.onerror = function () { finish({ ok: false, code: 'NETWORK' }); };
    (doc.head || root).appendChild(el);
  }

  /* ── the box ──────────────────────────────────────────────────────────── */
  var gate = null;
  function esc(s) { return String(s || '').replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }

  function mountGate(html) {
    if (!doc.body) { doc.addEventListener('DOMContentLoaded', function () { mountGate(html); }); return; }
    if (!gate) { gate = doc.createElement('div'); gate.id = 'fsis-gate'; doc.body.appendChild(gate); }
    gate.innerHTML = '<div class="g-card" role="dialog" aria-modal="true" aria-labelledby="fsis-g-h">' + html + '</div>';
  }

  function showWait() {
    mountGate('<div class="g-top">AFTS · Food Safety Intelligence System <span>Subscribers</span></div><div class="g-wait">Checking your sign-in…</div>');
  }

  function showSignin(note, kind) {
    if (!doc.body) {
      doc.addEventListener('DOMContentLoaded', function () { showSignin(note, kind); });
      return;
    }
    var s = load() || {};
    var rem = loadRem();
    if (rem) { s.first = rem.first; s.last = rem.last; s.email = rem.email; }
    mountGate(
      '<div class="g-top">AFTS · Food Safety Intelligence System <span>Subscribers</span></div>' +
      '<div class="g-body">' +
      '<h2 id="fsis-g-h">Subscriber sign-in</h2>' +
      '<p>FSIS is for subscribers. Sign in with the name and email of your subscription and the access token from your welcome email.</p>' +
      '<form id="fsis-g-form" autocomplete="on"><div class="g-grid">' +
      '<div><label for="fsis-g-first">First name</label><input id="fsis-g-first" name="given-name" autocomplete="given-name" placeholder="incl. middle name" value="' + esc(s.first) + '"></div>' +
      '<div><label for="fsis-g-last">Last name</label><input id="fsis-g-last" name="family-name" autocomplete="family-name" value="' + esc(s.last) + '"></div>' +
      '<div class="g-full"><label for="fsis-g-email">Email</label><input id="fsis-g-email" name="email" type="email" autocomplete="username" required value="' + esc(s.email) + '"></div>' +
      '<div class="g-full"><label for="fsis-g-token">Access token</label><input id="fsis-g-token" class="tok" type="password" name="password" placeholder="FSI-XXXX-XXXX-XXXX" required autocomplete="current-password" spellcheck="false" value="' + esc(rem ? rem.token : '') + '"></div>' +
      '</div>' +
      '<label class="g-rem"><input type="checkbox" id="fsis-g-remember"' + (rem ? ' checked' : '') + '> Remember me on this device</label>' +
      '<button type="submit" id="fsis-g-go">Sign in</button>' +
      '<div class="g-msg ' + (kind || '') + '" id="fsis-g-msg">' + esc(note || '') + '</div>' +
      '</form>' +
      '<div class="g-info"><b>Your subscription includes</b><ul>' +
      '<li>Live recall dashboard — 70+ official sources, 80+ countries, verified in three stages</li>' +
      '<li>Weekly report every Monday · monthly report on the 1st · daily briefs</li>' +
      '<li>Live Signals — weekly aberration detection · custom alerts</li></ul>' +
      '<div style="margin-top:8px"><b>Where is my token?</b> In your welcome email from AFTS (subject “Welcome to AFTS Food Safety Validation Intelligence” or “Your AFTS FSIS access token”). ' +
      'Not there? Use <i>Email me my token</i> below — it goes only to the address you subscribed with.</div>' +
      '<div style="margin-top:8px"><b>One device at a time.</b> Signing in on another device signs this one out. ' +
      'Tick <i>Remember me</i> and this device signs you in by itself next time.</div>' +
      '</div></div>' +
      '<div class="g-links"><a id="fsis-g-rec">Email me my token</a>' +
      '<a href="' + LINKS.preview + '" target="_top">Free preview</a>' +
      '<a href="' + LINKS.subscribe + '" target="_top">Subscribe</a>' +
      '<a href="mailto:' + LINKS.support + '">Help: ' + LINKS.support + '</a></div>'
    );
    var f = doc.getElementById('fsis-g-form');
    f.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var first = doc.getElementById('fsis-g-first').value.trim();
      var last = doc.getElementById('fsis-g-last').value.trim();
      var name = (first + ' ' + last).trim();
      var email = doc.getElementById('fsis-g-email').value.trim();
      var token = doc.getElementById('fsis-g-token').value.trim();
      var btn = doc.getElementById('fsis-g-go'), msg = doc.getElementById('fsis-g-msg');
      btn.disabled = true; msg.className = 'g-msg'; msg.textContent = 'Signing in…';
      call({ action: 'fsis_signin', name: name, email: email, token: token, device: deviceKey() }, function (r) {
        btn.disabled = false;
        if (r && r.ok) {
          save({ sid: r.sid, name: r.name || name, first: first, last: last, email: email, checked: Date.now() });
          if (doc.getElementById('fsis-g-remember').checked) saveRem({ first: first, last: last, email: email, token: token, signedOut: false });
          else clearRem();
          unlock();
          return;
        }
        msg.className = 'g-msg err';
        msg.textContent = (r && r.message) ||
          (r && r.code === 'NETWORK' ? 'The sign-in service did not answer. Check your connection and try again.' : 'Sign-in failed.');
      });
    });
    doc.getElementById('fsis-g-rec').addEventListener('click', function () {
      var email = doc.getElementById('fsis-g-email').value.trim();
      var msg = doc.getElementById('fsis-g-msg');
      if (!email) { msg.className = 'g-msg err'; msg.textContent = 'Enter your email address first.'; return; }
      msg.className = 'g-msg'; msg.textContent = 'Sending…';
      call({ action: 'fsis_recover', email: email }, function (r) {
        msg.className = 'g-msg ' + (r && r.ok ? 'ok' : 'err');
        msg.textContent = (r && r.message) || 'Could not send right now. Write to ' + LINKS.support + '.';
      });
    });
    setTimeout(function () {
      var el = doc.getElementById(s.email ? 'fsis-g-token' : 'fsis-g-first');
      if (el) try { el.focus(); } catch (e) {}
    }, 50);
  }

  /* ── lock / unlock ───────────────────────────────────────────────────── */
  var recheck = null;

  /* Signed-in bar at the top: who is signed in, and Sign out. Sign out ends
     the session on the server and forgets the token on this device (the
     name and email stay filled in), so Remember me cannot sign straight
     back in. */
  function showUserBar() {
    if (!doc.body) return;
    var s = load() || {};
    var bar = doc.getElementById('fsis-user');
    if (!bar) {
      var st = doc.createElement('style');
      st.textContent =
        '#fsis-user{display:flex;align-items:center;justify-content:flex-end;flex-wrap:wrap;gap:6px 14px;padding:7px 16px;' +
          'background:#111;color:#cfcfcf;border-bottom:1px solid #2a2a2a;font:500 11px/1.3 "DM Mono",ui-monospace,monospace;letter-spacing:.06em}' +
        '#fsis-user b{color:#fff;font-weight:600;letter-spacing:.02em}' +
        '#fsis-user button{background:#fff;color:#111;border:1px solid #fff;border-radius:3px;padding:5px 12px;cursor:pointer;' +
          'font:600 10.5px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase}' +
        '#fsis-user button:hover{background:#d9d9d9;border-color:#d9d9d9}';
      (doc.head || root).appendChild(st);
      bar = doc.createElement('div');
      bar.id = 'fsis-user';
      doc.body.insertBefore(bar, doc.body.firstChild);
    }
    var nm = s.name || ((s.first || '') + ' ' + (s.last || '')).trim() || s.email || '';
    bar.innerHTML = '<span>Signed in' + (nm ? ' · <b>' + esc(nm) + '</b>' : '') + '</span>' +
                    '<button type="button" id="fsis-signout">Sign out</button>';
    doc.getElementById('fsis-signout').addEventListener('click', signOut);
  }

  function signOut() {
    var s = load() || {};
    if (s.sid) call({ action: 'fsis_signout', sid: s.sid }, function () {});
    var rem = loadRem();
    // Keep the remembered token (operator 2026-10-01: Remember me must remember
    // the token). A flag stops the automatic sign-in until the next manual one.
    if (rem) { rem.signedOut = true; saveRem(rem); }
    clear(); save({ name: s.name, first: s.first, last: s.last, email: s.email });
    var bar = doc.getElementById('fsis-user');
    if (bar && bar.parentNode) bar.parentNode.removeChild(bar);
    lock('You are signed out.');
    var m = doc.getElementById('fsis-g-msg'); if (m) m.className = 'g-msg ok';
  }

  function unlock() {
    root.classList.remove('fsis-locked');
    if (gate && gate.parentNode) gate.parentNode.removeChild(gate);
    gate = null;
    if (doc.body) showUserBar(); else doc.addEventListener('DOMContentLoaded', showUserBar);
    if (ORIGINAL_HASH && ORIGINAL_HASH.length > 1) {
      var t = doc.getElementById(ORIGINAL_HASH.slice(1));
      if (t) setTimeout(function () { t.scrollIntoView(); }, 0);
    }
    if (!recheck) recheck = setInterval(check, RECHECK_MS);
  }

  function lock(note) {
    root.classList.add('fsis-locked');
    if (recheck) { clearInterval(recheck); recheck = null; }
    showSignin(note, 'err');
  }

  function check(first) {
    var s = load();
    if (!s || !s.sid) { if (first) showSignin(); return; }
    call({ action: 'fsis_check', sid: s.sid, device: deviceKey() }, function (r) {
      if (r && r.ok) {
        s.checked = Date.now(); save(s);
        if (first) unlock();
        return;
      }
      if (r && r.code === 'NETWORK' && s.checked && Date.now() - s.checked < GRACE_MS) {
        if (first) unlock();      // service unreachable: honour a recent check
        return;
      }
      var keep = { name: s.name, first: s.first, last: s.last, email: s.email };
      clear(); save(keep);
      lock((r && r.message) || 'Please sign in.');
    });
  }

  /* ── start ───────────────────────────────────────────────────────────── */
  if (handed) {
    var prev = load() || {};
    save({ sid: handed, name: prev.name || '', first: prev.first || '', last: prev.last || '', email: prev.email || '', checked: 0 });
  }
  function autoSignin() {
    var rem = loadRem();
    if (!rem || !rem.token || !rem.email || rem.signedOut) { showSignin(); return; }
    if (doc.body) showWait(); else doc.addEventListener('DOMContentLoaded', function () { if (root.classList.contains('fsis-locked') && !gate) showWait(); });
    call({ action: 'fsis_signin', name: (rem.first + ' ' + rem.last).trim(), email: rem.email,
           token: rem.token, device: deviceKey() }, function (r) {
      if (r && r.ok) {
        save({ sid: r.sid, name: r.name, first: rem.first, last: rem.last, email: rem.email, checked: Date.now() });
        unlock();
      } else {
        showSignin((r && r.message) || '', 'err');
      }
    });
  }

  if (load() && load().sid) {
    if (doc.body) showWait(); else doc.addEventListener('DOMContentLoaded', function () { if (root.classList.contains('fsis-locked') && !gate) showWait(); });
    check(true);
  } else {
    autoSignin();   // remembered details sign in by themselves; otherwise the box
  }
})();
