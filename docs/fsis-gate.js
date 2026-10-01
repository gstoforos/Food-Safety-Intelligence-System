/* fsis-gate.js — AFTS FSIS subscriber sign-in (2026-10-01)
 * ===========================================================================
 * Loaded in <head> of every subscriber page: the dashboard, weekly and
 * monthly reports, Live Signals, daily briefs, alerts. NOT on the preview
 * (index-promo.html), the hub or the marketing PDFs.
 *
 *   - The page is blurred and covered by the sign-in box until the Google
 *     script (FsisAccess.gs, through Router.gs) says this session is valid.
 *   - Sign in = name + email + access token from the welcome email.
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
  var KEY = 'fsis-session';
  var RECHECK_MS = 5 * 60 * 1000;
  var GRACE_MS = 12 * 3600 * 1000;   // service unreachable: keep a session checked in the last 12 h
  var LINKS = {
    preview:   'https://www.advfood.tech/fsispreview',
    subscribe: 'https://www.advfood.tech/fsis-plans',
    support:   'info@advfood.tech'
  };

  var doc = document, root = doc.documentElement;

  /* ── 1. Blur immediately, before anything paints ─────────────────────── */
  root.classList.add('fsis-locked');
  var css = doc.createElement('style');
  css.id = 'fsis-gate-css';
  css.textContent =
    'html.fsis-locked body>*:not(#fsis-gate){filter:blur(9px);pointer-events:none;user-select:none;-webkit-user-select:none}' +
    'html.fsis-locked,html.fsis-locked body{overflow:hidden!important}' +
    '#fsis-gate{position:fixed;inset:0;z-index:2147483000;display:flex;align-items:center;justify-content:center;' +
      'background:rgba(10,12,18,.55);padding:16px;font-family:Inter,-apple-system,"Segoe UI",Roboto,sans-serif}' +
    '#fsis-gate .g-card{width:100%;max-width:400px;background:#111318;color:#e8e8e8;border:1px solid #2a2d35;' +
      'border-top:3px solid #E8601A;border-radius:8px;padding:26px 24px 20px;box-shadow:0 18px 50px rgba(0,0,0,.45)}' +
    '#fsis-gate .g-eyebrow{font:500 10.5px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.16em;text-transform:uppercase;color:#9aa0aa;margin-bottom:10px}' +
    '#fsis-gate h2{margin:0 0 6px;font:700 21px/1.2 Inter,-apple-system,sans-serif;color:#fff;letter-spacing:-.01em}' +
    '#fsis-gate p{margin:0 0 16px;font-size:13.5px;line-height:1.55;color:#b9bec8}' +
    '#fsis-gate label{display:block;font:500 10px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:#9aa0aa;margin:12px 0 6px}' +
    '#fsis-gate input{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:6px;border:1px solid #33363f;background:#0b0c10;' +
      'color:#fff;font-size:14px;outline:none}' +
    '#fsis-gate input:focus{border-color:#E8601A;box-shadow:0 0 0 3px rgba(232,96,26,.18)}' +
    '#fsis-gate input.tok{font-family:"DM Mono",ui-monospace,monospace;letter-spacing:.08em;text-transform:uppercase}' +
    '#fsis-gate button{width:100%;margin-top:18px;padding:12px;border:0;border-radius:6px;background:#E8601A;color:#fff;' +
      'font:600 12px/1 "DM Mono",ui-monospace,monospace;letter-spacing:.14em;text-transform:uppercase;cursor:pointer}' +
    '#fsis-gate button[disabled]{opacity:.6;cursor:wait}' +
    '#fsis-gate .g-msg{min-height:18px;margin-top:12px;font-size:13px;line-height:1.5}' +
    '#fsis-gate .g-msg.err{color:#ff8a7a}#fsis-gate .g-msg.ok{color:#6ee7a8}' +
    '#fsis-gate .g-links{display:flex;flex-wrap:wrap;justify-content:space-between;gap:8px;margin-top:14px;padding-top:12px;' +
      'border-top:1px solid #23262d;font-size:12.5px}' +
    '#fsis-gate .g-links a{color:#c9cdd4;text-decoration:none;cursor:pointer}#fsis-gate .g-links a:hover{color:#E8601A}' +
    '#fsis-gate .g-wait{font:500 11px/1.4 "DM Mono",ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:#c9cdd4}';
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

  /* ── storage (may be blocked: everything works without it) ──────────── */
  function load() { try { return JSON.parse(localStorage.getItem(KEY) || 'null'); } catch (e) { return null; } }
  function save(s) { try { localStorage.setItem(KEY, JSON.stringify(s)); } catch (e) {} }
  function clear() { try { localStorage.removeItem(KEY); } catch (e) {} }

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
    mountGate('<div class="g-eyebrow">AFTS · Food Safety Intelligence</div><div class="g-wait">Checking your sign-in…</div>');
  }

  function showSignin(note, kind) {
    if (!doc.body) {
      doc.addEventListener('DOMContentLoaded', function () { showSignin(note, kind); });
      return;
    }
    var s = load() || {};
    mountGate(
      '<div class="g-eyebrow">AFTS · Food Safety Intelligence</div>' +
      '<h2 id="fsis-g-h">Subscriber sign-in</h2>' +
      '<p>Sign in with the access token from your welcome email.</p>' +
      '<form id="fsis-g-form" autocomplete="on">' +
      '<label for="fsis-g-name">Name</label><input id="fsis-g-name" name="name" autocomplete="name" value="' + esc(s.name) + '">' +
      '<label for="fsis-g-email">Email</label><input id="fsis-g-email" name="email" type="email" autocomplete="email" required value="' + esc(s.email) + '">' +
      '<label for="fsis-g-token">Access token</label><input id="fsis-g-token" class="tok" name="token" placeholder="FSI-XXXX-XXXX-XXXX" required autocomplete="off" spellcheck="false">' +
      '<button type="submit" id="fsis-g-go">Sign in</button>' +
      '<div class="g-msg ' + (kind || '') + '" id="fsis-g-msg">' + esc(note || '') + '</div>' +
      '</form>' +
      '<div class="g-links"><a id="fsis-g-rec">Email me my token</a>' +
      '<a href="' + LINKS.preview + '" target="_top">Preview</a>' +
      '<a href="' + LINKS.subscribe + '" target="_top">Subscribe</a></div>'
    );
    var f = doc.getElementById('fsis-g-form');
    f.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var name = doc.getElementById('fsis-g-name').value.trim();
      var email = doc.getElementById('fsis-g-email').value.trim();
      var token = doc.getElementById('fsis-g-token').value.trim();
      var btn = doc.getElementById('fsis-g-go'), msg = doc.getElementById('fsis-g-msg');
      btn.disabled = true; msg.className = 'g-msg'; msg.textContent = 'Signing in…';
      call({ action: 'fsis_signin', name: name, email: email, token: token, device: deviceKey() }, function (r) {
        btn.disabled = false;
        if (r && r.ok) {
          save({ sid: r.sid, name: r.name || name, email: email, checked: Date.now() });
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
      var el = doc.getElementById(s.email ? 'fsis-g-token' : 'fsis-g-name');
      if (el) try { el.focus(); } catch (e) {}
    }, 50);
  }

  /* ── lock / unlock ───────────────────────────────────────────────────── */
  var recheck = null;
  function unlock() {
    root.classList.remove('fsis-locked');
    if (gate && gate.parentNode) gate.parentNode.removeChild(gate);
    gate = null;
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
      var keep = { name: s.name, email: s.email };
      clear(); save(keep);
      lock((r && r.message) || 'Please sign in.');
    });
  }

  /* Links to other subscriber pages carry the session, so a report opened
     in its own tab (where the browser may keep separate storage from the
     Wix frame) does not ask again. The receiving page strips it at once. */
  doc.addEventListener('click', function (ev) {
    var a = ev.target && ev.target.closest ? ev.target.closest('a[href]') : null;
    if (!a) return;
    var s = load();
    if (!s || !s.sid) return;
    var u;
    try { u = new URL(a.getAttribute('href'), doc.baseURI); } catch (e) { return; }
    if (u.origin !== location.origin || !/\.html?$/i.test(u.pathname)) return;
    u.hash = 'fsis=' + s.sid;
    a.href = u.toString();
  }, true);

  /* ── start ───────────────────────────────────────────────────────────── */
  if (handed) {
    var prev = load() || {};
    save({ sid: handed, name: prev.name || '', email: prev.email || '', checked: 0 });
  }
  if (load() && load().sid) {
    if (doc.body) showWait(); else doc.addEventListener('DOMContentLoaded', function () { if (root.classList.contains('fsis-locked') && !gate) showWait(); });
    check(true);
  } else {
    showSignin();
  }
})();
