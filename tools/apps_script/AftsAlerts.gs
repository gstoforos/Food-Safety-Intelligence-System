/**
 * AFTS FSIS Alerts — Apps Script companion to alerts.html
 * ===========================================================
 * Paste this into the Apps Script project bound to the
 * "AFTS FSIS Subscribers" spreadsheet (Extensions → Apps Script).
 *
 * ---------------------------------------------------------------------------
 * REVISION 2026-07-25 — STALE-REPLAY FIX
 * ---------------------------------------------------------------------------
 * SYMPTOM
 *   On 2026-07-25 two "AFTS Alert: 9 new matching recalls" emails went out
 *   containing recalls published 2026-03-30 .. 2026-04-30 — up to 103 days
 *   old, with return deadlines that had already expired. The two emails did
 *   not overlap; they were the same backlog draining 9 rows at a time.
 *
 * ROOT CAUSE
 *   The matcher treated "a URL I have not tracked yet" as "a new recall".
 *   It is not. The FSIS pipeline BACK-FILLS historical recalls: as of this
 *   revision, 246 of the 445 Listeria rows have DateAdded > Date — i.e. they
 *   entered the dataset AFTER their publication date, some by as much as
 *   345 days. Every one of those back-fills looked brand new to the matcher.
 *
 *   The operator had already spotted this and stamped DateAdded on the
 *   back-filled rows ("so back-filled historical rows are not treated as new
 *   by the alert matcher"). That fix could never work, because
 *   pipeline.merge_master.mirror_json_from_xlsx STRIPS DateAdded /
 *   LastUpdated / LastChecked out of recalls.json. The matcher physically
 *   cannot see DateAdded. recalls.json exposes only the 14 public columns.
 *
 *   SEED mode did not help either: it only protects a rule's FIRST scan.
 *   Anything back-filled after the seed sails straight through.
 *
 * THE FIX — three independent gates, each of which alone stops the replay
 *   1. SUBSCRIPTION FLOOR (what the operator asked for)
 *      Never email a recall published on or before the subscriber's
 *      subscription date. Stored per-rule in the new `alert_floor_date`
 *      column. Set on create AND on rule change — changing your rule must
 *      not dredge up history for the new criteria.
 *
 *   2. FRESHNESS CEILING (what actually kills the July-sends-April problem)
 *      Never email a recall older than MAX_RECALL_AGE_DAYS at send time.
 *      A subscriber who joined in January is legitimately "after" an April
 *      recall — so gate 1 alone would still have let these two emails out.
 *      A recall whose return deadline has passed is not actionable news.
 *      Rows that fail this gate are recorded as seen and never re-considered.
 *
 *   3. HARD SEND CAPS
 *      MAX_ROWS_PER_EMAIL and MAX_EMAILS_PER_SCAN. No future bug can
 *      produce another 88-email event; worst case is one truncated email
 *      per rule per scan, with the overflow logged to SendLog.
 *
 * OTHER BUGS FIXED IN THIS REVISION
 *   • TRACKER EVICTION (severe, latent, about to fire).
 *     Old code: `Array.from(sentUrls).slice(-500)`. recalls.json is sorted
 *     NEWEST-FIRST, and a JS Set iterates in insertion order, so slice(-500)
 *     kept the OLDEST 500 and silently DROPPED THE NEWEST. Past 500 matches
 *     the newest recalls would never be seeded, so they would email on the
 *     very next scan, then get evicted again — a permanent re-send loop.
 *     The Listeria rule is at 445 of 500 today. Replaced with date-based
 *     pruning: a URL is only forgotten once its recall is older than the
 *     freshness ceiling, at which point it can never match again anyway.
 *
 *   • 50,000-CHARACTER CELL LIMIT. sent_urls is one cell. 445 URLs at a
 *     66-char average is already 30,240 chars; setValue() THROWS past 50k,
 *     which would abort the write and re-send everything forever. Now
 *     bounded and guarded, with a SendLog warning well before the ceiling.
 *
 *   • SITE_BASE_URL still pointed at gstoforos.github.io. The site moved to
 *     https://fsis.advfood.tech (docs/CNAME). Every magic link in every
 *     outbound email was relying on a redirect.
 *
 *   • Email rendering: the LINK column wrapped to "vie / w" on mobile, and
 *     Product was cut mid-token with no ellipsis ("...counter o", "use-by
 *     20"). Fixed with nowrap and word-boundary truncation.
 *
 *   • Recall date is now shown as "PUBLISHED", and an explicit age note is
 *     added, so a subscriber can never mistake a replay for breaking news.
 *
 *   • Defensive de-duplication of matches within a single email.
 *
 * MIGRATION — nothing to do by hand
 *   ensureAlertsSchema_() runs at the top of every scan and self-heals:
 *   it adds `sent_urls` and `alert_floor_date` if missing, and back-fills
 *   alert_floor_date = created_at for pre-existing rules. Gate 2 protects
 *   those legacy rules from the historical backlog.
 *
 *
 * ---------------------------------------------------------------------------
 * REVISION 2026-09-14 — SHARED VOCABULARY
 * ---------------------------------------------------------------------------
 * GENERATED FROM tools/alert_vocab.py in the FSIS repo. Do not hand-edit the
 * vocabulary block or the matching engine here; edit alert_vocab.py and run
 *
 *     python3 tools/gen_alert_vocab.py --write
 *
 * which rewrites BOTH this file and the word list docs/alerts.html offers,
 * so the two can never drift again. The base of this file —
 * tools/apps_script/AftsAlerts.base.gs — is the 2026-07-25 script verbatim.
 *
 * SYMPTOM
 *   On 13 Sep a subscriber whose rule is "Clostridium botulinum" was emailed
 *   about Clostridium PERFRINGENS, and the two rows in that email read as a
 *   duplicate.
 *
 * ROOT CAUSE 1 — the rule was truncated to its first token
 *   recallMatchesCriterion_ did this:
 *
 *       const base = v.split(/[\s\/()]/)[0];    // "clostridium botulinum"
 *       return !!base && p.indexOf(base) >= 0;  //   -> "clostridium"
 *
 *   The comment above it said the intent was "loose containment lets rule
 *   'Listeria' match 'Listeria monocytogenes'". That intent is right, but
 *   truncating the RULE does the opposite of what it says: it widens a
 *   SPECIES rule into a GENUS rule. Every one of these fired wrongly:
 *
 *       rule "Clostridium botulinum"   matched  "Clostridium perfringens"
 *       rule "Salmonella Enteritidis"  matched  "Salmonella spp."
 *       rule "Listeria monocytogenes"  matched  "Listeria innocua"
 *
 *   A subscriber watching for botulism — the one hazard in this register
 *   that kills without a dose response — was told a foie gras had it.
 *
 * ROOT CAUSE 2 — the form and this script never shared a definition
 *   alerts.html carried the comment "the backend matches on exact string
 *   equality so the Apps Script reuses this list". It did not, and never
 *   had. Five of the 28 words the form offered could not match anything the
 *   pipeline is capable of emitting:
 *
 *       Undeclared allergen   allergen-only notices are blocked by
 *                             pipeline/_publish_gate.py and never reach
 *                             recalls.json
 *       Rotavirus             no PATHOGEN_RULES entry, no row, ever
 *       Toxoplasma            ditto
 *       Trichinella           ditto
 *       Cryptosporidium       ditto
 *       Pet food (product)    no scraper ingests companion-animal recalls
 *
 *   and four more were near-misses against the register's own spelling:
 *   "Ochratoxin A" (the register writes both forms), "Czech Republic"
 *   (Czechia), "South Korea" (Korea, South), "Türkiye" (Turkey).
 *
 * THE FIX
 *   Every word the form offers is a key of PATHOGEN_VOCAB / PRODUCT_VOCAB /
 *   COUNTRY_LIST below with an EXPLICIT token list. Nothing is split at
 *   runtime. A token matches on word boundaries, tolerating a trailing
 *   plural "s":
 *
 *       \b<token>s?\b
 *
 *   (leading \b only when the token starts with a word character, trailing
 *   s?\b only when it ends with one, so "lead (" and "(don)" still work).
 *   The boundary is load-bearing: without it an "E. coli" rule reaches
 *   "Coliform / total bacterial count" through the substring "coli", and a
 *   "nut" token reaches "minute".
 *
 *   Haystacks:
 *       pathogen -> Pathogen
 *       product  -> Product + " " + Reason   (Product is free-form and
 *                                             multilingual, and is often only
 *                                             a lot reference; Reason usually
 *                                             names the food)
 *       country  -> Country
 *       brand    -> Brand + " " + Company    free text, no vocabulary
 *
 *   35 pathogen words now reach all 1,693 published rows — no row is
 *   invisible to every alert. Yersinia, Shigella and Brucella sit at zero
 *   rows deliberately: they are PATHOGEN_RULES labels, so the scrapers
 *   collect them and normalize_pathogen() can emit them. That is different
 *   from Rotavirus, which nothing anywhere can produce.
 *
 *   PATHOGEN_LEGACY maps the renamed words forward, so an existing
 *   subscriber whose stored rule says "Ochratoxin A" keeps matching. No
 *   existing alert stops firing.
 *
 * ALSO — the duplicate-looking email
 *   The two rows were RappelConso fiches 23501 and 23502: the same lot
 *   (VA238F001) of foie gras from the same producer, published the same day,
 *   one sold at the factory shop and one through METRO. Two notices of ONE
 *   incident, and the register says so — both carry
 *   [incident:fr:truffe-perigourdine-2026-09-11] in Notes, one of the 14
 *   public columns. The weekly report already counts them once; this email
 *   did not know to look. sendAlertMatchEmail_ now counts incidents first,
 *   then notices, and tags the rows that share one. Both rows are still
 *   listed: each names a different product and use-by date.
 *
 * NEW ADMIN FUNCTIONS
 *   test_fsisAlertVocab()  — 30+ matcher cases plus a vocabulary integrity
 *                            check. Sends nothing, writes nothing.
 *   auditStoredRules()     — lists active rules whose stored term is no
 *                            longer understood, with the reason. Run once
 *                            after deploying.
 * ---------------------------------------------------------------------------
 * PIPELINE (unchanged)
 *   1. alerts.html POSTs the payload to this /exec URL (doPost) and to
 *      Formspree in parallel.
 *   2. doPost() validates email + name against the Subscribers tab and
 *      writes the rule to the Alerts tab.
 *   3. Scheduled matchAlertsAgainstRecalls() (every 4h) diffs recalls.json
 *      against every active rule and emails on genuinely new, in-window,
 *      post-subscription matches only.
 *   4. doGet(?action=unsubscribe_alert|get_alert&tok=…) magic links.
 *
 * ONE-TIME SETUP CHECKLIST
 *   [ ] Update SHEET_ID below if it ever changes.
 *   [ ] Deploy → New deployment → Web app → Execute as: me,
 *       Who has access: Anyone. Copy the /exec URL.
 *   [ ] Paste the Formspree endpoint + /exec URL into alerts.html.
 *   [ ] Run installEvery4hTrigger() once from the editor. Grant permissions.
 *   [ ] Run previewNextScan() to see exactly what WOULD be sent, with no
 *       email and no writes. Do this before the first live scan.
 *
 * ADMIN TOOLS (manual, rare)
 *   verifyTriggerWiring()        — self-check the trigger. Sends nothing.
 *   previewNextScan()            — DRY RUN. No email, no writes. Start here.
 *   runMatcherNow()              — skip the 4h schedule and run now
 *   sendCatchupEmailNow(email)   — one deliberate digest, tracker untouched
 *   resetRuleAndReseed(email)    — clear tracker; next scan re-seeds silently
 *   setAlertFloor(email, 'YYYY-MM-DD') — move a rule's subscription floor
 */

// =============================================================================
// CONFIG
// =============================================================================

/** Spreadsheet ID of "AFTS FSIS Subscribers" — copy from the sheet's URL. */
const SHEET_ID = '1j9kH3KcDTPMrGfn9VOEvdE_jSeGqoh-1iKiMPlOiIwI';

/**
 * Public site base URL — used to build magic links in outbound emails.
 * FIXED 2026-07-25: was gstoforos.github.io, which now only works via a
 * redirect. The canonical host is the custom domain in docs/CNAME.
 */
const SITE_BASE_URL = 'https://fsis.advfood.tech';

/** Public recalls.json — mirror of the Recalls sheet, 14 public columns. */
const RECALLS_JSON_URL = SITE_BASE_URL + '/data/recalls.json';

/** Tab names inside the spreadsheet. */
const TAB_SUBSCRIBERS = 'Subscribers';
const TAB_ALERTS      = 'Alerts';
const TAB_SENDLOG     = 'SendLog';

/** Sender identity on outbound email. */
const FROM_NAME = 'AFTS Food Safety Intelligence';

// --- Send gates -------------------------------------------------------------

/**
 * GATE 2 — freshness ceiling, in days.
 *
 * Never email a recall whose publication date is more than this many days
 * before the scan. This is the gate that actually prevents the 2026-07-25
 * incident: a subscriber who joined in January is legitimately "after" an
 * April recall, so the subscription floor alone would still have let those
 * two emails out.
 *
 * 30 days is deliberately generous. RappelConso return deadlines run about
 * 18 days from publication, so anything past 30 days is unactionable by
 * definition. Raise it only if you decide subscribers want historical
 * digests — and if you do, expect exactly the behaviour we just fixed.
 */
const MAX_RECALL_AGE_DAYS = 30;

/** GATE 3a — hard cap on rows in a single alert email. */
const MAX_ROWS_PER_EMAIL = 25;

/** GATE 3b — hard cap on alert emails sent in a single scan, across all rules. */
const MAX_EMAILS_PER_SCAN = 20;

/** Google Sheets hard limit on a single cell, with headroom. */
const CELL_CHAR_LIMIT = 50000;
const CELL_CHAR_WARN  = 40000;


// =============================================================================
// WEBAPP ENTRY POINTS
// =============================================================================

/**
 * doPost — receives the alerts.html submission. The form POSTs the payload
 * directly (Content-Type: text/plain with a JSON body, to bypass the CORS
 * preflight that Apps Script doesn't serve). Two wrapped shapes are also
 * accepted in case a Formspree webhook or similar intermediary is added:
 *   { email:..., name:..., ... }            ← direct from alerts.html
 *   { data: { email:..., name:..., ... } }  ← some webhook wrappers
 *   { submission: { email:..., ... } }      ← alt webhook wrapper
 * Form-url-encoded bodies (Formspree classic form posts) are also decoded.
 */
function doPost(e) {
  try {
    const payload = _parsePayload_(e);
    const result = processAlertSubmission_(payload);
    return _json(result);
  } catch (err) {
    Logger.log('doPost error: ' + err + ' / raw: ' +
               (e && e.postData ? e.postData.contents : '(no body)'));
    return _json({ok: false, error: String(err)});
  }
}

function _parsePayload_(e) {
  if (!e || !e.postData) return {};
  const ct = (e.postData.type || '').toLowerCase();
  const raw = e.postData.contents || '';

  // Form-url-encoded (Formspree classic form post, etc.)
  if (ct.indexOf('application/x-www-form-urlencoded') >= 0) {
    const out = {};
    raw.split('&').forEach(kv => {
      const [k, v] = kv.split('=');
      if (k) out[decodeURIComponent(k.replace(/\+/g, ' '))] =
             decodeURIComponent((v || '').replace(/\+/g, ' '));
    });
    return out;
  }

  // Default: treat as JSON (text/plain + JSON body also falls here).
  let p;
  try { p = JSON.parse(raw || '{}'); } catch (err) { p = {}; }
  if (p && p.data && typeof p.data === 'object' && (p.data.email || p.data.name)) {
    return p.data;
  }
  if (p && p.submission && typeof p.submission === 'object' &&
      (p.submission.email || p.submission.name)) {
    return p.submission;
  }
  return p || {};
}

/**
 * doGet — magic-link handlers for the email-footer buttons:
 *   ?action=get_alert&tok=XXX         → JSON prefill for manage UI
 *   ?action=unsubscribe_alert&tok=XXX → deactivate the alert rule
 */
function doGet(e) {
  const action = (e.parameter.action || '').trim();
  const tok    = (e.parameter.tok    || '').trim();

  if (action === 'get_alert' && tok) {
    const row = findAlertByToken_(tok);
    if (!row) return _json({ok: false, error: 'not_found'});
    // Only surface the fields alerts.html needs for prefill; never leak
    // tokens or status internals to the client.
    return _json({
      ok: true,
      email:     row.email,
      name:      row.name,
      company:   row.company,
      slot1_cat: row.slot1_cat,
      slot1_val: row.slot1_val,
      slot2_cat: row.slot2_cat,
      slot2_val: row.slot2_val,
      logic:     row.logic,
    });
  }

  if (action === 'unsubscribe_alert' && tok) {
    const ok = unsubscribeByToken_(tok);
    return _html(ok
      ? '<h2 style="font-family:sans-serif">✓ Unsubscribed</h2>'
        + '<p style="font-family:sans-serif">This alert rule has been deactivated. '
        + 'You will no longer receive emails for it.</p>'
      : '<h2 style="font-family:sans-serif">Not found</h2>'
        + '<p style="font-family:sans-serif">That link has already been used or is invalid.</p>'
    );
  }

  return _html('<h2>AFTS Alerts webapp</h2><p>Awaiting submission.</p>');
}


// =============================================================================
// CORE: process a new submission
// =============================================================================

function processAlertSubmission_(payload) {
  const alerts = ensureAlertsSchema_();

  // Normalise inputs
  const email     = String(payload.email     || '').trim();
  const name      = String(payload.name      || '').trim();
  const company   = String(payload.company   || '').trim();
  const slot1_cat = String(payload.slot1_cat || '').trim();
  const slot1_val = String(payload.slot1_val || '').trim();
  const slot2_cat = String(payload.slot2_cat || '').trim();
  const slot2_val = String(payload.slot2_val || '').trim();
  const logic     = String(payload.logic     || '').trim();
  const rule_summary = String(payload.rule_summary || '').trim();

  // Basic sanity
  if (!email || !name)          return {ok: false, error: 'missing_identity'};
  if (!slot1_val && !slot2_val) return {ok: false, error: 'no_criteria'};

  // VOCABULARY GATE (2026-09-14).
  //
  // alerts.html offers a fixed word list generated from the same definition
  // as PATHOGEN_VOCAB / PRODUCT_VOCAB / COUNTRY_LIST below, so a term the
  // form can produce is always a term the matcher understands. A term it
  // CANNOT understand would be stored as an active rule that can never fire:
  // the subscriber waits forever and nothing anywhere says why. That is how
  // "Undeclared allergen" and "Rotavirus" sat on the form for months.
  //
  // Brand is exempt — it is free text by design.
  const unknown = _firstUnknownTerm_([[slot1_cat, slot1_val],
                                      [slot2_cat, slot2_val]]);
  if (unknown) {
    Logger.log('REJECT ' + email + ' — ' + unknown.cat + ' "' + unknown.val +
               '" is not in the alert vocabulary. ' + (unknown.reason || ''));
    return {ok: false, error: 'unknown_term', category: unknown.cat,
            value: unknown.val, reason: unknown.reason ||
            'not offered by the alert form'};
  }

  // Look up on the Subscribers tab — this is the gate.
  const subscriber = findSubscriber_(email);

  let status, notes;
  let sendUserEmail = false;

  if (!subscriber) {
    status = 'needs_subscription';
    notes  = 'Email not found in Subscribers tab';
    sendOnboardingEmail_(email, name);
  } else if (String(subscriber.status || '').toLowerCase() !== 'active') {
    status = 'subscriber_inactive';
    notes  = 'Subscriber status = "' + subscriber.status + '"';
  } else if (!namesMatch_(subscriber.name, name)) {
    status = 'name_mismatch';
    notes  = 'Submitted name "' + name + '" does not match subscriber name "'
           + subscriber.name + '"';
    sendNameMismatchAlert_(subscriber.email, subscriber.name, name);
  } else {
    status = 'active';
    notes  = 'Linked to Subscribers row ' + subscriber.rowIndex;
    sendUserEmail = true;
  }

  // Re-submission behaviour: one rule per email. If prior rows exist for
  // this email, UPDATE the most recent one in place (preserving created_at
  // and unsubscribe_tok so magic links in past emails keep working) and mark
  // older duplicates 'superseded'. At most one active rule per subscriber.
  const existingRows = findAlertRowsByEmail_(email);
  const existing = existingRows.length ? existingRows[existingRows.length - 1] : null;
  const isUpdate = !!existing;
  const tok = isUpdate ? (existing.unsubscribe_tok || newToken_()) : newToken_();

  // Supersede older duplicates (anything before the most recent row).
  if (existingRows.length > 1) {
    const {sh, H} = _alertsHeaderMap_();
    for (let i = 0; i < existingRows.length - 1; i++) {
      const r = existingRows[i];
      if (String(r.status).toLowerCase() !== 'superseded') {
        sh.getRange(r.__rowIndex, H.status + 1).setValue('superseded');
        sh.getRange(r.__rowIndex, H.notes + 1).setValue(
          String(r.notes || '') + ' | superseded ' + new Date().toISOString()
        );
      }
    }
  }

  const now = new Date();

  // GATE 1 — the subscription floor.
  //
  // Set on create AND on rule change. On create this is the subscription
  // date, which is exactly what was asked for: only recalls published from
  // the day you subscribed onward. On UPDATE we reset it too, because a
  // changed rule is effectively a new rule — leaving the old floor in place
  // would let a subscriber who joined months ago switch criteria and
  // immediately pull the entire historical backlog for the new pathogen.
  // That is the same flooding failure in a different costume.
  //
  // Stored at DATE granularity (midnight of the subscription day), NOT as a
  // timestamp. A recall row carries a date, not a time, so it parses to
  // 00:00. If the floor kept the wall-clock submission time, a subscriber
  // who signed up at 09:00 would silently miss a recall published later the
  // SAME day — it would test as "before" the floor forever. Midnight
  // granularity closes that gap: the day you subscribe is included, the day
  // before is not.
  const alertFloor = _startOfDay_(now);

  const rowValues = [
    isUpdate && existing.created_at ? existing.created_at : now,  // created_at
    email, name, company,
    slot1_cat, slot1_val, slot2_cat, slot2_val, logic,
    rule_summary, status, notes, tok,
    now,   // last_matched_at — bookkeeping only; NOT a gate
    ''     // last_matched_recall — cleared on every update
  ];

  if (isUpdate) {
    const {sh, H} = _alertsHeaderMap_();
    sh.getRange(existing.__rowIndex, 1, 1, rowValues.length).setValues([rowValues]);
    // Columns beyond the first 15 are written individually so a schema
    // change can never shift them.
    sh.getRange(existing.__rowIndex, H.alert_floor_date + 1).setValue(alertFloor);
    // Rule changed → the old URL tracker describes a different question.
    // Clear it so the next scan re-seeds silently against the new criteria.
    sh.getRange(existing.__rowIndex, H.sent_urls + 1).setValue('');
  } else {
    alerts.appendRow(rowValues);
    const {sh, H} = _alertsHeaderMap_();
    const r = sh.getLastRow();
    sh.getRange(r, H.alert_floor_date + 1).setValue(alertFloor);
  }

  if (sendUserEmail) {
    if (isUpdate) sendRuleUpdatedEmail_(email, name, payload, tok, alertFloor);
    else          sendConfirmationEmail_(email, name, payload, tok, alertFloor);
  }

  return {ok: true, status: status, notes: notes, is_update: isUpdate,
          alert_floor_date: _ymd(alertFloor)};
}

/**
 * Return all Alerts-tab rows whose email matches (case-insensitive),
 * in row order (oldest first).
 */
function findAlertRowsByEmail_(email) {
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  const needle = String(email || '').trim().toLowerCase();
  const out = [];
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.email]).trim().toLowerCase() === needle) {
      const row = {};
      Object.keys(H).forEach(k => { row[k] = data[i][H[k]]; });
      row.__rowIndex = i + 1;
      out.push(row);
    }
  }
  return out;
}


// =============================================================================
// SUBSCRIBERS VALIDATION
// =============================================================================

function findSubscriber_(email) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const sh = ss.getSheetByName(TAB_SUBSCRIBERS);
  if (!sh) return null;
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return null;

  const headers = data[0].map(h => String(h).toLowerCase().trim());
  const iEmail   = headers.indexOf('email');
  const iName    = headers.indexOf('name');
  const iCompany = headers.indexOf('company');
  const iStatus  = headers.indexOf('status');
  if (iEmail < 0) return null;

  const needle = String(email).trim().toLowerCase();
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][iEmail]).trim().toLowerCase() === needle) {
      return {
        rowIndex: i + 1,
        email:   data[i][iEmail],
        name:    iName    >= 0 ? data[i][iName]    : '',
        company: iCompany >= 0 ? data[i][iCompany] : '',
        status:  iStatus  >= 0 ? data[i][iStatus]  : 'active',
      };
    }
  }
  return null;
}

/**
 * Soft-equals two names. Lowercased, trimmed, punctuation stripped,
 * diacritics folded so "Nikolaos G. Stoforos" matches "nikolaos g stoforos".
 */
function namesMatch_(a, b) { return _normName(a) === _normName(b); }

function _normName(s) {
  if (!s) return '';
  return String(s)
    .toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')  // strip accents
    .replace(/[.,'"()]/g, '')                          // strip punctuation
    .replace(/\s+/g, ' ')
    .trim();
}


// =============================================================================
// ALERTS TAB SCHEMA  (self-healing)
// =============================================================================

/** Canonical column order for the Alerts tab. */
const ALERTS_HEADERS = [
  'created_at', 'email', 'name', 'company',
  'slot1_cat', 'slot1_val', 'slot2_cat', 'slot2_val', 'logic',
  'rule_summary', 'status', 'notes', 'unsubscribe_tok',
  'last_matched_at', 'last_matched_recall', 'sent_urls', 'alert_floor_date'
];

/**
 * Create the Alerts tab if absent, add any missing columns, and back-fill
 * alert_floor_date for rules that predate this revision.
 *
 * Idempotent. Called at the top of every scan and every submission, so the
 * migration needs no manual step and cannot be half-applied.
 */
function ensureAlertsSchema_() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  let sh = ss.getSheetByName(TAB_ALERTS);

  if (!sh) {
    sh = ss.insertSheet(TAB_ALERTS);
    sh.getRange(1, 1, 1, ALERTS_HEADERS.length).setValues([ALERTS_HEADERS])
      .setFontWeight('bold').setBackground('#0a0e1a').setFontColor('#fbbf24');
    sh.setFrozenRows(1);
    sh.autoResizeColumns(1, ALERTS_HEADERS.length);
    return sh;
  }

  // Append any missing columns, preserving whatever order already exists.
  let headers = sh.getRange(1, 1, 1, Math.max(sh.getLastColumn(), 1)).getValues()[0]
                  .map(h => String(h).trim());
  ALERTS_HEADERS.forEach(want => {
    if (headers.indexOf(want) < 0) {
      const col = sh.getLastColumn() + 1;
      sh.getRange(1, col).setValue(want)
        .setFontWeight('bold').setBackground('#0a0e1a').setFontColor('#fbbf24');
      headers.push(want);
      Logger.log('Schema: added missing column "' + want + '" at ' + col);
    }
  });

  // Back-fill alert_floor_date for pre-existing rules.
  //
  // Legacy rules get floor = created_at, which is the honest reading of
  // "only recalls published after you subscribed". They are NOT protected
  // from the historical backlog by that alone — a January subscriber is
  // legitimately "after" an April recall — but GATE 2 (freshness ceiling)
  // catches exactly that case, which is why both gates exist.
  const H = {};
  sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0]
    .forEach((h, j) => { H[String(h).trim()] = j; });

  const last = sh.getLastRow();
  if (last > 1 && H.alert_floor_date !== undefined) {
    const rng  = sh.getRange(2, H.alert_floor_date + 1, last - 1, 1);
    const vals = rng.getValues();
    const created = sh.getRange(2, H.created_at + 1, last - 1, 1).getValues();
    let filled = 0;
    for (let i = 0; i < vals.length; i++) {
      if (vals[i][0] === '' || vals[i][0] === null) {
        vals[i][0] = created[i][0] || new Date();
        filled++;
      }
    }
    if (filled) {
      rng.setValues(vals);
      Logger.log('Schema: back-filled alert_floor_date on ' + filled + ' legacy rule(s).');
    }
  }
  return sh;
}

function _alertsHeaderMap_() {
  const sh = ensureAlertsSchema_();
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const H = {};
  headers.forEach((h, j) => { H[String(h).trim()] = j; });
  return {sh, H, headers};
}

function findAlertByToken_(tok) {
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.unsubscribe_tok]).trim() === String(tok).trim()) {
      const row = {};
      Object.keys(H).forEach(k => { row[k] = data[i][H[k]]; });
      row.__rowIndex = i + 1;
      return row;
    }
  }
  return null;
}

function unsubscribeByToken_(tok) {
  const row = findAlertByToken_(tok);
  if (!row) return false;
  const {sh, H} = _alertsHeaderMap_();
  sh.getRange(row.__rowIndex, H.status + 1).setValue('unsubscribed');
  sh.getRange(row.__rowIndex, H.notes + 1).setValue(
    String(row.notes || '') + ' | unsubscribed ' + new Date().toISOString()
  );
  return true;
}


// =============================================================================
// ALERT VOCABULARY  (generated — see the 2026-09-14 revision note above)
// =============================================================================

// ===== GENERATED BY tools/gen_alert_vocab.py — DO NOT EDIT BY HAND =====

var FSIS_VOCAB_VERSION = "2026-09-14";

var PATHOGEN_VOCAB = {
  "Listeria": ["listeria"],
  "Salmonella": ["salmonella", "salmonelle"],
  "E. coli / STEC": ["e. coli", "e.coli", "escherichia coli", "stec", "vtec", "ehec", "shiga toxin", "o157", "o104", "o121", "o26", "o45", "o103", "o111", "o145", "o168"],
  "Clostridium botulinum": ["botulinum", "botulism", "botulisme"],
  "Clostridium perfringens": ["perfringens"],
  "Bacillus cereus / cereulide": ["bacillus cereus", "b. cereus", "cereulide", "emetic toxin"],
  "Campylobacter": ["campylobacter"],
  "Vibrio": ["vibrio", "vulnificus", "parahaemolyticus", "cholerae", "cholera", "alginolyticus"],
  "Cronobacter": ["cronobacter", "sakazakii"],
  "Staphylococcus aureus / enterotoxin": ["staphylococcus", "staph. aureus", "s. aureus", "staphylococcal", "entérotoxine"],
  "Yersinia": ["yersinia", "enterocolitica"],
  "Shigella": ["shigella"],
  "Brucella": ["brucella", "brucellosis"],
  "Norovirus": ["norovirus", "norwalk"],
  "Hepatitis A": ["hepatitis a"],
  "Cyclospora": ["cyclospora"],
  "Aflatoxin": ["aflatoxin"],
  "Ochratoxin": ["ochratoxin"],
  "Patulin": ["patulin"],
  "T-2 / HT-2 toxin": ["t-2", "ht-2", "t2 toxin"],
  "Deoxynivalenol (DON)": ["deoxynivalenol", "vomitoxin", "(don)"],
  "Zearalenone": ["zearalenone"],
  "Alternaria toxins": ["alternaria", "tenuazonic"],
  "Mycotoxins (other)": ["mycotoxin", "fumonisin", "citrinin", "ergot"],
  "Histamine / scombrotoxin": ["histamine", "scombrotox"],
  "Marine / shellfish biotoxins": ["marine biotoxin", "biotoxin", "shellfish toxin", "paralytic shellfish", "lipophilic", "okadaic", "domoic", "saxitoxin", "tetrodotoxin", "ciguatoxin", "ciguatera", "phytoplankton"],
  "Mushroom / plant toxins": ["mushroom toxin", "amanita", "muscimol", "muscaria", "hydrocyanic", "cyanogenic", "solanine", "tropane"],
  "Heavy metals": ["heavy metal", "cadmium", "lead (", "mercury", "arsenic", "molybdenum"],
  "Pesticide / veterinary residues": ["pesticide", "veterinary medicine", "veterinary chemical", "penicillin", "nitrofurazone", "chloramphenicol", "rodenticide", "residue"],
  "Industrial chemical contaminant": ["pfoa", "pfas", "dioxin", "ethylene oxide", "mineral oil", "moah", "mosh", "acrylamide", "melamine", "chemical hazard"],
  "Undeclared pharmacological ingredient": ["pharmacological", "yohimbine", "sildenafil", "tadalafil"],
  "Foreign material / physical hazard": ["foreign material", "foreign body", "physical/foreign", "physical hazard", "glass", "metal fragment", "hard plastic", "shell fragment", "stones", "sand)"],
  "Mold / spoilage": ["mold", "spoilage", "organoleptic", "coliform", "total bacterial count"],
  "Rodent / pest contamination": ["rodent", "mouse contamination", "rat poison", "insect", "pest infestation"],
  "Process deviation (sterilisation / pasteurisation)": ["sterilization", "sterilisation", "pasteurization", "pasteurisation", "process deviation"],
};

var PRODUCT_VOCAB = {
  "Dairy — raw milk": ["raw milk", "lait cru", "leche cruda", "latte crudo", "rohmilch", "mleko surowe", "unpasteurised", "unpasteurized"],
  "Dairy — cheese": ["cheese", "fromage", "queso", "formaggio", "käse", "kaese", "brie", "camembert", "mozzarella", "gorgonzola", "roquefort", "feta", "halloumi", "raclette", "reblochon", "tomme", "morbier", "comté"],
  "Dairy — other": ["milk", "lait", "leche", "latte", "milch", "yoghurt", "yogurt", "butter", "beurre", "cream", "crème", "ice cream", "glace"],
  "Meat — poultry": ["poultry", "chicken", "turkey meat", "duck", "volaille", "poulet", "drobiowym", "drób", "pollo", "geflügel"],
  "Meat — beef": ["beef", "bœuf", "boeuf", "veal", "steak", "manzo", "rind"],
  "Meat — pork": ["pork", "porc", "ham", "jambon", "bacon", "lardon", "salami", "sausage", "saucisse", "wurst", "prosciutto", "guanciale", "pancetta", "chorizo"],
  "Meat — cooked / ready-to-eat": ["ready-to-eat", "cooked meat", "charcuterie", "deli meat", "pâté", "rillettes", "foie gras"],
  "Seafood — fin-fish": ["fish", "poisson", "salmon", "saumon", "tuna", "thon", "mackerel", "sardine", "anchovy", "trout", "truite"],
  "Seafood — shellfish / molluscs": ["oyster", "huître", "huitre", "mussel", "moule", "clam", "scallop", "shellfish", "coquillage", "crab", "shrimp", "prawn", "crevette", "lobster"],
  "Seafood — smoked / cured": ["smoked fish", "saumon fumé", "smoked salmon", "gravlax", "hot-smoked", "cold-smoked", "fumé"],
  "Eggs & egg products": ["egg", "œuf", "oeuf", "huevo", "uova", "jaja"],
  "Fresh produce — leafy greens": ["lettuce", "spinach", "salad", "salade", "rocket", "arugula", "kale", "leafy"],
  "Fresh produce — sprouts": ["sprout", "germe", "alfalfa"],
  "Fresh produce — berries": ["berry", "berries", "strawberry", "raspberry", "blueberry", "fraise", "framboise"],
  "Fresh produce — melons": ["melon", "watermelon", "cantaloupe", "pastèque"],
  "Fresh produce — herbs": ["parsley", "persil", "coriander", "cilantro", "basil", "basilic", "mint", "herb"],
  "Fresh produce — other": ["vegetable", "légume", "legume", "tomato", "cucumber", "carrot", "onion", "pepper", "fruit", "apple", "pomme"],
  "Dried fruit / nuts": ["dried fig", "fig", "raisin", "dried date", "apricot", "prune", "nut", "walnut", "almond", "pistachio", "peanut", "cashew", "hazelnut", "noix", "amande", "arachide"],
  "Seeds & grains": ["seed", "grain", "cereal", "wheat", "maize", "corn", "rice", "oat", "barley", "flour", "farine", "sesame", "quinoa"],
  "Spices & seasonings": ["spice", "épice", "epice", "paprika", "cumin", "turmeric", "curry", "seasoning", "cinnamon", "nutmeg"],
  "Bakery & confectionery": ["bread", "pain", "pastry", "cake", "gâteau", "biscuit", "cookie", "chocolate", "chocolat", "candy", "sweets", "bakery", "boulangerie"],
  "Infant formula & baby food": ["infant formula", "baby food", "follow-on formula", "lait infantile", "babynahrung", "infant cereal"],
  "Beverages": ["juice", "soft drink", "bottled water", "eau minérale", "beer", "bière", "wine", "tea", "thé", "coffee", "café"],
  "Canned / jarred / vacuum-packed": ["canned", "conserve", "jarred", "vacuum-packed", "sous-vide", "sous vide", "low-acid", "bocal"],
  "Ready meals / sandwiches / salads": ["ready meal", "ready-to-eat meal", "sandwich", "wrap", "prepared salad", "plat préparé", "soup", "soupe", "broth"],
  "Sauces, condiments & dressings": ["sauce", "condiment", "dressing", "mayonnaise", "pesto", "hummus", "houmous", "tahini"],
  "Dietary supplements": ["supplement", "complément", "vitamin", "capsule", "food supplement"],
  "Ingredients / raw materials": ["ingredient", "raw material", "additive", "matière première"],
};

var COUNTRY_LIST = [
  "Afghanistan", "Argentina", "Australia", "Austria", "Azerbaijan",
  "Belgium", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil",
  "Bulgaria", "Canada", "Chile", "China", "Colombia", "Croatia", "Cyprus",
  "Czechia", "Denmark", "Ecuador", "Egypt", "Estonia", "Ethiopia",
  "Finland", "France", "Georgia", "Germany", "Greece", "Hong Kong",
  "Hungary", "India", "Indonesia", "Iran", "Ireland", "Italy", "Japan",
  "Kenya", "Korea, South", "Kosovo", "Latvia", "Lithuania", "Madagascar",
  "Malawi", "Mexico", "Morocco", "Nepal", "Netherlands", "New Zealand",
  "Nicaragua", "Nigeria", "Norway", "Pakistan", "Panama", "Peru",
  "Philippines", "Poland", "Portugal", "Romania", "Rwanda", "Serbia",
  "Singapore", "Slovakia", "Slovenia", "South Africa", "Spain",
  "Sri Lanka", "Sweden", "Switzerland", "Syria", "Taiwan", "Thailand",
  "Turkey", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom",
  "United States", "Uruguay", "Uzbekistan", "Vietnam",
  "EU-wide / multi-country"
];

var COUNTRY_ALIASES = {
  "Czechia": ["czechia", "czech republic"],
  "Korea, South": ["korea, south", "south korea", "republic of korea"],
  "Turkey": ["turkey", "türkiye", "turkiye"],
  "United Kingdom": ["united kingdom", "great britain", "northern ireland"],
  "United States": ["united states", "u.s.a"],
  "EU-wide / multi-country": ["european union", "multiple eu", "eu/eea", "comesa", "eu-wide"],
};

var PATHOGEN_LEGACY = {
  "ochratoxin a": "Ochratoxin",
  "marine biotoxins": "Marine / shellfish biotoxins",
  "staphylococcus aureus": "Staphylococcus aureus / enterotoxin",
  "bacillus cereus / cereulide": "Bacillus cereus / cereulide",
  "chemical contaminant": "Industrial chemical contaminant",
  "physical hazard": "Foreign material / physical hazard",
};

var RETIRED_PATHOGEN = {
  "Rotavirus": "not in PATHOGEN_RULES and never collected — no row can match",
  "Toxoplasma": "not in PATHOGEN_RULES and never collected — no row can match",
  "Trichinella": "not in PATHOGEN_RULES and never collected — no row can match",
  "Cryptosporidium": "not in PATHOGEN_RULES and never collected — no row can match",
  "Undeclared allergen": "allergen-only notices are blocked by pipeline/_publish_gate.py (scope blocks when hazard classes are a subset of {allergen, fermentation}), so they never reach recalls.json",
  "Chemical contaminant": "split into Heavy metals / Pesticide / Industrial",
  "Physical hazard": "renamed Foreign material / physical hazard",
  "Ochratoxin A": "register writes both 'Ochratoxin' and 'Ochratoxin A'; the term is now 'Ochratoxin', which matches both",
  "Marine biotoxins": "renamed Marine / shellfish biotoxins",
  "Staphylococcus aureus": "renamed Staphylococcus aureus / enterotoxin — the register mostly writes the toxin, not the organism",
};

var RETIRED_PRODUCT = {
  "Pet food (human-adjacent recalls)": "no scraper ingests pet-food notices",
};

// ===== END GENERATED =====


// =============================================================================
// MATCHER
// =============================================================================

/**
 * TRIGGER ENTRY POINT — this is the name installEvery4hTrigger() registers,
 * and the only name the time-driven trigger ever calls.
 *
 * `e` is the trigger event object. It is accepted and deliberately IGNORED.
 *
 * HOTFIX 2026-07-29 — DO NOT reintroduce a parameter here.
 * This function used to be the matcher itself, declared as
 *
 *     function matchAlertsAgainstRecalls(dryRun) {
 *       dryRun = !!dryRun;
 *
 * A Google Apps Script time-driven trigger does not call its handler with no
 * arguments. It calls it with an event object:
 *
 *     { authMode: ..., triggerUid: "8123456789", hour: 12, minute: 0, ... }
 *
 * Every object is truthy in JavaScript, so `!!eventObject === true`. Every
 * scheduled run therefore set dryRun = true: it fetched recalls.json, scanned
 * every rule, computed every match correctly — and then returned at the
 * dry-run branch. No email. No tracker write. No SendLog row.
 *
 * Manual runs were unaffected because runMatcherNow() passes an explicit
 * `false`, which is why the matcher tested clean by hand and failed only on
 * schedule — the worst possible failure shape.
 *
 * The rule this encodes: the trigger's argument must never reach a behaviour
 * flag. A scheduled run is always a LIVE run.
 */
function matchAlertsAgainstRecalls(e) {
  return _runMatcher_(false);
}


/**
 * The matcher itself. Fetches recalls.json, scans every active rule, and
 * emails on matches that pass all gates.
 *
 * PRIVATE — reachable only from matchAlertsAgainstRecalls() (live),
 * previewNextScan() (dry) and runMatcherNow() (live). Never wire a trigger
 * directly to this function: the trigger would pass its event object straight
 * into dryRun and silence the mailer again.
 *
 * @param {boolean} dryRun  true = compute everything, send and write nothing.
 */
function _runMatcher_(dryRun) {
  dryRun = dryRun === true;   // strict: only a real boolean true means dry run
  ensureAlertsSchema_();

  const scanStart = new Date();
  const recalls = fetchRecalls_();
  if (!recalls || !recalls.length) {
    logScan_({rulesScanned: 0, rulesSeeded: 0, rulesWithNew: 0, emailsSent: 0,
              totalNewRecalls: 0, suppressedStale: 0, suppressedPreFloor: 0,
              note: 'fetchRecalls_ returned empty (network error, quota, or 404) — ' +
                    RECALLS_JSON_URL});
    return;
  }

  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();

  // GATE 2 boundary — recalls published before this are never emailed.
  // Normalised to local midnight so the boundary is a clean calendar day and
  // so DST transitions in Athens cannot shift it by an hour.
  const staleCutoff = _startOfDay_(
    new Date(scanStart.getTime() - MAX_RECALL_AGE_DAYS * 86400000));

  let rulesScanned = 0, rulesSeeded = 0, rulesWithNew = 0;
  let emailsSent = 0, totalNewRecalls = 0;
  let suppressedStale = 0, suppressedPreFloor = 0, truncated = 0;
  const preview = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (String(row[H.status]).toLowerCase() !== 'active') continue;
    rulesScanned++;

    const rule = {
      email:     row[H.email],
      name:      row[H.name],
      slot1_cat: row[H.slot1_cat],
      slot1_val: row[H.slot1_val],
      slot2_cat: row[H.slot2_cat],
      slot2_val: row[H.slot2_val],
      logic:     row[H.logic],
      tok:       row[H.unsubscribe_tok],
    };

    // GATE 1 boundary, at date granularity. A missing floor is treated as
    // today — fail closed, never fail open into a backlog blast.
    const floor = _startOfDay_(_parseDate(row[H.alert_floor_date]) || scanStart);

    const sentUrlsRaw = String(row[H.sent_urls] || '').trim();
    const isSeeded = sentUrlsRaw.length > 0;   // '__SEEDED__' counts as seeded
    const sentUrls = new Set(
      sentUrlsRaw.split('\n').filter(s => s && s !== '__SEEDED__')
    );

    // Every recall currently matching this rule, de-duplicated by URL.
    const seenUrl = new Set();
    const currentMatches = recalls.filter(r => {
      if (!r.URL || seenUrl.has(r.URL)) return false;
      if (!recallMatchesRule_(r, rule)) return false;
      seenUrl.add(r.URL);
      return true;
    });

    // -----------------------------------------------------------------
    // SEED MODE — first scan of a rule. Record everything as delivered,
    // email nothing. Protects against a brand-new rule dumping history.
    // -----------------------------------------------------------------
    if (!isSeeded) {
      rulesSeeded++;
      currentMatches.forEach(r => sentUrls.add(r.URL));
      const seedList = _boundTracker_(sentUrls, staleCutoff, recalls);
      if (!dryRun) {
        sh.getRange(i + 1, H.sent_urls + 1)
          .setValue(seedList.length ? seedList.join('\n') : '__SEEDED__');
        sh.getRange(i + 1, H.last_matched_at + 1).setValue(scanStart);
        sh.getRange(i + 1, H.last_matched_recall + 1).setValue(
          'SEEDED on ' + _ymd(scanStart) + ' — tracking ' + seedList.length +
          ' existing matches (no email sent)');
      }
      Logger.log('SEED ' + rule.email + ' — tracked ' + seedList.length +
                 ' current matches, no email.');
      continue;
    }

    // -----------------------------------------------------------------
    // NORMAL MODE — apply the gates.
    // -----------------------------------------------------------------
    const untracked = currentMatches.filter(r => !sentUrls.has(r.URL));

    const sendable = [];
    const suppressed = [];
    untracked.forEach(r => {
      const d = _parseDate(r.Date);
      if (!d)                { suppressed.push([r, 'unparseable_date']); return; }
      if (d < floor)         { suppressedPreFloor++; suppressed.push([r, 'pre_subscription']); return; }
      if (d < staleCutoff)   { suppressedStale++;    suppressed.push([r, 'older_than_' + MAX_RECALL_AGE_DAYS + 'd']); return; }
      sendable.push(r);
    });

    // Suppressed rows are marked as seen so they can never resurface later.
    // This is the difference between suppressing a replay once and
    // suppressing it forever.
    suppressed.forEach(([r]) => sentUrls.add(r.URL));

    if (!sendable.length) {
      if (suppressed.length && !dryRun) {
        _writeTracker_(sh, i + 1, H, sentUrls, staleCutoff, recalls);
        sh.getRange(i + 1, H.last_matched_at + 1).setValue(scanStart);
        sh.getRange(i + 1, H.last_matched_recall + 1).setValue(
          'SUPPRESSED ' + suppressed.length + ' stale/pre-subscription match(es) on ' +
          _ymd(scanStart) + ' — ' +
          suppressed.slice(0, 5).map(([r, why]) => r.Date + ' ' + why).join(' ; ')
        );
      }
      Logger.log('OK   ' + rule.email + ' — no sendable matches (' +
                 suppressed.length + ' suppressed)');
      if (dryRun && suppressed.length) {
        preview.push({email: rule.email, would_send: 0, suppressed: suppressed.length,
                      sample: suppressed.slice(0, 3).map(([r, why]) => r.Date + ' ' + why)});
      }
      continue;
    }

    // Newest first, then cap.
    sendable.sort((a, b) => String(b.Date).localeCompare(String(a.Date)));
    const overflow = Math.max(0, sendable.length - MAX_ROWS_PER_EMAIL);
    const toSend = sendable.slice(0, MAX_ROWS_PER_EMAIL);
    if (overflow) truncated += overflow;

    rulesWithNew++;
    totalNewRecalls += toSend.length;

    if (dryRun) {
      preview.push({
        email: rule.email,
        would_send: toSend.length,
        overflow: overflow,
        suppressed: suppressed.length,
        floor: _ymd(floor),
        rows: toSend.slice(0, 10).map(r => r.Date + ' | ' + (r.Company || '') +
                                           ' | ' + (r.Pathogen || ''))
      });
      continue;
    }

    if (emailsSent >= MAX_EMAILS_PER_SCAN) {
      Logger.log('CAP  ' + rule.email + ' — MAX_EMAILS_PER_SCAN reached, deferring.');
      continue;   // tracker untouched → retried next scan
    }

    sendAlertMatchEmail_(rule, toSend, overflow, scanStart);
    emailsSent++;

    toSend.forEach(r => sentUrls.add(r.URL));
    _writeTracker_(sh, i + 1, H, sentUrls, staleCutoff, recalls);
    sh.getRange(i + 1, H.last_matched_at + 1).setValue(scanStart);
    sh.getRange(i + 1, H.last_matched_recall + 1).setValue(
      toSend.slice(0, 5).map(r => r.Date + '|' + r.Company + '|' + r.Pathogen)
            .join(' ; ').substring(0, 500)
    );
    Logger.log('SEND ' + rule.email + ' — ' + toSend.length + ' new recalls' +
               (overflow ? (' (+' + overflow + ' held back)') : '') +
               ', ' + suppressed.length + ' suppressed');
  }

  if (dryRun) {
    Logger.log('=== DRY RUN — nothing sent, nothing written ===');
    Logger.log('rules scanned: ' + rulesScanned + ', would seed: ' + rulesSeeded +
               ', would email: ' + rulesWithNew);
    Logger.log('suppressed pre-subscription: ' + suppressedPreFloor +
               ', suppressed stale (>' + MAX_RECALL_AGE_DAYS + 'd): ' + suppressedStale);
    preview.forEach(p => Logger.log(JSON.stringify(p)));
    return preview;
  }

  logScan_({
    rulesScanned, rulesSeeded, rulesWithNew, emailsSent, totalNewRecalls,
    suppressedStale, suppressedPreFloor,
    note: [
      rulesSeeded ? ('seeded ' + rulesSeeded + ' rule(s), no email') : '',
      suppressedStale ? ('suppressed ' + suppressedStale + ' recall(s) older than ' +
                         MAX_RECALL_AGE_DAYS + 'd') : '',
      suppressedPreFloor ? ('suppressed ' + suppressedPreFloor +
                            ' pre-subscription recall(s)') : '',
      truncated ? ('held back ' + truncated + ' row(s) over MAX_ROWS_PER_EMAIL') : '',
      (!emailsSent && !rulesSeeded) ? 'no new matches — heartbeat OK' : ''
    ].filter(Boolean).join('; ')
  });
  Logger.log('Scan complete: rules=' + rulesScanned + ', seeded=' + rulesSeeded +
             ', emails=' + emailsSent + ', newRecalls=' + totalNewRecalls +
             ', suppressedStale=' + suppressedStale +
             ', suppressedPreFloor=' + suppressedPreFloor);
}

/**
 * Bound the tracker set for storage.
 *
 * REPLACES the old `Array.from(set).slice(-500)`, which was actively
 * harmful: recalls.json is sorted newest-first and a Set iterates in
 * insertion order, so slice(-500) retained the OLDEST 500 and discarded
 * the NEWEST. Past 500 matches the newest recalls would never be recorded
 * and would re-email on every single scan. The Listeria rule sits at 445
 * of 500 today.
 *
 * The correct bound is by DATE, not by count: once a recall is older than
 * the freshness ceiling, GATE 2 can never let it send again, so forgetting
 * it is provably safe. URLs not present in the current feed are kept —
 * absence means the row left the dataset, and we must not forget it.
 */
function _boundTracker_(sentUrls, staleCutoff, recalls) {
  const dateByUrl = {};
  recalls.forEach(r => { if (r.URL) dateByUrl[r.URL] = r.Date; });

  const keep = [];
  sentUrls.forEach(u => {
    const d = dateByUrl[u];
    if (d === undefined) { keep.push(u); return; }   // not in feed → keep
    const dd = _parseDate(d);
    if (!dd || dd >= staleCutoff) keep.push(u);      // still eligible → keep
    // else: provably unsendable forever → safe to forget
  });

  // Absolute backstop against the 50,000-char cell limit. If we ever get
  // here the retained set is enormous; drop the OLDEST first (correct
  // direction — the old code dropped the newest).
  let joined = keep.join('\n');
  if (joined.length > CELL_CHAR_LIMIT - 1000) {
    keep.sort((a, b) => String(dateByUrl[b] || '').localeCompare(String(dateByUrl[a] || '')));
    while (keep.length && keep.join('\n').length > CELL_CHAR_LIMIT - 1000) keep.pop();
    Logger.log('WARN tracker hit cell limit — trimmed to ' + keep.length + ' URLs (oldest dropped)');
  }
  return keep;
}

function _writeTracker_(sh, rowIndex, H, sentUrls, staleCutoff, recalls) {
  const list = _boundTracker_(sentUrls, staleCutoff, recalls);
  const payload = list.length ? list.join('\n') : '__SEEDED__';
  if (payload.length > CELL_CHAR_WARN) {
    Logger.log('WARN sent_urls cell at ' + payload.length + ' chars (limit ' +
               CELL_CHAR_LIMIT + ') on row ' + rowIndex);
  }
  sh.getRange(rowIndex, H.sent_urls + 1).setValue(payload);
}

/**
 * Append one row to SendLog describing this scan. Creates the tab on first
 * call. Keeps the most recent 500 rows.
 */
function logScan_(summary) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  let sh = ss.getSheetByName(TAB_SENDLOG);
  const headers = ['timestamp', 'rules_scanned', 'rules_seeded',
                   'rules_with_new_matches', 'emails_sent', 'total_new_recalls',
                   'suppressed_stale', 'suppressed_pre_subscription', 'note'];
  if (!sh) {
    sh = ss.insertSheet(TAB_SENDLOG);
    sh.getRange(1, 1, 1, headers.length).setValues([headers])
      .setFontWeight('bold').setBackground('#0a0e1a').setFontColor('#fbbf24');
    sh.setFrozenRows(1);
  } else if (sh.getLastColumn() < headers.length) {
    // Widen the legacy 7-column SendLog to carry the new counters.
    sh.getRange(1, 1, 1, headers.length).setValues([headers])
      .setFontWeight('bold').setBackground('#0a0e1a').setFontColor('#fbbf24');
  }
  sh.appendRow([
    new Date(),
    summary.rulesScanned       | 0,
    summary.rulesSeeded        | 0,
    summary.rulesWithNew       | 0,
    summary.emailsSent         | 0,
    summary.totalNewRecalls    | 0,
    summary.suppressedStale    | 0,
    summary.suppressedPreFloor | 0,
    summary.note || ''
  ]);
  const last = sh.getLastRow();
  if (last > 501) sh.deleteRows(2, last - 501);
}

function fetchRecalls_() {
  try {
    const r = UrlFetchApp.fetch(RECALLS_JSON_URL, {muteHttpExceptions: true,
                                                   followRedirects: true});
    if (r.getResponseCode() !== 200) {
      Logger.log('fetchRecalls_ HTTP ' + r.getResponseCode() + ' from ' + RECALLS_JSON_URL);
      return [];
    }
    const j = JSON.parse(r.getContentText());
    return Array.isArray(j) ? j : [];
  } catch (e) {
    Logger.log('fetchRecalls_ error: ' + e);
    return [];
  }
}

function recallMatchesRule_(recall, rule) {
  const s1 = (rule.slot1_cat && rule.slot1_val)
           ? recallMatchesCriterion_(recall, rule.slot1_cat, rule.slot1_val) : null;
  const s2 = (rule.slot2_cat && rule.slot2_val)
           ? recallMatchesCriterion_(recall, rule.slot2_cat, rule.slot2_val) : null;
  if (s1 === null && s2 === null) return false;
  if (s1 === null) return !!s2;
  if (s2 === null) return !!s1;
  return String(rule.logic).toUpperCase() === 'OR' ? (s1 || s2) : (s1 && s2);
}

// ---------------------------------------------------------------------------
// MATCHING ENGINE  (2026-09-14 — replaces the first-token truncation)
// ---------------------------------------------------------------------------

/**
 * Regex source for one vocabulary token: word-boundaried, plural-tolerant.
 * Mirrors alert_vocab.token_pattern in the FSIS repo, and tests/
 * test_alert_vocab.py fails if the two ever disagree on a case.
 */
function _fsisTokenRe_(token) {
  var pat = String(token).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  if (/^\w/.test(token)) { pat = '\\b' + pat; }
  if (/\w$/.test(token)) { pat = pat + 's?\\b'; }
  return new RegExp(pat, 'i');
}

/**
 * Token list for a subscriber rule.
 *
 * A term that is not in the vocabulary — a brand, or a legacy rule typed by
 * hand before the vocabulary existed — falls back to the term AS WRITTEN.
 * It is never split. That fallback is the whole difference from the old
 * behaviour: an unrecognised rule now matches too little, not too much.
 */
function _fsisTokensFor_(cat, term) {
  var low = String(term || '').toLowerCase().trim();
  if (!low) { return []; }
  var k;
  if (cat === 'pathogen') {
    for (k in PATHOGEN_VOCAB) {
      if (k.toLowerCase() === low) { return PATHOGEN_VOCAB[k]; }
    }
    if (PATHOGEN_LEGACY[low] && PATHOGEN_VOCAB[PATHOGEN_LEGACY[low]]) {
      return PATHOGEN_VOCAB[PATHOGEN_LEGACY[low]];
    }
    return [low];
  }
  if (cat === 'product') {
    for (k in PRODUCT_VOCAB) {
      if (k.toLowerCase() === low) { return PRODUCT_VOCAB[k]; }
    }
    return [low];
  }
  if (cat === 'country') {
    for (k in COUNTRY_ALIASES) {
      if (k.toLowerCase() === low) { return COUNTRY_ALIASES[k]; }
    }
    return [low];
  }
  return [low];
}

/**
 * The text a category is matched against.
 *
 * `product` deliberately includes Reason. Product is free-form and
 * multilingual (fr/pl/it/es/de all occur) and is frequently nothing but a lot
 * reference — "lot 362627", "rillebio-130gr" — while Reason usually names the
 * food. Searching Product alone loses those rows entirely.
 */
function _fsisHaystack_(recall, cat) {
  if (cat === 'pathogen') { return String(recall.Pathogen || ''); }
  if (cat === 'product') {
    return String(recall.Product || '') + ' ' + String(recall.Reason || '');
  }
  if (cat === 'country') { return String(recall.Country || ''); }
  if (cat === 'brand') {
    return String(recall.Brand || '') + ' ' + String(recall.Company || '');
  }
  return '';
}

/**
 * Does this register row satisfy the subscriber rule (cat, val)?
 * Same name and signature as the function it replaces.
 */
function recallMatchesCriterion_(recall, cat, val) {
  var toks = _fsisTokensFor_(cat, val);
  if (!toks.length) { return false; }
  var hay = _fsisHaystack_(recall, cat);
  if (!hay.replace(/\s+/g, '')) { return false; }
  for (var i = 0; i < toks.length; i++) {
    if (_fsisTokenRe_(toks[i]).test(hay)) { return true; }
  }
  return false;
}

/** Every word the subscribe form may offer for a category. */
function fsisOfferedTerms_(cat) {
  if (cat === 'pathogen') { return Object.keys(PATHOGEN_VOCAB); }
  if (cat === 'product') { return Object.keys(PRODUCT_VOCAB); }
  if (cat === 'country') { return COUNTRY_LIST.slice(); }
  return [];   // brand is free text
}

/**
 * True when a stored rule can still fire. Brand rules are always accepted;
 * they are free text by design.
 */
function fsisRuleIsUnderstood_(cat, val) {
  if (cat === 'brand') { return true; }
  var low = String(val || '').toLowerCase().trim();
  if (!low) { return false; }
  var offered = fsisOfferedTerms_(cat);
  for (var i = 0; i < offered.length; i++) {
    if (offered[i].toLowerCase() === low) { return true; }
  }
  if (cat === 'pathogen' && PATHOGEN_LEGACY[low]) { return true; }
  return false;
}

/** Why a retired word was retired, for the rejection message and the audit. */
function fsisRetiredReason_(cat, val) {
  var low = String(val || '').toLowerCase().trim();
  var src = (cat === 'product') ? RETIRED_PRODUCT : RETIRED_PATHOGEN;
  for (var k in src) {
    if (k.toLowerCase() === low) { return src[k]; }
  }
  return '';
}

/**
 * First (category, value) pair in the list that the matcher cannot understand,
 * or null. Empty slots and brand slots are skipped.
 */
function _firstUnknownTerm_(pairs) {
  for (var i = 0; i < pairs.length; i++) {
    var cat = String(pairs[i][0] || '').trim();
    var val = String(pairs[i][1] || '').trim();
    if (!cat || !val || cat === 'brand') { continue; }
    if (!fsisRuleIsUnderstood_(cat, val)) {
      return {cat: cat, val: val, reason: fsisRetiredReason_(cat, val)};
    }
  }
  return null;
}


// =============================================================================
// OUTBOUND EMAIL
// =============================================================================

function sendOnboardingEmail_(email, name) {
  MailApp.sendEmail({
    to: email, name: FROM_NAME,
    subject: 'AFTS Alerts — please subscribe to the weekly briefing first',
    htmlBody:
      '<p>Hi ' + _esc(name) + ',</p>' +
      '<p>Thanks for your interest in <strong>AFTS Custom Alerts</strong>. ' +
      'Alerts are a feature for our existing weekly subscribers — we could not ' +
      'find <em>' + _esc(email) + '</em> in our subscriber list, so your alert ' +
      'rule has been queued but is not active yet.</p>' +
      '<p><a href="https://www.advfood.tech/fsis-home" ' +
      'style="display:inline-block;background:#0a0e1a;color:#fbbf24;padding:10px 18px;' +
      'text-decoration:none;font-family:monospace;font-weight:700;letter-spacing:.08em">' +
      'SUBSCRIBE TO WEEKLY BRIEFING →</a></p>' +
      '<p>Once you are subscribed, re-submit your alert rule and it will activate immediately.</p>' +
      _footer_()
  });
}

function sendNameMismatchAlert_(subscriberEmail, subscriberName, submittedName) {
  MailApp.sendEmail({
    to: subscriberEmail, name: FROM_NAME,
    subject: 'AFTS Alerts — alert submitted using your email but a different name',
    htmlBody:
      '<p>Hi ' + _esc(subscriberName) + ',</p>' +
      '<p>Someone just submitted an <strong>AFTS Custom Alert</strong> rule using ' +
      'your email address <em>' + _esc(subscriberEmail) + '</em>, but the name on ' +
      'the submission was <strong>' + _esc(submittedName) + '</strong> rather ' +
      'than <strong>' + _esc(subscriberName) + '</strong>.</p>' +
      '<p>The rule has NOT been activated. If this was you, please re-submit with ' +
      'the name on your subscription. If you did not submit this, you can ignore ' +
      'this message — no alert will be sent.</p>' +
      _footer_()
  });
}

function sendConfirmationEmail_(email, name, payload, tok, floor) {
  MailApp.sendEmail({
    to: email, name: FROM_NAME,
    subject: 'AFTS Alert activated',
    htmlBody:
      '<p>Hi ' + _esc(name) + ',</p>' +
      '<p>Your <strong>AFTS Custom Alert</strong> is active. We will email you ' +
      'the moment a new recall matches your rule.</p>' +
      _ruleBox_(payload.rule_summary) +
      '<p style="font-size:13px;color:#334155">You will receive alerts for recalls ' +
      '<strong>published after ' + _esc(_ymd(floor)) + '</strong> — the date you set ' +
      'up this alert. Recalls published before then are already covered by the weekly ' +
      'and monthly briefings, so we will not re-send them.</p>' +
      _manageFooter_(tok)
  });
}

function sendRuleUpdatedEmail_(email, name, payload, tok, floor) {
  MailApp.sendEmail({
    to: email, name: FROM_NAME,
    subject: 'AFTS Alert rule updated',
    htmlBody:
      '<p>Hi ' + _esc(name) + ',</p>' +
      '<p>Your <strong>AFTS Custom Alert</strong> rule has been updated. From now on ' +
      'we will email you whenever a new recall matches the updated rule:</p>' +
      _ruleBox_(payload.rule_summary) +
      '<p style="font-size:13px;color:#334155">Because the rule changed, alerts run ' +
      'from <strong>' + _esc(_ymd(floor)) + '</strong> forward. We will not send you ' +
      'historical recalls that match the new criteria.</p>' +
      '<p style="color:#64748b;font-size:12px">If you did not make this change, ' +
      '<a href="' + _unsubUrl_(tok) + '">unsubscribe here</a> and email ' +
      '<a href="mailto:info@advfood.tech">info@advfood.tech</a>.</p>' +
      _manageFooter_(tok)
  });
}

/**
 * The match email.
 *
 * WHY THE 13 SEP EMAIL LOOKED DUPLICATED. The two rows were RappelConso fiches
 * 23501 and 23502: the same lot (VA238F001) of foie gras from the same
 * producer, published the same day — one sold at the factory shop, one through
 * METRO. They are two notices of ONE incident, and the register says so: both
 * rows carry [incident:fr:truffe-perigourdine-2026-09-11] in Notes, which is
 * one of the 14 public columns in recalls.json. The weekly report already
 * counts them once. This email did not know to look.
 *
 * Both rows are still listed — each names a different product and a different
 * use-by date, and a subscriber pulling stock needs both. What changes is that
 * the email no longer presents them as two unrelated findings.
 *
 * Grouping uses the operator-assigned [incident:<id>] tag only. It is never
 * inferred here, exactly as in pipeline/_incident_id.py.
 */
function sendAlertMatchEmail_(rule, matches, overflow, scanStart) {
  var incidentOf = function (r) {
    var m = /\[incident:([^\]]+)\]/i.exec(String(r.Notes || ''));
    return m ? m[1].trim().toLowerCase() : null;
  };

  var counts = {};
  matches.forEach(function (r) {
    var id = incidentOf(r);
    if (id) { counts[id] = (counts[id] || 0) + 1; }
  });
  var incidentCount = matches.reduce(function (n, r) {
    var id = incidentOf(r);
    return n + (id && counts[id] > 1 ? 1 / counts[id] : 1);
  }, 0);
  var incidents = Math.round(incidentCount);

  var rows = matches.map(function (r) {
    var age = _ageDays_(r.Date, scanStart);
    var ageTag = (age !== null && age > 7)
      ? ' <span style="color:#b45309;font-size:10px">(' + age + 'd ago)</span>' : '';
    var id = incidentOf(r);
    var incTag = (id && counts[id] > 1)
      ? ' <span style="color:#64748b;font-size:10px;white-space:nowrap">' +
        '· same incident (' + counts[id] + ' notices)</span>' : '';
    return '<tr>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-family:monospace;' +
        'font-size:11px;color:#64748b;white-space:nowrap">' + _esc(r.Date) + ageTag + '</td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb">' +
        _esc(r.Company || r.Brand || '—') + incTag + '</td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:12px">' +
        _esc(_trim(r.Product, 70)) + '</td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-family:monospace;' +
        'font-size:11px;color:#ef4444"><em>' + _esc(r.Pathogen || '') + '</em></td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:12px;' +
        'white-space:nowrap">' + _esc(r.Country || '') + '</td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;white-space:nowrap">' +
        (r.URL ? '<a href="' + _esc(r.URL) + '">view</a>' : '—') + '</td>' +
      '</tr>';
  }).join('');

  var n = matches.length;
  var th = 'padding:8px 10px;text-align:left;white-space:nowrap';

  var subject = (incidents < n)
    ? 'AFTS Alert: ' + incidents + ' new matching incident' + (incidents > 1 ? 's' : '') +
      ' (' + n + ' notices)'
    : 'AFTS Alert: ' + n + ' new matching recall' + (n > 1 ? 's' : '');

  var lead = (incidents < n)
    ? 'Your alert rule matched <strong>' + incidents + '</strong> new incident' +
      (incidents > 1 ? 's' : '') + ', published as <strong>' + n + '</strong> separate ' +
      'regulator notices. Notices from the same incident are marked below — each ' +
      'names a different product or pack, so check all of them.'
    : 'Your alert rule matched <strong>' + n + '</strong> new recall' + (n > 1 ? 's' : '') + ':';

  MailApp.sendEmail({
    to: rule.email, name: FROM_NAME,
    subject: subject,
    htmlBody:
      '<p>Hi ' + _esc(rule.name) + ',</p>' +
      '<p>' + lead + '</p>' +
      '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;' +
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">' +
      '<thead><tr style="background:#0a0e1a;color:#fbbf24;font-family:monospace;' +
        'font-size:10px;letter-spacing:.08em">' +
      '<th style="' + th + '">PUBLISHED</th><th style="' + th + '">FIRM</th>' +
      '<th style="' + th + '">PRODUCT</th><th style="' + th + '">PATHOGEN</th>' +
      '<th style="' + th + '">COUNTRY</th><th style="' + th + '">LINK</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table>' +
      (overflow
        ? '<p style="font-size:12px;color:#b45309;margin-top:14px">' +
          '+' + overflow + ' further match' + (overflow > 1 ? 'es' : '') +
          ' held back from this email. They will follow in the next scan.</p>'
        : '') +
      '<p style="font-size:12px;color:#64748b;margin-top:14px">PUBLISHED is the date the ' +
      'regulator issued the notice. Always check the source page for the current return ' +
      'or destruction deadline before acting.</p>' +
      _manageFooter_(rule.tok)
  });
}


// =============================================================================
// EMAIL FRAGMENTS
// =============================================================================

function _manageUrl_(tok) {
  return SITE_BASE_URL + '/alerts.html?action=manage&tok=' + encodeURIComponent(tok);
}
function _unsubUrl_(tok) {
  return SITE_BASE_URL + '/alerts.html?action=unsubscribe&tok=' + encodeURIComponent(tok);
}
function _ruleBox_(summary) {
  return '<div style="background:#f5f5f7;border-left:3px solid #fbbf24;padding:12px 16px;' +
         'font-family:monospace;font-size:13px;margin:18px 0">' +
         _esc(summary || '(rule)') + '</div>';
}
function _footer_() {
  return '<p style="color:#64748b;font-size:12px;margin-top:28px">' +
         '— Advanced Food-Tech Solutions · advfood.tech</p>';
}
function _manageFooter_(tok) {
  return '<p style="color:#64748b;font-size:12px;margin-top:28px">' +
         '<a href="' + _manageUrl_(tok) + '">Modify rule</a> · ' +
         '<a href="' + _unsubUrl_(tok) + '">Unsubscribe from this alert</a></p>' +
         '<p style="color:#64748b;font-size:12px">Unsubscribing from this alert only stops ' +
         '<em>this</em> email stream — your weekly and monthly AFTS briefings are ' +
         'unaffected.</p>' + _footer_();
}


// =============================================================================
// UTILITIES
// =============================================================================

function newToken_() {
  return Utilities.getUuid().replace(/-/g, '').substring(0, 24);
}

function _json(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function _html(html) { return HtmlService.createHtmlOutput(html); }

function _esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
    ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'})[c]);
}

/**
 * Parse a spreadsheet Date cell or an ISO "YYYY-MM-DD" string.
 * Returns null on anything unparseable — every caller fails closed.
 *
 * TIMEZONE: ISO strings are parsed into LOCAL midnight, not UTC midnight.
 * Spreadsheet Date cells already arrive as local midnight in the script's
 * timezone. Parsing strings the same way means both input shapes land on
 * the same instant for the same calendar day, so date comparisons are
 * apples-to-apples with no off-by-one. Using `new Date('...T00:00:00Z')`
 * here would be UTC midnight, which reads back as the PREVIOUS day for any
 * script timezone west of UTC.
 */
function _parseDate(d) {
  if (d === null || d === undefined || d === '') return null;
  if (Object.prototype.toString.call(d) === '[object Date]') {
    return isNaN(d.getTime()) ? null : d;
  }
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(d).trim());
  if (!m) return null;
  const dt = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return isNaN(dt.getTime()) ? null : dt;
}

/** Format as YYYY-MM-DD from LOCAL components (see _parseDate on why). */
function _ymd(d) {
  const dd = _parseDate(d);
  if (!dd) return '';
  const p = n => (n < 10 ? '0' : '') + n;
  return dd.getFullYear() + '-' + p(dd.getMonth() + 1) + '-' + p(dd.getDate());
}

/**
 * Normalise any date to LOCAL midnight of the calendar day it represents,
 * so a floor stored with a wall-clock time still compares cleanly against
 * date-only recall rows. Paired with _parseDate's local-midnight parsing.
 */
function _startOfDay_(d) {
  const dd = _parseDate(d);
  if (!dd) return null;
  return new Date(dd.getFullYear(), dd.getMonth(), dd.getDate());
}

function _ageDays_(recallDate, now) {
  const d = _parseDate(recallDate);
  if (!d) return null;
  return Math.floor((now.getTime() - d.getTime()) / 86400000);
}

/** Truncate on a word boundary and append an ellipsis. */
function _trim(s, n) {
  const t = String(s == null ? '' : s).trim();
  if (t.length <= n) return t;
  const cut = t.substring(0, n);
  const sp = cut.lastIndexOf(' ');
  return (sp > n * 0.6 ? cut.substring(0, sp) : cut).replace(/[\s,;(—-]+$/, '') + '…';
}


// =============================================================================
// TRIGGER + ADMIN
// =============================================================================

/**
 * Install the every-4h time-driven trigger. Safe to re-run — removes any
 * existing matcher trigger first, then creates one fresh.
 */
function installEvery4hTrigger() {
  const existing = ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'matchAlertsAgainstRecalls');
  existing.forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('matchAlertsAgainstRecalls').timeBased().everyHours(4).create();
  Logger.log('Installed every-4h trigger. Removed ' + existing.length + ' old trigger(s).');
}

/**
 * DRY RUN — compute the next scan exactly as it would run, but send no email
 * and write nothing. Run this FIRST after deploying. Check the execution log.
 */
function previewNextScan() {
  const p = _runMatcher_(true);
  Logger.log('Preview complete. Nothing was sent and nothing was written.');
  return p;
}

/** Manual kick: run the matcher for real, now. */
function runMatcherNow() {
  _runMatcher_(false);
  Logger.log('Manual run complete. Check Alerts tab + SendLog tab.');
}

/**
 * One-shot self-check for the 2026-07-29 silent-dry-run failure. Confirms:
 *   1. a trigger is installed and points at matchAlertsAgainstRecalls
 *   2. the entry point does NOT forward its argument to the dry-run flag
 *   3. the entry point forces a live run
 *
 * Sends no email and writes nothing. Run it from the editor after any edit to
 * the matcher's signature or to the trigger wiring.
 */
function verifyTriggerWiring() {
  const triggers = ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'matchAlertsAgainstRecalls');
  Logger.log('Triggers pointing at matchAlertsAgainstRecalls: ' + triggers.length);
  triggers.forEach(t => Logger.log('   uid=' + t.getUniqueId() +
                                   ' source=' + t.getTriggerSource()));
  if (!triggers.length) {
    Logger.log('FAIL — no trigger installed. Run installEvery4hTrigger().');
    return false;
  }

  // No trigger may point straight at the private worker — that would hand the
  // event object to dryRun and reproduce the original bug exactly.
  const direct = ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === '_runMatcher_');
  if (direct.length) {
    Logger.log('FAIL — ' + direct.length + ' trigger(s) point at _runMatcher_ ' +
               'directly. Delete them and run installEvery4hTrigger().');
    return false;
  }

  // Reproduce the argument a time-driven trigger actually supplies.
  const fakeEvent = {authMode: 'FULL', triggerUid: '0000000000',
                     hour: 12, minute: 0};
  Logger.log('A trigger-shaped argument is truthy: ' + (!!fakeEvent) +
             '  <-- this is what silently disabled every scheduled send');

  const src = String(matchAlertsAgainstRecalls);
  const forwardsArg = /_runMatcher_\s*\(\s*(e|dryRun|arguments)/.test(src);
  const callsLive   = /_runMatcher_\s*\(\s*false\s*\)/.test(src);
  Logger.log('Entry point forwards its argument to the flag: ' + forwardsArg +
             '   (must be false)');
  Logger.log('Entry point forces a LIVE run: ' + callsLive +
             '   (must be true)');

  // The worker must also reject a truthy non-boolean, belt and braces.
  const workerStrict = /dryRun\s*===\s*true/.test(String(_runMatcher_));
  Logger.log('Worker uses a strict boolean test: ' + workerStrict +
             '   (must be true)');

  const ok = callsLive && !forwardsArg && workerStrict;
  Logger.log(ok ? 'PASS — scheduled runs will send for real.'
                : 'FAIL — the entry point is still wired to the flag.');
  return ok;
}

/** Public alias for the schema migration, if you want to run it by hand. */
function ensureAlertsSchema() { ensureAlertsSchema_(); }

/**
 * Move a rule's subscription floor. Use to grant or deny history explicitly.
 *   setAlertFloor('someone@example.com', '2026-07-25')
 */
function setAlertFloor(email, ymd) {
  if (!email || !ymd) { Logger.log('setAlertFloor(email, "YYYY-MM-DD") required'); return; }
  const d = _parseDate(ymd);
  if (!d) { Logger.log('Bad date: ' + ymd); return; }
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  let n = 0;
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.email]).toLowerCase() !== String(email).toLowerCase()) continue;
    sh.getRange(i + 1, H.alert_floor_date + 1).setValue(d);
    n++;
  }
  Logger.log('Set alert_floor_date=' + _ymd(d) + ' on ' + n + ' row(s) for ' + email);
}

/**
 * Reset a subscriber's tracker so the next scan re-seeds silently (no email).
 * With SEED mode plus the gates, this cannot produce a catchup blast.
 */
function resetRuleAndReseed(email) {
  if (!email) { Logger.log('resetRuleAndReseed: email required'); return; }
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  let reset = 0;
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.email]).toLowerCase() !== String(email).toLowerCase()) continue;
    if (String(data[i][H.status]).toLowerCase() !== 'active') continue;
    sh.getRange(i + 1, H.last_matched_at + 1).setValue('');
    sh.getRange(i + 1, H.sent_urls + 1).setValue('');
    sh.getRange(i + 1, H.last_matched_recall + 1).setValue(
      'RESET ' + _ymd(new Date()) + ' — will re-seed on next scan');
    reset++;
  }
  Logger.log('Reset ' + reset + ' rule(s) for ' + email +
             '. Next scan seeds silently; email only on new matches after that.');
}

/**
 * DELIBERATE catchup — one email of every CURRENTLY ELIGIBLE match for a
 * subscriber, tracker untouched. Still honours the freshness ceiling and the
 * row cap, so it can never become another 88-email event.
 */
function sendCatchupEmailNow(email) {
  if (!email) { Logger.log('sendCatchupEmailNow: email required'); return; }
  const recalls = fetchRecalls_();
  if (!recalls.length) { Logger.log('No recalls loaded — abort.'); return; }
  const now = new Date();
  const staleCutoff = _startOfDay_(new Date(now.getTime() - MAX_RECALL_AGE_DAYS * 86400000));
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  let sent = 0;
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.email]).toLowerCase() !== String(email).toLowerCase()) continue;
    if (String(data[i][H.status]).toLowerCase() !== 'active') continue;
    const rule = {
      email: data[i][H.email], name: data[i][H.name],
      slot1_cat: data[i][H.slot1_cat], slot1_val: data[i][H.slot1_val],
      slot2_cat: data[i][H.slot2_cat], slot2_val: data[i][H.slot2_val],
      logic: data[i][H.logic], tok: data[i][H.unsubscribe_tok],
    };
    const matches = recalls
      .filter(r => r.URL && recallMatchesRule_(r, rule))
      .filter(r => { const d = _parseDate(r.Date); return d && d >= staleCutoff; })
      .sort((a, b) => String(b.Date).localeCompare(String(a.Date)));
    if (!matches.length) { Logger.log('No in-window matches for ' + email); continue; }
    const overflow = Math.max(0, matches.length - MAX_ROWS_PER_EMAIL);
    sendAlertMatchEmail_(rule, matches.slice(0, MAX_ROWS_PER_EMAIL), overflow, now);
    sent++;
    Logger.log('Catchup sent: ' + Math.min(matches.length, MAX_ROWS_PER_EMAIL) +
               ' rows to ' + email + (overflow ? (' (+' + overflow + ' omitted)') : ''));
  }
  Logger.log('Total catchup emails: ' + sent + ' (tracker not modified)');
}


// =============================================================================
// SMOKE TESTS — run manually from the editor
// =============================================================================

function test_findSubscriber() {
  const s = findSubscriber_('georgestof@gmail.com');
  Logger.log(s ? ('Found: ' + s.name + ' (row ' + s.rowIndex + ', status ' + s.status + ')')
              : 'Not found');
}

function test_schema() {
  const sh = ensureAlertsSchema_();
  Logger.log('Alerts tab columns = ' + sh.getLastColumn() +
             ' (expected >= ' + ALERTS_HEADERS.length + ')');
  Logger.log(sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].join(' | '));
}

/** Verify the date gates in isolation — no sheet, no email. */
/**
 * Verify the date gates in isolation — no sheet, no email, no network.
 * The comparison chain below is a deliberate mirror of the one inside
 * _runMatcher_(); if you change one, change both.
 *
 * Expected output when run against a 2026-07-25 scan, floor 2026-01-10:
 *   2026-04-03  SUPPRESS stale             ← the first incident email
 *   2026-03-30  SUPPRESS stale             ← the second incident email
 *   2026-07-25  SEND                       ← today
 *   2026-06-25  SEND                       ← exactly 30d, inclusive edge
 *   2026-06-24  SUPPRESS stale             ← 31d, just outside
 *   2026-01-10  SUPPRESS stale             ← on the floor, but far too old
 *   2026-01-09  SUPPRESS pre-subscription  ← before the floor
 */
function test_gates() {
  const now = new Date(2026, 6, 25, 13, 49);   // 25 Jul 2026, script timezone
  const staleCutoff = _startOfDay_(new Date(now.getTime() - MAX_RECALL_AGE_DAYS * 86400000));
  const floor = _startOfDay_(_parseDate('2026-01-10'));
  const cases = [
    ['2026-04-03', 'first incident email — must NOT send'],
    ['2026-03-30', 'second incident email — must NOT send'],
    ['2026-07-25', 'today — should send'],
    ['2026-07-24', 'yesterday — should send'],
    ['2026-06-25', 'exactly ' + MAX_RECALL_AGE_DAYS + 'd — inclusive edge, should send'],
    ['2026-06-24', (MAX_RECALL_AGE_DAYS + 1) + 'd — just outside, should not send'],
    ['2026-01-10', 'on the floor but far older than the ceiling'],
    ['2026-01-09', 'before subscription'],
    ['not-a-date', 'unparseable — must fail closed'],
    ['',           'empty — must fail closed'],
  ];
  Logger.log('scan=' + _ymd(now) + '  floor=' + _ymd(floor) +
             '  staleCutoff=' + _ymd(staleCutoff));
  cases.forEach(([d, label]) => {
    const dd = _parseDate(d);
    let verdict;
    if (!dd)                   verdict = 'SUPPRESS unparseable';
    else if (dd < floor)       verdict = 'SUPPRESS pre-subscription';
    else if (dd < staleCutoff) verdict = 'SUPPRESS stale (>' + MAX_RECALL_AGE_DAYS + 'd)';
    else                       verdict = 'SEND';
    Logger.log('  ' + (d || '(blank)') + '  ' + verdict + '   — ' + label);
  });
}

/**
 * Same-day check: a subscriber who signs up at 09:00 must still receive a
 * recall published later that SAME day. This is why alert_floor_date is
 * stored at date granularity rather than as a submission timestamp.
 */
function test_sameDayFloor() {
  const joined = new Date(2026, 6, 25, 9, 0);      // signed up 09:00
  const floor  = _startOfDay_(joined);
  [['2026-07-25', 'published later the same day — must SEND'],
   ['2026-07-26', 'next day — must SEND'],
   ['2026-07-24', 'day before signing up — must SUPPRESS']
  ].forEach(([d, label]) => {
    const dd = _parseDate(d);
    Logger.log('  ' + d + '  ' + (dd < floor ? 'SUPPRESS pre-subscription' : 'SEND') +
               '   — ' + label);
  });
}

function test_trimAndDates() {
  Logger.log(_trim('Ground beef (steak haché), sold at the traditional counter of the store', 70));
  Logger.log(_trim('Rabbit pâté (GTIN 3346650206674, lot 000011500830, use-by 2026-04-20)', 70));
  Logger.log('age of 2026-04-03 = ' + _ageDays_('2026-04-03', new Date('2026-07-25T00:00:00Z')) + 'd');
}


/**
 * Report every ACTIVE rule whose stored term the matcher no longer
 * understands — a rule that will never fire again, silently.
 *
 * Sends nothing and writes nothing. Run it once after deploying: anything it
 * lists is a subscriber who needs an email asking them to pick a new word.
 * Legacy spellings that PATHOGEN_LEGACY maps forward ("Ochratoxin A" and
 * friends) are NOT listed — those still match.
 */
function auditStoredRules() {
  const {sh, H} = _alertsHeaderMap_();
  const data = sh.getDataRange().getValues();
  let active = 0, dead = 0;
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][H.status]).toLowerCase() !== 'active') continue;
    active++;
    [['slot1', data[i][H.slot1_cat], data[i][H.slot1_val]],
     ['slot2', data[i][H.slot2_cat], data[i][H.slot2_val]]
    ].forEach(([slot, cat, val]) => {
      cat = String(cat || '').trim();
      val = String(val || '').trim();
      if (!cat || !val || cat === 'brand') return;
      if (fsisRuleIsUnderstood_(cat, val)) return;
      dead++;
      const why = fsisRetiredReason_(cat, val) || 'not in the alert vocabulary';
      Logger.log('DEAD  row ' + (i + 1) + '  ' + data[i][H.email] +
                 '  ' + slot + ' ' + cat + '="' + val + '"  — ' + why);
    });
  }
  Logger.log('Audit: ' + active + ' active rule(s), ' + dead +
             ' criterion(s) that can never match.');
  return dead;
}

/**
 * Alert-vocabulary self-test. Sends nothing, writes nothing, touches no sheet
 * and makes no network call. Run it from the editor after every regeneration.
 */
function test_fsisAlertVocab() {
  var bad = 0;
  var check = function (label, got, want) {
    if (got !== want) { bad++; }
    Logger.log((got === want ? 'ok   ' : 'FAIL ') + label + ' -> ' + got);
  };

  // --- the 13 Sep defect and its neighbours ------------------------------
  var cases = [
    ['pathogen', 'Clostridium botulinum',   {Pathogen: 'Clostridium perfringens'},          false, 'THE BUG'],
    ['pathogen', 'Clostridium botulinum',   {Pathogen: 'C. botulinum'},                     true,  'abbreviated genus'],
    ['pathogen', 'Clostridium botulinum',   {Pathogen: 'Clostridium botulinum'},            true,  'exact'],
    ['pathogen', 'Clostridium perfringens', {Pathogen: 'Clostridium perfringens'},          true,  '13 Sep row still matches its own rule'],
    ['pathogen', 'Listeria',                {Pathogen: 'Listeria monocytogenes'},           true,  'genus rule, species row'],
    ['pathogen', 'Salmonella',              {Pathogen: 'Salmonella Enteritidis'},           true,  'genus rule, serovar row'],
    ['pathogen', 'E. coli / STEC',          {Pathogen: 'Shiga toxin-producing E. coli (STEC)'}, true, 'STEC'],
    ['pathogen', 'E. coli / STEC',          {Pathogen: 'Coliform / total bacterial count'}, false, 'the word-boundary trap'],
    ['pathogen', 'Bacillus cereus / cereulide', {Pathogen: 'Cereulide (B. cereus toxin)'},  true,  'toxin spelling'],
    ['pathogen', 'Ochratoxin',              {Pathogen: 'Ochratoxin A'},                     true,  'A-suffix'],
    ['pathogen', 'Ochratoxin A',            {Pathogen: 'Ochratoxin'},                       true,  'legacy rule still fires'],
    ['pathogen', 'Aflatoxin',               {Pathogen: 'Aflatoxin B1 + Ochratoxin A'},      true,  'compound row'],
    ['pathogen', 'Mold / spoilage',         {Pathogen: 'Mold'},                             true,  'US spelling is canonical'],
    ['pathogen', 'Foreign material / physical hazard', {Pathogen: 'Foreign material (glass)'}, true, 'free-text family'],
    ['pathogen', 'Heavy metals',            {Pathogen: 'Cadmium (heavy metal)'},            true,  'free-text family'],
    ['pathogen', 'T-2 / HT-2 toxin',        {Pathogen: 'Mycotoxins (T-2 and HT-2 toxin)'},  true,  'T-2 inside a phrase'],
    ['pathogen', 'Listeria',                {Pathogen: ''},                                 false, 'empty field never matches'],

    ['country',  'Czechia',                 {Country: 'Czechia'},                           true,  'register spelling'],
    ['country',  'Czechia',                 {Country: 'Czech Republic'},                    true,  'alias'],
    ['country',  'Korea, South',            {Country: 'Korea, South'},                      true,  'register spelling'],
    ['country',  'Turkey',                  {Country: 'Turkey'},                            true,  'register spelling'],
    ['country',  'EU-wide / multi-country', {Country: 'Multiple EU/EEA + UK'},              true,  'multi-country bucket'],
    ['country',  'France',                  {Country: 'Belgium / France'},                  true,  'compound origin'],

    ['product',  'Dairy — cheese',          {Product: 'Tomme de Burdignes', Reason: ''},    true,  'regional cheese name'],
    ['product',  'Dairy — cheese',          {Product: 'lardons fumés pxm 200g', Reason: ''}, false, 'not cheese'],
    ['product',  'Meat — pork',             {Product: 'lardons fumés pxm 200g', Reason: ''}, true,  'fr pork'],
    ['product',  'Dried fruit / nuts',      {Product: 'Ochratoxin A in organic dried figs from Turkey', Reason: ''}, true, 'plural token'],
    ['product',  'Dried fruit / nuts',      {Product: 'best before date 12/2026', Reason: ''}, false, '"date" is not a dried date'],

    ['brand',    'Ferrarini',               {Brand: 'Ferrarini', Company: ''},              true,  'free-text brand'],
    ['brand',    'Ferrarini',               {Brand: '', Company: 'Ferrarini S.p.A.'},       true,  'brand found in company'],
    ['brand',    'Ferrarini',               {Brand: 'Ferrari', Company: ''},                false, 'not a prefix match']
  ];
  cases.forEach(function (c) {
    check('[' + c[0] + '] "' + c[1] + '" vs ' + JSON.stringify(c[2]) + '  (' + c[4] + ')',
          recallMatchesCriterion_(c[2], c[0], c[1]), c[3]);
  });

  // --- vocabulary integrity ---------------------------------------------
  var k, i;
  for (k in PATHOGEN_VOCAB) {
    if (!PATHOGEN_VOCAB[k].length) { bad++; Logger.log('FAIL empty token list: ' + k); }
  }
  for (k in PRODUCT_VOCAB) {
    if (!PRODUCT_VOCAB[k].length) { bad++; Logger.log('FAIL empty token list: ' + k); }
  }
  for (k in PATHOGEN_LEGACY) {
    if (!PATHOGEN_VOCAB[PATHOGEN_LEGACY[k]]) {
      bad++; Logger.log('FAIL legacy "' + k + '" points at a term that no longer exists');
    }
  }
  for (k in COUNTRY_ALIASES) {
    if (COUNTRY_LIST.indexOf(k) < 0) {
      bad++; Logger.log('FAIL alias key not in COUNTRY_LIST: ' + k);
    }
  }
  // Every retired word must be REJECTED by the form and, if it is a pathogen
  // with a legacy mapping, must still fire for existing subscribers.
  for (k in RETIRED_PATHOGEN) {
    if (fsisRuleIsUnderstood_('pathogen', k) && !PATHOGEN_LEGACY[k.toLowerCase()]) {
      bad++; Logger.log('FAIL retired word still offered: ' + k);
    }
  }

  check('Yersinia is offered (recognised, no rows yet)',
        fsisRuleIsUnderstood_('pathogen', 'Yersinia'), true);
  check('"Undeclared allergen" is no longer offered',
        fsisRuleIsUnderstood_('pathogen', 'Undeclared allergen'), false);
  check('"Rotavirus" is no longer offered',
        fsisRuleIsUnderstood_('pathogen', 'Rotavirus'), false);

  Logger.log(bad ? ('FAILURES: ' + bad) : 'All alert-vocabulary cases pass.');
  return bad === 0;
}
