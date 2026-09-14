# Apps Script — the alerts mailer

The AFTS mailers run in a Google Apps Script project that is **not** under
version control. Two files here bridge that gap:

| file | what it is |
|---|---|
| `AftsAlerts.base.gs` | the **deployed script, verbatim**, as the operator sent it on 2026-09-14 (revision 2026-07-25 + the 07-29 trigger hotfix). Never edited by tooling. `git diff` on it is how you see the Apps Script project drifting away from what we generate against. |
| `AftsAlerts.gs` | **GENERATED.** The complete file to paste into the editor: the base, plus the shared vocabulary, the new matching engine and the incident-grouped mailer. |

Regenerate with:

    python3 tools/gen_alert_vocab.py --write

which rewrites `AftsAlerts.gs` **and** the word list in `docs/alerts.html` from
one definition, `tools/alert_vocab.py`, so the two can never drift again.

## Installing

Select all in the Apps Script editor, paste `AftsAlerts.gs` over it, save.
There is nothing to delete by hand and no second file to add — this *is* the
whole project file. Then, from the editor:

    test_fsisAlertVocab()   30+ matcher cases + vocabulary integrity.
                            Sends nothing, writes nothing.
    auditStoredRules()      lists active rules whose stored term is no longer
                            understood, with the reason. Run once after
                            deploying; anything it names is a subscriber who
                            needs to pick a new word.
    verifyTriggerWiring()   unchanged from the base — still worth running.
    previewNextScan()       dry run, unchanged.

## What the merge changes, and nothing else

1. A revision block in the header docstring.
2. A vocabulary gate in `processAlertSubmission_` — a rule the matcher cannot
   understand is now refused at submission, instead of being stored as an
   active rule that silently never fires.
3. `recallMatchesCriterion_` — replaced.
4. `sendAlertMatchEmail_` — replaced.
5. The vocabulary tables, added before the MATCHER section.
6. `test_fsisAlertVocab()` and `auditStoredRules()`, appended.

Everything else — `doPost`, `doGet`, the Subscribers validation, the schema
self-heal, the three send gates, the tracker, SendLog, every email fragment and
utility, the trigger wiring and the existing smoke tests — is byte-identical to
the base.

## Why

Before 2026-09-14 the form and the mailer never shared a definition.
`alerts.html` said in a comment that "the backend matches on exact string
equality so the Apps Script reuses this list". It did not. The mailer truncated
the **subscriber's rule** to its first token —

```js
const base = v.split(/[\s\/()]/)[0];    // "clostridium botulinum" -> "clostridium"
return !!base && p.indexOf(base) >= 0;  // matches "Clostridium perfringens"
```

— which is the wrong alert of 13 Sep, and which turned every species rule into
a genus rule. Five of the 28 words the form offered could also never match
anything the pipeline is capable of emitting.

`tests/test_alert_vocab.py` (48 tests) holds the contract: every offered word
has an explicit token list, every offered word is producible, retired words
stay retired, the generated files are in sync, the merge never drops a function
from the base, the truncation never comes back, and the JavaScript and the
Python agree case for case (the JS half runs under `node` when available).
