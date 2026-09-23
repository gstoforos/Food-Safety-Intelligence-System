# Country coverage research — 2026-09-23

Why this file exists: the expensive part of adding a country is not writing
the config, it is establishing what the regulator's per-recall URL looks
like. That research is easy to redo by accident and easy to get wrong in a
way that fails silently. So both outcomes are written down — the countries
that produced a config, and the countries that did not and why.

**The rule that makes this necessary.** A config whose
`authority_item_url_regex` matches nothing does not raise, log, or fail a
workflow. It produces a country that runs every day, finds candidates, and
accepts zero of them. Hungary did exactly that for nine runs before anyone
looked. So a country without a verified per-recall URL gets no config at
all — an absent country is visible, a silently-empty one is not.

---

## The route

Every config here implements the Greek route:

1. Find the recall in **local media** (RSS, then Google News restricted to
   the country's curated outlets).
2. **Confirm** product, date and hazard from the article itself.
3. Resolve the article back to the **regulator's own page**.
4. Publish **that** URL.

The authority-URL gate stays absolute. Only the route to the authority URL
changes. This exists because a direct fetch of a regulator's site from a
datacentre IP is the request most likely to be refused — measured 403s on
fda.gov, fsis.usda.gov, fda.gov.ph and gov.il, and every scraper fetch
attempted from the audit sandbox was refused at the proxy.

---

## Added, with verified per-recall URLs

| Code | Authority | Verified item URL shape |
|---|---|---|
| `sg` | SFA | `/news-publications/newsroom/[YYYY/]<slug-with-recall>` |
| `hk` † | CFS | `/<lang>/press/<YYYYMMDD>_<n>.html`, `/<lang>/whatsnew/whatsnew_{fa,sfpa}/<YYYY>_<n>.html`, `/<lang>/rc/subject/files/<YYYYMMDD>_<n>.pdf` |
| `kr` | MFDS | `/[eng/]brd/m_<n>/view.do?seq=<n>` |
| `jp` | CAA | `/result/detail.php?rcl=<n>` |
| `tw` | TFDA | `/{tc,eng}/newsContent.aspx?...id=[t]<n>` |
| `ph` | FDA PH | `/fda-advisory-no-<YYYY>-<n>-<slug>/` |
| `id` | BPOM | `/{siaran-pers,penjelasan-publik}/<slug>` |
| `vn` | VFA | `/{tin-tuc,xu-ly-vi-pham-attp}/<slug>.html` |
| `br` | ANVISA | `/anvisa/pt-br/assuntos/noticias-anvisa/<YYYY>/<slug>` |
| `mx` † | COFEPRIS | `/cofepris/{articulos,prensa}/<slug>`, `/cms/uploads/attachment/file/<id>/Alerta_*.pdf` |
| `co` † | INVIMA | `/biblioteca/preview/<n>`, `/blog/<section>/<slug>` |
| `cl` | ACHIPIA | `/<YYYY>/<MM>/<DD>/<slug>/` |
| `sa` | SFDA | `/{en,ar}/news/<n>` |
| `ae` | MOCCAE | `/{en,ar}/media-center/news/<D>/<M>/<YYYY>/<slug>` |
| `us` ‡ | USDA FSIS | `/recalls-alerts/<slug>` |
| `ch` * | BLV | `/dam/blv/<lang>/dokumente/{oeffentliche-warnungen,rueckrufe}/…`, `/<lang>/newnsb/<id>` |

`ch` (*) is not new — its regex was rewritten today. The three marked †
were corrected within hours of being written, by the register rather than
by more searching. Both stories are in the next section. Every one of these is asserted against a real URL, plus at least one page
that must NOT match, in `tests/test_country_config_conformance.py`. The
negatives are the half that matters: an index page accepted as a recall is
how a row ends up with a navigation label for a company name.

---

## The register is a better oracle than web search

Added later the same day, after the configs above were checked against the
URLs **already in the register**. Web search tells you what a regulator's
recall page looks like. The register tells you what that regulator
*actually publishes*, and for three of the fourteen the two differed.

Each of these would have run daily, found candidates, rejected all of them
at the authority gate, and reported success.

| Code | Took | Missed | Damage |
|---|---|---|---|
| `hk` | `/press/<date>_<n>.html` | Food Alerts (`/whatsnew/whatsnew_fa/<yr>_<n>.html`) and Food Incident Post PDFs (`/rc/subject/files/<date>_<n>.pdf`) | 20 of 22 refused |
| `mx` | `/cofepris/<section>/<slug>` | CMS alert PDFs (`/cms/uploads/attachment/file/<id>/Alerta_*.pdf`) | 2 of 2 refused |
| `co` | `/biblioteca/preview/<id>` | Press-room articles (`/blog/<section>/<slug>`) | 1 of 1 refused |

Hong Kong supplied its own counter-example: a Pending row scraped at 18:08
UTC that day, "CFS orders recall of US raw oysters after excessive E.
coli", on a board the config did not know existed.

**`ch` — Switzerland, live since long before today, and the worst of all
of them.** Its regex was `(warnung|rappel|richiamo|news|aktuell)` — a word
match against the whole URL, wrong in both directions at once:

* **Too narrow.** Every Swiss *recall* document lives under `/rueckrufe/`
  and contains none of those words. The gate refused all six `rr-*.pdf`
  recall notices, 9 of the 14 Swiss URLs in Recalls.
* **Too wide.** `rappel` matched `/fr/mises-en-garde-et-rappels-aliments` —
  the BLV **landing page**. That is the documented origin of a Recalls row
  whose Company and Brand were the page title and whose Product was
  "aliments".

Rewritten to require one of the three structures BLV actually uses
(`/dam/blv/<lang>/dokumente/{oeffentliche-warnungen,rueckrufe}/…` or
`/<lang>/newnsb/<id>`). All 16 real Swiss URLs now pass; the landing page
does not.

This check is now permanent: `tests/test_register_urls_pass_their_own_gate.py`
asserts that every authority URL in Recalls or Pending passes its own
country's item gate, at 100% for these fifteen, and prints the rate for
the older configs. A URL the register holds but the gate refuses is a
recall the system found once and cannot find again.

Two older configs still show refusals and are left alone deliberately,
because some of them are the gate working: `gr` refuses
`efet.gr/…/deltia-typou`, which is a listing page that reached Recalls
before the item regexes existed, and `it` refuses two in-store
`cartello*` notice PDFs while *accepting* its own listing page
`/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari` — the same
defect as Switzerland's, unfixed because Italy needs its own evidence pass.

---

## ‡ The United States — found by a question, not by the audit

The audit looked at Asia, Latin America, the Middle East and Africa.
**North America was not examined, because it looked fine.** The FDA
scraper is fine: 121 URLs in the register and a row placed the same day.

Then someone asked whether one specific recall had been captured:

> **Star Meat Delivery Inc.** — 167,639 lb of raw beef, pork and goat,
> distributed **nationwide**, produced without federal inspection and
> bearing **false USDA inspection marks**. FSIS **Class I**: "reasonable
> probability that use of the product will cause serious, adverse health
> consequences or death." Recalled **2026-09-23**.

It was not in the register. The most recent FSIS row was **2026-09-08** —
fifteen days earlier.

`fsis.usda.gov` returns **403** to datacentre traffic. It refused the audit
sandbox exactly as it refuses the scraper. `usda_fsis.py` is not broken; it
is blocked, in the largest meat-recall jurisdiction the register covers.

**The lesson is about the health metric, not about the US.** North America
had a working scraper and a blocked one, and the region-level view showed
the working one. A regulator can be silent behind a healthy sibling — which
is why `scraper_health.json` is keyed per AGENCY and not per region, and
why `test_no_country_goes_dark.py` asserts per country.

`us.py` covers FSIS **only**. FDA is deliberately out of scope: putting a
gap finder on top of a scraper that already works produces nothing but
duplicates for dedupe to clean up. If the FDA scraper goes quiet, widen it
then — and record it there.

Built from the register's own 24 FSIS URLs rather than from search. The
item/listing distinction is one character:

```
item     /recalls-alerts/cs-beef-packers-llc-recalls-ground-beef-products…
listing  /recalls-alerts?search=019-020-2026
```

All three listing forms are already sitting in the Rejected sheet, which is
the gate working. Public health alerts (`PHA-…`) are **in** scope — FSIS
issues them instead of a recall when the product is no longer recallable,
they use the same board and URL shape, and the register already carries
them.

---

## Researched, no config written

These were investigated on 2026-09-23 and **no per-recall page could be
verified**. Each needs a different decision, not more of the same search.

### India — FSSAI
Found: `old.fssai.gov.in/Product_Recall.aspx` and
`foscos.fssai.gov.in/food-recall`. Both are **systems, not registers** —
the first is a legacy portal, the second the portal where a food business
*files* a recall. Neither publishes a public per-recall page.

India appears to have no public recall register at all; recalls surface
through state Food Safety Commissioners and the press. **Candidate for
`news_authority_mode=True`**, which would need a curated outlet whitelist
doing real work, because Indian food-safety reporting is high-volume and
much of it is not a recall.

### Malaysia — MOH / BKKM
Found: `fsq.moh.gov.my/v6/xs/page.php?id=199`, a CMS with opaque numeric
page ids and no recall board; `fosim.moh.gov.my` is the import clearance
system. No per-recall page found. **Needs a fresh look at where MOH
actually publishes recalls** — possibly only as press statements on
`moh.gov.my`. Do not guess a `page.php?id=` pattern: it would match every
page on the site including the front page.

### Morocco — ONSSA
Found: WordPress with slug pages (`/reglementation/`,
`/controle-des-produits-alimentaires/...`), all standing content. No
recall or communiqué board found. **Needs a fresh look**, including
whether recalls are published only via MAPM press releases.

### Israel — Ministry of Health
Found: `gov.il/he/departments/topics/food-recall` and
`.../topics/recalls/govil-landing-page` — **topic landing pages**; and
`gov.il/he/departments/dynamiccollectors/products-recall-and-warning-search`,
a **search form**. gov.il is also on the measured 403 list.

The dynamiccollectors endpoint is the one worth investigating: gov.il
collectors usually have a JSON API behind them with stable per-item ids.
If it does, Israel becomes a normal config. **That is the next thing to
check for Israel**, not another web search.

### Argentina — ANMAT
Found: `argentina.gob.ar/anmat/alertas/alimentos/retiros` is guidance, and
the actual recall register is an external ASP.NET consultation system,
`retirosmercado.anmat.gob.ar/consultaExterna.aspx` — a **search form with
no per-recall URL**. ANMAT alerts also appear as `argentina.gob.ar`
news items, which would need their own verification.

Note that `argentina.gob.ar` is a shared government host like `gov.br`, so
any eventual regex must be scoped by path to `/anmat/`.

### Turkey — Tarım ve Orman Bakanlığı
Found: `guvenilirgida.tarimorman.gov.tr/GuvenilirGida/gkd/TaklitVeyaTagsisListe1`
— the *taklit ve tağşiş* (adulteration) list, which is a **table of
products, published as a table**, not one page per action. Also
`tarimorman.gov.tr/Duyuru/<id>/<slug>`, a general ministry announcement
board that is per-item but not recall-specific.

Turkey is a real structural mismatch: the enforcement output is a periodic
list of hundreds of products, not discrete recalls. **It needs a decision
about representation before it needs a config** — one row per listed
product, with the list's publication date and URL, is probably right, but
that is a modelling choice, not a regex.

### Thailand — FDA Thailand
Found: `fda.moph.go.th/news/<slug>` where the slug is **percent-encoded
Thai**, and `safetyalert.fda.moph.go.th`. Plausibly workable, but no
individual recall notice was verified, and a `[a-z0-9-]+` slug pattern
would be wrong for percent-encoded Thai. **Closest to ready** of the
countries in this section; needs one verified recall URL.

### Not yet researched
`cn`, `pe`, `ec`, `uy`, `qa`, `in`'s state regulators.

---

## Two defects this research turned up in existing code

Recorded here because both are the kind that produce a confident-looking
green.

**The registry did not discover its own members.** `base.py` carried a
hand-written import tuple naming all 28 country modules, so a new config
was invisible until someone remembered a second place — and `get()` only
raises for the code you asked about, so 27 countries kept working while
the 28th did not exist. Now walks the package with `pkgutil`.

**Four regexes named their own hostname.** The regex is matched against
the full URL in `authority_url_finder` and `extractor`, but against
`path?query` — **netloc stripped** — in `search_verifier`. A regex naming
the host works in two places and silently fails in the third, dropping
every bulk-index hit as a portal page. Affected `br`, `hk`, `mx` (written
that morning) and `hu`, live since before June. Hungary's run_log is the
signature: nine runs, candidates found every time, `extracted_accepted: 0`
every time, and every `efet_url` in `verified.jsonl` a news URL rather
than a `nebih.gov.hu` one.

Both are now asserted, not remembered:
`test_the_registry_discovers_countries_rather_than_listing_them` and
`test_the_regex_matches_both_forms_the_pipeline_uses`.

Also corrected while the tz assertions were being written: Iceland ran at
19:00/20:00 local instead of 21:00 (Central-European offsets copied into a
GMT country), and Egypt ran an hour late for five months a year (its
config said "no DST since 2014"; Egypt reinstated DST in 2023).
