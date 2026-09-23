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
| `hk` | CFS | `/{english,tc_chi,sc_chi}/press/<YYYYMMDD>_<n>.html` |
| `kr` | MFDS | `/[eng/]brd/m_<n>/view.do?seq=<n>` |
| `jp` | CAA | `/result/detail.php?rcl=<n>` |
| `tw` | TFDA | `/{tc,eng}/newsContent.aspx?...id=[t]<n>` |
| `ph` | FDA PH | `/fda-advisory-no-<YYYY>-<n>-<slug>/` |
| `id` | BPOM | `/{siaran-pers,penjelasan-publik}/<slug>` |
| `vn` | VFA | `/{tin-tuc,xu-ly-vi-pham-attp}/<slug>.html` |
| `br` | ANVISA | `/anvisa/pt-br/assuntos/noticias-anvisa/<YYYY>/<slug>` |
| `mx` | COFEPRIS | `/cofepris/{articulos,prensa}/<slug>` |
| `co` | INVIMA | `/biblioteca/preview/<n>` |
| `cl` | ACHIPIA | `/<YYYY>/<MM>/<DD>/<slug>/` |
| `sa` | SFDA | `/{en,ar}/news/<n>` |
| `ae` | MOCCAE | `/{en,ar}/media-center/news/<D>/<M>/<YYYY>/<slug>` |

Every one of these is asserted against a real URL, plus at least one page
that must NOT match, in `tests/test_country_config_conformance.py`. The
negatives are the half that matters: an index page accepted as a recall is
how a row ends up with a navigation label for a company name.

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
