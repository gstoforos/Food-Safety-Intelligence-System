# AFTS Training Data — Quality Audit Sample — 2026-05-14

Random samples for hand-review. **Goal: read each, mark ✓ / ✗ / ?**
in the verdict checklist below each example.

- Promote classifier samples: **20** (out of 563 total)
- Tier assignment samples: **18** (out of 527 total)
- Field correction samples: **20** (out of 33 total)

Seed: `42` — same seed gives the same samples for reproducibility.

> **How to use this report:** read each sample, judge whether the gold label
> is correct given the input. If gold is wrong, mark ✗ and add a one-line
> reason. We'll aim for **≥85% ✓** as the threshold to proceed to training.

---

## Task 1 — Promote classifier (20 samples)

Stratified: half promote, half reject.

### Sample 1 — Promote classifier — gold: `REJECT: date_pre_`

**Model input:**

```
- **Date**: (empty)
- **Source**: CFIA
- **Company**: Certain Your Fresh Market brand Broccoli Florets
- **Brand**: Certain Your Fresh Market brand Broccoli Florets
- **Product**: due to Salmonella - Canada.ca
- **Pathogen**: Salmonella
- **Class**: Recall
- **Country**: Canada
- **Tier**: 1
- **Outbreak**: 1
- **URL**: https://recalls-rappels.canada.ca/en/alert-recall/certain-your-fresh-market-brand-broccoli-florets-recalled-due-salmonella
- **Notes excerpt**: REJECTED: Claude check: date_pre_2026: 2025-11-28 (deterministic extract, no LLM call) | There have been no reported illnesses associated with the consumption of this product.

  What is being done

The Canadian Food Inspection Agency (CFIA) is conducting a food safety investigation, which may lead to the recall of other products.

The CFIA is verifying that industry is removing recalle  [via Tavily gap-finder, deterministic extract] [date-unknown: claude-check please verify Date] [gap-gate 2026-05-13: pending_gap → pending_gap_v1] [claude-check 2026-05-13: fail; date_pre_2026: 2025-11-28 (det
```

**Gold output:**

```
REJECT: date_pre_2026: date_pre_2026: 2025-11-28 (deterministic extract, no LLM call)
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 2 — Promote classifier — gold: `REJECT: date_pre_`

**Model input:**

```
- **Date**: (empty)
- **Source**: CFIA
- **Company**: Certain ground beef products
- **Brand**: Certain ground beef products
- **Product**: due to E. coli O157:H7 - Canada.ca
- **Pathogen**: Shiga toxin-producing E. coli (STEC)
- **Class**: Recall
- **Country**: Canada
- **Tier**: 1
- **Outbreak**: 1
- **URL**: https://recalls-rappels.canada.ca/en/alert-recall/certain-ground-beef-products-recalled-due-e-coli-o157h7
- **Notes excerpt**: REJECTED: Claude check: date_pre_2026: 2025-10-24 (deterministic extract, no LLM call) | Background

This recall was triggered by test results.

There have been no reported illnesses associated with the consumption of these products.

  What is being done

The Canadian Food Inspection Agency (CFIA) is conducting a food safety investigation, which may lead to the recall of other products  [via Tavily gap-finder, deterministic extract] [date-unknown: claude-check please verify Date] [gap-gate 2026-05-13: pending_gap → pending_gap_v1] [claude-check 2026-05-13: fail; date_pre_2026: 2025-10-24 (det
```

**Gold output:**

```
REJECT: date_pre_2026: date_pre_2026: 2025-10-24 (deterministic extract, no LLM call)
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 3 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-10
- **Source**: RappelConso (FR)
- **Company**: DROME ARDECHE TRADITION Drôme Ardèche Tradition
- **Brand**: Jules Courtial
- **Product**: SAUCISSON TRADITION VPF (lot details per fiche — fetch deferred for batch speed)
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22004/Interne
- **Notes excerpt**: [fiche 22004 / 2026-04-10; part of Jules Courtial 4-SKU Listeria cluster (sisters: 22001/22002/22006); replaces polluted Notes from your row]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 4 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-06
- **Source**: RappelConso (FR)
- **Company**: Luna Food
- **Brand**: Unknown (LUNA FOOD multi-brand recall)
- **Product**: Ready meal / sandwich (SKU not extractable)
- **Pathogen**: Listeria monocytogenes
- **Class**: Mandatory
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/21918/Interne
- **Notes excerpt**: [fiche 21918 / 2026-04-06; LUNA FOOD multi-brand cluster — brand TBD; REPAIRED from broken row] [dedupe-fix 2026-05-04: 'LUNA FOOD LUNA FOOD' -> 'Luna Food']
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 5 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-03-27
- **Source**: RappelConso (FR)
- **Company**: SICAREV
- **Brand**: Unbranded
- **Product**: Steak haché 5% MG façon bouchère 2x125 g (Lot D60760286, use-by 2026-03-25)
- **Pathogen**: Shiga toxin-producing E. coli (STEC)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/21785/Interne
- **Notes excerpt**: [fiche 21785 / ref. 2026-03-0329 / page date 2026-03-27 (your row had 2026-03-25 — corrected); SICAREV is national French beef processor; nationwide distribution; Class corrected from "Alert" to "Voluntary" per page; Brand was "N/A" on page, set to Unbranded]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 6 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-10
- **Source**: RappelConso (FR)
- **Company**: LE FROMAGER DES HALLES
- **Brand**: Unbranded
- **Product**: BRIQUE BREBIS FERMIER 180 g (Lot 076085, use-by 2026-05-01)
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/21990/Interne
- **Notes excerpt**: [fiche 21990 / ref. 2026-04-0160 / 2026-04-10; same lot 076085 as 21991 — same farm-sheep-cheese inspection at LE FROMAGER DES HALLES]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 7 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-07
- **Source**: RappelConso (FR)
- **Company**: Néonail
- **Brand**: Néonail
- **Product**: led lamp,12w/36 led lamp lampe led uv 12w avec alimentation externe (aucun fabricant indiqué, avec numéro de type / modèle xh-ce241000-a1) et instructions, dimensions: 215 × 168 × 90 mm. produit vendu en ligne, notamment via www.rossmann.de, www.dm.de, www.douglas.de, lyko.com, neonail.de et amazon.
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49599/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; not a recall page | Provided page text is a recall listing page, not the specific recall page for the product, company, or date in the row's URL.]
```

**Gold output:**

```
REJECT: other: '' | fail; not a recall page | Provided page text is a recall listing page, not the specific recall page for the product, company, or date in the row's URL.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 8 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-03-17
- **Source**: CFS (HK) - aggregator (CFIA)
- **Company**: — (multiple)
- **Brand**: —
- **Product**: Various pistachios and pistachio-containing products
- **Pathogen**: Salmonella spp.
- **Class**: Recall (update)
- **Country**: Canada
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://www.cfs.gov.hk/english/rc/subject/files/20260317_4.pdf
- **Notes excerpt**: [STRICT-OUTBREAK-AUDIT 2026-05-01: Outbreak 1→0 — CFS HK aggregator update on global pistachio outbreak from Iran; downstream HK aggregator entry, no HK cases stated] Update to Food Incident Post 16 January 2026. Linked to global pistachio outbreak from Iran [outbreak 2026-04-29: 0→1 — explicitly notes 'Linked to global pistachio outbreak from Iran' (PHAC: 189 illnesses, 26 hospitalisations)]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 9 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-05-12
- **Source**: FSAI (IE)
- **Company**: Lidl
- **Brand**: —
- **Product**: Picture of Free Range 100% Irish Chicken Breast Fillets
- **Pathogen**: Salmonella
- **Class**: Alert
- **Country**: Ireland
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-free-range-100-irish-chicken
- **Notes excerpt**: FSAI HTML listing fallback [gemini-check 2026-05-12: fix; corrections: Company→Lidl, Pathogen→Salmonella; fix; company mismatch; pathogen mismatch | Company and Pathogen are incorrect; corrected from page. Tier also corrected.] [enrich-gate 2026-05-12: pending_enrichment → pending (Claude verified)]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 10 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-05-07
- **Source**: RASFF (EU)
- **Company**: Origin: United States | Notifying: Belgium
- **Brand**: BEL, DEU
- **Product**: Aflatoxins in pistachios from US via Jordan to Belgium and Germany
- **Pathogen**: Aflatoxin
- **Class**: Alert
- **Country**: USA
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842515
- **Notes excerpt**: [RASFF #2026.4020; classification: alert notification; category: nuts, nut products and seeds; notifId=842515] [gemini-enrich 2026-05-08: Brand '—'→'BEL, DEU'] [claude-check 2026-05-09: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 11 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-06
- **Source**: RappelConso (FR)
- **Company**: 1. Land Rover 2. Range
- **Brand**: 1. Land Rover 2. Range Rover
- **Product**: range rover sport (lw), range rover evoque (lv) voiture de tourisme.
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49481/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; not a recall page; pathogen mismatch | Pathogen is empty and the recall reason (airbag defect in a car) is not a food safety pathogen.]
```

**Gold output:**

```
REJECT: other: '' | fail; not a recall page; pathogen mismatch | Pathogen is empty and the recall reason (airbag defect in a car) is not a food safety pathogen.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 12 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-07
- **Source**: RappelConso (FR)
- **Company**: Luna Food
- **Brand**: Foodles
- **Product**: Cooked salmon poke
- **Pathogen**: Listeria monocytogenes
- **Class**: Mandatory
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/21950/Interne
- **Notes excerpt**: [fiche 21950 / 2026-04-07; LUNA FOOD Foodles cluster (also see 21992/21993/21994 Apr 10); replaces polluted Notes] [dedupe-fix 2026-05-04: 'LUNA FOOD LUNA FOOD' -> 'Luna Food']
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 13 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-14
- **Source**: Min. Salute (IT)
- **Company**: La Vacje Salumerie Snc
- **Brand**: La Vecje Salumerie
- **Product**: Salame Nostrano (~800g; lot L6CCTD)
- **Pathogen**: Listeria monocytogenes
- **Class**: Alert
- **Country**: Italy
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://ilfattoalimentare.it/richiamo-salame-nostrano-2.html
- **Notes excerpt**: Ministero della Salute richiamo
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 14 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-08
- **Source**: RappelConso (FR)
- **Company**: (empty)
- **Brand**: Unbranded
- **Product**: bijoux perçants pour le corps. produit vendu en ligne, notamment via temu.
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49639/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; not a recall page; company mismatch; product mismatch; pathogen mismatch | Page does not contain the specific recall information for this product; it is a listing page. Also, cadmium is not an in-scope pathogen for FSIS Tier-1.]
```

**Gold output:**

```
REJECT: other: '' | fail; not a recall page; company mismatch; product mismatch; pathogen mismatch | Page does not contain the specific recall information for this product; it is a listing page. Also, cadmium is not
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 15 — Promote classifier — gold: `PROMOTE`

**Model input:**

```
- **Date**: 2026-04-03
- **Source**: RappelConso (FR)
- **Company**: FROMAGERIE LE CENTURION CENTURION
- **Brand**: Petit Marché
- **Product**: Cheese cubes with walnuts PMA 120 g
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/21862/Interne
- **Notes excerpt**: [fiche 21862 / ref. 2026-04-0034 / 2026-04-03; REPAIRED from broken row — Notes had multi-distributor leak; SISTER to 21855 (150 g Carrefour Classic, batch 8) — your earlier "150 g + 120 g" row conflated TWO distinct fiches with TWO different producers (Carrefour France vs Fromagerie Le Centurion); distributors: Auchan, Carrefour, Leclerc, Système U, Intermarché, Cora, Coop Saveurs]
```

**Gold output:**

```
PROMOTE
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 16 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RappelConso (FR)
- **Company**: (empty)
- **Brand**: Unbranded
- **Product**: pa
- **Pathogen**: (empty)
- **Class**: volontaire (sans arrêté préfectoral)
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22217/Interne
- **Notes excerpt**: boutiques - magasins de souvenirs - sites touristiques [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; company mismatch; product mismatch; pathogen mismatch | Pathogen (cadmium) is out of FSIS Tier-1 scope.]
```

**Gold output:**

```
REJECT: other: '' | fail; company mismatch; product mismatch; pathogen mismatch | Pathogen (cadmium) is out of FSIS Tier-1 scope.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 17 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-06
- **Source**: RappelConso (FR)
- **Company**: Baker Ross
- **Brand**: Baker Ross
- **Product**: rainbow coloured sand bags x7 ensemble de sept sacs de sable destinés à l'artisanat, en sept couleurs différentes. produit vendu en ligne, notamment via amazon (asin: b093pwrqz6).
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49561/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; not a recall page; pathogen mismatch | Pathogen (asbestos) is out of scope, and the provided page text is a listing page that does not contain the specific recall details.]
```

**Gold output:**

```
REJECT: other: '' | fail; not a recall page; pathogen mismatch | Pathogen (asbestos) is out of scope, and the provided page text is a listing page that does not contain the specific recall details.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 18 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-12
- **Source**: RappelConso (FR)
- **Company**: Grand Frais / Fresh / Mon-Marché.Fr
- **Brand**: Grand Frais / Fresh / Mon-Marché.Fr
- **Product**: brisures de saumon fumé 150g
- **Pathogen**: Listeria monocytogenes
- **Class**: volontaire (sans arrêté préfectoral)
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22234/Interne
- **Notes excerpt**: grand frais/ fresh/mon-marché [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: fail; company mismatch | company_is_distributor]
```

**Gold output:**

```
REJECT: other: company mismatch | company_is_distributor
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 19 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-08
- **Source**: RappelConso (FR)
- **Company**: (empty)
- **Brand**: Unbranded
- **Product**: mini steamateur à oeufs mini cuiseur à œufs. produit vendu en ligne, notamment via temu.
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49793/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; not a recall page; product mismatch | Provided page text is a recall listing page and does not contain the specific product mentioned in the row.]
```

**Gold output:**

```
REJECT: other: '' | fail; not a recall page; product mismatch | Provided page text is a recall listing page and does not contain the specific product mentioned in the row.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 20 — Promote classifier — gold: `REJECT: other`

**Model input:**

```
- **Date**: 2026-05-08
- **Source**: RappelConso (FR)
- **Company**: (empty)
- **Brand**: Unbranded
- **Product**: bottes en caoutchouc, destinées aux enfants, avec un motif de dessin animé de différentes couleurs et tailles. tous les lots sont concernés. produit vendu en ligne, notamment via joom.
- **Pathogen**: (empty)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/49792/Interne
- **Notes excerpt**: [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: pathogen_out_of_scope: '' | fail; product mismatch; pathogen mismatch | Product 'bottes en caoutchouc' not found on the provided page text; the reason for recall (phthalates) is not a pathogen in FSIS scope.]
```

**Gold output:**

```
REJECT: other: '' | fail; product mismatch; pathogen mismatch | Product 'bottes en caoutchouc' not found on the provided page text; the reason for recall (phthalates) is not a pathogen in FSIS scope.
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

## Task 2 — Tier assignment (18 samples)

Stratified across Tier 1, 2, 3.

### Sample 1 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Listeria monocytogenes
- **Outbreak**: 0
- **Class**: Alert
- **Product**: British Bresaola
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 2 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Shiga toxin-producing E. coli (STEC)
- **Outbreak**: 0
- **Class**: Recall
- **Product**: Goat-cheese Pyramides — multiple lots (80/8m, 80/8v, 61.34, 61.47, 68.x, 70.x, 75.x, 69.x, 76.59, 77.x, 79.105, 82.x, 89.x, 91.x, 92.89), best-before 2026-04-15 to 2026-06-01
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 3 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Listeria monocytogenes
- **Outbreak**: 0
- **Class**: Mandatory
- **Product**: Onigiri Spicy Tuna Mayo
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 4 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: Histamine / scombrotoxin
- **Outbreak**: 0
- **Class**: Voluntary
- **Product**: produits de la pêche et d'aquaculture
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 5 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Salmonella
- **Outbreak**: 0
- **Class**: Class 2
- **Product**: Royal Zaatar (450 g)
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 6 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: E. coli
- **Outbreak**: 0
- **Class**: Recall
- **Product**: K Classic Delikatess Salami 1A fettreduziert, 100 g pack (best-before 2026-04-17, 2026-04-24, 2026-05-01)
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 7 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Salmonella
- **Outbreak**: 0
- **Class**: Voluntary
- **Product**: 7 herb-flavoured Lionor sausages tray 140 g
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 8 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: Ochratoxin
- **Outbreak**: 0
- **Class**: Alert
- **Product**: Stwierdzenie przekroczenia ochratoksyny A na poziomie 146 µg/kg w papryce ostrej pochodzącej z Hiszpanii // exceeding the level of ochratoxin A at 146 µg/kg in hot peppers from Spain
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 9 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: Ochratoxin
- **Outbreak**: 0
- **Class**: Alert
- **Product**: Ochratoxin in organic sultanas from Uzbekistan via Germany
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 10 — Tier assignment — gold: `Tier 1`

**Model input:**

```
- **Pathogen**: Listeria monocytogenes
- **Outbreak**: 0
- **Class**: Voluntary
- **Product**: Pork-head brawn (parsleyed) — both moulded and demoulded variants (GTIN 3760190830289, Lot 166044, use-by 2026-05-06)
```

**Gold output:**

```
1
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 11 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: Aflatoxins
- **Outbreak**: 0
- **Class**: Border Rejection
- **Product**: Dried figs from Turkey
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 12 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: Norovirus / unspecified shellfish hazard
- **Outbreak**: 0
- **Class**: Voluntary
- **Product**: Cockles 3 kg bag
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 13 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: Peanut
- **Outbreak**: 0
- **Class**: Recall
- **Product**: Certified Organic Garlic Powder — 250 g, 500 g, 1 kg, 5 kg, 10 kg (best-before 2028-12-08)
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 14 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: Physical/foreign-body contamination
- **Outbreak**: 0
- **Class**: Recall
- **Product**: White Delights-Pistazie
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 15 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: PFOA / PFAS
- **Outbreak**: 0
- **Class**: Recall
- **Product**: Blue crab 1 kg, frozen
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 16 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: Ochratoxin
- **Outbreak**: 0
- **Class**: Border Rejection
- **Product**: Presence of Ochratoxin A in dried figs from Turkey
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 17 — Tier assignment — gold: `Tier 3`

**Model input:**

```
- **Pathogen**: Staphylococcus enterotoxin
- **Outbreak**: 0
- **Class**: Voluntary
- **Product**: Fresh raw-milk goat cheese (round)
```

**Gold output:**

```
3
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 18 — Tier assignment — gold: `Tier 2`

**Model input:**

```
- **Pathogen**: Aflatoxin
- **Outbreak**: 0
- **Class**: Information
- **Product**: Aflatoxinas en almendras de EEUU/ Aflatoxins ind almond from USA
```

**Gold output:**

```
2
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

## Task 3 — Field corrections (20 samples)

Random sample of the FIX-extraction examples.

### Sample 1 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: Netherlands | Notifying: Netherlands
- **Brand**: (empty or garbage in original scrape)
- **Product**: Eggs from a Dutch stable that tested positive for Salmonella
- **Pathogen**: Salmonella
- **Class**: Information
- **Country**: Netherlands
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842725
- **Notes excerpt**: [RASFF #2026.4102; classification: information notification for follow-up; category: eggs and egg products; notifId=842725] [gemini-enrich 2026-05-12: Brand '—'→'Netherlands'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "Netherlands"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 2 — Field corrections — gold: `fix Company, Pathogen`

**Model input:**

```
- **Date**: 2026-05-07
- **Source**: RappelConso (FR)
- **Company**: (empty or garbage in original scrape)
- **Brand**: Unbranded
- **Product**: huitres st vaast n°2 – 5kg et n°3 – 6kg
- **Pathogen**: (empty or garbage in original scrape)
- **Class**: Voluntary
- **Country**: France
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22220/Interne
- **Notes excerpt**: cooperative u [url-gate 2026-05-09: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-enrich 2026-05-09: Company ''→'COOPERATIVE U ENSEIGNE COOPERATIVE U'] [gemini-enrich 2026-05-09: Pathogen ''→'Escherichia coli'] [claude-check 2026-05-11: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Company": "COOPERATIVE U ENSEIGNE COOPERATIVE U", "Pathogen": "Escherichia coli"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 3 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-08
- **Source**: RASFF (EU)
- **Company**: Origin: Netherlands | Notifying: Netherlands
- **Brand**: (empty or garbage in original scrape)
- **Product**: Listeria monocytogenes present in a ready to eat cheese board
- **Pathogen**: Listeria monocytogenes
- **Class**: Alert
- **Country**: Netherlands
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842836
- **Notes excerpt**: [RASFF #2026.4058; classification: alert notification; category: prepared dishes and snacks; notifId=842836] [gemini-enrich 2026-05-09: Brand '—'→'NLD'] [claude-check 2026-05-10: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "NLD"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 4 — Field corrections — gold: `fix Company, Brand, Product, Pathogen`

**Model input:**

```
- **Date**: 2026-05-02
- **Source**: FSAI
- **Company**: (empty or garbage in original scrape)
- **Brand**: (empty or garbage in original scrape)
- **Product**: (empty or garbage in original scrape)
- **Pathogen**: (empty or garbage in original scrape)
- **Class**: Recall
- **Country**: Ireland
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-specific-batches-of-green-box-limited-en
- **Notes excerpt**: Food Safety Authority of Ireland

# Food Alerts

## Recall of specific batches of Green Box Limited Enoki Mushrooms due to the presence of Listeria monocytogenes

Saturday, 02 May 2026

| Alert Summary | |
 --- |
| Category 1: | For Action |
| Alert Notification: | 2026.19 |
| Product Identification  [via Tavily gap-finder, deterministic extract] [gap-gate 2026-05-08: pending_gap → pending_gap_v1] [gap-gate 2026-05-09: pending_gap_v1 → pending_gap_v2] [claude-check 2026-05-09: fix; corrections: Company→Green Box Limited, Brand→Green Box Limited, Product→Green Box Limited Enoki Mushroom (Cendaw
```

**Gold output:**

```
{"corrections": {"Company": "Green Box Limited", "Brand": "Green Box Limited", "Product": "Green Box Limited Enoki Mushroom (Cendaw", "Pathogen": "Listeria monocytogenes"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 5 — Field corrections — gold: `fix Company`

**Model input:**

```
- **Date**: 2026-05-06
- **Source**: CFIA
- **Company**: (empty or garbage in original scrape)
- **Brand**: Germina
- **Product**: "Brocoli Calabrese" seeds
- **Pathogen**: Escherichia coli (generic)
- **Class**: Class 1
- **Country**: Canada
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://recalls-rappels.canada.ca/en/alert-recall/germina-brand-brocoli-calabrese-seeds-recalled-due-possible-contamination-pathogenic-e
- **Notes excerpt**: CFIA open-data NID=82039 [gemini-enrich 2026-05-12: Company ''→'Germina'] [gemini-check 2026-05-12: fix; corrections: Company→Germina; placeholder_company_filled_from_page | fix; company mismatch | Company field was empty and corrected from page; Tier mis]
```

**Gold output:**

```
{"corrections": {"Company": "Germina"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 6 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: Madagascar | Notifying: Croatia
- **Brand**: (empty or garbage in original scrape)
- **Product**: Bacillus cereus in ground cinnamon from Madagascar
- **Pathogen**: Bacillus cereus
- **Class**: Alert
- **Country**: Madagascar
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/843280
- **Notes excerpt**: [RASFF #2026.4119; classification: alert notification; category: herbs and spices; notifId=843280] [gemini-enrich 2026-05-12: Brand '—'→'Madagascar'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)] [region-fix 2026-05-12: Madagascar→Africa]
```

**Gold output:**

```
{"corrections": {"Brand": "Madagascar"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 7 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: South Korea | Notifying: Sweden
- **Brand**: (empty or garbage in original scrape)
- **Product**: Norovirus in nori (roasted seaweed)
- **Pathogen**: Norovirus
- **Class**: Information
- **Country**: South Korea
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/843057
- **Notes excerpt**: [RASFF #2026.4123; classification: information notification for attention; category: fruits and vegetables; notifId=843057] [gemini-enrich 2026-05-12: Brand '—'→'SWE'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "SWE"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 8 — Field corrections — gold: `fix Company`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RappelConso (FR)
- **Company**: (empty or garbage in original scrape)
- **Brand**: Unbranded
- **Product**: gorgonzola dop à la cuillère
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22229/Interne
- **Notes excerpt**: produit distribué dans les enseignes grand frais. [url-gate 2026-05-11: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-enrich 2026-05-11: Company ''→'RappelConso']
```

**Gold output:**

```
{"corrections": {"Company": "RappelConso"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 9 — Field corrections — gold: `fix Company, Brand, Product, Pathogen`

**Model input:**

```
- **Date**: (empty)
- **Source**: CFIA
- **Company**: (empty or garbage in original scrape)
- **Brand**: (empty or garbage in original scrape)
- **Product**: (empty or garbage in original scrape)
- **Pathogen**: (empty or garbage in original scrape)
- **Class**: Recall
- **Country**: Canada
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://recalls-rappels.canada.ca/en/alert-recall/mon-pere-brand-camembert-recalled-due-listeria-monocytogenes
- **Notes excerpt**: Food - Dairy. Companies. Recalling firm: D. Tyers Foods International Inc. Published by. Canadian Food Inspection Agency. Audience. General public. Distribution. Ontario. Recall class.  [via DDGS gap-finder, deterministic extract] [date-unknown: claude-check please verify Date] [claude-check 2026-05-12: fix; corrections: Company→D. Tyers Foods International Inc., Brand→Mon Père, Product→Camembert, Pathogen→Listeria monocytogenes; fix; company mismatch | Company field should be 'D. Tyers Foods International Inc.' (recalling firm named on page), not ] [gap-gate 2026-05-12: pending_gap → pending_
```

**Gold output:**

```
{"corrections": {"Company": "D. Tyers Foods International Inc.", "Brand": "Mon Père", "Product": "Camembert", "Pathogen": "Listeria monocytogenes"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 10 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: France | Notifying: Italy
- **Brand**: (empty or garbage in original scrape)
- **Product**: norovirus in oysters from France
- **Pathogen**: Norovirus
- **Class**: Information
- **Country**: France
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842863
- **Notes excerpt**: [RASFF #2026.4110; classification: information notification for attention; category: bivalve molluscs and products thereof; notifId=842863] [gemini-enrich 2026-05-12: Brand '—'→'France'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "France"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 11 — Field corrections — gold: `fix Date`

**Model input:**

```
- **Date**: (empty or garbage in original scrape)
- **Source**: FSA
- **Company**: Danone
- **Brand**: Danone
- **Product**: several Aptamil and Cow & Gate First Infant Milk and Follow on Milk formula products
- **Pathogen**: Cereulide (B. cereus toxin)
- **Class**: Recall
- **Country**: United Kingdom
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://www.food.gov.uk/news-alerts/alert/fsa-prin-05-2026
- **Notes excerpt**: ### About product recalls and withdrawals

If there is a problem with a food product that means it should not be sold, then it might be 'withdrawn' (taken off the shelves) or 'recalled' (when customers are asked to return the product). The FSA issues Product Recall Information Notices to let consume  [via Tavily gap-finder, deterministic extract] [gap-gate 2026-05-06: pending_gap → pending_gap_v1] [gap-gate 2026-05-06: pending_gap_v1 → pending_gap_v2] [Claude-flag 2026-05-06: Notes field contain [claude-check 2026-05-07: fix; Date 2026-06-11 → 2026-02-06; fix; date on page=2026-02-06 | Row dat
```

**Gold output:**

```
{"corrections": {"Date": "2026-02-06"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 12 — Field corrections — gold: `fix Company`

**Model input:**

```
- **Date**: 2026-05-12
- **Source**: RappelConso (FR)
- **Company**: (empty or garbage in original scrape)
- **Brand**: Les Créations de Pierre
- **Product**: poitrine fumée 350g pierre schmidt
- **Pathogen**: Listeria monocytogenes
- **Class**: volontaire (sans arrêté préfectoral)
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22238/Interne
- **Notes excerpt**: carrefour¤système u [url-gate 2026-05-12: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [gemini-check 2026-05-12: fix; corrections: Company→CHARCUTERIE PIERRE SCHMIDT Pierre Schmid; fix; company mismatch | Company name in the row does not match the recalling firm ('Origine de la fiche') on the page.]
```

**Gold output:**

```
{"corrections": {"Company": "CHARCUTERIE PIERRE SCHMIDT Pierre Schmid"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 13 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-07
- **Source**: RASFF (EU)
- **Company**: Origin: United States | Notifying: Belgium
- **Brand**: (empty or garbage in original scrape)
- **Product**: Aflatoxins in pistachios from US via Jordan to Belgium and Germany
- **Pathogen**: Aflatoxin
- **Class**: Alert
- **Country**: USA
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842515
- **Notes excerpt**: [RASFF #2026.4020; classification: alert notification; category: nuts, nut products and seeds; notifId=842515] [gemini-enrich 2026-05-08: Brand '—'→'BEL, DEU'] [claude-check 2026-05-09: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "BEL"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 14 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: Ukraine | Notifying: Germany
- **Brand**: (empty or garbage in original scrape)
- **Product**: Salmonella Stanley in instant noodle meal from Ukraine, via Poland
- **Pathogen**: Salmonella
- **Class**: Alert
- **Country**: Ukraine
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/843316
- **Notes excerpt**: [RASFF #2026.4131; classification: alert notification; category: prepared dishes and snacks; notifId=843316] [gemini-enrich 2026-05-12: Brand '—'→'Ukraine'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "Ukraine"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 15 — Field corrections — gold: `fix Company, Brand`

**Model input:**

```
- **Date**: 2026-05-04
- **Source**: RappelConso (FR)
- **Company**: (empty or garbage in original scrape)
- **Brand**: (empty or garbage in original scrape)
- **Product**: viandes
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22173/Interne
- **Notes excerpt**: leclerc lesparre 33340 [url-gate 2026-05-04: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [claude-check 2026-05-04: fix-upgraded; Company→Leclerc Lesparre; Brand→Leclerc Lesparre; Tier verified=1 H4 Listeria]
```

**Gold output:**

```
{"corrections": {"Company": "Leclerc Lesparre", "Brand": "Leclerc Lesparre"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 16 — Field corrections — gold: `fix Company, Brand`

**Model input:**

```
- **Date**: 2026-05-04
- **Source**: RappelConso (FR)
- **Company**: (empty or garbage in original scrape)
- **Brand**: (empty or garbage in original scrape)
- **Product**: viandes
- **Pathogen**: Listeria monocytogenes
- **Class**: Voluntary
- **Country**: France
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://rappel.conso.gouv.fr/fiche-rappel/22177/Interne
- **Notes excerpt**: boucherie aux enfants [url-gate 2026-05-04: API fixed https://rappel.conso.gouv.fr/fiche-rappe... -> https://rappel.conso.gouv.fr/fiche-rappe...] [claude-check 2026-05-04: fix-upgraded; Company→Boucherie Aux Enfants; Brand→Boucherie Aux Enfants; Tier verified=1 H4 Listeria]
```

**Gold output:**

```
{"corrections": {"Company": "Boucherie Aux Enfants", "Brand": "Boucherie Aux Enfants"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 17 — Field corrections — gold: `fix Company, Pathogen`

**Model input:**

```
- **Date**: 2026-05-12
- **Source**: FSAI (IE)
- **Company**: (empty or garbage in original scrape)
- **Brand**: —
- **Product**: Picture of Free Range 100% Irish Chicken Breast Fillets
- **Pathogen**: (empty or garbage in original scrape)
- **Class**: Alert
- **Country**: Ireland
- **Tier**: 3
- **Outbreak**: 0
- **URL**: https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-free-range-100-irish-chicken
- **Notes excerpt**: FSAI HTML listing fallback [gemini-check 2026-05-12: fix; corrections: Company→Lidl, Pathogen→Salmonella; fix; company mismatch; pathogen mismatch | Company and Pathogen are incorrect; corrected from page. Tier also corrected.] [enrich-gate 2026-05-12: pending_enrichment → pending (Claude verified)]
```

**Gold output:**

```
{"corrections": {"Company": "Lidl", "Pathogen": "Salmonella"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 18 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-08
- **Source**: RASFF (EU)
- **Company**: Origin: Thailand | Notifying: Germany
- **Brand**: (empty or garbage in original scrape)
- **Product**: Salmonella spp. in salted and dried fish from Thailand
- **Pathogen**: Salmonella
- **Class**: Alert
- **Country**: Thailand
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842255
- **Notes excerpt**: [RASFF #2026.4042; classification: alert notification; category: fish and fish products; notifId=842255] [gemini-enrich 2026-05-09: Brand '—'→'DEU, EST'] [claude-check 2026-05-10: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "DEU"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 19 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-11
- **Source**: RASFF (EU)
- **Company**: Origin: China | Notifying: Greece
- **Brand**: (empty or garbage in original scrape)
- **Product**: Aflatoxins in blanched groundnut kernels from China
- **Pathogen**: Aflatoxin
- **Class**: Border Rejection
- **Country**: China
- **Tier**: 2
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/842971
- **Notes excerpt**: [RASFF #2026.4094; classification: border rejection notification; category: nuts, nut products and seeds; notifId=842971] [gemini-enrich 2026-05-12: Brand '—'→'China'] [gemini-check 2026-05-12: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "China"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

### Sample 20 — Field corrections — gold: `fix Brand`

**Model input:**

```
- **Date**: 2026-05-07
- **Source**: RASFF (EU)
- **Company**: Origin: unknown origin | Notifying: Finland
- **Brand**: (empty or garbage in original scrape)
- **Product**: Salmonella and ochratoxin A in pistachio nuts via Sweden.
- **Pathogen**: Salmonella
- **Class**: Alert
- **Country**: unknown origin
- **Tier**: 1
- **Outbreak**: 0
- **URL**: https://webgate.ec.europa.eu/rasff-window/screen/notification/2026.4017
- **Notes excerpt**: [RASFF #2026.4017; classification: alert notification; category: nuts, nut products and seeds; notifId=842548] [url-gate 2026-05-08: Gemini fixed https://webgate.ec.europa.eu/rasff-windo... -> https://webgate.ec.europa.eu/rasff-windo...] [gemini-enrich 2026-05-08: Brand '—'→'SWE'] [claude-check 2026-05-09: pass; clean-row shortcut (gemini-enriched + all fields valid)]
```

**Gold output:**

```
{"corrections": {"Brand": "SWE"}}
```

**Your verdict** (edit this line — click the checkbox in the GH UI):

- [ ] ✓ correct, trainable as-is
- [ ] ✗ wrong (input is garbage OR gold label is wrong)
- [ ] ? borderline — note reason below

_Notes:_

---

## Rejection-category audit

Every reject example, showing parsed category vs raw assistant message.
Look for examples bucketed as `other` that should have been a specific category.

| # | Category | Raw assistant message (truncated) |
|---:|---|---|
| 1 | `other` | REJECT: other: not a recall page \| rasff_company_format |
| 2 | `date_pre_` | REJECT: date_pre_2026: date_pre_2026: 2025-10-24 (deterministic extract, no LLM call) |
| 3 | `other` | REJECT: other: company mismatch; product mismatch; pathogen mismatch \| Company in row is not the recalling firm; the page identifies 'Western Brand' as the recalling firm, while the row lists product |
| 4 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Page is a listing index, not the specific recall page for the product. Additionally, the product is not a food item. |
| 5 | `other` | REJECT: other: '' \| fail; not a recall page; company mismatch; product mismatch; pathogen mismatch \| Page does not contain the specific recall information for this product; it is a listing page. Als |
| 6 | `date_pre_` | REJECT: date_pre_2026: date_pre_2026: 2025-11-28 (deterministic extract, no LLM call) |
| 7 | `other` | REJECT: other: '' \| fail; company mismatch; pathogen mismatch \| Pathogen 'Présence de corps étranger métallique' is out of scope for FSIS Tier-1 pathogens. |
| 8 | `other` | REJECT: other: '' \| fail; not a recall page; pathogen mismatch \| Pathogen is empty and the recall reason (airbag defect in a car) is not a food safety pathogen. |
| 9 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Pathogen is empty and product is a car, not a food item, making it out of scope for food safety. Page is also a listing. |
| 10 | `other` | REJECT: other: '' \| fail; not a recall page; pathogen mismatch \| Provided page text is a list of recalls and does not contain the product described in the row. Reason for recall (heavy metals) is ou |
| 11 | `other` | REJECT: other: fail; not a recall page; company mismatch; product mismatch; pathogen mismatch; date on page= \| Page is a generic FAQ la |
| 12 | `date_pre_` | REJECT: date_pre_2026: Recalls and safety alerts. Food recall warning. Organic pumpkin seeds recalled due to Salmonella.  [via DDGS gap-finder, deterministic extract] [date-unknown: claude-check pleas |
| 13 | `other` | REJECT: other: corrections: Date→2026-02-25, Company→Nestlé, Danone, Product→Infant formula / zuigelingenvoeding, Pathogen→Cereulide; fail; company mismatch; product mismatch; date on page=2026-02-25  |
| 14 | `other` | REJECT: other: '' \| fail; not a recall page \| Provided page text is a recall listing page, not the specific recall detail page for the product in the row. Cannot verify details. |
| 15 | `other` | REJECT: other: '' \| fail; product mismatch; pathogen mismatch \| Product 'bottes en caoutchouc' not found on the provided page text; the reason for recall (phthalates) is not a pathogen in FSIS scope |
| 16 | `other` | REJECT: other: company mismatch \| company_is_distributor |
| 17 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Page is a listing index, not the specific recall fiche; also, the recall is for a motorcycle brake defect, not a food safety issue. |
| 18 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Page is a recall listing, not the specific recall fiche for the product. The product 'rainbow balloons' is not found on the page. |
| 19 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Provided page text is a recall listing page and does not contain the specific product mentioned in the row. |
| 20 | `date_pre_` | REJECT: date_pre_2026: date_pre_2026: 2025-09-02 \| fail; date on page=2025-09-02 \| date_pre_2026: recall published 2025-09-02, before FSIS scop |
| 21 | `other` | REJECT: other: '' \| fail; company mismatch; product mismatch; pathogen mismatch \| Pathogen (cadmium) is out of FSIS Tier-1 scope. |
| 22 | `date_pre_` | REJECT: date_pre_2026: Food - Dairy. Companies. Recalling firm: D. Tyers Foods International Inc. Published by. Canadian Food Inspection Agency. Audience. General public. Distribution. Ontario. Recall |
| 23 | `other` | REJECT: other: fail; not a recall page; company mismatch; product mismatch; pathogen mismatch; date on page= \| Page is a FAQ/informatio |
| 24 | `other` | REJECT: other: '' \| fail; company mismatch; pathogen mismatch \| Pathogen 'Noyade' is out of scope. |
| 25 | `other` | REJECT: other: fail; not a recall page; company mismatch; product mismatch; pathogen mismatch; date on page= \| Page is inaccessible or |
| 26 | `date_pre_` | REJECT: date_pre_2026: date_pre_2026: 2025-07-25 \| fix; date on page=2025-07-25 \| Row Date field is empty; page shows publication date 2025-07- |
| 27 | `date_pre_` | REJECT: date_pre_2026: date_pre_2026: 2023-03-21 \| fail; not a recall page; company mismatch; product mismatch; date on page=2023-03-21 \| Page |
| 28 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Provided page text is a general recall listing page and does not contain information about the specific product or recall. |
| 29 | `other` | REJECT: other: '' \| fail; not a recall page; pathogen mismatch \| Provided page text is a list of recalls, not the specific recall page for the URL, and the claimed hazard (heavy metals) is out of FS |
| 30 | `other` | REJECT: other: '' \| fail; not a recall page; product mismatch \| Product described in the row is not found on the provided page text, which appears to be a list of recalls rather than the specific re |
| 31 | `other` | REJECT: other: '' \| fail; not a recall page \| Provided page text is a recall listing page, not the specific recall page for the product, company, or date in the row's URL. |
| 32 | `other` | REJECT: other: fail; not a recall page; company mismatch; product mismatch; pathogen mismatch; date on page= \| Page is a news/policy ar |
| 33 | `other` | REJECT: other: '' \| fail; not a recall page; pathogen mismatch \| Pathogen (asbestos) is out of scope, and the provided page text is a listing page that does not contain the specific recall details. |

**Category breakdown:**

- `other`: 26
- `date_pre_`: 7

## After review — record your numbers here

Fill these in as you go, then commit the file again:

| Task | ✓ count | ✗ count | ? count | Clean rate |
|---|---:|---:|---:|---:|
| Promote classifier | __ / 20 | __ | __ | __% |
| Tier assignment | __ / 18 | __ | __ | __% |
| Field corrections | __ / 20 | __ | __ | __% |

**Decision threshold:** ≥85% ✓ on each task → proceed to training. <85% → write a filter step, re-export, re-audit.
