# Arista Invoice Analytics — demo

Standalone demo for **Arista Logistics (Λαμία)**: reads sales invoices of
Papadopoulou products (biscuits, rusks, bread), converts every invoice line to
kilograms using the product master (net weight per piece, pieces per case) and
reports **kg sold per product code, per customer market, per date range**.

The client's requirement, verbatim:

> Θέλω να παίρνω αποτέλεσμα εξόδου προϊόντων μετρημένα σε κιλά και να
> υπολογίζει πόσα κιλά έχω πουλήσει ανά κωδικό για ένα συγκεκριμένο χρονικό
> διάστημα και για συγκεκριμένη κατηγορία πελατών.

## Run the demo

Open `arista/index.html` in any modern browser (no server, no dependencies,
works offline). Then:

1. Press **▶ Demo δεδομένα** to load a deterministic 9‑month sample
   (~1.400 invoices, 25 customers, 21 product codes).
2. Press **Παρουσίαση** for a 7‑step guided walkthrough for the client meeting.
3. Go to **Αναφορά Κιλών**, pick a date range and a customer category
   (Μεγάλα Supermarket / Μικρή αγορά) and read kg per code. Export to CSV
   (opens in Excel) or print to PDF.

Opening `index.html#demo` loads the demo data automatically.

## What it does

| Tab | Purpose |
|---|---|
| Επισκόπηση | KPI tiles, kg per month by market, top‑10 codes, market share, top customers |
| Αναφορά Κιλών | kg per product code for a date range × customer category, with per‑market split, sortable table, charts, CSV/PDF export, per‑code customer drill‑down |
| Τιμολόγια | all parsed invoices; click one to see how each line was converted to kg |
| Εισαγωγή Τιμολογίων | import CSV/JSON exports, or paste raw invoice text (OCR simulation) and watch the parser extract customer, date, codes and quantities |
| Πελάτες & Αγορές | customer master with market classification (rule‑based keywords + manual override) |
| Κωδικοί Προϊόντων | product master: net kg per piece, pieces per case (editable) |
| Πώς λειτουργεί | architecture and production roadmap for the presentation |

Computation:

```
kg(line)  = pieces × netKg(code)
pieces    = qty            if unit = ΤΕΜ
          = qty × perCase  if unit = ΚΙΒ
```

Two customer markets are tracked: **BIG** (Σκλαβενίτης, ΑΒ, Lidl, Μασούτης,
Γαλαξίας, My Market, Market In, Κρητικός, …) and **SMALL** (mini markets,
ψιλικά, περίπτερα, φούρνοι, παντοπωλεία). Changing a customer's market
re-computes every report, including historical invoices.

All state is kept in the browser's `localStorage`; **Διαγραφή όλων** on the
invoices tab clears the invoices.

## CSV import format

```
invoice_no,date,customer,code,qty,unit
ΤΔΑ-90501,15/09/2026,ΣΚΛΑΒΕΝΙΤΗΣ ΑΕΕ – Λαμία (Κέντρο),1001,10,ΚΙΒ
```

Greek or English headers are accepted (`Αρ. τιμολογίου`, `Ημερομηνία`,
`Πελάτης`/`ΑΦΜ`, `Κωδικός`, `Ποσότητα`, `Μονάδα`). Delimiter can be `,`, `;`
or tab. A customer that is not in the master is created and classified by the
keyword rules. See `sample-invoices.csv`.

## Production version (not in this demo)

* Invoice acquisition: myDATA (ΑΑΔΕ) API, ERP export (SoftOne / Epsilon /
  Entersoft / Pylon) or PDF folder + OCR.
* Product master synced with the official Papadopoulou price list.
* Nightly ingestion, monthly report e‑mailed automatically.
* Multi‑user web app with login, history and backups.

The product codes, weights, customer names and ΑΦΜ values in the demo are
illustrative only.

## Entry gate

The page opens with a joke gate: *Ποια είναι η καλύτερη ομάδα στον κόσμο;*
Only the ΠΑΟΚ button can be clicked. The other options dodge the cursor on
desktop and jump away on touch, with a rotating taunt line. Passing the gate is
remembered for the browser tab via `sessionStorage` and auto-loads the demo
data, so a shared link opens straight into a working app.

The page is also published as a shareable artifact. CSV export there goes
through the artifact `downloads` capability; opened as a local file it falls
back to an ordinary browser download.
