#!/usr/bin/env python3
"""Re-apply the 2026-08-28 operator review to the current workbook.

Content-keyed, not row-indexed, and idempotent: the live pipeline commits to
recalls.xlsx every few minutes, so a review written against row numbers is
stale before it can be pushed. Run this against whatever main holds.
"""
import sys, openpyxl
from datetime import date
from pathlib import Path
sys.path.insert(0, '.')
from pipeline.weekly_rejected_capture import record_rejections

X = Path('docs/data/recalls.xlsx')
wb = openpyxl.load_workbook(X); ws = wb['Recalls']
h = [str(c.value or "") for c in ws[1]]
def g(r, c): return ws.cell(row=r, column=h.index(c)+1).value
def s(r, c, v):
    if c in h: ws.cell(row=r, column=h.index(c)+1, value=v)
def note(r, txt):
    cur = str(g(r, 'Notes') or "")
    if txt[:40] in cur: return
    s(r, 'Notes', (cur + f" [operator review 2026-08-28: {txt}]").strip())
    s(r, 'LastUpdated', date.today().isoformat())

def find(pred):
    return [r for r in range(2, ws.max_row+1) if pred(r)]

# ── corrections ────────────────────────────────────────────────────────
NVWA = {
 "2026-08-27_Jumbo_Biologische": (
   "https://www.nvwa.nl/actueel/nieuws/2026/08/27/veiligheidswaarschuwing-biologische-brie-van-jumbo",
   "Jumbo Biologische brie", "2026-08-27"),
 "2026-08-26_Jumbo_Gorgonzola": (
   "https://www.nvwa.nl/actueel/nieuws/2026/08/26/veiligheidswaarschuwing-gorgonzola-dolce-van-jumbo",
   "Jumbo Gorgonzola Dolce (EAN 8718452637508; best-before 03/08/10-09-2026)", "2026-08-26"),
}
fixed = 0
for key, (url, prod, dt) in NVWA.items():
    for r in find(lambda r, k=key: k in str(g(r, 'URL') or '')):
        s(r,'Source','NVWA (NL)'); s(r,'Country','Netherlands'); s(r,'Region','Europe')
        s(r,'URL',url); s(r,'Product',prod)
        note(r, "regulator and country corrected. The row was filed as AFSCA / Belgium "
                "on a favv-afsca.be URL built to a slug pattern no verified AFSCA row in "
                "this register uses. Jumbo is a Dutch retailer and the notice is the NVWA "
                f"safety warning of {dt}, verified live at the URL now stored.")
        fixed += 1
for r in find(lambda r: 'CARREFOUR FRANCE CARREFOUR' in str(g(r,'Company') or '')
                        and 'boeuf' in str(g(r,'Product') or '').lower()):
    s(r,'Company','Carrefour Market Gometz-la-Ville')
    s(r,'Reason','Presence of Listeria monocytogenes')
    s(r,'Product','Rôti de boeuf cuit — sold unbranded at the traditional counter')
    note(r, "verified vs RappelConso fiche 23343. Reason was 'Presence of listéria "
            "monocytogènes' — French spelling in an English field, breaking the "
            "English-output rule. Company was the doubled scraper string "
            "'CARREFOUR FRANCE CARREFOUR'; the fiche names the distributor as "
            "Carrefour Market Gometz La Ville.")
    fixed += 1

# ── rejections ─────────────────────────────────────────────────────────
FSA_DUP = {'6841':'fsa-prin-41-2026','6837':'fsa-prin-40-2026','3176':'fsa-prin-32-2026',
           '3133':'fsa-prin-29-2026','3122':'fsa-prin-28-2026'}
def fsa_reason(ref):
    return (f"duplicate of {ref}, already published on the row citing "
            f"alerts.food.gov.uk/news-alerts/alert/{ref}. The FSA serves every alert at "
            f"TWO addresses — /news-alerts/alert/<ref> and /article/<id>/<slug> — and the "
            f"dedup key is URL-based, so the same alert publishes twice. This copy is the "
            f"thinner of the two; nothing in it was absent from the retained row.")
targets = []
for r in range(2, ws.max_row+1):
    u = str(g(r,'URL') or '')
    if 'alerts.food.gov.uk/article/' in u:
        aid = u.split('/article/')[1].split('/')[0]
        if aid in FSA_DUP: targets.append((r, fsa_reason(FSA_DUP[aid])))
    elif 'shanghai-ravioli' in u:
        targets.append((r,
          "no hazard in AFTS scope, and the pathogen was fabricated. Pathogen read "
          "'Hepatitis A virus'; the FSIS notice and Food Safety News both state the recall "
          "is for product 'produced without the benefit of inspection' bearing a false mark "
          "for EST. 18004, with 'no confirmed reports of illness or injury'. No pathogen, "
          "biotoxin, mycotoxin, foreign material, pest or chemical hazard is named. Product "
          "and Reason were also truncated mid-word. The fabricated pathogen had already "
          "reached recalls.json and the 08-26 daily brief."))
    elif '2026-08-27_Monoprix_Gorgonzola' in u:
        targets.append((r,
          "Monoprix is a French retailer with no Belgian estate, so a Belgian AFSCA recall "
          "by Monoprix cannot exist. The real notice is the RappelConso fiche 23340 row of "
          "2026-08-26 in this register — Les Fromagers de Saint Omer, brand Casa Azzurra, "
          "gorgonzola AOP doux — whose distribution list names Monoprix as one of eight "
          "chains. The chain was lifted from that list and a Belgian notice constructed "
          "around it. It was ranked the #2 threat in the W35 weekly."))
    elif '2026-08-21_MeatMore' in u:
        targets.append((r,
          "unverifiable provenance. The favv-afsca.be URL follows the same constructed slug "
          "pattern as the three other rows added on 2026-08-28, all of which proved wrong: "
          "one impossible, two mis-attributed to Belgium when they are Dutch NVWA notices. "
          "AFSCA's real URLs here take the form /fr/produits/rappel-de-<company>-<n>. "
          "Meat & More is a genuine Belgian chain so the recall may well be real, but "
          "nothing here can be verified. Archived, not deleted."))

archived = []
for r, why in targets:
    row = {c: g(r, c) for c in h}
    row['RejectReason'] = why; row['RejectedBy'] = 'operator-review'
    archived.append(row)
for r, _ in sorted(targets, key=lambda t: -t[0]):
    ws.delete_rows(r, 1)
wb.save(X)
print(f"corrected {fixed} row(s); rejected {len(targets)}")
if archived: print("archived:", record_rejections(archived, X))
