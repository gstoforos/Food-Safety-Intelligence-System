"""A semicolon is not a language border (2026-10-01).

englishify_reason() — run by recall_confirm_agent on every Reason it
publishes — split on "; " and kept the most English-looking clause. On the
register that cut the pumpkin-seed outbreak Reason (rows for fiches 23359 /
23360) from 311 characters to 53.
"""
from pipeline._language import englishify_reason, split_bilingual

PUMPKIN = ("Detection of Salmonella. Ministère de l'Agriculture, 'Cas groupés de "
           "salmonellose : retrait-rappel de graines de courge' (2026-09-04): 81 cases "
           "of one Salmonella Enteritidis strain isolated 3 May - 12 Aug 2026, pumpkin "
           "seeds from a Lot-et-Garonne processor; the notice cites this fiche (23359) "
           "and 23360 by URL.")


def test_the_outbreak_reason_survives():
    out, _ = englishify_reason(PUMPKIN)
    assert "81 cases" in out and len(out) >= len(PUMPKIN) - 5


def test_a_french_list_keeps_every_item():
    s = ("Rillettes de canard au foie de canard 180 g; "
         "Rillettes de porc à l'ancienne pot de verre 220 g")
    assert split_bilingual(s) is None


def test_a_real_rasff_slash_split_still_works():
    s = ("Fumonisina en Harina de Maiz BIO procedente de Italia // "
         "Fumonisin in organic maize flour from Italy")
    out = split_bilingual(s)
    assert out and out.startswith("Fumonisin in organic")
