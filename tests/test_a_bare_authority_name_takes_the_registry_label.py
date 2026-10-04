"""A gap finder's bare authority name takes the registry label (2026-10-04).

The Czech gap finder writes Source "SZPI"; tools/monitored_sources calls it
"SZPI (CZ)". MASO WEST (Listeria, 2026-10-03) was the first Czech recall that
collector published, and it reached Recalls as "SZPI" — the same defect as the
Italian bare "Salute", one country later.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_bare_name_with_matching_country_maps():
    from pipeline.merge_master import registry_source_label as reg
    assert reg("SZPI", "Czechia") == "SZPI (CZ)"
    assert reg("GIS", "Poland") == "GIS (PL)"


def test_anything_else_is_left_alone():
    from pipeline.merge_master import registry_source_label as reg
    assert reg("SZPI", "Poland") == ""              # wrong country
    assert reg("SZPI (CZ)", "Czechia") == ""        # already a label
    assert reg("FDA", "United States") == ""        # not a bare short name
    assert reg("", "Czechia") == ""


def test_the_recalls_writer_applies_it_and_pending_keeps_the_raw_name(tmp_path):
    import pipeline.merge_master as m
    x = tmp_path / "r.xlsx"
    pub = {c: "" for c in m.RECALLS_SCHEMA}
    pub.update(Date="2026-10-03", Source="SZPI", Company="MASO WEST s.r.o.", Product="Italian salad",
               Pathogen="Listeria monocytogenes", Reason="Presence of Listeria monocytogenes",
               Class="Recall", Country="Czechia", Region="Europe", Tier=1, Outbreak=0,
               URL="https://www.szpi.gov.cz/clanek/varovani-pro-spotrebitele-listerie-ve-vlasskem-salatu.aspx")
    pen = {c: "" for c in m.PENDING_SCHEMA}
    pen.update(Date="2026-10-03", Source="SZPI", Company="X", Product="y", Country="Czechia",
               Pathogen="Salmonella", URL="https://www.szpi.gov.cz/x", Status="pending")
    m.save_xlsx_with_pending([pub], [pen], x)
    assert m._load_sheet(x, "Recalls", m.RECALLS_SCHEMA)[0]["Source"] == "SZPI (CZ)"
    assert m._load_sheet(x, "Pending", m.PENDING_SCHEMA)[0]["Source"] == "SZPI"
