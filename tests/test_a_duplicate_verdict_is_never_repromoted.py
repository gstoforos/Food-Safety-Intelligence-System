"""A URL removed as a DUPLICATE must not come back (outbreak check 2026-11-08).

FDA's outbreak-investigation page for the Sierra Nevada Graziers E. coli
O26:H11 outbreak was removed on 2026-10-04 as duplicate_of_published_recall:
the firm's own FDA recall notice is published with the same 13 illnesses.
On 2026-10-28 merge_master re-promoted it through the reachability
exception, because reject_refusal() matched a stale "Unable to find the
official regulator URL" in the archive row's Notes. The copy was counted as
a second October outbreak and ranked #2 in the October top-10, marketing PDF
and social cards.

Two pins:
  1. merge_master vetoes every re-promotion exception when the archive
     verdict is duplicate_of_published_recall;
  2. the register itself: no Recalls URL carries that verdict in an archive.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def test_merge_master_vetoes_repromotion_of_a_duplicate():
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    veto = src.find('"duplicate_of_published_recall" in _prior_reason.lower()')
    assert veto != -1, "the duplicate-verdict veto is gone from merge_master"
    story = src.find("if _repromote_ok:\n                # Each exception tells")
    assert story != -1 and veto < story, (
        "the veto must run AFTER every exception and BEFORE the re-promotion "
        "is stamped, or one of the exceptions can still re-open a duplicate")


def test_no_published_url_carries_a_duplicate_verdict():
    import openpyxl
    from pipeline.merge_master import _normalize_url_for_dedup as norm

    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    dup = {}
    for name in ("Rejected", "Weekly_Rejected"):
        if name not in wb.sheetnames:
            continue
        rows = list(wb[name].values)
        head = [str(h) for h in rows[0]]
        iu = head.index("URL")
        cols = [head.index(h) for h in ("RejectReason", "RejectionReason")
                if h in head]
        for r in rows[1:]:
            text = " ".join(str(r[c] or "") for c in cols if c < len(r))
            if "duplicate_of_published_recall" in text.lower():
                u = norm(str(r[iu] or ""))
                if u:
                    dup[u] = text[:160]
    rows = list(wb["Recalls"].values)
    head = [str(h) for h in rows[0]]
    iu = head.index("URL")
    wb.close()
    back = [(r[iu], dup[norm(str(r[iu] or ""))]) for r in rows[1:]
            if norm(str(r[iu] or "")) in dup]
    assert not back, (
        "published rows whose URL an archive row removed as a duplicate of "
        "another published row:\n" + "\n".join(f"  {u}\n    {w}" for u, w in back))
