"""A reviewer must see a row whatever case the collector wrote its Status in.

WHY THIS EXISTS (2026-09-27)
===========================
All three reviewers selected their lane with

    str(r.get("Status", "")).strip() in <a set of lowercase statuses>

— strip, but no lower. Four collectors write the status capitalised:

    pipeline/extractor.py                 "Status": "Pending"
    pipeline/gap_finder/extractor.py      "Status": "Pending"     (43-country fleet)
    pipeline/gap_finder_gr/extractor.py   "Status": "Pending"     (Greece)
    pipeline/official_feeds/extractor.py  "Status": "Pending"     (RASFF + national feeds)

So a row from those collectors landed in Pending with Status "Pending", which
matched no reviewer's lane, and stayed there permanently. Not skipped, not
rejected, not retried — INVISIBLE. Reviewer 1, 2 and 3 all agreed it did not
exist.

merge_master is why nobody noticed: that module lowercases when it compares
(`(r.get("Status") or "").lower() == STATUS_REJECTED`), so the row passed the
merge cleanly. Only the reviewers were blind, and a reviewer that sees nothing
produces no error — it reports "0 rows in lane" and exits 0.

Measured on the 2026-09-27 workbook: 86 rows whose Status is canonical only
after lowercasing, across EIGHT countries —

    Spain (AESAN) 20 · Greece (EFET) 17 · Portugal (ASAE) 16 ·
    Nigeria (NAFDAC) 13 · South Africa (NCC) 10 · Italy (Salute) 8 ·
    Poland (GIS) 1 · Norway (Mattilsynet) 1

Fixed at both ends: the writers now emit the canonical lowercase value, and
every lane test lowercases before comparing. The second half is deliberate
redundancy — a Status is data arriving from a collector, and a reviewer that
can only see one capitalisation of it is one typo away from going blind to a
whole country again.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

REVIEWERS = {
    "reviewer 1 (url agent)": ROOT / "pipeline" / "recall_url_agent.py",
    "reviewer 2 (review agent)": ROOT / "pipeline" / "recall_review_agent.py",
    "reviewer 3 (confirm agent)": ROOT / "pipeline" / "recall_confirm_agent.py",
}

WRITERS = [
    ROOT / "pipeline" / "extractor.py",
    ROOT / "pipeline" / "gap_finder" / "extractor.py",
    ROOT / "pipeline" / "gap_finder_gr" / "extractor.py",
    ROOT / "pipeline" / "official_feeds" / "extractor.py",
]

CANONICAL = {
    "pending", "pending_gap", "pending_gap_v1", "pending_gap_v2",
    "pending_gap_v3", "pending_enrichment", "pending_retry", "rejected",
    "approved", "published", "",
}


class TestEveryLaneTestLowercases:

    @pytest.mark.parametrize("name", sorted(REVIEWERS))
    def test_no_status_read_compares_without_lowering(self, name):
        """Any `.get("Status"...).strip()` that is then COMPARED must lower
        first. This is the exact line shape that went blind."""
        src = REVIEWERS[name].read_text(encoding="utf-8")
        offenders = []
        for i, line in enumerate(src.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if '.get("Status"' not in line:
                continue
            # a read that is compared, or fed to a lane membership test
            if not re.search(r"\.strip\(\)\s*(?:in|==|!=)|\.strip\(\)\s*$", line):
                continue
            if ".lower()" in line:
                continue
            offenders.append(f"{i}: {line.strip()[:96]}")
        assert not offenders, (
            f"{name} compares a Status without lowercasing — a collector that "
            f"writes 'Pending' becomes invisible to it:\n  "
            + "\n  ".join(offenders))

    @pytest.mark.parametrize("name", sorted(REVIEWERS))
    def test_the_lane_is_still_a_lowercase_set(self, name):
        """Lowering the haystack only works while the needles are lowercase."""
        src = REVIEWERS[name].read_text(encoding="utf-8")
        for m in re.finditer(r'"(pending[a-z_0-9]*|rejected|approved)"', src):
            assert m.group(1) == m.group(1).lower()


class TestTheWritersEmitCanonicalStatuses:

    @pytest.mark.parametrize("path", WRITERS, ids=lambda p: p.name)
    def test_no_capitalised_status_literal(self, path):
        src = path.read_text(encoding="utf-8")
        bad = []
        for i, line in enumerate(src.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            for m in re.finditer(r'"Status"\s*:\s*"([^"]*)"', line):
                if m.group(1) not in CANONICAL:
                    bad.append(f"{i}: Status={m.group(1)!r}")
        assert not bad, (
            f"{path.name} writes a non-canonical Status. Four collectors wrote "
            f"'Pending' and put 86 rows across 8 countries beyond every "
            f"reviewer's reach:\n  " + "\n  ".join(bad))

    @pytest.mark.parametrize("path", WRITERS, ids=lambda p: p.name)
    def test_it_writes_pending_lowercase(self, path):
        src = path.read_text(encoding="utf-8")
        assert '"Status":      "pending"' in src or '"Status": "pending"' in src, (
            f"{path.name} no longer writes a pending status at all — check the "
            f"rename did not lose it")


class TestTheLanesTogetherCoverEveryWriteableStatus:
    """The union of the three lanes must admit every status a collector or a
    reviewer can write, case-insensitively. A status nothing reads is a row
    nothing reviews."""

    @staticmethod
    def _lane(path, names):
        src = path.read_text(encoding="utf-8")
        consts = dict(re.findall(r'^([A-Z_0-9]+)\s*=\s*"([^"]+)"', src, re.M))
        out = set()
        for blob in names:
            i = src.find(blob)
            if i < 0:
                continue
            seg = src[i:i + 400]
            out |= {x.lower() for x in re.findall(r'"([a-zA-Z_0-9]+)"', seg)}
            out |= {consts[c].lower() for c in re.findall(r"\b([A-Z_0-9]{4,})\b", seg)
                    if c in consts}
        return out

    def test_the_union_admits_pending_from_a_collector(self):
        union = set()
        union |= self._lane(REVIEWERS["reviewer 1 (url agent)"],
                            ["AGENT1_STATUSES = "])
        union |= self._lane(REVIEWERS["reviewer 2 (review agent)"],
                            ["AGENT2_STATUSES = "])
        union |= self._lane(REVIEWERS["reviewer 3 (confirm agent)"],
                            ["lane = [r for r in pending"])
        # What the collectors actually write, lowercased.
        for written in ("pending", "pending_gap", "pending_enrichment",
                        "pending_gap_v1", "pending_gap_v2", "pending_gap_v3"):
            assert written in union, (
                f"{written!r} is written by a collector or reviewer and read by "
                f"no lane — rows with it are invisible")


class TestTheHeredocsCarryTheFix:
    """Every reviewer is written to disk from a workflow heredoc, and reviewer
    2 and 3 have TWO each. A fix that lands only in the .py never runs."""

    PAIRS = [
        ("pipeline/recall_url_agent.py", "recall-url-agent.yml", "PYEOF_A1"),
        ("pipeline/recall_review_agent.py", "recall-review-agent.yml", "PYEOF_AGENT"),
        ("pipeline/recall_review_agent.py", "recallreviewagent.yml", "PYEOF_AGENT"),
        ("pipeline/recall_confirm_agent.py", "recall-confirm-agent.yml", "PYEOF_A3"),
        ("pipeline/recall_confirm_agent.py", "recallconfirmagent.yml", "PYEOF_A3"),
    ]

    @pytest.mark.parametrize("src,wf,tag", PAIRS, ids=lambda x: str(x)[:34])
    def test_the_embedded_copy_is_byte_identical(self, src, wf, tag):
        disk = (ROOT / src).read_text(encoding="utf-8")
        lines = (ROOT / ".github" / "workflows" / wf).read_text(
            encoding="utf-8").split("\n")
        s = next(i for i, l in enumerate(lines) if f"{tag}'" in l)
        e = next(i for i, l in enumerate(lines)
                 if l.strip() == tag and i > s)
        indent = 0
        for l in lines[s + 1:e]:
            if l.strip():
                indent = len(l) - len(l.lstrip())
                break
        embedded = "\n".join(l[indent:] if l.startswith(" " * indent) else l
                             for l in lines[s + 1:e])
        assert embedded.rstrip("\n") == disk.rstrip("\n"), (
            f"{wf} writes {src} from this heredoc — the case fix must be in "
            f"both or the reviewer stays blind in production")

    @pytest.mark.parametrize("src,wf,tag", PAIRS, ids=lambda x: str(x)[:34])
    def test_the_embedded_copy_lowercases(self, src, wf, tag):
        text = (ROOT / ".github" / "workflows" / wf).read_text(encoding="utf-8")
        assert '.strip().lower() in' in text, (
            f"{wf}'s embedded reviewer does not lowercase its lane test")
