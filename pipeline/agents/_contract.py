"""The proposal contract between the intel agent and the curator.

WHY TWO AGENTS AND NOT ONE
--------------------------
A single agent that researches and then writes to recalls.xlsx is the most
dangerous thing that could be added to this repo, and we have the receipts:

  * the gap-finder produced correct-looking product and pathogen fields and
    attached them to RappelConso fiche numbers that describe a SHEIN plush
    toy, a vitamin-D supplement and a mackerel histamine recall (verified
    2026-08-28). The publish gate passed all three: the URL was well-formed
    and it resolved. It resolved to the wrong recall.
  * scrapers have written page headlines into Company and Brand.
  * a bulk pattern-fix once corrupted 155 rows.
  * rows have been promoted carrying the regulator's own French and Italian
    wording, against a standing English-output rule.

Every one of those is a case of plausible output being trusted because it
looked right. So:

    the INTEL agent may only PROPOSE. It never opens the workbook.
    the CURATOR only applies proposals that DETERMINISTIC CODE re-verifies.

The curator is not a second opinion from a model. It is the publish gate,
a real HTTP fetch, a language check, a scope check and a duplicate check.
A model proposes; code disposes. A proposal with no evidence is refused,
not downgraded.

FILE LAYOUT
-----------
    docs/data/agent-proposals/<utc-stamp>-<agent>.json

One file per run, append-only, committed. The audit trail is the point:
every write to the register can be traced back to the proposal that asked
for it and the verdict that allowed it.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent.parent
PROPOSAL_DIR = ROOT / "docs" / "data" / "agent-proposals"

SCHEMA_VERSION = 1

# What a proposal may ask for. Anything else is refused by the curator.
ACTIONS = ("promote", "enrich", "reject", "link_event", "flag")

# Evidence kinds, strongest first. A proposal carrying only `model_inference`
# can never be applied — it may only raise a flag for a human.
EVIDENCE_KINDS = ("regulator_page", "regulator_api", "news_article",
                  "corpus_row", "model_inference")

APPLICABLE_WITHOUT_HUMAN = ("regulator_page", "regulator_api", "corpus_row")


@dataclass
class Evidence:
    kind: str
    url: str = ""
    quote: str = ""           # verbatim from the source, never paraphrased
    retrieved_utc: str = ""

    def is_hard(self) -> bool:
        return self.kind in APPLICABLE_WITHOUT_HUMAN


@dataclass
class Proposal:
    action: str
    target: Dict[str, Any]            # how to find the row: url / sheet+index
    changes: Dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    evidence: List[Evidence] = field(default_factory=list)
    confidence: float = 0.0
    agent: str = ""
    proposal_id: str = ""

    def __post_init__(self):
        if not self.proposal_id:
            blob = json.dumps({"a": self.action, "t": self.target,
                               "c": self.changes}, sort_keys=True,
                              ensure_ascii=False)
            self.proposal_id = hashlib.sha256(blob.encode()).hexdigest()[:16]

    def hard_evidence(self) -> List[Evidence]:
        return [e for e in self.evidence if e.is_hard()]

    def structural_problems(self) -> List[str]:
        """Faults visible without touching the network or the workbook."""
        out = []
        if self.action not in ACTIONS:
            out.append(f"unknown action {self.action!r}")
        for e in self.evidence:
            if e.kind not in EVIDENCE_KINDS:
                out.append(f"unknown evidence kind {e.kind!r}")
        if not self.evidence:
            out.append("no evidence at all")
        elif not self.hard_evidence() and self.action != "flag":
            out.append("only soft evidence — a model inference cannot move a "
                       "row on its own; the strongest action available is 'flag'")
        if self.action in ("promote", "enrich") and not self.target.get("url"):
            out.append("no target URL — a row's URL is its provenance")
        if self.action == "enrich" and not self.changes:
            out.append("enrich with no changes")
        if not (0.0 <= float(self.confidence or 0) <= 1.0):
            out.append("confidence outside 0..1")
        return out


def to_jsonable(p: Proposal) -> Dict[str, Any]:
    d = asdict(p)
    d["evidence"] = [asdict(e) for e in p.evidence]
    return d


def from_jsonable(d: Dict[str, Any]) -> Proposal:
    ev = [Evidence(**e) for e in d.get("evidence", [])]
    return Proposal(action=d.get("action", ""), target=d.get("target", {}),
                    changes=d.get("changes", {}), reason=d.get("reason", ""),
                    evidence=ev, confidence=float(d.get("confidence", 0) or 0),
                    agent=d.get("agent", ""),
                    proposal_id=d.get("proposal_id", ""))


def write_proposals(proposals: List[Proposal], agent: str,
                    out_dir: Path = PROPOSAL_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}-{agent}.json"
    path.write_text(json.dumps(
        {"schema": SCHEMA_VERSION, "agent": agent, "generated_utc": stamp,
         "proposals": [to_jsonable(p) for p in proposals]},
        indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def read_proposals(path: Path) -> List[Proposal]:
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    if d.get("schema") != SCHEMA_VERSION:
        raise SystemExit(f"{path}: schema {d.get('schema')} — this curator "
                         f"speaks version {SCHEMA_VERSION} only")
    return [from_jsonable(x) for x in d.get("proposals", [])]
