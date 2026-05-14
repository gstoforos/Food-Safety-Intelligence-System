#!/usr/bin/env python3
"""
quality_sample.py
=================

Renders a hand-reviewable Markdown report of N random training examples per
task, plus a rejection-category audit that exposes which raw assistant
messages got bucketed as "other".

Inputs:
    --data-dir   directory containing *.train.jsonl files from export v2
    --out        path to write the Markdown report to
    --n          examples per task (default 20)
    --seed       RNG seed (default 42)

Output: one Markdown file with stratified samples + audit sections.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ─── JSONL I/O ────────────────────────────────────────────────────────────

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


# ─── Label extractors ─────────────────────────────────────────────────────

def promote_label(example: Dict[str, Any]) -> str:
    """Extract 'promote' or 'reject' from the assistant message."""
    asst = example["messages"][-1]["content"].strip()
    return "reject" if asst.upper().startswith("REJECT") else "promote"


def promote_category(example: Dict[str, Any]) -> str:
    """For reject examples, extract the parsed category."""
    asst = example["messages"][-1]["content"].strip()
    m = re.match(r"REJECT:\s*(?P<cat>[a-z_]+)", asst, re.IGNORECASE)
    return m.group("cat").lower() if m else ""


def tier_label(example: Dict[str, Any]) -> str:
    """Extract '1' / '2' / '3' from tier_assignment example."""
    return example["messages"][-1]["content"].strip()


# ─── Stratified samplers ──────────────────────────────────────────────────

def sample_promote(examples: List[Dict[str, Any]],
                   n: int, seed: int) -> List[Tuple[int, Dict[str, Any]]]:
    """Return (original_index, example) tuples, half promote, half reject."""
    rng = random.Random(seed)
    promote = [(i, e) for i, e in enumerate(examples)
               if promote_label(e) == "promote"]
    reject = [(i, e) for i, e in enumerate(examples)
              if promote_label(e) == "reject"]
    rng.shuffle(promote)
    rng.shuffle(reject)
    n_each = max(1, n // 2)
    samples = promote[:n_each] + reject[:n_each]
    rng.shuffle(samples)
    return samples


def sample_tier(examples: List[Dict[str, Any]],
                n: int, seed: int) -> List[Tuple[int, Dict[str, Any]]]:
    """Stratified by Tier (1, 2, 3)."""
    rng = random.Random(seed)
    by_tier = defaultdict(list)
    for i, e in enumerate(examples):
        by_tier[tier_label(e)].append((i, e))
    samples = []
    n_per_tier = max(1, n // 3)
    for t in ("1", "2", "3"):
        bucket = by_tier.get(t, [])
        rng.shuffle(bucket)
        samples.extend(bucket[:n_per_tier])
    rng.shuffle(samples)
    return samples


def sample_random(examples: List[Dict[str, Any]],
                  n: int, seed: int) -> List[Tuple[int, Dict[str, Any]]]:
    rng = random.Random(seed)
    idx = list(range(len(examples)))
    rng.shuffle(idx)
    return [(i, examples[i]) for i in idx[:n]]


# ─── Markdown rendering ───────────────────────────────────────────────────

def render_example(sample_num: int,
                   task_name: str,
                   gold_summary: str,
                   example: Dict[str, Any]) -> str:
    user = example["messages"][1]["content"]
    asst = example["messages"][-1]["content"]

    parts = []
    parts.append(f"### Sample {sample_num} — {task_name} — gold: `{gold_summary}`")
    parts.append("")
    parts.append("**Model input:**")
    parts.append("")
    parts.append("```")
    parts.append(user[:1800].rstrip())
    parts.append("```")
    parts.append("")
    parts.append("**Gold output:**")
    parts.append("")
    parts.append("```")
    parts.append(asst[:600].rstrip())
    parts.append("```")
    parts.append("")
    parts.append("**Your verdict** (edit this line — click the checkbox in the GH UI):")
    parts.append("")
    parts.append("- [ ] ✓ correct, trainable as-is")
    parts.append("- [ ] ✗ wrong (input is garbage OR gold label is wrong)")
    parts.append("- [ ] ? borderline — note reason below")
    parts.append("")
    parts.append("_Notes:_")
    parts.append("")
    parts.append("---")
    parts.append("")
    return "\n".join(parts)


def render_section_header(title: str, description: str = "") -> str:
    out = [f"## {title}", ""]
    if description:
        out.append(description)
        out.append("")
    return "\n".join(out)


def render_rejection_audit(reject_examples: List[Dict[str, Any]]) -> str:
    """Dump each reject example's parsed category + raw assistant message."""
    out = []
    out.append("## Rejection-category audit")
    out.append("")
    out.append("Every reject example, showing parsed category vs raw assistant message.")
    out.append("Look for examples bucketed as `other` that should have been a specific category.")
    out.append("")
    out.append("| # | Category | Raw assistant message (truncated) |")
    out.append("|---:|---|---|")
    for i, ex in enumerate(reject_examples, 1):
        asst = ex["messages"][-1]["content"].strip()
        cat = promote_category(ex)
        # Markdown-escape pipes and newlines
        asst_safe = asst.replace("|", "\\|").replace("\n", " ")[:200]
        out.append(f"| {i} | `{cat}` | {asst_safe} |")
    out.append("")
    out.append("**Category breakdown:**")
    out.append("")
    cats = Counter(promote_category(e) for e in reject_examples)
    for cat, n in cats.most_common():
        out.append(f"- `{cat}`: {n}")
    out.append("")
    return "\n".join(out)


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path,
                    default=Path("tools/training/data/v2"))
    ap.add_argument("--out", type=Path,
                    default=Path("docs/audits/quality_samples.md"))
    ap.add_argument("--n", type=int, default=20,
                    help="Samples per task")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    promote_path = args.data_dir / "promote_classifier.train.jsonl"
    tier_path = args.data_dir / "tier_assignment.train.jsonl"
    fix_path = args.data_dir / "field_corrections.train.jsonl"

    promote_ex = read_jsonl(promote_path)
    tier_ex = read_jsonl(tier_path)
    fix_ex = read_jsonl(fix_path)

    if not (promote_ex or tier_ex or fix_ex):
        print(f"ERROR: no JSONL files found in {args.data_dir}")
        print(f"       expected at least one of:")
        print(f"         {promote_path}")
        print(f"         {tier_path}")
        print(f"         {fix_path}")
        raise SystemExit(1)

    print(f"Loaded: promote={len(promote_ex)}, tier={len(tier_ex)}, fix={len(fix_ex)}")

    # ── Sample ──
    promote_samples = sample_promote(promote_ex, args.n, args.seed)
    tier_samples = sample_tier(tier_ex, args.n, args.seed)
    fix_samples = sample_random(fix_ex, min(args.n, len(fix_ex)), args.seed)

    # ── Render report ──
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    md = []
    md.append(f"# AFTS Training Data — Quality Audit Sample — {today}")
    md.append("")
    md.append(f"Random samples for hand-review. **Goal: read each, mark ✓ / ✗ / ?**")
    md.append(f"in the verdict checklist below each example.")
    md.append("")
    md.append(f"- Promote classifier samples: **{len(promote_samples)}** "
              f"(out of {len(promote_ex)} total)")
    md.append(f"- Tier assignment samples: **{len(tier_samples)}** "
              f"(out of {len(tier_ex)} total)")
    md.append(f"- Field correction samples: **{len(fix_samples)}** "
              f"(out of {len(fix_ex)} total)")
    md.append("")
    md.append(f"Seed: `{args.seed}` — same seed gives the same samples for reproducibility.")
    md.append("")
    md.append("> **How to use this report:** read each sample, judge whether the gold label")
    md.append("> is correct given the input. If gold is wrong, mark ✗ and add a one-line")
    md.append("> reason. We'll aim for **≥85% ✓** as the threshold to proceed to training.")
    md.append("")
    md.append("---")
    md.append("")

    # ── Promote classifier section ──
    md.append(render_section_header(
        f"Task 1 — Promote classifier ({len(promote_samples)} samples)",
        "Stratified: half promote, half reject."))
    sample_num = 1
    for orig_idx, ex in promote_samples:
        label = promote_label(ex)
        if label == "reject":
            gold = f"REJECT: {promote_category(ex)}"
        else:
            gold = "PROMOTE"
        md.append(render_example(sample_num, "Promote classifier", gold, ex))
        sample_num += 1

    # ── Tier assignment section ──
    md.append(render_section_header(
        f"Task 2 — Tier assignment ({len(tier_samples)} samples)",
        "Stratified across Tier 1, 2, 3."))
    sample_num = 1
    for orig_idx, ex in tier_samples:
        gold = f"Tier {tier_label(ex)}"
        md.append(render_example(sample_num, "Tier assignment", gold, ex))
        sample_num += 1

    # ── Field corrections section ──
    md.append(render_section_header(
        f"Task 3 — Field corrections ({len(fix_samples)} samples)",
        "Random sample of the FIX-extraction examples."))
    sample_num = 1
    for orig_idx, ex in fix_samples:
        asst = ex["messages"][-1]["content"]
        try:
            corr = json.loads(asst).get("corrections", {})
            gold = f"fix {', '.join(corr.keys())}"
        except json.JSONDecodeError:
            gold = "fix"
        md.append(render_example(sample_num, "Field corrections", gold, ex))
        sample_num += 1

    # ── Rejection-category audit ──
    reject_only = [e for e in promote_ex if promote_label(e) == "reject"]
    md.append(render_rejection_audit(reject_only))

    # ── Audit checklist summary ──
    md.append("## After review — record your numbers here")
    md.append("")
    md.append("Fill these in as you go, then commit the file again:")
    md.append("")
    md.append("| Task | ✓ count | ✗ count | ? count | Clean rate |")
    md.append("|---|---:|---:|---:|---:|")
    md.append(f"| Promote classifier | __ / {len(promote_samples)} | __ | __ | __% |")
    md.append(f"| Tier assignment | __ / {len(tier_samples)} | __ | __ | __% |")
    md.append(f"| Field corrections | __ / {len(fix_samples)} | __ | __ | __% |")
    md.append("")
    md.append("**Decision threshold:** ≥85% ✓ on each task → proceed to training. "
              "<85% → write a filter step, re-export, re-audit.")
    md.append("")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {args.out} ({args.out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
