#!/usr/bin/env python3
"""
train_tier_classifier.py
========================

Fine-tunes DeBERTa-v3-base as a 3-class FSIS Tier classifier (Tier 1 / 2 / 3)
on the v2.2 tier_assignment.train.jsonl dataset.

Designed to run inside a GitHub Actions Ubuntu runner — no GPU required.
Training a 184M-param encoder on ~550 examples takes 15-25 min on the
2-vCPU runner. Memory peak is ~3 GB (fits in the 7 GB available).

Inputs:
    --train-jsonl   path to tier_assignment.train.jsonl
    --val-jsonl     path to tier_assignment.val.jsonl
    --out-dir       output directory for model + tokenizer + metrics

Output structure:
    out-dir/
        model.safetensors          fine-tuned weights
        config.json                model config
        tokenizer.json + vocab     tokenizer files
        metrics.json               train/val loss + accuracy + F1
        confusion_matrix.txt       3x3 confusion matrix on val set
        val_predictions.jsonl      per-example predictions on val
        training_args.json         reproducibility info

Usage:
    python train_tier_classifier.py \\
        --train-jsonl tools/training/data/v2_2/tier_assignment.train.jsonl \\
        --val-jsonl tools/training/data/v2_2/tier_assignment.val.jsonl \\
        --out-dir tools/fsis-lm/tier-classifier-v1/
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np


# ─── Data loading ─────────────────────────────────────────────────────────

def load_chat_jsonl(path: Path) -> List[Tuple[str, int]]:
    """Load a chat-template JSONL and extract (input_text, tier_label) pairs.

    The chat-format JSONL has messages: system / user / assistant. We use
    the user message as input and the assistant message as the gold label
    (a single character: "1", "2", or "3").
    """
    pairs = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            msgs = obj["messages"]
            user_text = next(m["content"] for m in msgs if m["role"] == "user")
            asst_text = next(m["content"] for m in msgs if m["role"] == "assistant")
            label_char = asst_text.strip()[0]
            if label_char not in "123":
                continue
            label = int(label_char) - 1  # 0-indexed (Tier 1 → 0, Tier 2 → 1, Tier 3 → 2)
            pairs.append((user_text, label))
    return pairs


# ─── Training ─────────────────────────────────────────────────────────────

def train_and_evaluate(args):
    # Heavy imports deferred so --help works without HF stack installed
    import torch
    from transformers import (
        AutoTokenizer, AutoModelForSequenceClassification,
        DataCollatorWithPadding, Trainer, TrainingArguments,
        set_seed,
    )
    from datasets import Dataset
    from sklearn.metrics import (
        accuracy_score, f1_score, precision_recall_fscore_support,
        confusion_matrix, classification_report,
    )

    set_seed(args.seed)
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    print(f"Loading training data: {args.train_jsonl}")
    train_pairs = load_chat_jsonl(args.train_jsonl)
    print(f"  {len(train_pairs)} training examples")
    if not train_pairs:
        print("ERROR: no training examples found", file=sys.stderr)
        sys.exit(1)

    print(f"Loading validation data: {args.val_jsonl}")
    val_pairs = load_chat_jsonl(args.val_jsonl)
    print(f"  {len(val_pairs)} validation examples")

    # ── Class distribution ──
    from collections import Counter
    train_dist = Counter(p[1] for p in train_pairs)
    val_dist = Counter(p[1] for p in val_pairs)
    print(f"  train tier dist: { {f'Tier {k+1}': v for k, v in sorted(train_dist.items())} }")
    print(f"  val tier dist:   { {f'Tier {k+1}': v for k, v in sorted(val_dist.items())} }")

    # ── Tokenizer + model ──
    print(f"\nLoading tokenizer + model: {args.base_model}")
    tokenizer = AutoTokenizer.from_pretrained(args.base_model)
    model = AutoModelForSequenceClassification.from_pretrained(
        args.base_model,
        num_labels=3,
        id2label={0: "Tier 1", 1: "Tier 2", 2: "Tier 3"},
        label2id={"Tier 1": 0, "Tier 2": 1, "Tier 3": 2},
    )

    # ── Tokenize ──
    def tokenize(examples):
        return tokenizer(
            examples["text"],
            truncation=True,
            max_length=args.max_length,
        )

    train_ds = Dataset.from_dict({
        "text": [p[0] for p in train_pairs],
        "label": [p[1] for p in train_pairs],
    }).map(tokenize, batched=True, remove_columns=["text"])
    val_ds = Dataset.from_dict({
        "text": [p[0] for p in val_pairs],
        "label": [p[1] for p in val_pairs],
    }).map(tokenize, batched=True, remove_columns=["text"])

    collator = DataCollatorWithPadding(tokenizer=tokenizer)

    # ── Class weights (handle Tier 3 imbalance) ──
    n_total = sum(train_dist.values())
    class_weights = torch.tensor([
        n_total / (3 * train_dist.get(i, 1)) for i in range(3)
    ], dtype=torch.float32)
    print(f"  class weights: {class_weights.tolist()}")

    # Custom trainer that applies class weights
    class WeightedTrainer(Trainer):
        def compute_loss(self, model, inputs, return_outputs=False,
                         num_items_in_batch=None):
            labels = inputs.pop("labels")
            outputs = model(**inputs)
            logits = outputs.logits
            # Upcast logits to float32 for the loss computation. transformers
            # 5.x may return Half-precision logits even on CPU, which mismatches
            # our float32 class_weights and CPU fp16 is slow/quirky anyway.
            logits_fp32 = logits.float()
            weight = class_weights.to(device=logits.device, dtype=torch.float32)
            loss_fct = torch.nn.CrossEntropyLoss(weight=weight)
            loss = loss_fct(logits_fp32, labels)
            return (loss, outputs) if return_outputs else loss

    # ── Metrics ──
    def compute_metrics(eval_pred):
        logits, labels = eval_pred
        preds = np.argmax(logits, axis=-1)
        acc = accuracy_score(labels, preds)
        f1_macro = f1_score(labels, preds, average="macro", zero_division=0)
        f1_weighted = f1_score(labels, preds, average="weighted", zero_division=0)
        return {
            "accuracy": acc,
            "f1_macro": f1_macro,
            "f1_weighted": f1_weighted,
        }

    # ── Training args ──
    args.out_dir.mkdir(parents=True, exist_ok=True)
    training_args = TrainingArguments(
        output_dir=str(args.out_dir / "_checkpoints"),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.lr,
        warmup_ratio=0.1,
        weight_decay=0.01,
        logging_steps=20,
        eval_strategy="epoch",
        save_strategy="no",  # we save manually at end
        report_to="none",
        seed=args.seed,
        fp16=False,    # CPU-only run
        dataloader_num_workers=0,
        disable_tqdm=False,
        load_best_model_at_end=False,
    )

    # transformers >= 4.46 renamed `tokenizer=` to `processing_class=`.
    # transformers 5.x removed `tokenizer=` entirely. Detect at runtime.
    import inspect
    trainer_kwargs = dict(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        data_collator=collator,
        compute_metrics=compute_metrics,
    )
    sig = inspect.signature(Trainer.__init__)
    if "processing_class" in sig.parameters:
        trainer_kwargs["processing_class"] = tokenizer
    elif "tokenizer" in sig.parameters:
        trainer_kwargs["tokenizer"] = tokenizer
    # else: omit — DataCollatorWithPadding already holds the tokenizer

    trainer = WeightedTrainer(**trainer_kwargs)

    print(f"\n══════════ TRAINING ══════════")
    train_result = trainer.train()
    print(f"\nTraining complete.")
    print(f"  final train loss: {train_result.training_loss:.4f}")
    print(f"  total steps:      {train_result.global_step}")

    # ── Final eval ──
    print(f"\n══════════ FINAL EVAL ══════════")
    eval_results = trainer.evaluate()
    for k, v in eval_results.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.4f}")
        else:
            print(f"  {k}: {v}")

    # ── Predictions + confusion matrix ──
    print(f"\n══════════ PREDICTIONS ══════════")
    predictions = trainer.predict(val_ds)
    preds = np.argmax(predictions.predictions, axis=-1)
    labels = predictions.label_ids
    cm = confusion_matrix(labels, preds, labels=[0, 1, 2])
    report = classification_report(
        labels, preds,
        labels=[0, 1, 2],
        target_names=["Tier 1", "Tier 2", "Tier 3"],
        digits=4, zero_division=0,
    )
    print("\nConfusion matrix (rows=true, cols=pred):")
    print(f"          Tier1  Tier2  Tier3")
    for i, name in enumerate(["Tier 1", "Tier 2", "Tier 3"]):
        print(f"  {name}  {cm[i][0]:5d}  {cm[i][1]:5d}  {cm[i][2]:5d}")
    print(f"\nClassification report:")
    print(report)

    # ── Save model + tokenizer ──
    print(f"\n══════════ SAVING ══════════")
    model.save_pretrained(args.out_dir)
    tokenizer.save_pretrained(args.out_dir)
    print(f"  model + tokenizer → {args.out_dir}")

    # ── Save metrics ──
    metrics = {
        "trained_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "base_model": args.base_model,
        "n_train": len(train_pairs),
        "n_val": len(val_pairs),
        "train_class_dist": {f"Tier {k+1}": v
                             for k, v in sorted(train_dist.items())},
        "val_class_dist": {f"Tier {k+1}": v
                           for k, v in sorted(val_dist.items())},
        "class_weights": class_weights.tolist(),
        "epochs": args.epochs,
        "lr": args.lr,
        "batch_size": args.batch_size,
        "max_length": args.max_length,
        "seed": args.seed,
        "final_train_loss": float(train_result.training_loss),
        "eval": {k: (float(v) if isinstance(v, (int, float)) else v)
                 for k, v in eval_results.items()},
        "confusion_matrix": cm.tolist(),
    }
    (args.out_dir / "metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"  metrics → {args.out_dir / 'metrics.json'}")

    # ── Save confusion matrix as text ──
    (args.out_dir / "confusion_matrix.txt").write_text(
        f"Confusion matrix (rows=true, cols=pred):\n\n"
        f"          Tier1  Tier2  Tier3\n" +
        "\n".join(
            f"  Tier {i+1}  {cm[i][0]:5d}  {cm[i][1]:5d}  {cm[i][2]:5d}"
            for i in range(3)
        ) + "\n\n" + report,
        encoding="utf-8",
    )

    # ── Save val predictions ──
    with (args.out_dir / "val_predictions.jsonl").open("w", encoding="utf-8") as f:
        # Cast to float32 + subtract max for numerically stable softmax
        # (predictions may come back as Half from a fp16-leaning model).
        raw = predictions.predictions.astype(np.float32)
        raw -= raw.max(axis=-1, keepdims=True)
        exp = np.exp(raw)
        probs = exp / exp.sum(axis=-1, keepdims=True)
        for i, ((text, gold), pred, p) in enumerate(zip(val_pairs, preds, probs)):
            f.write(json.dumps({
                "i": i,
                "input": text[:500],
                "gold_tier": int(gold) + 1,
                "pred_tier": int(pred) + 1,
                "correct": bool(int(gold) == int(pred)),
                "probs": {f"Tier {k+1}": float(p[k]) for k in range(3)},
            }, ensure_ascii=False) + "\n")

    # ── Clean up checkpoint dir ──
    import shutil
    ckpt_dir = args.out_dir / "_checkpoints"
    if ckpt_dir.exists():
        shutil.rmtree(ckpt_dir)

    # ── Final adequacy gate ──
    print(f"\n══════════ TRAINING GATE ══════════")
    target_acc = 0.85
    target_f1 = 0.75  # macro F1, harder to game with imbalance
    acc = eval_results.get("eval_accuracy", 0)
    f1m = eval_results.get("eval_f1_macro", 0)
    if acc >= target_acc and f1m >= target_f1:
        print(f"✓ PASS: accuracy={acc:.3f} (≥{target_acc}) and "
              f"macro-F1={f1m:.3f} (≥{target_f1})")
        print(f"  Model is ready for shadow-mode evaluation.")
    else:
        print(f"⚠ BELOW THRESHOLD: accuracy={acc:.3f} (target ≥{target_acc}), "
              f"macro-F1={f1m:.3f} (target ≥{target_f1})")
        print(f"  Inspect val_predictions.jsonl for failure cases.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-jsonl", type=Path, required=True)
    ap.add_argument("--val-jsonl", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--base-model", default="microsoft/MiniLM-L12-H384-uncased")
    ap.add_argument("--epochs", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--lr", type=float, default=2e-5)
    ap.add_argument("--max-length", type=int, default=128)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if not args.train_jsonl.exists():
        print(f"ERROR: training file not found: {args.train_jsonl}",
              file=sys.stderr)
        sys.exit(1)
    if not args.val_jsonl.exists():
        print(f"ERROR: validation file not found: {args.val_jsonl}",
              file=sys.stderr)
        sys.exit(1)

    train_and_evaluate(args)


if __name__ == "__main__":
    main()
