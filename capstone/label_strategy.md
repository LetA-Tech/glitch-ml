# Capstone Label Strategy — Fraud (v0.1, Chapter 2)

> Chapter 2 component. Before we can train anything, we must define **the label**: what exactly
> is `is_fraud`, where does it come from, and what's hard about it. This is the supervised-
> classification framing of the whole capstone.

---

## 1. The problem type

Fraud detection = **supervised, binary classification.** Input = transaction features; output =
one of two categories (`fraud` / `not-fraud`). (Mellions parallel: swap the label for `category`
and it becomes multi-class classification.)

---

## 2. Defining the label precisely

"Fraud" is not self-evident — we must define it so labels are consistent.

- **Positive (`is_fraud = True`):** a transaction the account holder did **not** authorize, OR one
  confirmed fraudulent via chargeback / bank confirmation / analyst review.
- **Negative (`is_fraud = False`):** a transaction confirmed legitimate (no dispute within the
  chargeback window, or explicitly confirmed by the user).
- **Unknown / unlabeled:** brand-new transactions whose status isn't yet determined — these are
  what we **predict** at inference time.

> Ambiguity to resolve later: "friendly fraud" (user disputes a charge they actually made),
> and the lag before a transaction is confirmed fraud.

---

## 3. Where labels come from (and the timing trap)

| Source | Reliability | Lag |
|--------|-------------|-----|
| Chargebacks | high | weeks–months |
| Confirmed customer reports | high | days |
| Analyst manual review | high | hours–days |
| Heuristic auto-rules | low/noisy | instant |

- **At training time** the label is **known** (we use confirmed historical outcomes).
- **At inference time** the label is **unknown** — that's the whole point of predicting.
- **Timing trap (for Ch 4):** labels arrive *late* (a transaction may only be confirmed fraud
  months later). So a naive random train/test split can leak the future into the past → we'll
  prefer a **time-based split**.

---

## 4. The defining challenge — CLASS IMBALANCE ⚠️

Real fraud is **rare: ~0.1–2%** of transactions. Consequences:

- **Accuracy is a liar.** A model that predicts "not fraud" for *everything* scores ~99% accuracy
  and catches **zero** fraud. (We'll *prove* this in the Ch 2 coding exercise.)
- We need metrics that respect imbalance — **precision, recall, PR curves, cost-weighting** — the
  whole of **Chapter 7**.
- Training needs care: resampling, class weights, threshold tuning (later chapters).

---

## 5. Label quality caps everything

- **Noisy labels** (a fraud mislabeled legit, or vice-versa) put a ceiling on model quality —
  "garbage labels in, garbage model out."
- Label **coverage** matters: if only big frauds get reported, the model never learns small fraud.

---

## 6. Data contract update

Add to `data_contract.md`'s `is_fraud` field:
- nullable in production (unknown at scoring time);
- required + **trusted** in training data (must come from a confirmed source above);
- carry a `label_source` and `label_timestamp` so we can do time-based splits (Ch 4).

---

## 7. Open questions → carried forward
- Exact fraud definition incl. friendly fraud (Ch 2 → refine).
- Time-based split to avoid label leakage (Ch 4).
- Imbalance-aware metrics + thresholding (Ch 7).
- Resampling / class weights at training (Ch 5–6, 12).
