# Scorecard — Chapter 1 (What is Machine Learning?)

> Status: **FINAL** — locked after Test 1 (knowledge) and Test 2 (coding) reviewed.

| Area | Score /10 | Notes |
|------|-----------|-------|
| Conceptual understanding | 8 | Grasped the programming→ML inversion and model-as-output after one correction. Reached ahead to overfitting unprompted. |
| Practical coding ability | 8 | Strong upward trajectory. Threshold sweep: debugged `NameError` + accuracy-vs-label bug independently, clean `max(..., key=...)`. Two-feature stretch: correct AND/OR models written solo, and instinctively added a *separate* `predict_with_or` to keep comparisons clean. Produced and can interpret a counterintuitive result. Minor refactor available: `accuracy`/`accuracy_with_or` duplicate logic — could pass the predict fn or a `rule` param. |
| Ability to explain | 7 | Clear ideas; tighten *feature vs label* precision when speaking. |
| Interview readiness | 7 | Has the vocabulary; needs to rehearse the 40-sec answer aloud. |
| Connect to real AI/DE systems | 8 | Plaid/categorization example was spot-on; linked fraud ↔ Mellions naturally. |
| Capstone progress | 7 | Data contract drafted; label definition deferred to Ch 2. |
| Notebook quality | 8 | `ch01_what_is_ml.md` complete and well structured. |

**Final average: 7.6 / 10.** ✅ Cleared to advance to Chapter 2. (Threshold deep-dive, sweep exercise, AND two-feature stretch all completed — the last produced a counterintuitive result he can now explain.)

### Biggest win
Saw firsthand that **working code can still learn nothing if the data has no signal**
(coin-flip label → ~50% accuracy). That's a top-tier ML intuition to own this early.

### Carry into Chapter 2+
1. **Say the 40-second interview answer out loud** twice without reading.
2. **Optional stretch:** extend `ToyModel` to use a 2nd feature (`is_new_account`) and see if test accuracy moves.
3. Tighten *feature vs label* wording when explaining aloud.

### Carry-forward seeds (planted, to formalize later)
- Class imbalance → why accuracy lies (Ch 7).
- Time-based vs random train/test split for financial data (Ch 4).
- Overfitting = the train-minus-test accuracy gap (Ch 4).
- Label noise / signal quality caps model quality (Ch 2/8).

### Test 2 result (recorded)
`threshold=$604.63 · train acc 0.933 · test acc 0.911 · baseline 0.667` — model beats baseline; small train>test gap = overfitting in miniature.
