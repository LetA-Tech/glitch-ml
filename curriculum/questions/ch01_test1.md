# Test 1 — Core Knowledge — Chapter 1

> Format: question · my answer · instructor evaluation. A record of what I actually got right/wrong.

---

## Reading guiding questions (first pass)

**Q1. What is ML, and how does it differ from regular programming?**
- *My answer:* ML mimics human cognitive/logical thinking; process = remember–formulate–predict.
- *Eval:* Right on the framework; **missed the core contrast** — in traditional programming the human writes the rules; in ML the machine derives the rules from data + labels. ✅ corrected.

**Q2. Explain remember–formulate–predict with a personal example.**
- *My answer:* Trade-war example — remember past effects, formulate a rule, predict future term.
- *Eval:* Framework mapping correct. Bonus lesson: generalizing from ~1 example = intuitive preview of **overfitting**.

**Q3. What does it mean for a machine to "learn from data"? What does it produce?**
- *My answer:* Learns the feature→label relationship; produces a model.
- *Eval:* Correct. Sharpened: feature ≠ label; the **model** is the deliverable.

**Q4. Fraud detection through the framework.**
- *My answer:* Remember abnormal behavior; formulate (e.g., deposit > 1M → laundering); predict.
- *Eval:* Caught a key mistake — "deposit > 1M" is a **human-written rule**, not learned. Remember must use **labeled** history. ✅ corrected.

---

## Test 1 — Core Knowledge questions

**1. Industry terms for remember/formulate/predict?**
- *My answer:* Training, modeling, inference.
- *Eval:* ✅ Correct.

**2. What does training output and what's its job?**
- *My answer:* A model (rule set capturing feature–label relationship).
- *Eval:* ✅ Correct (job = predict/generalize on new data).

**3. Why is categorizing 50,000 merchants an ML problem, not if/else?**
- *My answer:* A human can't write that many branching rules (20 if/else is already too much).
- *Eval:* ✅ Correct intuition — rule space explodes.

**4. Teammate: "99.9% accuracy on training data, ship it." Push back?**
- *My answer:* Might be overfitting — test on unseen test data.
- *Eval:* ✅✅ Correct, and ahead of schedule. Added: class-imbalance trap (Ch 7).

**5. Interview: what is ML vs traditional programming? (say out loud)**
- *My answer:* ML inverts traditional programming; feed algorithm features+labels to create a model; generalization = predict unseen from what was trained.
- *Eval:* ✅ Content complete. Polished 40-sec version stored in `interview/ch01_interview.md`.

**6. Mellions auto-categorization — first engineering thing needed?**
- *My answer:* A labeled training set (~1M) + held-out test set (~500K), split by transaction_id to avoid leakage.
- *Eval:* ✅ Correct. Nuance for Ch 4: in financial data, **time-based** split is usually safer than random ID split (same card in train+test can leak).

---

**Test 1 score: 9 / 10.** Strong conceptual grip; reaching ahead correctly. Tighten precision on feature-vs-label wording when explaining aloud.

---

## Threshold deep-dive — Q&A (self-requested, before Ch 2)

**T1. What is a threshold?**
- *Understanding reached:* A dividing line on one feature; `amount > threshold → fraud`. Analogies: height bar, fever cutoff, pass mark.

**T2. Why isn't the toy model perfect?**
- *Understanding reached:* The classes overlap (~$300–600); overlap points are misclassified by any single fence → accuracy ~0.90, not 1.00.

**T3. Threshold-sweep exercise (coding).**
- *Did:* wrote `accuracy_at`, swept $0–$1500, found best with `max(range(...), key=lambda th: accuracy_at(th, data))` → **$600 @ 0.900**.
- *Bugs caught & fixed:* (a) `accuracy_at` defined *inside* `draw_picture` → `NameError`; moved to top level. (b) counted `amount > threshold` instead of correctness; added `== x.is_fraud`. (Tell: reported 1.0 for "flag everything".)
- *Eval:* ✅ Both bugs fixed independently; clean idiomatic `max(..., key=...)`.

**T4. What should do the fence-search automatically?**
- *My answer:* "the train function does it" → *refined:* the **training / optimization** step (gradient descent, Ch 3). Our `learn_threshold` only shortcuts; it doesn't truly search.
- *Eval:* ✅ Right concept; sharpened the vocabulary and the guess-vs-search distinction.

**Insight earned:** midpoint shortcut ($607) ≈ searched optimum ($600) here *only* because the data is balanced/symmetric; with real-world imbalance it would drift — which is why real optimization matters.
