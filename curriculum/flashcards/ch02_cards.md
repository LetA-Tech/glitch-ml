# Flashcards — Chapter 2 (Types of Machine Learning)

> Spaced-review deck. Cover the answer, recall, then check. Revisit before each new chapter.

---

**Card 1**
Q: What are the three types of ML?
A: Supervised, unsupervised, reinforcement.

**Card 2**
Q: Regression vs classification?
A: Regression predicts a number on a scale; classification predicts a category from a finite set.

**Card 3**
Q: What makes a classifier's score "regression-like"?
A: It predicts a continuous score first, then thresholds it into a category (→ logistic regression, Ch 6).

**Card 4**
Q: What are the core pieces of a reinforcement learning setup?
A: Agent, environment, policy, reward (cumulative), exploration.

**Card 5**
Q: Why can't a robot learn to walk from labeled data?
A: No one can label every correct joint angle at every instant; it learns from a reward signal instead.

**Card 6**
Q: Does "unlabeled" mean "private / safe"?
A: No — the features themselves carry PII regardless of whether a label exists.

**Card 7**
Q: What is reward hacking?
A: Optimizing a proxy reward instead of the true goal (e.g., "lift a leg" instead of "walk forward without falling").

---

## Carried seed → Chapter 2 Test 2 (coding)
Card 10 from Ch1 said 99.9% accuracy can be a bad sign under class imbalance. Ch2's coding
exercise (`src/ch02/ch02_imbalance.py`) makes you *prove* that on ~1% fraud data — the direct
setup for Chapter 7 (precision/recall).
