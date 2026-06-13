# Flashcards — Chapter 1 (What is Machine Learning?)

> Spaced-review deck. Cover the answer, recall, then check. Revisit before each new chapter.

---

**Card 1**
Q: How does ML invert traditional programming?
A: Traditional = data + rules → answers. ML = data + answers (labels) → rules (a model).

**Card 2**
Q: What are *remember / formulate / predict* in industry terms?
A: Training / the model / inference.

**Card 3**
Q: What does training output, and what is that output's job?
A: A **model** (a formula mapping features → label); its job is to predict correctly on **new, unseen** data.

**Card 4**
Q: Feature vs label?
A: Feature = an input clue (amount, merchant, hour). Label = the answer to predict (fraud / not-fraud, or a category).

**Card 5**
Q: What is generalization, and what is its failure mode?
A: Performing well on data not seen in training. Failure = **overfitting** (memorizing training data).

**Card 6**
Q: Is `if amount > 1,000,000: flag fraud` machine learning?
A: No — that's a human-written rule = traditional programming. ML would *learn* the threshold from labeled data.

**Card 7**
Q: Fraud detection vs transaction categorization — how do their labels differ?
A: Fraud = **binary** (2 classes). Categorization = **multi-class** (many categories).

**Card 8**
Q: Why is "categorize 50,000 merchants" an ML problem, not an if/else problem?
A: The rule space is too large, messy, and changing for a human to hand-write and maintain.

**Card 9**
Q: Before training any model, what must exist first?
A: A **labeled dataset** (and a data contract defining the rows) — no labels, no supervised learning.

**Card 10** *(reach-ahead)*
Q: Why can 99.9% accuracy be a bad sign on fraud data?
A: Two reasons — (1) it may be **overfitting** (test on unseen data); (2) with ~0.1% fraud, a model that always says "not fraud" is 99.9% accurate and useless = the **class imbalance** trap (Ch 7).
