# ML Foundation Concepts
### Based on: Grokking Machine Learning — Chapter 1
---

## The Big Picture: AI → ML → Deep Learning

Think of three concentric circles:

- **Artificial Intelligence (AI)** — broadest category. Any system where a computer makes decisions. Route-finding, rule-based chatbots, game-playing. All of it.
- **Machine Learning (ML)** — a subset of AI where the computer makes decisions *based on data*, not hand-written rules.
- **Deep Learning (DL)** — a subset of ML that uses a specific architecture called neural networks. Powers image recognition, LLMs, self-driving cars.

> **Key distinction:** ML ≠ AI. You can have AI without ML (e.g. a rule-based chatbot has no learning). But all ML is AI.
>
> Book analogy: if AI is vehicles, ML is cars, and DL is Ferraris.

---

## The Core Framework: Remember → Formulate → Predict

This is the central mental model of the entire book. Every ML system — no matter how complex — follows this loop.

| Step | What it means | Real world analogy |
|---|---|---|
| **Remember** | Load and absorb the training data | A doctor reviewing 5 years of patient records |
| **Formulate** | Find the best rule/pattern that fits the data | The doctor developing diagnostic rules from experience |
| **Predict** | Apply the rule to new, unseen data | The doctor diagnosing a new patient |

### The Training Loop (inside Formulate)
The machine doesn't invent a rule in one shot. It *searches* for the best one:

```
try a rule → score it → adjust → try again → score again → adjust...
```

This loop runs thousands of times until the score stops improving. That process is called **training**.

---

## Three Distinct Concepts: Feature vs Rule vs Label

These are often used interchangeably but they are fundamentally different things.

### Feature
Any measurable property of the data used as input to the model.

> e.g. `age`, `blood_pressure`, `cholesterol_level`, `email_size_kb`, `day_of_week`

Features live in the data. They exist before any model is built.

### Label
The thing you are trying to predict. The "answer column" in your dataset.

> e.g. `heart_attack: yes/no`, `email: spam/ham`, `house_price: $450,000`

### Rule
What the model does *with* the features to arrive at a prediction.

> e.g. `IF age >= 35 AND blood_pressure == high AND cholesterol == high → heart_attack: yes`

**The key distinction:**
- You can have the same feature and build completely different rules from it.
- A feature is an input. A rule is the model's interpretation of that input.
- Features come from data. Rules are learned by the algorithm.

### Why Features are the Intersection of ML and Data Engineering
The Data Engineer decides what features even *exist*. Before any model runs, someone has to collect, clean, structure, and pipeline the data. The ML engineer says "I need these features." The Data Engineer is who makes them available, reliable, and correct.

> If the pipeline is broken → the feature is garbage → the rule learned is garbage → the model fails.
> **Garbage in, garbage out.**

---

## The 4 Spam Examples — How ML Actually Works

These 4 examples from Chapter 1 compress the entire concept of ML into a simple story. Each one evolves by introducing new features.

### Example 1 — No features, just counting
**Remember:** Bob sent 10 emails. 6 were spam, 4 were ham.
**Formulate:** Rule = "60% of Bob's emails are spam."
**Predict:** New email from Bob → probably spam.

Weakness: no signal, just a base rate.

### Example 2 — One feature: day of week
**Remember:** Weekday emails were all ham. Weekend emails were all spam.
**Formulate:** Rule = "IF weekend → spam. IF weekday → ham."
**Predict:** Sunday email → spam.

Weakness: Bob sent a birthday party invite on Sunday. We missed it.

### Example 3 — One feature: email size
**Remember:** Large emails (>10 KB) were spam. Small emails were ham.
**Formulate:** Rule = "IF size >= 10 KB → spam."
**Predict:** 19 KB email → spam.

Different lens, still one feature.

### Example 4 — Multiple features combined
Combine day of week AND size. The rules get powerful enough that a human can't easily design them anymore. This is exactly the moment you *need* the computer — it searches thousands of combinations and finds the one that scores best.

> **The core insight:** A model is only as good as the features you feed it. More relevant features = more signal = better rules.

---

## Scoring a Model — The Loss Function

How does the machine measure "fits the data best"?

Compare the rule's prediction against the known label for every row:
- Prediction correct → +1
- Prediction wrong → -1

Produce a final accuracy score. The machine adjusts the rule to maximize this score. This scoring mechanism is called a **loss function** (or error function). We'll go deep on this in later chapters.

---

## What Can Go Wrong — Three Failure Modes

This is critical. The same symptom (model fails in production) can have three completely different causes.

### 1. Garbage Training Data
The data collected was incomplete, mislabeled, or corrupted from the start. The rules learned are built on a broken foundation.

### 2. Overfitting
The model trained *too hard* on the training data. Instead of learning general patterns, it memorized the specific training examples.

> Analogy: A student who memorizes past exam questions word for word. Give them the exact same exam — 100%. Change one word — they fail.

The model isn't learning "high cholesterol = risk." It's learning "patient #4, John, 58 years old, these exact numbers."

### 3. Distribution Shift
The training population is fundamentally different from the production population. The rules are real and valid — but valid for the *wrong group*.

> e.g. A model trained exclusively on overweight white North American men deployed to diagnose patients in Southern China. Different genetics, diet, lifestyle, risk factors. The model is confidently wrong.

This is one of the most serious problems in applied ML today. Biased training data has caused real harm in medical diagnosis, credit scoring, hiring, and facial recognition.

### How to Distinguish Between Them

You cannot diagnose from one number alone. You need a **test set** — a second dataset the model has *never seen during training*.

| Training score | Test set score | Likely diagnosis |
|---|---|---|
| 92% | 91% | Healthy — model generalizes well |
| 92% | 54% | Overfitting — memorized training data |
| 92% | 60% | Distribution shift or bad data |

> **The test set is sacred.** Never let the model see it during training. This is one of the most fundamental disciplines in ML. Covered in depth in Chapter 4.

---

## Key Vocabulary

| Term | Definition |
|---|---|
| **Model** | A set of rules that represents data and is used to make predictions |
| **Algorithm** | The process used to *build* the model from data |
| **Feature** | A measurable input property used by the model (a column in your data) |
| **Label** | The output you are trying to predict (the "answer column") |
| **Training** | Running an algorithm on data to produce a model |
| **Inference** | Using a trained model to make predictions on new data |
| **Loss function** | The scoring mechanism that measures how wrong the model's predictions are |
| **Overfitting** | When a model memorizes training data instead of learning general patterns |
| **Distribution shift** | When the training population differs from the production population |
| **Test set** | A held-out dataset used to evaluate a model on data it has never seen |

---

## Chapter 1 — Quiz & Answers

### Q1: What is the Remember step in the hospital heart attack example?

**Answer:** The Remember step is the training dataset — 5 years of patient records stored in a table. Each row is one patient: age, blood pressure, cholesterol level, and whether they had a heart attack. The machine has no intuition or experience. It only sees numbers in rows.

**Key clarification:** In the ML version, the "experience" is not the doctor's years of practice. It is the dataset. The doctor's knowledge is useful context, but the machine only works from data.

**One row looks like:**

| age | blood_pressure | cholesterol | heart_attack |
|-----|---------------|-------------|--------------|
| 58  | high          | high        | yes          |

---

### Q2: What is the Formulate step, and what does it produce?

**Answer:** The Formulate step is the training loop. The machine tries a rule, scores it against the known labels, adjusts slightly, scores again, and repeats thousands of times. At the end it produces the best-scoring rule it found.

Example rule produced: `IF age >= 35 AND blood_pressure == high AND cholesterol == high → heart_attack: yes`

The machine doesn't think — it *searches*.

---

### Q3: A model scores 92% on training data but fails in production. What went wrong?

**Answer:** You cannot tell from one number alone. There are three possible causes:

1. **Garbage training data** — the data was bad from the start.
2. **Overfitting** — the model memorized the training data instead of learning general patterns. It performs perfectly on data it has already seen, but fails on new data.
3. **Distribution shift** — the training population doesn't match the production population. e.g. A model trained on North American patients deployed in Southern China. The rules are valid, but for the wrong group.

To distinguish between them, you need to compare the training score against a **test set score** — data the model has never seen during training.

---

### Q4: What is overfitting in your own words?

**Answer:** Overfitting is when the model trains too hard on the same dataset and stops learning general patterns. Instead, it memorizes the specific training examples. It performs near-perfectly on training data but fails on new, unseen data because it never learned rules that generalize.

The training loop ran so many times on the same data that the model stopped learning *patterns* and started learning *individuals*.

---

*Notes maintained session by session. Next: Chapter 2 — Types of Machine Learning.*
