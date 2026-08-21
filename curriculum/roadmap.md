# AI & Data Engineering — Deep Learning Track

**Learner:** Lucas
**Anchor book:** *Grokking Machine Learning* — Luis G. Serrano (Manning)
**Goal:** Transform theory into real understanding, practical coding skill, and interview-ready experience.
**Capstone:** Real-time fraud / anomaly detection system (grows one component per chapter).

---

## The book stack (in `/ML/books-to-read/`)

We anchor on **Grokking Machine Learning** and pull in the others as just-in-time support so you build *system* knowledge, not just model knowledge.

| Book | Role in our track | Pulled in around |
|------|-------------------|------------------|
| **Grokking Machine Learning** ⭐ | Anchor. Chapter-by-chapter spine. | Throughout |
| **Grokking Bayes** | Deepens probability + Bayesian reasoning. | Ch 7–8 (metrics, naive Bayes) |
| **Grokking Data Structures** | Arrays, hash maps, trees, heaps — the substrate under feature stores & fast lookups. | Ch 3–4 + capstone perf |
| **Grokking Algorithms** | Big-O, sorting, search, graphs, DP — coding-interview backbone + pipeline efficiency. | Ch 5–6 + interview prep |
| **Grokking AI Algorithms (2nd Ed)** | Broader AI: search, optimization, evolutionary, RL, deeper neural nets. | Ch 10–12 + capstone extensions |

> Rule of thumb: **ML book sets the topic; the others go deeper where the topic touches their domain.** I'll tell you exactly when to open a second book and which pages — never as busywork.

---

## How we work each chapter

1. **Read together** — section-by-section guidance on *what* and *why*.
2. **Core concepts** — simple first, then deeper.
3. **Deep dive** — intuition, math, when/why, mistakes, real projects, interview angle.
4. **Notebook** — structured Markdown (`notebooks/chXX_*.md`) + Python (`code/chXX_*.py`).
5. **Test 1** — flashcards, conceptual, short-answer, scenario, interview questions.
6. **Test 2** — coding exercises (Python for ML; Go/SQL/Python for DE).
7. **Chapter mini-project** — adds one component to the capstone.
8. **Interview translation** — explain it to a hiring manager.
9. **Evaluation** — score 7 areas (1–10) + what to improve before moving on.

**Rule:** We do not advance until you can *apply* the chapter, not just recall it.

### Every concept is taught in three layers (mastery-first)

1. **Deep technical mastery** — implement it, test it, break it, understand the math & trade-offs. This is the core.
2. **Systems & real-world grounding** — how the idea is used in real systems: today **Mellions** (PFM) + the fraud capstone; tomorrow **Physical AI** (robotics, perception, control). See `north_star_physical_ai.md` and `product_context_mellions.md`.
3. **Clear explanation** — be able to teach the idea simply, in your own words. (A mastery skill that *also* makes interviews trivial — a byproduct, never the goal.)

> **North Star:** build deep, durable foundations in ML, AI, and Data Engineering, then move into **Physical AI** (embodied / robotic systems). We learn for love of the field and real capability — not to pass a test.

---

## Roadmap — chapter → concept → capstone component

| Ch | Book topic | Core ML idea you gain | Capstone component added |
|----|-----------|----------------------|--------------------------|
| 1 | What is ML? | Remember → Formulate → Predict; ML vocabulary | Problem framing + project skeleton + data contract for transactions |
| 2 | Types of ML | Supervised vs unsupervised; regression vs classification; labels | Label strategy for fraud (what is "fraud", class imbalance) |
| 3 | Linear regression | Fitting a line; gradient descent; loss | First numeric predictor + the gradient-descent engine we reuse |
| 4 | Over/underfitting, testing, regularization | Train/val/test, bias-variance, L1/L2 | Train/test split + leakage-safe evaluation harness for fraud |
| 5 | Perceptron | Linear classifier, decision boundary | First binary fraud classifier (baseline) |
| 6 | Logistic classifier | Probabilities, log loss, sigmoid | Fraud *probability* scores + threshold tuning |
| 7 | Classification metrics | Accuracy, precision, recall, F1, ROC/PR | The metric layer — why accuracy lies on imbalanced fraud data |
| 8 | Naive Bayes | Probabilistic classification, conditional prob | Probabilistic fraud signal + feature independence discussion |
| 9 | Decision trees | Splitting, entropy/Gini, interpretability | Interpretable rules engine for analysts |
| 10 | Neural networks | Layers, non-linearity, backprop intuition | Non-linear fraud detector + feature representation |
| 11 | SVMs & kernels | Margins, kernel trick | High-dimensional boundary model + comparison |
| 12 | Ensembles | Bagging, boosting, random forests | Production-grade ensemble model (likely our best fraud model) |
| 13 | Real-life ML + DE | End-to-end project practice | Full pipeline integration, serving API, deployment write-up |

> Chapters 3–4 build the engine; 5–8 build classifiers; 9–12 make them strong and interpretable; 13 ties the system together.

---

## Data Engineering thread (runs in parallel)

Even though the book is ML-first, each chapter we attach a DE concept so the capstone becomes a *system*, not just a model:

- **Ingestion:** simulated transaction stream (CSV → batch → streaming).
- **Storage & schema:** a data contract; later a SQL store.
- **Transformation / ETL:** cleaning, feature engineering as a pipeline step.
- **Feature engineering:** velocity features, aggregates, time windows.
- **Orchestration:** how the pipeline steps are scheduled/chained.
- **Serving:** a scoring API.
- **Monitoring:** drift, metric tracking, experiment logging.

---

## Folder structure

```
AI-DataEng-Learning/
├── README_Roadmap.md        ← this file
├── notebooks/               ← chXX_title.md (concepts, notes, Mermaid diagrams)
│   └── _TEMPLATE.md         ← reusable chapter template
├── code/                    ← chXX_title.py (exercises + capstone code)
├── capstone/                ← fraud detection system (grows each chapter)
│   └── README.md            ← problem statement + architecture
└── flashcards/              ← chXX_cards.md (spaced-review questions)
```

---

## Progress tracker

| Ch | Concepts | Coding | Explain | Interview | Real-world link | Capstone | Notebook | Status |
|----|----------|--------|---------|-----------|-----------------|----------|----------|--------|
| 1  | – | – | – | – | – | – | – | In progress |

(Scores filled in 1–10 as we complete each chapter.)
