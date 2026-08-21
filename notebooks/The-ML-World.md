# The ML World
### Based on: Grokking Machine Learning — Chapter 2 + Deep Dives
---

## The Three Branches of ML

Everything in ML falls into one of three branches, determined by the *nature of the data and the problem*:

```
Machine Learning
├── Supervised Learning     → labeled data, predict the label
├── Unsupervised Learning   → unlabeled data, find hidden structure
└── Reinforcement Learning  → no dataset, agent learns by trial and error
```

> **Critical distinction:** These are not just categories — they require fundamentally different algorithms, loss functions, and evaluation strategies.

---

## The Axis That Splits Everything: Labeled vs Unlabeled Data

**Labeled data** — every data point has a known "answer column" (the label).
> e.g. patient records where `heart_attack: yes/no` is already filled in

**Unlabeled data** — data points exist but there is no answer column to predict.
> e.g. a dataset of customer transactions with no pre-assigned categories

This single distinction determines which branch of ML you're in:
- Have labels → Supervised Learning
- No labels → Unsupervised Learning

---

## Supervised Learning — Predicting the Label

### Regression vs Classification — Why They Are Fundamentally Different

| | Regression | Classification |
|---|---|---|
| Label type | Numerical — infinite continuous possibilities | Categorical — finite discrete set |
| Example | Predict house price: $247,000 | Predict image: cat or dog |
| "Closeness" | Meaningful — being off by $5,000 is better than $50,000 | Meaningless — wrong is wrong |
| Loss function | Measures *distance* from true value | Measures *correctness* of bucket chosen |
| Real example | Lucid Motors: predicting component age at failure | Lucid Motors: predicting defect code for a service visit |

---

## Linear Regression — The Foundation

### Single Feature (Simple Linear Regression)

One feature, one label, one line:

```
y = mx + b
```

- `x` → the feature (e.g. house size in sq ft)
- `y` → the label (e.g. house price)
- `m` → slope: how much price increases per extra sq ft
- `b` → intercept: base price before counting any features

The algorithm's job: find the values of `m` and `b` that minimize the distance between the line and all the data points. That distance measurement is the **loss function**.

### Multiple Features (Multiple Linear Regression)

Each feature gets its own weight (coefficient):

```
y = m1·x1 + m2·x2 + m3·x3 + m4·x4 + b
```

For house pricing:
- `x1` = size in sq ft → weight `m1`
- `x2` = number of bedrooms → weight `m2`
- `x3` = neighborhood quality → weight `m3`
- `x4` = age of building → weight `m4`
- `b` = base price

**Why each feature needs its own weight:** size and number of bedrooms don't contribute equally to price. Each weight captures the specific contribution of that feature.

### Key Vocabulary
- `m1, m2, m3, m4` → called **weights** or **coefficients** or **parameters**
- The algorithm *learns* these from data during training
- With 4 features: finding 5 numbers (4 weights + 1 intercept)
- With 1000 features: finding 1001 numbers simultaneously — impossible by hand

### Limitation of Linear Regression
Assumes the relationship between features and label follows a straight line. If the relationship has thresholds, jumps, or non-linear patterns — linear regression fails. This is the problem decision trees were invented to solve.

---

## Decision Trees — Rules as a Tree of Questions

A decision tree is a series of questions, each one splitting data into smaller groups until an answer is reached.

```
Is size > 2000 sq ft?
├── YES → Is neighborhood quality high?
│         ├── YES → predict $800,000
│         └── NO  → predict $500,000
└── NO  → Is number of bedrooms > 3?
          ├── YES → predict $350,000
          └── NO  → predict $200,000
```

**Node** — a question / decision point
**Leaf** — a final answer / prediction
**Depth** — how many questions deep the tree goes

### The Overfitting Problem with Decision Trees

A fully grown tree (no limits) will eventually create one leaf per training data point — perfectly memorizing every individual. 100% training accuracy, useless on new data.

**Two solutions:**

**Pre-pruning** — set limits before the tree grows:
- `max_depth` — maximum number of questions deep
- `min_samples_split` — a node only splits if it has at least N data points
- `min_samples_leaf` — a leaf must contain at least N data points

**Post-pruning** — grow fully then cut branches that don't improve validation performance.

> Pruning deliberately accepts slightly lower training accuracy in exchange for better generalization. That tradeoff is the entire point.

---

## Ensemble Methods — Many Models Beating One

The insight: one model's errors are unpredictable. Many diverse models' errors are *uncorrelated* — they cancel out when averaged.

Real-world analogy: A hospital tumor board — each specialist reviews different aspects of the patient (blood pressure, cardiac history, medications). No single doctor sees everything, but their consensus is more reliable than any one expert alone.

### Random Forest — Bagging

Build many trees **in parallel**, independently, then combine their predictions.

**Two sources of randomness that make trees different:**

1. **Bagging (Bootstrap Aggregating)** — each tree trains on a different random sample of the data (drawn with replacement). Some rows appear twice, some not at all.

2. **Feature Randomness** — at every node, each tree only considers a random *subset* of features for splitting. Different trees specialize in different features.

```
For each of N trees:
    1. Sample ~80% of training data randomly (with replacement)
    2. Grow a tree — at each node, consider only a random feature subset
    3. Apply pruning limits

Final prediction:
    → Regression:     average all tree predictions
    → Classification: majority vote across all trees
```

Why it works: each tree is weak and biased, but they make *different* errors. Averaging diverse mistakes produces a more accurate and robust result than any single perfect model.

---

## Gradient Boosting — LightGBM, XGBoost, CatBoost

A different ensemble family from Random Forest.

### Bagging vs Boosting — The Core Difference

| | Bagging (Random Forest) | Boosting (LightGBM) |
|---|---|---|
| Tree construction | Parallel — trees built independently | Sequential — each tree fixes previous errors |
| Learning style | All trees learn from full problem | Each tree focuses on current mistakes |
| Analogy | 100 independent doctors voting | Doctor 1 diagnoses → Doctor 2 reviews only what Doctor 1 got wrong → Doctor 3 reviews what Doctor 2 still got wrong |

### Why LightGBM Specifically?

**Leaf-wise vs Level-wise tree growth:**

```
XGBoost — Level-wise:           LightGBM — Leaf-wise:
Expand all nodes at depth 1     Always expand the single leaf
Expand all nodes at depth 2     that reduces error the most
Expand all nodes at depth 3     regardless of depth
```

Leaf-wise finds better splits faster and uses less memory — critical advantage on large datasets with many features.

**LightGBM advantages in practice:**
- Handles mixed feature types (numerical, categorical, embeddings) natively
- Learns optimal handling of missing values from data — no manual imputation
- Faster training on large datasets than XGBoost
- Excellent SHAP integration for interpretability

### Why Interpretability Matters in Production

In regulated or high-stakes industries (automotive, medical, finance), predicting correctly is not enough. You must explain *why* the model made a specific prediction.

SHAP (SHapley Additive exPlanations) decomposes each prediction into feature contributions:
> "This vehicle was flagged for defect D4231 primarily because of high defect_history_frequency for the cooling system, combined with age_at_service of 3.2 years."

A neural network may match LightGBM's accuracy but cannot provide this explanation as cleanly.

---

## Real-World Case Study: Lucid Motors Vehicle Diagnosis Model

### Problem
Predict the most likely defect code for a service visit so technicians diagnose faster.

### Why LightGBM Was the Right Choice
- Mixed feature types: numerical ages, frequency vectors, multi-hot encoded symptoms, Sentence-BERT text embeddings
- Need for interpretability: technicians need to trust and understand predictions
- Large dataset with many features: leaf-wise growth advantage
- Missing values from incomplete technician notes and genuinely new vehicles with no prior service history

### Feature Design Decisions
| Feature | Type | Reasoning |
|---|---|---|
| `age_at_service` | Numerical | Component wear correlates with age |
| `component_ages` per key part | Numerical vector | Different components age differently |
| `defect_history_frequency_vector` | Numerical vector | Prior defects for this VIN predict future ones |
| `symptom_codes` | Multi-hot encoded categorical | Technician-observed symptoms |
| Free-text technician notes | Sentence-BERT embeddings | Captures unstructured diagnostic language |

### The Anti-Leakage Decision — Split by VIN, Not by Row
Splitting randomly by row means the same vehicle could appear in both training and test sets. The model sees a vehicle's history during training then "predicts" that same vehicle's defect during testing — that's memorization, not prediction. Test accuracy would be artificially inflated.

Splitting by VIN guarantees the model has never seen *any* service event from test vehicles. This simulates real deployment — new vehicles the model has never encountered.

> This directly applies the distribution shift lesson: engineer your validation to match the real deployment condition.

### Handling Class Imbalance
Rare defect codes appear far less often than common ones. A naive model ignores rare defects and still achieves high accuracy — useless in practice since rare defects are often the most critical.

Three-layer solution:
- **SMOTE** — synthetically generates new examples of rare defect classes
- **Class weights** — penalizes the model more heavily for missing rare classes
- **F1-macro** — evaluates performance equally across all classes regardless of frequency

### The Two Models Are Fundamentally Different Problems

| Vehicle Diagnosis Model | Recall Prediction Model |
|---|---|
| Per-vehicle classification | Population-level anomaly detection |
| "What defect does this vehicle have?" | "Is this defect appearing across the fleet at an abnormal rate?" |
| Supervised ML — LightGBM | Classical statistics — binomial/Poisson tests |
| Individual label to predict | No individual label — measuring rate deviation from baseline |

> **Key maturity:** Knowing when *not* to use ML. When the question is whether a count deviates from an expected baseline, classical statistics is more appropriate than ML.

---

## Unsupervised Learning — Finding Structure Without Labels

*(To be completed)*

---

## Reinforcement Learning — Learning by Trial and Error

*(To be completed)*

---

*Notes maintained session by session. Currently: Chapter 2 in progress.*
