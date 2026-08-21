# Product Context — Mellions (Personal Financial Management)

> Standing reference. Mellions is the **applied grounding** for concepts today; the **North Star**
> is Physical AI (see `north_star_physical_ai.md`). We learn for deep mastery and love of the field
> — interview skill is just a byproduct.

---

## The three learning layers (mastery-first — apply to EVERY concept)

1. **Deep technical mastery** — implement it, test it, break it, understand the math and trade-offs. The core.
2. **Systems & real-world grounding** — connect it to real systems: **Mellions** (PFM) + the fraud capstone today, and the **Physical AI bridge** (robotics/perception/control) when relevant.
3. **Clear explanation** — teach it simply in your own words (also makes interviews easy — a byproduct).

When teaching any concept, I cover mastery first, then grounding, then explanation. If a concept has no honest product/physical tie-in, I say so rather than forcing one.

---

## What Mellions is

A **Personal Financial Management** product. Core promise: help users understand and
improve their financial life through ingested financial data, intelligent categorization,
behavioral insight, and AI-driven guidance — safely and at scale.

## Mellions surfaces to connect concepts to

| Surface | What it does | ML/DE concepts it pulls from |
|---------|--------------|------------------------------|
| **Financial data ingestion** | Pull transactions/accounts (e.g., aggregator APIs) | pipelines, schema/data contracts, idempotency, streaming vs batch |
| **Transaction categorization** | Label each transaction (groceries, rent, income…) | supervised classification, text features, naive Bayes, trees, ensembles |
| **Spending behavior analysis** | Detect patterns, trends, anomalies in spend | unsupervised learning, time-series, anomaly detection, regression |
| **Personalized financial insights** | "You spent 30% more on dining this month" | aggregation, statistics, thresholds, segmentation |
| **AI financial assistant** | Conversational guidance | LLMs, retrieval, recommendation, evaluation/reliability |
| **Recommendations** | Suggest budgets, savings, actions | recommender systems, ranking, personalization |
| **Data privacy & security** | Protect sensitive financial data | PII handling, minimization, on-device vs cloud, governance |
| **Model evaluation & reliability** | Trust the predictions | metrics, calibration, monitoring, drift, A/B testing |
| **Data pipelines & data quality** | Clean, consistent, fresh data | ETL, validation, lineage, deduplication |
| **Production architecture & scale** | Serve millions of users reliably | serving, latency, feature stores, batch vs realtime |

## Recurring Mellions design tensions (great for layer-3 discussion)

- **Precision vs recall** in categorization: a wrong category erodes trust fast.
- **Personalization vs privacy**: better models want more data; users want less exposure.
- **Automation vs control**: how much should the AI decide vs suggest?
- **Latency vs richness**: real-time insight vs heavy feature computation.
- **Cold start**: new users with little transaction history.

---

## How fraud capstone and Mellions relate

The fraud-detection **capstone** is our hands-on build (concrete, self-contained, teaches
imbalance/cost-sensitivity/streaming). **Mellions** is the running product lens for *every*
concept. Many techniques transfer directly: fraud detection ≈ anomaly detection in spending;
transaction scoring ≈ transaction categorization; both live on a real-time financial data pipeline.
