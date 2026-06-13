# Product Context — Mellions (Personal Financial Management)

> This file is a standing reference. Every chapter, I connect the ML/DE concept we learn
> to a real surface of Mellions. Interview prep is a *byproduct* of real mastery, not the goal.
> The goal: become capable of building, explaining, improving, and scaling real AI/DE products.

---

## The three learning layers (apply to EVERY concept)

1. **Interview readiness** — explain the concept clearly; answer technical follow-ups.
2. **Real technical mastery** — implement it, test it, break it, understand the math and trade-offs.
3. **Product application** — connect it to real systems, especially **Mellions** and PFM broadly.

When teaching any concept, I cover all three. If a concept has no honest product tie-in, I say so rather than forcing one.

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
