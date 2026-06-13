# Capstone Data Contract — Transaction (v0.1)

> First capstone artifact (Chapter 1). A **data contract** defines the shape, types, and rules
> of the data flowing through the system *before* any model exists. In real teams this is the
> agreement between the data producer (ingestion) and consumers (features, model, API).
> Everything downstream depends on it — get it wrong and the whole pipeline inherits the bug.

---

## Why define this in Chapter 1?

Chapter 1's lesson: ML learns from **labeled data**. So the very first engineering task isn't a
model — it's deciding *what a data point looks like* and *where the label comes from*. This
contract is the foundation the fraud model (and a parallel Mellions categorizer) will sit on.

---

## Transaction schema

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `transaction_id` | string (UUID) | yes | Unique id; primary key; enables idempotent ingestion | `"a1b2-..."` |
| `account_id` | string | yes | Which account this belongs to | `"acct_7741"` |
| `timestamp` | datetime (UTC, ISO-8601) | yes | When it occurred; basis for time/velocity features | `"2026-06-13T14:05:00Z"` |
| `amount` | decimal(12,2) | yes | Signed; negative = debit, positive = credit | `-82.50` |
| `currency` | string (ISO-4217) | yes | 3-letter code | `"USD"` |
| `merchant_name` | string | no | Raw merchant string (messy!) | `"SQ *BLUE BOTTLE"` |
| `merchant_category` | string | no | Provider-supplied category if any | `"coffee_shop"` |
| `counterparty` | string | no | Other party (e.g., Doordash) | `"DOORDASH"` |
| `location_city` | string | no | City of transaction | `"West Hollywood"` |
| `location_country` | string (ISO-3166) | no | Country | `"US"` |
| `channel` | enum | no | `card_present` / `online` / `transfer` / `atm` | `"online"` |
| `device_id` | string | no | Device used (fraud signal) | `"dev_55"` |
| **`is_fraud`** | boolean | label | **Target label** — known only for historical/reviewed data | `false` |

### Field rules / constraints
- `transaction_id` unique; duplicate ⇒ reject or dedupe (idempotency).
- `amount` never null; `0.00` allowed but flagged.
- `timestamp` must be ≤ now (no future-dated); reject otherwise.
- `currency` validated against allowed list.
- `is_fraud` is **nullable in production** (we don't know yet at scoring time) and **required in training data** (must be labeled).

---

## The label problem (preview of Chapter 2)

- `is_fraud` is the **label**. At **inference** time it's unknown (that's what we predict). At
  **training** time it must be known — which means a labeling process exists (analyst review,
  chargebacks, confirmed reports). **No labels → no supervised learning.**
- This is the first design question we resolve in Chapter 2.

## Mellions parallel

The same row, with the label swapped from `is_fraud` (boolean) to `category` (multi-class:
groceries/rent/dining/...), becomes the **transaction categorization** training row. One data
contract, two products — which is exactly why fraud detection is a faithful teaching proxy for
Mellions.

---

## Open questions (carry to Ch 2)
- How precisely do we define a positive (fraud / a given category)?
- How do we handle the severe class imbalance this label will have?
- Where do labels physically come from, and how fresh are they?
