# Dataset Policy

Large datasets do not belong in Git.

## Default dataset

Use reproducible synthetic financial-transaction data generated locally. The generator should produce:

- customers
- accounts
- transactions
- merchant/category attributes
- event timestamps
- deliberately skewed customer/account keys
- a rare binary label suitable for practical classification exercises
- optional malformed/schema-drift rows for data-quality exercises

## Storage layout

Generated data lives under `ray-learning/datasets/generated/` and should be gitignored.

Prefer Parquet for distributed-engineering labs; CSV/JSONL may be generated specifically for ingest/streaming exercises.

## Reproducibility

Every generated dataset must be reproducible from:

- seed
- row count
- skew setting
- anomaly/fraud rate
- schema version

Benchmark reports must record those parameters.

## External datasets

If an external public dataset is useful, commit only a retrieval script, checksums/metadata, and license/reference information unless redistribution is clearly allowed and the data is small.
