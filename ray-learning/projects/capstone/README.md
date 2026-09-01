# Capstone: Distributed Transaction Intelligence Pipeline

## Goal

Build one realistic system that forces deliberate use of Spark and Ray rather than treating either tool as the answer to every distributed problem.

## Required architecture

```text
raw transaction/account/customer data
        |
        v
PySpark ingest + schema enforcement
        |
        v
joins / cleaning / aggregates / partitioned Parquet
        |
        v
Ray Data or Ray tasks for Python/ML-heavy processing
        |
        v
Ray Train + optional Tune
        |
        v
model/evaluation artifacts
        |
        +--> optional Ray Serve inference API
```

## Required engineering concerns

- explicit schemas and data contracts;
- partition strategy and file layout;
- shuffle/join analysis;
- skew handling;
- bounded-memory processing;
- serialization/data-movement awareness;
- Ray resource declarations;
- checkpointing and durable state boundaries;
- failure injection;
- observability and runbook;
- benchmark before/after optimization.

## Required deliverables

1. `ARCHITECTURE.md`
2. `DATA_CONTRACT.md`
3. Spark preparation job
4. Ray processing/training stage
5. tests
6. benchmark script and results
7. failure drill report
8. runbook
9. final Spark-vs-Ray decision memo

## Success criteria

The capstone is complete only if it runs from a clean environment with documented commands, produces deterministic artifacts, passes tests, survives the required failure drills, and you can explain why every major stage belongs in Spark, Ray, or neither.
