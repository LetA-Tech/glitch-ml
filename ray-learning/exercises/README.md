# Exercise Index

Use this as the durable catalog. Each exercise should have a problem statement, starter code if useful, tests or verification criteria, and a post-attempt solution only when needed.

## Python

- PY-01 Streaming normalizer: process a large JSONL/CSV source with bounded memory.
- PY-02 Concurrency benchmark: sequential vs threads vs processes for CPU and I/O workloads.
- PY-03 Serialization failure: diagnose captured non-picklable resources.
- PY-04 Profiling: identify the dominant CPU and allocation hotspots before optimizing.

## Spark

- SP-01 Explicit-schema ingest and Parquet write.
- SP-02 Remove Python UDFs using Spark SQL built-ins.
- SP-03 Join strategy: broadcast vs sort-merge with plan evidence.
- SP-04 Shuffle partition experiment.
- SP-05 Hot-key skew generation and mitigation.
- SP-06 AQE / caching / file-size optimization.
- SP-07 Structured Streaming late-event + checkpoint restart exercise.
- SP-08 Distributed feature preparation for ML.

## Ray Core

- RY-01 Task conversion and task-granularity benchmark.
- RY-02 Bounded fan-out using `ray.wait`.
- RY-03 Stateful rate-limited actor.
- RY-04 Custom resources and unschedulable task diagnosis.
- RY-05 Placement group PACK/SPREAD experiment.
- RY-06 Large-object pipeline and driver-data-movement cleanup.
- RY-07 Task failure/retry injection.
- RY-08 Actor restart and state-loss experiment.
- RY-09 Object reconstruction/ownership reasoning exercise.

## Ray Data / ML

- RD-01 Parquet -> map_batches -> output pipeline.
- RD-02 Block/concurrency experiment.
- RD-03 Spark vs Ray Data benchmark + decision memo.
- ML-01 Leakage-safe scikit-learn baseline.
- RT-01 Ray Train checkpoint/resume.
- TU-01 Tune random search vs ASHA under fixed budget.
- SV-01 Serve pipeline + load test + replica failure.

## Integration

- IN-01 Spark -> Parquet -> Ray processing boundary.
- IN-02 Failure-injected end-to-end pipeline.
- IN-03 Final architecture defense and runbook.

## Verification rule

An exercise is not complete because the expected output appeared once. Record at least one of: automated test, benchmark, execution-plan evidence, fault-injection result, or explicit invariant check.
