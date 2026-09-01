# Reading Map

This file maps the 20-day program to the supplied Ray books and current authoritative references.

## Supplied Ray books

### Learning Ray: Flexible Distributed Python for Machine Learning
Use as the conceptual spine:

- Ch. 1: Ray overview and ecosystem
- Ch. 2: Ray Core, tasks, actors, object store, scheduling/system components
- Ch. 5: Tune
- Ch. 6: Ray Data
- Ch. 7: Ray Train
- Ch. 8: Ray Serve
- Ch. 9: clusters/Kubernetes/autoscaling
- Ch. 11: ecosystem and comparisons

Optional/deprioritized in the 20-day critical path:
- Ch. 3-4 reinforcement learning / RLlib
- Ch. 10 AIR as a historical integration model

### Scaling Python with Ray: Adventures in Cloud and Serverless Patterns
Use as the engineering/production counterweight:

- Ch. 1: where Ray fits
- Ch. 2-4: local execution, remote functions, actors
- Ch. 5: fault tolerance, objects, serialization, resources, autoscaler, placement groups, runtime environments, jobs
- Ch. 7: microservices / Serve
- Ch. 9: advanced data processing and limits
- Ch. 10-11: ML, GPUs/accelerators
- Ch. 12: enterprise operation, monitoring, security considerations
- Appendix A: integrated actor/Kubernetes case study
- Appendix B-C: deployment and debugging

Treat Ch. 8 Ray Workflows as architecture/history only. Do not use deprecated Workflows APIs as a new-project foundation.

## Ray API freshness rule

Both books are 2022/2023-era material. Preserve their systems reasoning, but verify exact APIs against current Ray documentation before implementation. Pay particular attention to:

- Ray Data execution and deprecated `DatasetPipeline` material;
- modern Train recovery/checkpoint APIs;
- Tune `Tuner` / `ResultGrid` patterns;
- Serve deployment/autoscaling APIs;
- KubeRay CRDs and recommended Kubernetes deployment;
- GCS fault-tolerance/HA behavior;
- Ray Jobs vs Ray Client for long-running work.

Current docs: https://docs.ray.io/

## Spark

Primary exact-reference source: https://spark.apache.org/docs/latest/

High-value sections:
- SQL/DataFrames and schemas
- SQL performance tuning
- Adaptive Query Execution
- joins and broadcast behavior
- Structured Streaming programming guide
- ML pipelines
- monitoring / Spark UI

Recommended book companion if available: *Learning Spark, 2nd Edition* by Damji et al. Read selectively; do not attempt a cover-to-cover pass in this program.

## Python

Use official Python 3 documentation for exact semantics:
- iterators/generators
- `concurrent.futures`
- `multiprocessing`
- `pickle`
- profiling (`cProfile`, `tracemalloc`)
- typing/dataclasses

## Practical ML

Use scikit-learn official documentation for:
- preprocessing and Pipelines
- train/test split and cross-validation
- classification metrics
- model evaluation
- common pitfalls / data leakage

The objective is ML literacy for data/ML pipelines, not a deep survey of algorithms.

## Reading discipline

Each day has a reading cap of roughly 45-75 minutes. Read enough to build the day's mental model, then move into code. If a source becomes a rabbit hole that does not improve the day's implementation or debugging exercise, defer it.
