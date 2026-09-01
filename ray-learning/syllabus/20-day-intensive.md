# 20-Day Intensive: Python, Spark, Ray, Distributed Data & Practical ML

**Window:** 2026-09-01 through 2026-09-20  
**Target effort:** 4.5-6 focused hours/day  
**Hands-on floor:** 60% of study time  
**Daily loop:** concept -> reading -> implementation -> experiment -> exercise -> verification

## Program outcomes

By Day 20 you should be able to:

- write production-quality Python for data pipelines and computational workloads;
- reason about memory, serialization, concurrency, I/O, profiling, and failure boundaries;
- build and tune PySpark DataFrame/SQL jobs with explicit understanding of partitions, shuffles, joins, query plans, skew, caching, and failure/recomputation;
- explain Ray's tasks, actors, ObjectRefs, object store, scheduling, resources, placement groups, fault tolerance, Ray Data, Train, Tune, and Serve;
- choose intelligently between Spark and Ray for a workload, and use them together when appropriate;
- build a practical ML pipeline without confusing data engineering with model research;
- benchmark, diagnose, and improve a distributed workload rather than merely make it run;
- design and defend a production-oriented data/ML architecture.

## Scope discipline

**Critical path:** Python DE, PySpark, Ray Core/Data/Train/Tune/Serve, distributed execution, performance, failure behavior, practical supervised ML, integration.

**Deprioritized:** deep RLlib, proof-heavy ML theory, exhaustive Spark internals, every cloud deployment option, deprecated Ray Workflows APIs, algorithm trivia, broad interview-question collections.

---

## Day 01 - Python execution model for data engineering

**Concepts:** iterators/generators, memory ownership, copies vs views, file/stream I/O, context managers, dataclasses/typing, exceptions, deterministic transforms.

**Reading:** Python docs on iterators, generators, `concurrent.futures`, `multiprocessing`; review repository Python conventions.

**Implementation:** build a typed line-oriented transaction normalizer that streams input instead of loading the full file.

**Experiment:** compare list materialization vs generator pipeline for memory and runtime.

**Exercise:** malformed rows, schema drift, deterministic error handling, bounded-memory output.

**Verification:** tests pass; peak memory is measured; explain why the streaming version behaves differently.

**Complete when:** code + benchmark + short interpretation + Notion synthesis exist.

## Day 02 - Python parallelism, serialization, profiling

**Concepts:** GIL, threads vs processes, CPU-bound vs I/O-bound, pickling, process startup, IPC, vectorization.

**Reading:** Python multiprocessing/process-pool docs; selected `cloudpickle` background as preparation for Ray.

**Implementation:** parallel feature-extraction pipeline using threads and processes.

**Experiment:** benchmark sequential, ThreadPoolExecutor, ProcessPoolExecutor across CPU- and I/O-heavy variants.

**Exercise:** intentionally pass a non-picklable closure/resource and diagnose the failure.

**Verification:** produce a decision table for threads/processes/vectorization/distribution.

## Day 03 - Spark mental model and DataFrame foundations

**Concepts:** driver, executors, jobs/stages/tasks, lazy evaluation, partitions, transformations/actions, Catalyst/Tungsten at a practical level.

**Reading:** *Learning Spark, 2e* Ch. 1-3 or equivalent official Spark SQL/DataFrame docs.

**Implementation:** ingest generated transactions with explicit schema; project/filter/aggregate/write Parquet.

**Experiment:** inspect `explain()` before and after transformations; compare schema inference vs explicit schema.

**Exercise:** remove unnecessary Python UDFs and express logic with built-ins.

**Verification:** explain driver/executor/partition relationships from the observed plan.

## Day 04 - Spark partitions, shuffles, joins, skew

**Concepts:** narrow vs wide transformations, exchange/shuffle, partition count, broadcast joins, sort-merge joins, skew.

**Reading:** Spark SQL performance tuning docs; *Learning Spark* performance/join sections.

**Implementation:** customer/account/transaction joins and daily aggregates.

**Experiment:** vary `spark.sql.shuffle.partitions`; compare broadcast vs non-broadcast plan/runtime.

**Exercise:** create a hot-key dataset and mitigate skew using filtering, salting, repartitioning, or AQE-aware design.

**Verification:** before/after plans and runtime evidence demonstrate the optimization.

## Day 05 - Spark performance engineering checkpoint

**Concepts:** caching, persistence, file sizing, predicate pushdown, column pruning, AQE, spill, serialization, avoiding driver collection.

**Reading:** Spark tuning + AQE docs.

**Implementation:** optimize one deliberately inefficient pipeline.

**Experiment:** baseline vs optimized runtime; inspect Spark UI metrics if available.

**Exercise:** identify at least five anti-patterns in a supplied/created bad job.

**Assessment 1:** 60-minute build/debug challenge using unseen transformations.

**Verification:** checkpoint rubric >= 75%; corrections documented before proceeding.

## Day 06 - Spark incremental and streaming thinking

**Concepts:** batch vs streaming, event time, watermarking, state, checkpoints, idempotent sinks, at-least-once implications.

**Reading:** Structured Streaming programming guide.

**Implementation:** file/Kafka-like micro-batch transaction stream with windowed aggregation.

**Experiment:** inject late events and restart the query from checkpoint.

**Exercise:** explain what state survives and what does not.

**Verification:** restart produces expected result without manually repairing state.

## Day 07 - Practical ML foundations for data engineers

**Concepts:** train/validation/test, leakage, baseline, feature pipelines, classification/regression, metrics, imbalance, reproducibility.

**Reading:** selected scikit-learn user guide sections; focus on pipelines, preprocessing, metrics, validation.

**Implementation:** transaction anomaly/fraud-risk baseline using scikit-learn pipeline.

**Experiment:** deliberately introduce leakage, observe inflated metrics, then remove it.

**Exercise:** compare precision/recall/F1/ROC-AUC/PR-AUC for an imbalanced target.

**Verification:** one-page model/data contract explains features, label, split, metric, leakage risks.

## Day 08 - Spark ML workloads and feature pipelines

**Concepts:** distributed feature engineering, Spark ML pipelines, vector assembly, fit/transform separation, model artifacts.

**Reading:** Spark ML pipeline guide.

**Implementation:** reproduce part of Day 07 feature pipeline in PySpark; train a modest Spark ML model or export training data cleanly.

**Experiment:** compare local pandas/scikit-learn preparation with Spark preparation at increasing scale.

**Exercise:** decide which stage belongs in Spark vs local ML and justify.

**Verification:** architecture note makes the scale boundary explicit.

## Day 09 - Ray fundamentals: tasks, ObjectRefs, actors

**Concepts:** remote tasks, asynchronous execution, task dependencies, ObjectRefs, actors, `ray.get`, `ray.wait`, task granularity.

**Primary books:** *Learning Ray* Ch. 1-2; *Scaling Python with Ray* Ch. 1-4.

**Implementation:** convert Day 02 workload to Ray tasks; then implement a stateful rate-limited actor.

**Experiment:** submit tasks at several granularities; measure when overhead dominates.

**Exercise:** replace an eager `ray.get` pattern with dependency chaining / bounded `ray.wait`.

**Verification:** draw one task's path from driver to worker to returned ObjectRef.

## Day 10 - Ray architecture, scheduling, resources

**Concepts:** Raylet, GCS, workers, logical resources, scheduling, locality, custom resources, placement groups, autoscaler concepts.

**Primary books:** *Learning Ray* Ch. 2 architecture sections; *Scaling Python with Ray* Ch. 5.

**Implementation:** CPU, GPU-like/custom-resource tasks; PACK/SPREAD placement experiments.

**Experiment:** create an unschedulable task and diagnose exactly why it remains pending.

**Exercise:** design gang-scheduled multi-worker training resources.

**Verification:** explain feasible vs available resources and why placement groups exist.

## Day 11 - Ray object store, data movement, memory

**Concepts:** shared-memory object store, serialization, zero/low-copy paths, ownership, reference lifetime, spilling, distributed transfer.

**Primary books:** *Scaling Python with Ray* Ch. 5 objects/serialization/Arrow; *Learning Ray* Ch. 2 + Data internals context.

**Implementation:** large-object producer/consumer pipeline.

**Experiment:** vary object sizes, force pressure/spilling where practical, inspect memory behavior.

**Exercise:** remove unnecessary data movement through the driver.

**Verification:** distinguish Python heap, object-store memory, spill storage, and durable storage.

## Day 12 - Ray fault tolerance and failure semantics

**Concepts:** task retry, application vs system failure, actor restart, lost in-memory state, object reconstruction, owner failure, idempotency.

**Reading:** both books' fault-tolerance material plus current official Ray fault-tolerance docs.

**Implementation:** failure-injection harness for task/actor/object cases.

**Experiment:** kill a worker; throw a Python exception; restart actor; test persisted vs unpersisted state.

**Exercise:** build a failure-semantics matrix.

**Verification:** correctly predict behavior before each failure injection and compare with actual outcome.

## Day 13 - Ray Data and Spark vs Ray judgment

**Concepts:** Dataset blocks, streaming execution, map/map_batches, actor pools, block sizing, backpressure, shuffles, CPU->GPU pipelines.

**Primary books:** *Learning Ray* Ch. 6; *Scaling Python with Ray* Ch. 9; current Ray Data docs for API changes.

**Implementation:** Parquet -> transform -> batch inference-style pipeline.

**Experiment:** block size/concurrency changes; compare one equivalent workload in Spark and Ray Data.

**Exercise:** write a Spark-vs-Ray decision memo using measured evidence.

**Verification:** can state when Spark is clearly better, when Ray is clearly better, and when they should compose.

## Day 14 - Ray Train and distributed ML execution

**Concepts:** worker groups, data parallelism, ranks/world size, checkpointing, resource topology, failure/restart.

**Primary books:** *Learning Ray* Ch. 7; *Scaling Python with Ray* Ch. 10-11 selected; current Train docs.

**Implementation:** adapt a PyTorch/scikit-compatible training workload to Ray Train where appropriate.

**Experiment:** multi-worker local simulation; checkpoint and resume.

**Exercise:** explain what checkpoint state must be durable for a real restart.

**Verification:** training resumes correctly from an explicit durable checkpoint.

## Day 15 - Ray Tune and resource-aware experimentation

**Concepts:** search spaces, trials, schedulers, search algorithms, resource allocation, ASHA, Optuna integration, ResultGrid.

**Primary books:** *Learning Ray* Ch. 5; *Scaling Python with Ray* Ch. 10 tuning sections; current Tune docs.

**Implementation:** Tune the Day 14 model with bounded concurrency.

**Experiment:** compare random search and ASHA under the same resource budget.

**Exercise:** choose between many 1-GPU trials vs fewer multi-GPU trials conceptually and justify.

**Verification:** best configuration is reproducible and resource usage is explainable.

## Day 16 - Ray Serve and online inference

**Concepts:** deployments, replicas, handles, composition, batching, autoscaling, latency vs throughput, resource isolation.

**Primary books:** *Learning Ray* Ch. 8; *Scaling Python with Ray* Ch. 7; current Serve docs.

**Implementation:** preprocess -> model -> postprocess Serve graph/API.

**Experiment:** load test; measure P50/P95/P99 and batch-size trade-offs.

**Exercise:** kill a replica during load and observe recovery.

**Verification:** provide capacity/latency evidence and explain bottleneck.

## Day 17 - Production observability, debugging, deployment thinking

**Concepts:** Ray Dashboard/State API, logs, metrics, task timeline, memory diagnostics, Spark UI, distributed debugging, KubeRay/RayJob/RayService concepts.

**Primary books:** *Learning Ray* Ch. 9; *Scaling Python with Ray* Ch. 12 + Appendix B/C.

**Implementation:** instrument custom metrics/logging and create one containerized/Kubernetes-ready deployment spec or documented local equivalent.

**Experiment:** diagnose serialization error, OOM-like pressure, and impossible resource request.

**Exercise:** production readiness checklist for a Spark+Ray workload.

**Verification:** issue diagnosis is based on observable evidence, not guessing.

## Day 18 - Integration Project I: Spark -> curated features -> Ray

**Project:** build a reproducible pipeline where Spark performs large-scale relational/data-intensive preparation and Ray performs Python/ML-heavy distributed work.

**Required flow:** raw generated transactions -> Spark cleanse/join/aggregate -> Parquet feature dataset -> Ray Data/Train or task pipeline -> model/checkpoint -> evaluation artifact.

**Experiment:** benchmark the boundary between engines and record data-transfer/file-layout effects.

**Verification:** full run from clean checkout works with documented commands.

## Day 19 - Integration Project II: failure, performance, architecture defense

**Project work:** optimize Day 18, inject failures, add observability, and produce architecture decision records.

**Required failures:** bad input, skew/hot key, task/worker failure, actor/model failure where applicable, memory pressure or unschedulable resources.

**Assessment 2:** explain the architecture without notes and defend Spark/Ray boundaries.

**Verification:** project passes functional tests and the failure drills; performance delta is measured.

## Day 20 - Final capstone, senior-engineer review, consolidation

**Capstone final:** production-oriented distributed transaction intelligence pipeline.

**Required deliverables:**
- architecture diagram;
- data contract and partition strategy;
- Spark preparation job;
- Ray distributed/ML stage;
- reproducible benchmark;
- failure-semantics report;
- observability/debugging notes;
- runbook;
- `when-to-use-spark-vs-ray.md` final decision framework;
- 30-minute self-review or oral walkthrough.

**Final verification:** run from scratch, pass tests, survive required fault drills, explain major execution plans and scheduling decisions, and score >= 80% on final rubric.

---

# Checkpoints

- **Checkpoint A - Day 05:** Python/Spark fundamentals and performance.
- **Checkpoint B - Day 12:** Ray Core architecture and failure semantics.
- **Checkpoint C - Day 17:** end-to-end operational readiness.
- **Final - Day 20:** architecture + implementation + debugging + judgment.

# Definition of Done for every day

A day is **Done** only when all six are true:

1. **Concept:** you can explain the day's central mechanism without copying notes.
2. **Reading:** required bounded reading is complete; gaps/questions are captured in Notion.
3. **Implementation:** runnable code/artifact exists in GitHub.
4. **Experiment:** at least one variable was changed and actual behavior measured/observed.
5. **Exercise:** the assigned problem was completed without merely following a tutorial.
6. **Verification:** tests/checks/rubric pass and the result is interpreted in writing.

If one is missing, Linear remains In Progress.
