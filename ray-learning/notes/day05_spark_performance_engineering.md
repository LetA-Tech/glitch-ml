# Day 05 — Spark Performance Engineering Checkpoint

**Sources:** *High Performance Spark, 2nd Ed.* (Karau, Polak, Warren — O'Reilly, June 2026) Ch.2 p.17 (in-memory persistence: deserialized/serialized/disk, LRU eviction), Ch.5 pp.101-108 (Partitions on write, Query Optimizer, Logical/Physical plans, Static Optimizer Rules, Adaptive Query Execution, Code Generation, Large Query Plans, SparkSession Extensions/custom optimizer rules, Debugging Spark SQL Queries, JDBC/ODBC server), Appendix D pp.349-350 (Gang Scheduling/barrier execution mode, Resource Profiles for GPU/heterogeneous hardware), **Appendix F pp.365-376 (full — "The Spark Web UI: Debugging and Optimizing Your Jobs," a complete real-world small-files performance investigation)**, Appendix C pp.343-348 (full — "When Not to Use Spark," production judgment on when Spark itself is the wrong tool). Installed version here: PySpark 4.2.0.

**Cross-links:** what a shuffle/skew actually costs → [Day 04](day04_spark_partitions_shuffles_joins.md). Stage/task/DAG vocabulary this file assumes → [Day 03](day03_spark_mental_model_dataframes.md). Ray's own "self-inflicted vs. irreducible cost" debugging lesson (the `uv run` per-worker rebuild) is the same class of judgment call, different system → [Day 09](day09_ray_core_tasks_actors.md) §7.6.

---

## 1. Definitions and terminology

| Term | Definition |
|---|---|
| **`persist()` / `cache()`** | Tells Spark to retain an RDD/DataFrame's computed partitions (in memory, serialized, or on disk) instead of recomputing them from lineage on the next action. `cache()` is `persist()` with the default storage level. |
| **LRU caching** | Spark's default eviction policy for cached partitions: when memory is needed, the *least recently used* partition is evicted first — overridable via `persistencePriority()`. |
| **Predicate pushdown** | Pushing a `WHERE`-clause filter down to the storage layer itself, so only matching rows/files are read at all, instead of reading everything and filtering in Spark. |
| **Column pruning** | Reading only the columns actually referenced by the query from a columnar format (Parquet), instead of every column. |
| **AQE (Adaptive Query Execution)** | Spark's runtime-statistics-driven re-optimization: adjusts partition counts and join strategies *during* execution based on actual observed data sizes, not just the static query plan. |
| **Catalyst** | Spark SQL's query optimizer — transforms your DataFrame/SQL operations into logical, then physical, execution plans. |
| **Logical plan** | The resolved, type-checked representation of your query before Spark decides *how* to execute it — subject to Catalyst's static optimizer rules. |
| **Physical plan** | The concrete execution plan chosen from among candidate physical plans, using both rule-based and cost-based optimization; what actually runs. |
| **Code generation (Whole-Stage Codegen)** | Catalyst compiling several logical operators into one piece of generated Java bytecode (via Janino) instead of interpreting each operator separately — can be a >10x speedup on suitable queries. |
| **Spill** | Writing in-progress operational data (a shuffle, a sort, an aggregation buffer) to disk because it doesn't fit in the memory Spark allocated for it. |
| **Resource Profile** | A per-stage/task resource request (e.g. "2 GPUs, 8GB RAM") distinct from the executor-wide default — lets one job mix ordinary CPU stages with GPU-heavy ones. |
| **Gang scheduling / barrier execution mode** | Scheduling that guarantees all tasks in a stage start (and, on failure, retry) *together* — needed for distributed training-style workloads where partial execution is meaningless. |
| **DataFlint** | An open-source Spark UI plugin (Apache 2.0) that adds automated alerts (small files, partition skew, memory issues, idle cores) on top of the native, purely-descriptive Spark Web UI. |

---

## 2. Architecture and internal behavior

**Three storage tiers for cached data, in order of speed vs. memory-efficiency** (HPS Ch.2 p.17):
1. Deserialized JVM objects — fastest access, worst memory efficiency.
2. Serialized bytes (Kryo beats Java serialization) — slower access, better memory efficiency.
3. Disk — for partitions too large for RAM; usually paired with iterator-to-iterator transformations to stay feasible.

Spark's default LRU eviction can be overridden per-RDD via `persistencePriority()` if you know some cached data is more valuable to keep than "most recently used" would predict.

**Catalyst's two-phase optimization, precisely** (HPS p.102-105): a DataFrame/Dataset transformation first builds an **unresolved logical plan**; resolving references/types produces the **logical plan**, to which Catalyst applies a battery of static rules (reordering/pushdown, operator combining, simplification) to produce an **optimized logical plan**. Only then does Spark generate one or more **physical plans** and pick among them using a cost model — this final phase is where **predicate pushdown to the data source** happens, one of the highest-value optimizations Catalyst performs.

**Static optimizer rules are not omniscient, and you can work around them deliberately** (p.103): mark a UDF `asNondeterministic()`/mix in `Nondeterministic` to stop the optimizer from reordering around it; insert a `cache()`/`persist()`/checkpoint specifically to stop operations from being pushed "through" one side to the other, when you know you'll reuse that intermediate result. Unnecessary or ill-placed cache/persist calls can equally *prevent* otherwise-available optimizations — this is a real two-sided tradeoff, not a free lever.

**AQE reacts to runtime statistics the static optimizer never sees** (p.104-105): it can coalesce partitions that turn out "too small" post-hoc (`spark.sql.adaptive.advisoryPartitionSizeInBytes`, with a floor via `spark.sql.adaptive.coalescePartitions.minPartitionNum`), and it can propagate an empty-relation result through a query to skip work entirely — occasionally producing plans that look "impossible" (queries referencing nonexistent columns succeeding) because a subquery got pruned. **AQE optimizations are invisible to `explain()`** — you must run the actual query and read the Spark UI's finalized plan to see what AQE really did. AQE is *generally* beneficial but not unconditionally: matching output partitioning to a target table's existing layout can cause a measured ~4x performance *regression* in cases with heavy key skew, because AQE's smart partitioning gets undone by the target table's own partitioning scheme (p.105) — a real, documented case where the "smart default" needs a manual override (e.g. Iceberg's `write.distribution-mode=none`).

**Code generation is not automatic for everything** — it was too costly to enable universally with the older Scala-quasiquote approach; using Janino instead made whole-stage codegen practical, with real gains (>10x on some TPCDS queries) but it can also become a *bottleneck itself* on very large, iterative query plans (ML/graph algorithms) — the documented workaround is round-tripping through an RDD (and back) each iteration specifically to reset/cut the accumulating query plan (p.105-106).

---

## 3. How the concepts relate to each other

- **Caching and AQE are both about *not repeating expensive work*, at different layers** — caching is you telling Spark "keep this materialized result," AQE is Spark deciding at runtime how to physically execute a plan better than the static estimate could. They can conflict: caching/checkpointing intentionally blocks certain optimizer rewrites (§2), which is a deliberate tradeoff you make with full knowledge, not a bug.
- **Everything in this file is diagnosed through one interface: the Spark UI** (§9) — the Stages tab's Input Size/Shuffle Read-Write/GC-time metrics are the *empirical* evidence for whether caching, AQE, or a partition-count change actually helped; `explain()` alone cannot show you AQE's runtime decisions.
- **Skew (Day 04) and spill (this file) are frequently the same root cause wearing two different symptoms** — an overloaded partition both takes longer (straggler) *and* is more likely to exceed its memory budget and spill to disk, compounding the slowdown.
- **Resource Profiles/gang scheduling connect Spark's performance story to Ray's** (Day 09/Day 14) — the same "declare the shape, let the scheduler provision it" pattern from `ScalingConfig(num_workers=N)` in Ray Train appears here as `ResourceProfileBuilder`, and "all tasks in a stage must start together" gang scheduling is architecturally the same requirement as Ray Train's synchronized worker-group data parallelism.
- **Appendix C ("When Not to Use Spark") is the production-judgment counterweight to everything else in this file** — knowing *how* to make Spark fast is only useful once you've confirmed Spark is the right tool for the job at all; see §8.

---

## 4. What needs to be understood deeply

**A cache/persist call is simultaneously a performance tool and an optimizer-blocking tool — know which effect you're reaching for.** Caching data you'll reuse saves real recomputation; caching data you won't reuse (or over-caching) burns memory *and* can prevent Catalyst from pushing an operation through to where it would have been cheaper. This is a genuine two-sided cost, not a "more caching is safer" default.

**AQE is not a substitute for understanding static planning — it's a correction layer on top of it.** Because AQE decisions are invisible to `explain()` and only visible in the executed UI, you cannot reason about final performance from the static plan alone once AQE is in play; you must run the query for real. This matters for anyone used to "read the plan, predict the runtime" — that workflow silently breaks the moment AQE's runtime rewrites become material.

**The Spark UI gives you data, not diagnosis, by design** (Appendix F p.371) — "it provides data but leaves diagnosis to you." A stage with too many tiny tasks relative to its input size, near-zero compute time per task, and large gaps in the Event Timeline is a *small files problem*, but the native UI never says so directly; you infer it by cross-referencing three separate tabs (Jobs, Stages, Executors). This is the actual skill Day 05 is training — not memorizing what each tab shows, but the diagnostic habit of triangulating across them.

**"Big rather than small changes" is sometimes the honest performance answer** (echoing Day 04's Goldilocks conclusion) — the real Appendix F case study's fix was a **single added line** (`.repartition(df("ss_quantity"))` before write) that took a job from 51 seconds to 15 seconds, a >3x speedup, precisely because it addressed the actual root cause (small output files from unbalanced write-side partitioning) rather than a surface-level tuning knob.

---

## 5. Concepts that are easy to confuse

| A | B | The distinction |
|---|---|---|
| **Static optimizer rules** | **AQE** | Static rules run once, at plan-compile time, using no runtime data. AQE runs *during* execution using actual observed partition sizes/skew — and is invisible to `explain()`. |
| **`explain()`** | **The Spark UI's SQL tab** | `explain()` shows the *static* logical/physical plan before any AQE adjustment. The SQL tab (post-execution) shows what actually ran, including any AQE rewrites — check the UI, not just `explain()`, when AQE is enabled. |
| **Predicate pushdown** | **Column pruning** | Pushdown reduces *rows* read (via a `WHERE` filter evaluated at the storage layer). Pruning reduces *columns* read (only referenced columns are fetched from a columnar format). Both reduce I/O; they act on different axes of the data. |
| **Spill (this file)** | **Skew's straggler tasks (Day 04)** | Related but distinct: skew causes an *uneven* partition size, which frequently *causes* spill on the overloaded partition — but a job can spill uniformly under a globally-too-small memory budget with no skew at all. Check both independently. |
| **Whole-Stage Codegen being "good"** | **Codegen being a bottleneck** | Codegen fusing operators into one compiled unit is usually a speedup. On very large/iterative query plans (ML/graph loops), the codegen compilation step itself can dominate — the documented workaround is deliberately cutting the plan (round-trip through RDD) each iteration. |
| **Gang scheduling / barrier mode** | **Resource Profiles** | Gang scheduling is about *timing*: all tasks in a stage start (and retry) together. Resource Profiles are about *shape*: a specific stage/task can request different hardware (e.g. GPUs) than the application's default. Often used together, but they solve different problems. |

---

## 6. Practical engineering patterns

**Cache deliberately, at the point of actual reuse, not preemptively:**
```python
filtered = df.filter(F.col("amount") > 0)
filtered.cache()          # only if `filtered` feeds >1 downstream action/branch
filtered.count()          # materializes the cache (an action)
branch_a = filtered.groupBy("category").count()
branch_b = filtered.groupBy("region").sum("amount")
```

**Force predicate/column pushdown visibility with `explain()`:**
```python
df.filter(F.col("amount") > 100).select("id", "amount").explain(True)
# check the Physical Plan for "PushedFilters" — confirms pushdown actually happened
```

**Cut an accumulating query plan on an iterative algorithm** (HPS Example 5-52, p.106):
```python
for i in range(num_iterations):
    df = df.transform(one_iteration_step)
    if i % checkpoint_every == 0:
        rdd = df.rdd
        rdd.cache()
        df = spark.createDataFrame(rdd, df.schema)   # cuts the query plan
```

**Repartition before write to avoid the small-files problem** (Appendix F's actual, measured fix):
```python
df_filtered.repartition(df_filtered("partition_key")) \
    .write.mode("overwrite").partitionBy("partition_key").parquet(output_path)
```

**Request a Resource Profile for a GPU-heavy stage only** (HPS Appendix D Example D-1):
```python
from pyspark import ResourceProfileBuilder, ExecutorResourceRequests, TaskResourceRequests

gpu_profile = (
    ResourceProfileBuilder()
    .require(ExecutorResourceRequests().resource("gpu", 2, vendor="nvidia"))
    .require(TaskResourceRequests().resource("gpu", 1))
    .build()
)
rdd_gpu_stage = sc.parallelize(range(4), 4).withResources(gpu_profile).map(gpu_work)
```

---

## 7. Common mistakes and misconceptions

*(this file's own syllabus exercise: "identify at least five anti-patterns in a supplied/created bad job" — the following five are exactly that list, each independently real):*

1. **Caching everything "just in case."** Burns memory, triggers more LRU eviction churn, and can block optimizer rewrites that would otherwise have made the uncached path faster.
2. **Reading `explain()` and stopping there when AQE is enabled.** AQE's actual runtime decisions (partition coalescing, skew-join handling, empty-relation propagation) are invisible to `explain()` — you must execute the query and check the UI's SQL tab for the *actual* plan.
3. **Writing without repartitioning first, producing many tiny output files.** The exact Appendix F case study: filtering a small-files-partitioned input and writing straight through propagates (and often worsens) the small-files problem downstream, invisible from the code alone — "the logic is straightforward: read, filter, write... [but] the performance is terrible" (p.365).
4. **Treating the native Spark UI's silence as "no problem found."** The UI shows raw metrics (task counts, durations, sizes) and explicitly does not tell you "you have a small files problem, here's the fix" — assuming no alert means no issue is a category error about what the tool does.
5. **Adding a checkpoint/cache reflexively to "cut the plan" on every iterative loop**, even when the plan isn't actually large enough to matter yet — this trades real memory/IO cost for a codegen problem you don't have yet; profile the actual plan size before applying the workaround.
6. *(bonus, tightly related)* **Ignoring `spark.sql.planChangeLog.level`** when debugging *why* an optimization did or didn't apply — logged at trace level by default specifically because it's noisy, but it is the direct evidence for "did rule X actually fire," rather than guessing from output alone.

---

## 8. Production considerations

**Appendix C's "When Not to Use Spark" is the production-judgment layer this entire file sits under** — performance-tuning code that should never have been Spark in the first place is not a performance win. Named cases worth holding onto exactly (HPS Appendix C, pp.343-348):

| Situation | Why Spark is the wrong default |
|---|---|
| Small data and small tasks | Data-parallel overhead isn't worth paying if it fits in memory on one machine — "Data parallelism is beneficial when your data is too big *or* the aggregate processing time is too long" (not merely "big data"). |
| Real-time updates / OLTP | Spark reconciles updates far slower than a traditional or sharded OLTP database — even Delta/Hudi/Iceberg row-level updates are "still much slower" than an OLTP-optimized store. |
| User-facing interactive latency | A user-facing page load is generally too latency-sensitive for a live Spark query — trigger jobs off a queue and serve pre-computed results instead. |
| Long and highly variable per-record processing time | If processing time varies per-record rather than per-partition-size (e.g. a flaky external API), disable speculative execution or Spark will duplicate work assuming it's a slow machine, not a slow call. |
| Untrusted/multitenant queries | Spark SQL executes arbitrary code; one user's query can potentially access another's credentials in a shared deployment without vendor-added restricted execution. |
| Non-idempotent per-record side effects | Spark's retry semantics assume re-running a task is safe; a billing API call without a transaction ID can be double-applied on retry. |
| Records larger than ~1GB | Arrow auto-chunks huge records, breaking Python/Java interoperability (`mapInPandas` and similar) — the fix is splitting records (pivoting, exploding arrays), not tuning Spark harder. |
| No ops support/budget | Spark is "best with some level of operations support" — a managed platform (Databricks, cloud vendor) is the honest alternative to self-hosting without a dedicated team. |
| Global in-order processing required | Data parallelism provides zero benefit for a workload that fundamentally cannot be parallelized — forcing it (a global sort + `toLocalIterator`) gets you correctness with none of Spark's actual value. |

- **GPU/heterogeneous-hardware support is now real but still deliberately scoped** (Appendix D) — Resource Profiles and gang scheduling exist specifically for ML/distributed-training use cases, and the book's own recommendation is still to persist data before training so it's reusable across training configurations, rather than recomputing a Spark pipeline per run.
- **Small-files problems compound in cost, not just latency** — many tiny files mean many tiny GET/PUT operations against object storage (S3/HDFS), a direct, measurable cost line independent of compute time.

---

## 9. Debugging and performance reasoning

**Full walkthrough, from a real case (Appendix F): a job that should take seconds takes ~1 minute for 273MB.**

1. **Jobs tab** — one job, "51 seconds for what should be a subsecond operation," 2 stages (read+filter, write).
2. **Stages tab** — stage-level Input Size/Output Size/Shuffle Read/Shuffle Write. The tell: **an unusually high task count relative to the data size** — reading 273MB with thousands of tasks means the source data is fragmented into tiny files.
3. **Stage detail → DAG visualization** — look for `WholeStageCodegen` (fused, good), `Exchange` (a shuffle — a stage boundary), `FileScan` (a read). A "simple flow" DAG here means the problem isn't the *logic*, so look elsewhere.
4. **Stage detail → Event Timeline** — color-coded bars (green=compute, blue=shuffle read, orange=shuffle write, red=serialization) plotted by executor and time. The tell: **more scheduling-overhead gaps than actual green compute time** — thousands of tasks completing almost instantly, with overhead between them, is the classic small-files signature.
5. **Stage detail → Summary Metrics** — min/25th/median/75th/max for Duration, Input size/records, GC time, shuffle spill. If min and max duration are both tiny, tasks are too small. If max is ~10x median, that's skew (Day 04), a different diagnosis from "too small."
6. **SQL tab** — the four-stage plan (Parsed → Analyzed → Optimized Logical → Physical). Confirm predicate pushdown happened (`PushedFilters` under `FileScan`); check for `BroadcastHashJoin` vs `SortMergeJoin` on any joins; count `Exchange` nodes (each is a real stage boundary/shuffle).
7. **Executors tab** — Storage memory (near-limit → LRU eviction risk), GC time >10% (memory pressure), shuffle read/write skew across executors (uneven work), task failure counts.
8. **The honest limitation of the native UI:** it never says "you're reading small files, here's the fix" — it gives you the raw signals (tiny tasks, high overhead, small input per task) and leaves the inference to you. A plugin like DataFlint adds automated alerts (small files, partition skew, memory issues, idle cores) with a suggested fix on top of the same underlying metrics — worth knowing exists, not required to use it.
9. **The actual fix, applied and confirmed:** `repartition(df("ss_quantity"))` before `.write.partitionBy(...)` — result: **51 seconds → 15 seconds**, confirmed by the "writing small files" alert disappearing and the physical plan showing a repartition-then-coalesce step.

| Symptom | Likely cause | Where to look |
|---|---|---|
| Many tiny, near-instant tasks; low compute time per task | Small input files (or over-partitioned write) | Stages tab task count vs. input size; Event Timeline gap ratio |
| GC time >10% of task time (highlighted red in Executors tab) | Memory pressure | Increase executor memory or reduce partition size |
| Nonzero shuffle spill (memory/disk) | Insufficient memory for the shuffle/sort/aggregation buffer | Stages tab shuffle spill metrics; consider more partitions or more memory |
| Physical plan lacks expected `PushedFilters` | Filter isn't pushdown-compatible with the data source | SQL tab, Physical Plan detail |
| `SortMergeJoin` appears where `BroadcastHashJoin` was expected | Broadcast side exceeds `spark.sql.autoBroadcastJoinThreshold` | Check actual DataFrame size vs. the threshold |
| Query "impossible" result (references a column that doesn't exist, but succeeds) | AQE pruned an empty-relation subquery | Known AQE behavior (HPS p.105) — not a bug, re-check whether the pruning was correct for your data |

---

## 10. Examples and exercises

### Worked example — baseline vs. optimized, with UI evidence (adapted from Appendix F)

```python
# BASELINE — deliberately inefficient (small-files-producing)
df = spark.read.load("ray-learning/datasets/generated")
df_filtered = df.filter(df["amount"] > 0)
df_filtered.write.mode("overwrite").partitionBy("is_fraud").parquet("/tmp/day05_baseline")

# OPTIMIZED — repartition to match the write-side partitioning before writing
df_repartitioned = df_filtered.repartition(df_filtered["is_fraud"])
df_repartitioned.write.mode("overwrite").partitionBy("is_fraud").parquet("/tmp/day05_optimized")
```
Time both. Open the Spark UI for each run; compare task counts, Event Timeline density, and the SQL tab's physical plan.

### Exercises (unsolved — write these yourself, get reviewed)

1. **Optimize one deliberately inefficient pipeline** (this day's own syllabus implementation task) — write a pipeline with at least two of: no explicit schema, an unnecessary `collect()`, a missing `repartition` before write, an avoidable UDF, and no caching of a value reused 3+ times. Fix each, measuring baseline vs. optimized runtime.
2. **Inspect Spark UI metrics for both runs above.** Using the Stages/Executors/SQL tabs, produce concrete before/after evidence (not just wall-clock time) that your fix addressed the *actual* mechanism, not just a symptom.
3. **Identify at least five anti-patterns in a supplied/created bad job** (this day's own syllabus exercise) — use §7's list as your checklist, but confirm each one is *actually present* in your specific job via the UI, not merely plausible.
4. **60-minute build/debug challenge using unseen transformations** (this day's own Assessment 1) — have someone else (or a delayed version of yourself) hand you a Spark job with an unknown performance problem and diagnose it cold using only the Spark UI, within a time limit.
5. **Reproduce the AQE partitioning regression from §2.** Write a table with a partitioning scheme that conflicts with a naturally skewed dataset's ideal shuffle partitioning, enable AQE's target-table partition matching, and measure whether it helps or (as HPS documents) regresses performance for your specific skew pattern.
