# Day 06 — Spark Incremental and Streaming Thinking

**Sources:** *High Performance Spark, 2nd Ed.* (Karau, Polak, Warren — O'Reilly, June 2026) Appendix E "Spark Streaming" pp.351-354 (streaming basics, driver-restart operational considerations, execution/trigger modes, micro-batching). Official Apache Spark 4.2.0 documentation, `structured-streaming/apis-on-dataframes-and-datasets.html` (fetched live, 2026-09-02): watermarking semantics, output-mode compatibility, checkpointing/fault-tolerance guarantees, exactly-once sink table, `foreachBatch` deduplication pattern — this page is the authority for exact API/config names per `references/reading-map.md`'s own freshness rule (the book covers the same concepts but the live docs are quoted verbatim below for anything version-sensitive). Installed version here: PySpark 4.2.0.

**Cross-links:** batch execution model this file contrasts with → [Day 03](day03_spark_mental_model_dataframes.md). Checkpointing here is conceptually the same durability problem as Ray Train's checkpoint requirement → [Day 14](day14_ray_train.md) §4. Ray Data's streaming *execution engine* (continuous operator overlap) is a different, unrelated use of the word "streaming" from this file's event-time/micro-batch streaming — don't conflate them; see [Day 13](day13_ray_data_spark_vs_ray.md) §5.

---

## 1. Definitions and terminology

| Term | Definition |
|---|---|
| **Structured Streaming** | Spark's current (Dataset-based) streaming API — "modern Spark refers to this Dataset-based API as Structured Streaming, and this is where virtually all new development occurs" (HPS p.351). The older, RDD-based DStream API still exists but is legacy. |
| **Micro-batching** | The default execution mode: the engine repeatedly runs a new "batch" of newly-arrived data through the same query plan on a trigger interval. |
| **Continuous processing** | An experimental, low-latency execution mode (record-by-record rather than batch-by-batch) — still limited in functionality and not the default. |
| **Trigger** | The setting that controls *when* a new micro-batch (or continuous epoch) starts. Named types: `ProcessingTime`, `Once`, `AvailableNow`, `RealTime` (see §2). |
| **Watermark** | A declared bound (`withWatermark(eventTimeCol, delay)`) on how late event-time data is allowed to arrive before the engine stops waiting for it and drops/cleans up related state. |
| **Output mode** | How the Result Table's changes are emitted to the sink each trigger: **Append** (new rows only), **Update** (changed rows only), **Complete** (the whole table, every trigger). |
| **Checkpoint location** | A required, HDFS-compatible-fault-tolerant directory (`option("checkpointLocation", ...)`) where a streaming query persists its progress and state — the basis for restart/recovery. |
| **Idempotent sink** | A sink designed so that reprocessing the same batch (after a restart) does not duplicate its effect — typically achieved via a `batchId`-based deduplication check in `foreachBatch`. |
| **At-least-once vs. exactly-once** | At-least-once: a record may be delivered/applied more than once after a failure/restart. Exactly-once: guaranteed single application — not free; depends entirely on which sink you use (see §2's sink table). |
| **State (in a streaming aggregation)** | The running, per-key intermediate result (e.g. a partial count/sum) Spark must keep alive across micro-batches until a watermark says it can be finalized and dropped. |

---

## 2. Architecture and internal behavior

**Watermarking's actual guarantee, quoted exactly** (Spark 4.2.0 docs, fetched live): *"A watermark delay (set with `withWatermark`) of '2 hours' guarantees that the engine will never drop any data that is less than 2 hours delayed. In other words, any data less than 2 hours behind (in terms of event-time) the latest data processed till then is guaranteed to be aggregated. However, the guarantee is strict only in one direction. Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated."* This is a one-sided guarantee — hold that precisely, don't round it up to "data older than the watermark is always dropped."

Four conditions must **all** hold for a watermark to actually clean up aggregation state:
1. Output mode must be **Append or Update** (Complete mode must retain everything by definition).
2. The aggregation must key on the event-time column, or a `window` over it.
3. `withWatermark` must be called on the **same column** used as the timestamp in the aggregation.
4. `withWatermark` must be called **before** the aggregation, not after.

**Output-mode compatibility is not uniform across query shapes** (live docs):

| Query type | Supported output modes |
|---|---|
| Aggregation on event-time with watermark | Append, Update, Complete |
| Other aggregations (no watermark) | Complete, Update only — Append not supported, since aggregates can still update |
| `mapGroupsWithState` | Update only |
| `flatMapGroupsWithState` (Append-mode operator) | Append (aggregations allowed *after* it) |
| `flatMapGroupsWithState` (Update-mode operator) | Update (aggregations not allowed after it) |
| Queries with joins | Append only — Update/Complete not yet supported |

**Fault tolerance depends entirely on the sink, not on Spark uniformly** (live docs, exact table):

| Sink | Fault-tolerant | Semantics |
|---|---|---|
| File Sink | Yes | **exactly-once** |
| Kafka Sink | Yes | at-least-once |
| Foreach Sink | Yes | at-least-once |
| ForeachBatch Sink | Depends on your implementation | — |
| Console Sink | No | — |
| Memory Sink | No | — |

To get exactly-once out of an at-least-once sink like Kafka, the documented pattern is deduplicating on the `batchId` `foreachBatch` provides you: *"By default, `foreachBatch` provides only at-least-once write guarantees. However, you can use the batchId provided to the function as way to deduplicate the output and get an exactly-once guarantee."*

**Trigger types, with their exact names and behavior** (HPS Appendix E p.353-354):
- **`Trigger.ProcessingTime`** — the classic micro-batch trigger; starts a new micro-batch at the specified interval (or immediately when unspecified/the previous batch just finished). Supports the full feature surface: state, joins, aggregations.
- **`Trigger.Once`** — processes *all* currently-available data into a single micro-batch, then stops. Useful for small backfills; can overload the cluster if data volume exceeds what it's sized for.
- **`Trigger.AvailableNow`** (Spark 3.4+) — processes all available data but splits it into multiple micro-batches as needed, then stops; batch sizing controlled by the source's own rate limits (`maxFilesPerTrigger`, `maxOffsetsPerTrigger`). The safer, chunked alternative to `Once` for larger backfills.
- **`Trigger.RealTime`** (Spark 4.1+) — the new ultra-low-latency mode (~5-300ms p99 latencies), record-by-record rather than micro-batch-by-micro-batch; a time interval here bounds how long a specific transaction can be held within a checkpoint, not a batch delay.
- **Continuous processing** (the older experimental low-latency mode, distinct from `RealTime`) remains experimental in Spark 3.x/4.x with real functional limitations and no built-in fault tolerance — HPS's explicit advice: approach cautiously, expect it to be superseded by the newer real-time mode as that gains stability.

**Driver survivability is the single biggest operational difference from batch Spark** (HPS p.352): "the key operational difference in Spark Streaming is that long-running drivers, especially for stateful queries, tend to accumulate pressure over time and will eventually require a restart." Recovery relies on checkpoints; but "even with checkpoint recovery, you may experience data loss" with non-replayable receivers, and non-idempotent sinks can double-process records on a restart. HPS's own operational practice: schedule **periodic (at least weekly) driver restarts during business hours** deliberately, so restart issues surface on your terms rather than at 2am during an unplanned failure — and test *non-graceful* kill/recovery separately from graceful `query.stop()`.

---

## 3. How the concepts relate to each other

- **Watermarking and output mode are jointly necessary, not independently sufficient** — a watermark alone does nothing to state cleanup unless the output mode is Append or Update (§2, condition 1); this is a common source of "I set a watermark and nothing changed" confusion.
- **Checkpointing here is the same durability problem as Ray Train's checkpoint requirement** (Day 14 §4) — in both systems, the *process* (a Spark driver, a Ray Train worker) is disposable and restartable, but only if the actual *state* was persisted somewhere durable the restart can read back from. "Fault-tolerant" is only true in practice, not just on paper, if you've actually wired the persistence — identical lesson, two different systems.
- **Batch vs. streaming reuses the exact same DataFrame/SQL code and Catalyst plan machinery from Day 03/05** — this is HPS's own stated design point: the same logic runs in `Trigger.AvailableNow` for a backfill and in `Trigger.ProcessingTime` for ongoing incremental processing (the "lambda architecture" pattern), meaning everything you learned about explain plans, joins, and AQE (Days 03-05) still applies — streaming adds watermarking/state/checkpointing on top, it doesn't replace the underlying execution model.
- **Do not confuse this file's "streaming" with Day 13's Ray Data "streaming execution engine.**" Ray Data's streaming refers to overlapping *pipeline stages* within one batch job (no event time, no watermarks, no unbounded input) — a completely different concept that happens to share a word. Structured Streaming here is about unbounded, event-time-ordered input.

---

## 4. What needs to be understood deeply

**A watermark is a promise about aggregation correctness, not a data-retention policy for your whole pipeline.** It bounds how long the engine keeps *aggregation state* alive per key/window — it does not mean "any data older than the watermark is deleted from your source" or "your sink stops accepting old records." Conflating these is a common and costly misreading.

**"Exactly-once" is a per-sink property you must verify, not a Spark-wide guarantee you get by using Structured Streaming.** The sink table in §2 is the actual ground truth: File Sink gives you exactly-once; Kafka and Foreach give you at-least-once by default, requiring your own `batchId`-based deduplication to reach exactly-once. Claiming "my pipeline is exactly-once" without checking which sink you're actually writing to is an unverified claim.

**Checkpointing recovers *progress*, and only genuinely recovers state if your logic avoided uncheckpointed mutable state.** HPS's explicit warning: avoid Spark accumulators and anything stored in "regular" driver-side variables for anything you need to survive a restart — a `HashMap` built up in the driver outside Spark's own state-tracking machinery is silently lost on restart, checkpoint or not. This is exactly the kind of gap between "looks fault-tolerant" and "is fault-tolerant" that a restart drill (not just reading the code) actually catches.

**`Trigger.Once` and `Trigger.AvailableNow` solve the same backfill problem with a real reliability tradeoff.** `Once` forces everything into a *single* micro-batch — simple, but can overload a cluster sized for incremental load if the backlog is large. `AvailableNow` chunks the same backfill into multiple, source-rate-limited micro-batches — safer at scale, marginally more complex to reason about. Choosing between them is a capacity-planning decision, not a stylistic one.

---

## 5. Concepts that are easy to confuse

| A | B | The distinction |
|---|---|---|
| **Append output mode** | **Update output mode** | Append emits only brand-new rows since the last trigger — required for non-aggregation queries and supported for watermarked aggregations. Update emits only *changed* rows (including revisions to an existing aggregate) — required for `mapGroupsWithState` and plain aggregations without a matching watermark shape. |
| **Watermark delay** | **Trigger interval** | Watermark delay bounds how late *event-time* data may arrive before its window's state is dropped. Trigger interval bounds how often a new micro-batch *starts* (processing-time, unrelated to the data's own timestamps). |
| **At-least-once (the sink's default)** | **Exactly-once (what you build on top)** | At-least-once is what Kafka/Foreach sinks give you natively. Exactly-once is not automatic — it requires your own `batchId` deduplication logic inside `foreachBatch`, or choosing a sink (File) that provides it natively. |
| **`Trigger.Once`** | **`Trigger.AvailableNow`** | Both process all currently-available data then stop. `Once` does it as one single (potentially huge) micro-batch. `AvailableNow` splits it into multiple, rate-limited micro-batches — the generally safer choice for large backfills. |
| **Continuous processing (older, experimental)** | **`Trigger.RealTime` (Spark 4.1+)** | Both aim at low latency and both remain non-default. `RealTime` is the newer, more actively-developed low-latency mode (5-300ms p99); continuous processing is the older experimental mode HPS explicitly advises treating with more caution as `RealTime` matures. Don't assume they're the same feature under two names. |
| **Graceful restart (`query.stop()`)** | **Non-graceful kill/crash recovery** | Both should be tested — HPS's explicit operational point is that they exercise genuinely different code paths (clean shutdown vs. recovery from an unclean state), and only testing the graceful path leaves the actual failure mode you'll hit in production unverified. |

---

## 6. Practical engineering patterns

**Watermarked windowed aggregation, output mode chosen correctly:**
```python
from pyspark.sql import functions as F

events = (
    spark.readStream.format("parquet").schema(schema).load(input_path)
    .withWatermark("event_time", "10 minutes")
)

windowed_counts = (
    events.groupBy(F.window("event_time", "5 minutes"), "is_fraud")
    .count()
)

query = (
    windowed_counts.writeStream
    .outputMode("update")   # NOT append — aggregation without an already-closed window still updates
    .format("parquet")
    .option("checkpointLocation", "/tmp/day06_checkpoint")
    .option("path", "/tmp/day06_output")
    .start()
)
```

**Exactly-once on an at-least-once sink via `foreachBatch` + `batchId` dedup:**
```python
def write_batch(batch_df, batch_id):
    already_written = check_if_batch_already_applied(batch_id)  # your own durable tracking
    if not already_written:
        batch_df.write.format("your-sink").mode("append").save(...)
        mark_batch_applied(batch_id)

query = (
    streaming_df.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", "/tmp/day06_checkpoint_dedup")
    .start()
)
```

**Reusing one query for both backfill and ongoing processing (the "lambda" pattern HPS names):**
```python
# Backfill: process everything currently available, then stop
backfill_query = streaming_df.writeStream.trigger(availableNow=True).start()
backfill_query.awaitTermination()

# Ongoing: the SAME query logic, now on a regular processing-time trigger
ongoing_query = streaming_df.writeStream.trigger(processingTime="1 minute").start()
```

**Avoid uncheckpointed driver-side state:**
```python
# WRONG — lost on restart, checkpoint or not
running_totals = {}
def unsafe_update(batch_df, batch_id):
    for row in batch_df.collect():
        running_totals[row.key] = running_totals.get(row.key, 0) + row.value

# RIGHT — let Spark's own state-tracking (mapGroupsWithState / aggregation) own the state
```

---

## 7. Common mistakes and misconceptions

1. **Setting a watermark and expecting state cleanup with Complete output mode.** Complete mode is defined to retain everything; watermark-driven state cleanup only fires under Append/Update (§2).
2. **Assuming any streaming sink gives exactly-once by default.** Only File Sink does. Kafka/Foreach are at-least-once unless you add your own `batchId` deduplication.
3. **Storing running state in a plain Python/Scala variable instead of Spark's own state-tracking.** HashMaps or counters kept outside `mapGroupsWithState`/aggregation/accumulator machinery are invisible to checkpointing and silently reset on restart.
4. **Using `Trigger.Once` for a backfill large enough to overload the cluster.** A single micro-batch means no internal chunking — `AvailableNow` exists specifically to chunk a large backfill safely.
5. **Never testing non-graceful failure.** Only exercising `query.stop()` verifies the easy path; a real crash/kill exercises checkpoint recovery differently and is where data-loss/duplication bugs actually hide.
6. **Treating a `withWatermark` call placed *after* the aggregation as equivalent to one placed before it.** Per §2's four conditions, ordering matters — a watermark declared post-aggregation does not enable the intended state cleanup.

---

## 8. Production considerations

- **Streaming driver restarts are an operational practice, not a failure mode to avoid entirely.** HPS's stated experience: "having periodic (at least weekly) restarts of your driver during business hours is a good way to ensure that driver restart issues are found and addressed, not at 2 a.m." — schedule the disruption on your terms.
- **Checkpoint location must be genuinely fault-tolerant storage** (an HDFS-compatible directory, typically cloud object storage or HDFS itself) — a checkpoint on ephemeral local disk defeats the entire mechanism the moment the node holding it is lost, identical in spirit to Day 14's warning about Ray Train checkpoints on ephemeral cluster storage.
- **Idempotency is a pipeline-design decision, made once, that determines your actual delivery guarantee** — retrofitting exactly-once semantics onto a sink that wasn't designed for it (no natural dedup key, no `batchId`-aware write path) after an incident is far more expensive than choosing the right sink or dedup strategy up front.
- **A watermark delay is a business/product tradeoff, not just a technical parameter** — a longer delay tolerates more late data (fewer dropped/undercounted records) at the cost of higher latency before a window's result is considered final; a shorter delay gets faster results at real risk of silently undercounting genuinely-late events. This decision belongs with whoever owns the metric's business meaning, not purely with the engineer tuning the pipeline.
- **This is where Day 08's orchestration material and this file meet**: a long-running streaming query is typically *not* triggered per-run by an external orchestrator (Airflow/Dagster) the way a batch job is — it's a standing service that the orchestrator monitors/restarts rather than launches fresh each time.

---

## 9. Debugging and performance reasoning

| Symptom | Likely cause | Where to look |
|---|---|---|
| Aggregation state never shrinks / driver memory grows unbounded | No watermark set, or one of §2's four conditions not met (wrong output mode, watermark on wrong column, or declared after the aggregation) | Confirm output mode is Append/Update; confirm `withWatermark` precedes the aggregation on the exact timestamp column used |
| Duplicate records in the sink after a restart | Sink is at-least-once (Kafka/Foreach) with no `batchId` dedup logic | Add `foreachBatch` + durable batch-tracking; or switch to File Sink if exactly-once is required and feasible |
| Data loss after a crash (not a graceful stop) | Non-replayable source, or checkpoint location isn't genuinely durable | Verify checkpoint location is real fault-tolerant storage; verify source supports replay from an offset |
| Query silently drops records you expected to see | Watermark delay too short for your actual event-time lateness distribution | Compare your watermark delay against measured real-world lateness; remember the guarantee is one-directional (§2) |
| `Trigger.Once` backfill overwhelms the cluster | Entire backlog forced into one micro-batch | Switch to `Trigger.AvailableNow` with an explicit `maxFilesPerTrigger`/`maxOffsetsPerTrigger` |
| Restart takes far longer than expected / fails to resume | Driver-side state that wasn't part of Spark's checkpointed state machinery | Audit for plain variables/HashMaps holding logic-critical state outside `mapGroupsWithState`/accumulators |

---

## 10. Examples and exercises

### Worked example — file/Kafka-like micro-batch stream with windowed aggregation (this day's own syllabus implementation task)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

spark = SparkSession.builder.appName("day06-streaming").getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("is_fraud", BooleanType(), False),
])

stream = (
    spark.readStream.format("csv").schema(schema)
    .option("maxFilesPerTrigger", 1)
    .load("ray-learning/datasets/generated_stream_source")   # a directory you drip-feed files into
    .withWatermark("event_time", "5 minutes")
)

windowed = stream.groupBy(F.window("event_time", "1 minute"), "is_fraud").agg(F.sum("amount"))

query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("checkpointLocation", "/tmp/day06_checkpoint_example")
    .trigger(processingTime="10 seconds")
    .start()
)
```

### Exercises (unsolved — write these yourself, get reviewed)

1. **Inject late events and restart the query from checkpoint** (this day's own syllabus experiment) — send events with timestamps both inside and outside your configured watermark delay, confirm which get aggregated and which are dropped, then kill and restart the query and confirm it resumes from checkpoint rather than reprocessing from scratch.
2. **Explain what state survives a restart and what does not** (this day's own syllabus exercise) — deliberately store one piece of state in Spark's own aggregation/`mapGroupsWithState` machinery and another in a plain driver-side variable, then crash and restart the query. Document precisely which survived and why.
3. **Confirm a restart produces the expected result without manually repairing state** (this day's own syllabus verification criterion) — after the restart in exercise 2, verify the aggregated output is correct without any manual intervention on your part.
4. **Build exactly-once on Kafka (or a Kafka-like sink) using `batchId` deduplication.** Implement the `foreachBatch` pattern from §6, then deliberately reprocess a batch (simulate a restart mid-write) and confirm no duplicate effect occurred.
5. **Compare `Trigger.Once` vs. `Trigger.AvailableNow` on the same backlog.** Build up a backlog of unprocessed files, then run the same query once with each trigger type. Measure and explain the difference in behavior and resource usage.
