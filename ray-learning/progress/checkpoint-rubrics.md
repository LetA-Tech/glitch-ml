# Checkpoint Rubrics

These are evidence rubrics, not a second progress tracker. Linear owns status.

## Checkpoint A - Day 05: Python + Spark

Score 0-2 each (10 points total):

1. Python execution judgment: chooses streaming/vectorization/threads/processes appropriately.
2. Spark execution model: explains driver/executors/jobs/stages/tasks/partitions correctly.
3. Query-plan reading: identifies exchanges, broadcasts, filters, scans, and avoidable UDFs.
4. Performance: measures a baseline and improves it with evidence.
5. Debugging: fixes one unseen malformed/skewed/inefficient pipeline without tutorial copying.

**Pass:** 7.5/10. Below pass requires targeted remediation before Day 06.

## Checkpoint B - Day 12: Ray Core + failure semantics

1. Tasks vs actors vs ObjectRefs.
2. Scheduling/resources/placement groups.
3. Object-store vs Python heap vs spill/durable storage.
4. Task/actor/object failure prediction.
5. Failure-injection implementation and interpretation.

**Pass:** 8/10.

## Checkpoint C - Day 17: Operational readiness

1. Spark/Ray observability.
2. Serialization/memory/resource debugging.
3. Production deployment concepts.
4. Checkpoint/durability boundaries.
5. Architecture judgment across Spark/Ray/local tools.

**Pass:** 8/10.

## Final - Day 20

Score 0-4 each (40 points):

- Python/data engineering quality
- Spark design and query-plan judgment
- Ray architecture understanding
- distributed data movement/memory reasoning
- fault-tolerance reasoning
- practical ML pipeline quality
- performance measurement/optimization
- observability/debugging
- architecture/runbook quality
- oral defense / ability to reason without notes

**Pass:** 32/40. 36+ indicates strong readiness for immediate production-oriented follow-on work.
