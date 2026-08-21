# Competency Map — what "Senior Data Engineer" means here

The target definition. Each competency is rated on the same 1–5 scale (see `assessments/rubric.md`).
`tracker/learning-state.md` holds my *current* rating per competency; this file defines what each
rating *looks like* so grading is honest and consistent.

## The 5 levels (applied to every competency)
1. **Novice** — aware of it; needs guidance for basic tasks.
2. **Advanced beginner** — can do standard tasks with reference; shallow "why".
3. **Competent** — works independently on common cases; explains the mechanism; some trade-off awareness.
4. **Proficient** — handles hard/edge cases; debugs failures; reasons about performance, cost, reliability; makes sound design choices.
5. **Senior** — designs from scratch under real constraints; anticipates failure modes; optimizes cost/latency/reliability; teaches and reviews others; defends trade-offs.

**Senior bar overall = consistent 4–5 on Apply + Design/Debug across the competencies below,
demonstrated on projects/architecture reviews (not quizzes).**

## Competencies

| # | Competency | Senior-level (5) looks like |
|---|-----------|------------------------------|
| C1 | **SQL** | Complex analytical SQL; reads query plans; tunes for the planner; reasons about isolation & concurrency. |
| C2 | **Data modeling** | Designs normalized OLTP and dimensional/vault OLAP schemas; picks grain & SCD strategy with justification. |
| C3 | **Warehouses & lakehouse** | Chooses storage/format/partitioning for cost & performance; understands MPP & table formats (Delta/Iceberg). |
| C4 | **Storage & file formats** | Picks row/columnar formats, compression, layout; solves small-files & partitioning problems. |
| C5 | **Distributed systems** | Reasons about partitioning, replication, consistency, shuffles, skew, fault tolerance. |
| C6 | **Batch processing (Spark)** | Writes and *tunes* Spark; reads physical plans; fixes skew/spill/shuffle problems. |
| C7 | **Streaming** | Designs streaming systems; reasons about delivery semantics, ordering, watermarks, state. |
| C8 | **Orchestration** | Builds idempotent, backfillable, dependency-aware pipelines; handles retries & SLAs. |
| C9 | **Transformation / analytics eng (dbt)** | Structures transformations like software: models, tests, incremental, lineage. |
| C10 | **Data quality & contracts** | Enforces validation, contracts, freshness/volume/schema checks; designs for bad data. |
| C11 | **Observability** | Instruments pipelines (freshness/volume/schema/lineage, logs/metrics/traces); sets SLOs & alerts. |
| C12 | **Reliability** | Designs for failure: idempotency, retries, DLQ, recovery, backfills; runs incident debugging. |
| C13 | **Performance & cost** | Profiles and optimizes queries/jobs; reasons about $ and latency trade-offs; capacity planning. |
| C14 | **Cloud & infrastructure** | Uses managed data services well; provisions with Terraform; CI/CD for data. |
| C15 | **Security & governance** | IAM, encryption, PII handling/masking, access control, catalogs, lineage, compliance basics. |
| C16 | **Production operations** | Deploys, monitors, and operates pipelines; on-call-grade debugging; runbooks. |
| C17 | **Architecture & system design** | Designs end-to-end platforms under constraints; defends trade-offs; reviews others' designs. |
| C18 | **Debugging** | Systematically isolates failures across SQL, Spark, pipelines, infra; forms & tests hypotheses. |
| C19 | **Communication of trade-offs** | Explains designs and decisions clearly to eng & stakeholders; writes crisp design docs. |

> These map to the roadmap phases. C17/C18/C19 are cross-cutting and assessed continuously.
