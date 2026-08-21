# Module 01 — SQL Mastery (sample syllabus / the pattern for all modules)

> Every module syllabus follows this shape. Claude uses it to teach, assign, and assess. It is a
> *plan*, not a log — progress is tracked in `tracker/`.

**Competencies:** C1 (SQL), supports C13 (performance), C18 (debugging).
**Prerequisite:** M0 environment (Postgres + DuckDB running locally).
**Estimated:** ~3–5 sessions. **Gate:** ≥ 4/5 Apply + Design/Debug on SQL tasks.

## Learning outcomes (what "done" means)
By the end I can, unaided:
1. Write correct multi-table queries with joins, aggregation, and set operations.
2. Use window functions fluently (ranking, running totals, lag/lead, framing).
3. Structure complex logic with CTEs (and know when a subquery/temp table is better).
4. Read `EXPLAIN (ANALYZE)` output and reason about the planner's choices.
5. Use indexes appropriately and explain when they help vs. hurt.
6. Reason about transactions, isolation levels, and concurrency anomalies.

## Concept sequence (teach simple → deep)
1. Query semantics & logical execution order (why `WHERE` before `SELECT`, etc.).
2. Joins deeply (inner/outer/semi/anti, join algorithms: nested loop / hash / merge).
3. Aggregation & `GROUP BY`/`HAVING`; grouping sets/rollup.
4. Window functions (partition/order/frame) — the senior differentiator.
5. CTEs, recursion, and readability vs. performance.
6. Indexes & the planner; `EXPLAIN ANALYZE`; selectivity, cardinality estimates.
7. Transactions & isolation (read phenomena, MVCC intuition).

## Labs (hands-on; local Docker Postgres + DuckDB)
- **L1** Load a seed dataset; write 10 progressively harder queries (→ `labs/m01-sql/`).
- **L2** Window-function gauntlet: top-N per group, running/moving aggregates, sessionization.
- **L3** Performance: take a slow query, read its plan, add an index, prove the improvement.

## Exercises (drills → `exercises/`)
- Daily SQL katas (timed) for fluency; one focused set per concept above.

## Assessment (per `assessments/rubric.md`)
- **Implementation task:** blank-page — answer 3 business questions on the dataset with correct, performant SQL.
- **Debugging scenario:** given a wrong-result query and a slow query — fix correctness, then performance.
- **Design/explain:** "why did the planner choose a hash join here, and when would a merge join win?"
- Record score in `assessments/results/`; update C1 rating + gaps in `tracker/learning-state.md`.

## References (curate into `references/`, don't paste docs)
- Postgres docs (indexes, `EXPLAIN`), window-function reference, isolation-levels primer.

## Notebook deliverable
- `notebooks/m01-sql.md` — my own explanations + a Mermaid diagram of logical execution order and join algorithms.
