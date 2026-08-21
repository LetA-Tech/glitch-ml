# Backlog — prioritized next actions (curated, NOT a log)

> The queue of what to do next. Top item = the "next action" a session resumes at.
> Edit in place: remove done items (they're logged in `sessions/`), reorder as priorities change.

## Next up
1. **Session 0 — Calibration & setup** (do this first):
   - Pick primary cloud track (AWS or GCP).
   - Rapid competency probe: 3–5 targeted tasks (a hard SQL query, a small modeling problem, a
     "design a batch pipeline" prompt, a debugging scenario) to rate C1–C19 honestly.
   - Set starting module (likely M1 SQL unless calibration shows otherwise).
   - Fill `tracker/learning-state.md` ratings + top gaps; set `tracker/progress.md` statuses.
   - Confirm local env: Docker + Postgres + DuckDB + Python (`uv`).

## Soon
- Draft `curriculum/syllabus/module-01-sql-foundations.md` into first labs/exercises.
- Stand up the local data stack in `scripts/` (docker-compose: Postgres + a seed dataset).

## Someday / parking lot
- Choose the project-spine domain (e.g., an e-commerce or IoT dataset) for the integrated build.
- Decide streaming stack for M10 (Kafka + Spark Structured Streaming vs Flink).
