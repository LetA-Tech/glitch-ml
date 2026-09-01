# Backlog — prioritized next actions (curated, NOT a log)

> The queue of what to do next. Top item = the "next action" a session resumes at.
> Edit in place: remove done items (they're logged in `sessions/`), reorder as priorities change.
> 3 tracks run in parallel as of 2026-08-31 (deadline 2026-10-05) — each keeps its own queue below.

## Next up — Track A (Grokking ML)
1. Ch2 Test 2: `src/ch02/ch02_imbalance.py` — accuracy-paradox exercise on ~1% fraud data.
2. Finalize Ch2 scorecard, then start Ch3 "Linear Regression."

## Next up — Track B (ray-learning)
1. `cd ray-learning && make data` — generate the synthetic dataset.
2. Day 01 (2026-09-01): Python execution model for DE. Move the matching Linear issue to In Progress.

## Next up — Track C (Senior-DE competency ladder)
1. **Session 0 — Calibration & setup** (parked — resume in the 2026-09-21 → 2026-10-05 window):
   - Pick primary cloud track (AWS or GCP).
   - Rapid competency probe: 3–5 targeted tasks (a hard SQL query, a small modeling problem, a
     "design a batch pipeline" prompt, a debugging scenario) to rate C1–C19 honestly.
   - Set starting module (likely M1 SQL unless calibration shows otherwise).
   - Fill `tracker/learning-state.md` ratings + top gaps; set `tracker/progress.md` statuses.
   - Confirm local env: Docker + Postgres + DuckDB + Python (`uv`).

## Soon
- Draft `curriculum/syllabus/module-01-sql-foundations.md` into first labs/exercises.
- Stand up the local data stack in `scripts/` (docker-compose: Postgres + a seed dataset).
- Extend `curriculum/competency_map.md` with ML/AI competencies (currently DE-only C1-C19,
  despite the target role being AI **and** Data Engineer) — needs its own focused pass.
- TensorFlow: runs in parallel with Track A, not deferred (Lucas's call, 2026-08-31) — anchor
  it at Grokking Ch10 (Neural Networks) using Géron's Hands-On ML w/ TensorFlow/Keras;
  reinforce again in the 2026-09-21 → 10-05 window if time allows.
- SAP data-layer pass: **Core Data Services for ABAP** (2024) + **SAP Gateway and OData**
  (3rd Ed.) — both in `ebook-library/12-SAP-and-Enterprise-Systems/`. Slot: 2026-09-21 → 10-05.
  Not the MM/SD/FI configuration books — those are out of scope per Lucas's DE+ML focus.

## Someday / parking lot
- Choose the project-spine domain (e.g., an e-commerce or IoT dataset) for the integrated build.
- Decide streaming stack for M10 (Kafka + Spark Structured Streaming vs Flink).
