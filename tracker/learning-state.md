# Learning State — AUTHORITATIVE (curated snapshot, NOT a log)

> This is the single source of truth for *what I currently know, what I can do, and where my gaps are*.
> **Edit it in place.** Do not append session narration here — that goes in `sessions/`.
> Last updated: 2026-08-31 — reconciled 3 previously-siloed tracks into one snapshot.

## Overall
- **Current stage:** 3 tracks running in parallel (decided 2026-08-31). Target: Senior AI &
  Data Engineer @ SAP, start date ~2026-10-05.
  - **Track A — Grokking ML foundations:** Ch1 complete (7.6/10), Ch2 in progress.
  - **Track B — ray-learning 20-day Spark/Ray/DE intensive:** starts 2026-09-01.
  - **Track C — Senior-DE competency ladder (M0-M18 below):** Session 0 calibration not yet run.
- **Primary cloud track:** _TBD in Session 0_
- **Senior-readiness (holistic):** _TBD_
- **Resolved 2026-08-31:** glitch-de was Lucas's dedicated DE-study repo but never grew past a
  ch01 stub — glitch-ml's tracker/ + ray-learning/ already serve that DE purpose actively.
  Archived on GitHub 2026-08-31 (read-only, not deleted) with a pointer back to glitch-ml.
  glitch-ml is now the sole active repo for this project.
  SAP role needs **CDS (Core Data Services for ABAP)** and **OData (SAP Gateway/OData)**
  specifically — both in `~/ebook-library/12-SAP-and-Enterprise-Systems/`. Daytime focus stays
  DE + ML; CDS/OData layered in during 2026-09-21 → 2026-10-05, not instead of DE/ML.

## Competency ratings (1–5; see `curriculum/competency_map.md`)
> Ratings rise to 4–5 only with Apply + Design/Debug evidence (see `assessments/rubric.md`).

| # | Competency | Rating | Evidence / notes |
|---|-----------|:------:|------------------|
| C1 | SQL | – | calibrate |
| C2 | Data modeling | – | |
| C3 | Warehouses & lakehouse | – | |
| C4 | Storage & file formats | – | |
| C5 | Distributed systems | – | |
| C6 | Batch processing (Spark) | – | |
| C7 | Streaming | – | |
| C8 | Orchestration | – | |
| C9 | Transformation / dbt | – | |
| C10 | Data quality & contracts | – | |
| C11 | Observability | – | |
| C12 | Reliability | – | |
| C13 | Performance & cost | – | |
| C14 | Cloud & infrastructure | – | |
| C15 | Security & governance | – | |
| C16 | Production operations | – | |
| C17 | Architecture & system design | – | |
| C18 | Debugging | – | |
| C19 | Communication of trade-offs | – | |

## Concepts solidly understood (rung 3–4)
- _(none yet — fill after Session 0)_

## Active gaps / weaknesses (prioritized)
- _(to be identified in calibration)_

## Reinforcement queue (things to revisit before they decay)
- _(none yet)_

## Current focus
- **Immediate (today, 2026-09-01):** two blocks, per Lucas's AM/PM structure —
  AM: ray-learning Day 01 (Python execution model for DE, streaming transaction normalizer).
  PM: Grokking ML Ch2 Test 2 — `src/ch02/ch02_imbalance.py` (accuracy-paradox exercise on
  imbalanced fraud data, still open from 2026-08-31).
- **Standing decision (2026-09-01):** distributed-systems fundamentals (architecture,
  execution model, scheduling, fault tolerance, data movement, scaling) run as a companion
  read — Kleppmann's *Designing Data-Intensive Applications* (in `~/ebook-library/`) — layered
  into ray-learning's own reading slots on the days it's most relevant (Day03-06 Spark,
  Day09-13 Ray architecture/objects/fault-tolerance) rather than a new 7th daily block. Keeps
  Day01-02's Python-only reading cap intact per `ray-learning/references/reading-map.md`'s
  own scope discipline.
- **Parked:** M0/Session-0 DE-ladder calibration (see `tracker/backlog.md`) — resume in the
  2026-09-21 → 2026-10-05 window pending the open questions above.
