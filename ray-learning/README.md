# ray-learning

A 20-day intensive engineering program for practical mastery of Python data engineering, Apache Spark / PySpark, Ray, distributed execution, and ML workloads relevant to a Senior Data Engineer.

This area is intentionally deadline-driven. The goal is not encyclopedic coverage. The goal is to become materially stronger at reasoning about and implementing distributed data/ML systems within 20 focused study days.

## Source-of-truth model

- **GitHub**: canonical syllabus, code, labs, experiments, datasets/generators, projects, durable notes, assessments, and reusable artifacts.
- **Linear**: execution state, daily work, deadlines, milestones, and completion.
- **Notion**: reading notes, explanations, mental models, research, misconceptions, experiment interpretation, and synthesis.

Do not duplicate the same content across all three tools. GitHub defines what to build; Linear tracks whether it is done; Notion captures what was learned.

## Start here

1. Read [`syllabus/20-day-intensive.md`](./syllabus/20-day-intensive.md).
2. Read [`guides/study-system.md`](./guides/study-system.md).
3. Review [`references/reading-map.md`](./references/reading-map.md).
4. Generate the synthetic dataset with `make data`.
5. Start Day 01 and move the matching Linear issue to **In Progress**.

## Daily operating loop

> concept -> reading -> implementation -> experiment -> exercise -> verification

Target **4.5-6 focused hours/day**, with at least **60% hands-on engineering**. Reading is capped so the program cannot turn into passive study.

A day is complete only when the required code or artifact exists, the experiment was actually run, the result was interpreted, the verification criteria pass, and the corresponding Notion workspace has a concise synthesis.

## Repository structure

```text
ray-learning/
├── syllabus/                 # canonical 20-day plan
├── references/               # book + current official-doc reading map
├── notes/                    # durable distilled notes only; working notes live in Notion
├── exercises/                # implementation exercises and index
├── labs/
│   ├── python/               # concurrency / serialization / profiling
│   ├── spark/                # partition, shuffle, query-plan, streaming work
│   ├── ray/                  # Core, object store, scheduling, failures
│   └── ml/                   # practical ML pipeline exercises
├── projects/
│   └── capstone/             # Spark -> Ray -> ML integration project
├── datasets/                 # dataset policy; generated data stays local
├── scripts/                  # reproducible dataset generators/utilities
├── solutions/                # solutions added only after an exercise is completed
├── progress/                 # checkpoint rubrics, not a second status tracker
├── pyproject.toml            # isolated learning environment
└── Makefile
```

## Primary Ray books

The Ray track is anchored on the two supplied books:

1. *Learning Ray: Flexible Distributed Python for Machine Learning* — Pumperla, Oakes, Liaw.
2. *Scaling Python with Ray: Adventures in Cloud and Serverless Patterns* — Karau, Lublinsky.

The books are study sources, not repository artifacts. Do not commit the PDFs. Their 2022/2023 APIs are reconciled with current official Ray documentation in [`references/reading-map.md`](./references/reading-map.md).

## Deliberate scope cuts

The 20-day critical path excludes deep RLlib, exhaustive ML theory, deprecated Ray Workflows APIs, broad cloud-provider surveys, and academic detours that do not improve near-term data-engineering judgment.

We prioritize: Python execution, Spark DataFrames/SQL/partitioning/shuffles, Ray Core/Data/Train/Tune/Serve, fault behavior, performance, observability, and one production-oriented integration capstone.
