# Study System: GitHub + Linear + Notion

This program uses three tools with deliberately different jobs.

## Authority model

| Tool | Owns | Does not own |
|---|---|---|
| GitHub | canonical syllabus, code, labs, exercises, projects, datasets/generators, durable technical notes, rubrics, architecture decisions | daily status, long scratch notes |
| Linear | execution state, daily issue, due date, milestone, blockers, completion | detailed learning notes, code |
| Notion | reading notes, explanations, questions, misconceptions, experiment interpretation, synthesis | canonical code, duplicate task tracking |

## Daily workflow

1. Open today's Linear issue and move it to **In Progress**.
2. Open its Notion Study Workspace.
3. Read the GitHub day definition and bounded sources.
4. Implement the required code in GitHub.
5. Run the experiment and capture raw evidence beside the code when useful.
6. Write interpretation, mental model, confusion, and synthesis in Notion.
7. Run the day's verification.
8. Commit durable artifacts to GitHub.
9. Mark Linear **Done** only when the GitHub Definition of Done is satisfied.

## Time budget

Default daily allocation:

- 45-75 min: reading/concept construction
- 2.5-3.5 h: coding/lab/experiment
- 45-60 min: exercise/debugging
- 30 min: verification + Notion synthesis

Reading may expand only when it directly unblocks implementation. No day should become a book-reading marathon.

## Source handling

The two Ray books are primary study material for Ray fundamentals and examples, but they were published around Ray 2.0-2.2. Current official Ray documentation is the authority for modern APIs and deprecated features.

For Spark, prioritize official Spark documentation plus selected chapters from a strong Spark reference such as *Learning Spark, 2nd Edition*.

For Python and scikit-learn, prefer official documentation for exact behavior and API details.

## What gets committed

Commit:
- runnable Python/PySpark/Ray code;
- tests;
- benchmark scripts/results when reproducibility matters;
- architecture diagrams/ADRs;
- concise durable reference notes;
- project runbooks;
- checkpoint artifacts.

Do not commit:
- copyrighted book PDFs;
- large generated datasets;
- transient scratch notes;
- copied documentation;
- duplicate Notion notes.

## Progress policy

`progress/` stores rubrics, checkpoint evidence, and final assessment artifacts. It is **not** a second checkbox tracker. Linear is the only authoritative execution-status system.

## Solution policy

`solutions/` begins nearly empty. Solutions are written or promoted only after an exercise has been attempted. This prevents the repository from becoming a tutorial to copy rather than a learning environment.
