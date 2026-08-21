# Grading & Evaluation Rubric

How Claude decides whether I **truly understand** a concept vs. merely recognize/repeat it.
Applied continuously and at explicit checkpoints. Results are recorded in `assessments/results/`
and summarized into `tracker/learning-state.md`.

## The Understanding Ladder (the core idea)
A concept is only "known" when I can climb this ladder. Recognition is the *bottom*, not the goal.

| Rung | Name | What I must do | Counts as mastery? |
|------|------|----------------|--------------------|
| 1 | **Recognize** | Pick the right answer / recall a definition | ❌ no |
| 2 | **Explain** | Teach it in my own words, with the mechanism and *why* | ❌ not alone |
| 3 | **Apply** | Build/implement it correctly on a new problem, unaided | ✅ required |
| 4 | **Transfer / Design** | Use it under a *new constraint*; design with it; debug when it breaks; justify trade-offs | ✅ required for senior |

> **Mastery = reliable rung 3 AND rung 4.** Explaining well but failing to build ≠ mastery.

## Scoring (1–5 per competency; see `competency_map.md` for level descriptors)
Rated per assessment on the dimension being tested:
- **1 Novice · 2 Advanced beginner · 3 Competent · 4 Proficient · 5 Senior.**
- A competency's tracker rating only rises to 4–5 after **Apply + Design/Debug** evidence, not a single quiz.

## Assessment types (rotate — never rely on one)
1. **Concept check (oral/written):** explain in own words + "why" + "when NOT to use it." (rung 2)
2. **Implementation task:** blank-page build to a spec, unaided. (rung 3)
3. **Debugging scenario:** I'm given a broken query/job/pipeline; I diagnose and fix, narrating hypotheses. (rungs 3–4, tests C18)
4. **Architecture review:** design a system under constraints, or critique a given design; defend trade-offs. (rung 4, tests C17/C19)
5. **Code/design review:** I review flawed code/design and find the issues a senior would. (rung 4)
6. **Timed drill:** e.g., SQL under time pressure — fluency, not just correctness.
7. **Spaced recall:** revisit earlier modules to prove retention (from `review/`).

## Anti-recognition techniques (Claude should use these often)
- Ask **"why"** and **"why not the alternative?"** — not just "what."
- Change one constraint and ask me to redesign (e.g., "now it must be exactly-once" / "now it's 100× data" / "now cost matters more than latency").
- Give me something **broken** to fix rather than something to describe.
- Ask me to **predict** an outcome (query plan, job behavior, failure mode) *before* running, then compare.
- Make me **teach it back** as if to a junior; probe the gaps that surface.
- Blank-page implementation with the reference **closed**.

## Checkpoints
- **Per concept:** quick rung-2/3 check before moving on.
- **Per module:** a graded implementation task + a debugging scenario + an architecture question. Record scores.
- **Per phase (gate):** integrated assessment; must hit ≥ 4/5 Apply + Design/Debug on the phase's competencies to advance.
- **Periodic spaced review:** to catch decay in earlier competencies.

## Recording results
Each graded assessment → a file in `assessments/results/` named `YYYY-MM-DD_mNN_type.md` with:
`competency, type, task, my result, rung reached, score /5, gaps found, reinforcement action`.
Then update the competency rating + gaps in `tracker/learning-state.md`.

## Honesty rules for the grader (Claude)
- Do not inflate. A confident wrong answer scores low. Partial credit is explicit.
- Distinguish **"recognized"** from **"can build"** in feedback every time.
- Always end an assessment with: the score, *why*, and the single most valuable next reinforcement.
