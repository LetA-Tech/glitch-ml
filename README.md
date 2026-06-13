# glitch-ml

A deep, hands-on journey into **AI, Machine Learning, and Data Engineering** — anchored on the
*Grokking* book series and built around a real capstone (a **real-time fraud / anomaly detection**
system) and a real product lens (**Mellions**, a Personal Financial Management app).

The repo holds two things side by side:
- **`curriculum/`** — structured learning: chapter notebooks, flashcards, test questions, interview prep, and scorecards.
- **`src/` + `capstone/`** — runnable code: from-scratch implementations and the capstone that grows one component per chapter.

Environment is managed with [**uv**](https://docs.astral.sh/uv/) on **Python 3.12**, with the full scientific, deep-learning, and LLM tooling stack installed.

> New here? Follow the step-by-step [**SETUP.md**](./SETUP.md) to get a working environment in a few minutes.
> Start with [`curriculum/roadmap.md`](./curriculum/roadmap.md) for the full learning plan.

## How the learning works (per chapter)

Read → understand core concepts → deep dive (intuition, math, when/why, mistakes, real systems) →
notebook → Test 1 (knowledge) → Test 2 (coding) → capstone component → interview translation →
scorecard. Every concept is taught in **three layers**: interview readiness, real technical mastery,
and product application (Mellions). See [`curriculum/product_context_mellions.md`](./curriculum/product_context_mellions.md).

## Stack

| Area | Packages |
|------|----------|
| **Core scientific** | numpy, pandas, scikit-learn, scipy, matplotlib, seaborn |
| **Notebooks** | jupyterlab, ipykernel |
| **Deep learning** | torch, torchvision (Apple **MPS** GPU acceleration), tensorflow |
| **LLM / AI** | transformers, datasets, accelerate, huggingface-hub, anthropic, openai |
| **Utilities** | python-dotenv |

Pinned versions live in [`pyproject.toml`](./pyproject.toml); the exact resolved lockfile is [`uv.lock`](./uv.lock).

## Quickstart

```bash
# 1. Install uv (once, if you don't have it)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Create the environment from the lockfile
cd ML/glitch-ml
uv sync

# 3. Run anything inside the env (no manual activation needed)
uv run python src/ch01_framework.py
uv run jupyter lab
```

`uv sync` reads `.python-version` (→ 3.12), downloads that interpreter if missing, creates `.venv/`, and installs the exact locked dependencies.

## Daily commands

| Task | Command |
|------|---------|
| Run a script | `uv run python src/ch01_framework.py` |
| Open JupyterLab | `uv run jupyter lab` |
| Start a REPL | `uv run python` |
| Add a package | `uv add <pkg>` |
| Remove a package | `uv remove <pkg>` |
| Re-create env from lock | `uv sync` |
| Update all deps | `uv lock --upgrade && uv sync` |
| Show installed tree | `uv tree` |

In notebooks, pick the **`Python (glitch-ml)`** kernel (already registered).

## Secrets / API keys

```bash
cp .env.example .env   # then edit .env with your real keys
```

Load them in code:

```python
from dotenv import load_dotenv
load_dotenv()  # reads .env → os.environ
```

`.env` is gitignored — never commit real keys.

## Hardware notes (Apple Silicon)

- **PyTorch** uses the Apple GPU via the **MPS** backend: `torch.device("mps")`. Verify with `torch.backends.mps.is_available()`.
- **TensorFlow** runs on CPU out of the box. For Metal GPU acceleration, optionally `uv add tensorflow-metal`.

## Project layout

```
glitch-ml/
├── curriculum/                     # the learning track (docs)
│   ├── roadmap.md                  # full plan: chapters → concepts → capstone
│   ├── product_context_mellions.md # Mellions product lens + 3 learning layers
│   ├── notebooks/                  # one structured notebook per chapter
│   │   ├── _template.md            # reusable chapter template
│   │   └── ch01_what_is_ml.md
│   ├── flashcards/                 # spaced-review decks  (ch01_cards.md …)
│   ├── questions/                  # Test 1 knowledge Q&A + evaluation
│   ├── interview/                  # interview translation per chapter
│   └── scorecards/                 # per-chapter evaluation (7 areas, 1–10)
├── src/                            # runnable code (exercises + from-scratch ML)
│   ├── __init__.py
│   └── ch01_framework.py
├── capstone/                       # fraud-detection system (grows each chapter)
│   ├── README.md                   # problem statement + architecture
│   └── data_contract.md            # transaction schema (Ch 1 artifact)
├── pyproject.toml / uv.lock / .python-version   # uv environment
├── .env.example / .gitignore
├── README.md
└── SETUP.md
```

> **Note:** the *Grokking* book PDFs are **not** in this repo. They're copyrighted; we study them
> locally only. `.gitignore` blocks `*.pdf` and `books-to-read/` so they can never be committed.
