# Setup Guide — glitch-ml

Step-by-step setup of the Python environments for this project. Target: **Python 3.12**, managed by **uv**. Tested on macOS (Apple Silicon).

**This repo has two separate, isolated uv environments** — deliberately not one, so version constraints don't collide (root wants TensorFlow, `ray-learning` pins Spark 4.x / Ray 2.x / PyTorch together):

| | Path | Purpose | Key deps |
|---|---|---|---|
| **Environment 1** | repo root (`.venv`) | Grokking ML track, general notebooks | numpy, pandas, sklearn, torch, tensorflow, jupyter |
| **Environment 2** | `ray-learning/` (own `.venv`) | 20-day Spark/Ray/DE intensive | pyspark, ray[default,data,train,tune,serve], torch, optuna |

Each has its own `pyproject.toml` + `uv.lock` + `.venv` — `cd` into the right directory (or use `uv run --project <dir>`) before running anything. Set up both below.

Total time: ~5 minutes each, plus download time for the ML libraries (torch/tensorflow/pyspark are large).

---

# Environment 1 — glitch-ml root

## Step 0 — Prerequisites

You need `git` and a terminal. Everything else (Python 3.12 itself, the virtual environment, all packages) is handled by `uv` in the steps below — you do **not** need to pre-install Python.

---

## Step 1 — Install uv

`uv` is the package & environment manager for this project (fast replacement for pip + venv + pyenv).

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then restart your shell (or `source ~/.zshrc`) and confirm:

```bash
uv --version
```

> Already installed? Make sure it's recent: `uv self update`.

---

## Step 2 — Get into the project

```bash
cd ~/ML/glitch-ml
```

The repo already contains everything `uv` needs:
- `pyproject.toml` — declares the dependencies
- `uv.lock` — the exact, reproducible versions
- `.python-version` — pins the interpreter to `3.12`

---

## Step 3 — Create the environment

```bash
uv sync
```

This single command:
1. Reads `.python-version` → installs a managed **CPython 3.12** if you don't have it.
2. Creates a virtual environment at `.venv/`.
3. Installs the **exact** locked dependencies from `uv.lock`.

You never have to `source .venv/bin/activate` — `uv run` (next step) handles activation automatically. (If you *want* a classic activated shell: `source .venv/bin/activate`.)

---

## Step 4 — Verify the install

Run the built-in smoke test:

```bash
uv run python -c "
import sys, platform
print('python', sys.version.split()[0], platform.machine())
import numpy, pandas, sklearn
print('scientific ok')
import torch
print('torch', torch.__version__, '| MPS (Apple GPU):', torch.backends.mps.is_available())
import tensorflow as tf
print('tensorflow', tf.__version__)
import transformers, anthropic, openai
print('llm tooling ok')
"
```

Expected output (versions may differ slightly):

```
python 3.12.12 arm64
scientific ok
torch 2.12.0 | MPS (Apple GPU): True
tensorflow 2.21.0
llm tooling ok
```

If you see that, the environment works. ✅

---

## Step 5 — API keys (optional, for LLM work)

If you'll call Anthropic / OpenAI / Hugging Face:

```bash
cp .env.example .env
```

Edit `.env` and paste your real keys. They load via:

```python
from dotenv import load_dotenv
load_dotenv()
```

`.env` is gitignored, so secrets stay out of version control.

---

## Step 6 — Jupyter (optional)

A kernel named **`Python (glitch-ml)`** is already registered for this environment. Launch JupyterLab:

```bash
uv run jupyter lab
```

Then in any notebook choose **Kernel → Python (glitch-ml)**.

If the kernel is missing (e.g. fresh machine), re-register it:

```bash
uv run python -m ipykernel install --user --name glitch-ml --display-name "Python (glitch-ml)"
```

---

## Everyday usage

```bash
uv run python src/ch01_framework.py   # run a script
uv run jupyter lab                # notebooks
uv run python                     # REPL
uv add <package>                  # install a new dependency (updates pyproject + lock)
uv remove <package>               # uninstall
uv sync                           # rebuild env to match the lockfile
uv tree                           # inspect the dependency tree
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `uv: command not found` | Re-run Step 1, then restart the shell. |
| Wrong Python version picked | Confirm `.python-version` contains `3.12`; run `uv python install 3.12` then `uv sync`. |
| `torch` MPS is `False` | Update macOS; MPS needs Apple Silicon + recent OS. CPU still works without it. |
| TensorFlow has no GPU | Expected on Mac. For Metal acceleration: `uv add tensorflow-metal`. |
| Want a clean rebuild | `rm -rf .venv && uv sync`. |
| Dependency conflict after `uv add` | `uv lock --upgrade && uv sync` to re-resolve. |

---

# Environment 2 — `ray-learning`

Isolated on purpose: pinned `pyspark>=4.0,<5` and `ray[...]>=2.50,<3` together, which the root environment has no reason to carry. Same tool (`uv`), same mechanics as Environment 1 — just a different directory, `pyproject.toml`, and `.venv`.

## Step 0 — Prerequisites

`uv` already installed from Environment 1, Step 1 above. Nothing new to install here.

## Step 1 — Get into the project

```bash
cd ~/Lucas-Machine-Learning-Space/glitch-ml/ray-learning
```

Contains its own `pyproject.toml` and `uv.lock` (committed — so `uv sync` reproduces the exact same versions on any machine). No `.python-version` file here; `pyproject.toml` pins `requires-python = ">=3.12,<3.14"` instead, and `uv` resolves an interpreter satisfying that.

## Step 2 — Create the environment

```bash
uv sync
```

Same three things as Environment 1's Step 3: installs a matching CPython if needed, creates `ray-learning/.venv`, installs the exact locked versions from `ray-learning/uv.lock`.

> **To practice this from a genuinely clean state** (the point of a from-scratch setup): `rm -rf .venv && uv sync` from inside `ray-learning/`. Safe — `.venv` holds nothing but installed packages, never your own code or data.

## Step 3 — Verify the install

```bash
uv run python -c "
import sys, platform
print('python', sys.version.split()[0], platform.machine())
import numpy, pandas, pyarrow, sklearn
print('scientific ok')
import pyspark
print('pyspark', pyspark.__version__)
import ray
print('ray', ray.__version__)
import torch
print('torch', torch.__version__)
import optuna, pytest, psutil, memory_profiler
print('tooling ok')
"
```

Verified output on this machine (2026-09-01):

```
python 3.12.12 arm64
scientific ok
pyspark 4.2.0
ray 2.58.0
torch 2.13.0
tooling ok
```

If you see that shape (exact versions may drift as the lockfile updates), the environment works.

## Step 4 — Generate the practice dataset

```bash
cd ~/Lucas-Machine-Learning-Space/glitch-ml     # repo ROOT, not ray-learning/
make -f ray-learning/Makefile data
```

**Important check — run this from the repo root, not from inside `ray-learning/`.** The Makefile's targets are written assuming root as the working directory (they reference paths like `ray-learning/scripts/...`). Running it as `cd ray-learning && make data` double-prefixes the path and fails — this was wrong in an earlier version of `tracker/backlog.md` and has been corrected.

Expected output: a JSON line reporting `rows`, `seed`, `fraud_rate_observed` (~0.0075), and `output: ray-learning/datasets/generated`.

## Step 5 — Ray sanity check

```bash
uv run python -c "
import ray
ray.init()
print(ray.cluster_resources())
ray.shutdown()
"
```

Should print a dict with your machine's CPU count and available memory — confirms Ray can actually start a local cluster (head process, GCS, object store, at least one raylet) before you build anything on top of it.

**Important check — verified 2026-09-01: launching the driver via `uv run` makes every Ray worker rebuild its own venv.** `uv run python -c "..."` inside `ray-learning/` triggers Ray's `uv`-integration: it detects the driver was launched through `uv run` and re-invokes `uv run` (fresh venv + all 102 packages) for **every worker process** before that worker runs your first task. Measured: 20 trivial 10ms tasks took **8.7-10s** this way. Same code, driver launched via an activated venv instead (`source .venv/bin/activate && python -c "..."`, no `uv run` wrapper) — **0.33s**. `ray.init(runtime_env={})` does NOT fix it (tested, still slow) — the trigger is the `uv run`-launched driver itself, not `runtime_env.working_dir` packaging.

**Fix:** for anything perf-sensitive, activate the venv and run plain `python`, don't wrap the driver in `uv run`:
```bash
source .venv/bin/activate
python your_script.py
deactivate
```
`uv run` is still fine for one-off exploratory commands where a few extra seconds don't matter.

## Everyday usage

```bash
uv run python <script.py>              # run a script (from inside ray-learning/)
uv run pytest                           # run the test suite (testpaths: exercises, labs, projects)
make -f ray-learning/Makefile data      # regenerate the dataset (from repo root)
make -f ray-learning/Makefile test      # run tests via the Makefile (from repo root)
uv add <package>                        # add a dependency (run inside ray-learning/ — updates its own pyproject+lock)
uv sync                                 # rebuild env to match ray-learning/uv.lock
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `make data` fails / paths look doubled | Run it from the repo root as `make -f ray-learning/Makefile data`, not from inside `ray-learning/`. |
| `.venv` in `ray-learning/` looks empty after `uv run` (but imports still work) | Some `uv run` invocations resolve without fully materializing `.venv` on disk. Run `uv sync` explicitly to force a real, persistent `.venv/bin/python`. |
| First Ray task run is unexpectedly slow (multi-second for trivial tasks) | You launched the driver with `uv run`. Activate the venv and run plain `python` instead — see Step 5's important check above. |
| Want a clean rebuild | `rm -rf ray-learning/.venv && cd ray-learning && uv sync`. |
| Dependency conflict after `uv add` | `cd ray-learning && uv lock --upgrade && uv sync`. |
