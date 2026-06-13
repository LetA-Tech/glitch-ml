# Setup Guide — glitch-ml

Step-by-step setup of the Python environment for this project. Target: **Python 3.12**, managed by **uv**. Tested on macOS (Apple Silicon).

Total time: ~5 minutes plus download time for the ML libraries (torch + tensorflow are large).

---

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
