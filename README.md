# glitch-ml

A machine learning / AI project. Environment is managed with [**uv**](https://docs.astral.sh/uv/) on **Python 3.12**, with the full scientific, deep-learning, and LLM tooling stack installed.

> New here? Follow the step-by-step [**SETUP.md**](./SETUP.md) to get a working environment in a few minutes.

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
├── src/                  # source code
│   ├── __init__.py
│   └── ch01_framework.py
├── pyproject.toml        # project + dependency declarations
├── uv.lock               # exact resolved versions (commit this)
├── .python-version       # pins Python 3.12 for uv
├── .env.example          # template for API keys (copy → .env)
├── .gitignore
├── README.md
└── SETUP.md              # step-by-step environment setup
```
