PYTHON ?= uv run python

.PHONY: sync data test

sync:
	uv sync --project ray-learning

# Default local dataset for the labs. Override ROWS/SEED if needed.
data:
	$(PYTHON) ray-learning/scripts/generate_transactions.py --rows $${ROWS:-500000} --seed $${SEED:-42}

test:
	uv run --project ray-learning pytest ray-learning
