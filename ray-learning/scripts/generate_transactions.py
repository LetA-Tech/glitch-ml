from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate(rows: int, seed: int, fraud_rate: float, output: Path) -> None:
    rng = np.random.default_rng(seed)
    output.mkdir(parents=True, exist_ok=True)

    customer_count = max(10_000, rows // 20)
    account_count = max(15_000, rows // 12)

    customers = pd.DataFrame({
        "customer_id": np.arange(customer_count, dtype=np.int64),
        "segment": rng.choice(["mass", "affluent", "student", "small_business"], customer_count, p=[0.64, 0.12, 0.14, 0.10]),
        "province": rng.choice(["QC", "ON", "BC", "AB", "NS"], customer_count, p=[0.34, 0.38, 0.13, 0.11, 0.04]),
    })

    accounts = pd.DataFrame({
        "account_id": np.arange(account_count, dtype=np.int64),
        "customer_id": rng.integers(0, customer_count, account_count, dtype=np.int64),
        "account_type": rng.choice(["chequing", "savings", "credit"], account_count, p=[0.48, 0.27, 0.25]),
    })

    # Deliberately skew 15% of events toward a tiny hot-account set for Spark/Ray labs.
    hot_accounts = np.arange(min(32, account_count), dtype=np.int64)
    account_ids = rng.integers(0, account_count, rows, dtype=np.int64)
    hot_mask = rng.random(rows) < 0.15
    account_ids[hot_mask] = rng.choice(hot_accounts, hot_mask.sum())

    amounts = np.round(rng.lognormal(mean=3.2, sigma=1.0, size=rows), 2)
    categories = rng.choice(
        ["groceries", "dining", "transport", "utilities", "shopping", "travel", "cash", "transfer"],
        rows,
        p=[0.20, 0.14, 0.12, 0.10, 0.16, 0.07, 0.05, 0.16],
    )
    timestamps = pd.Timestamp("2026-01-01", tz="UTC") + pd.to_timedelta(rng.integers(0, 90 * 24 * 3600, rows), unit="s")

    fraud = rng.random(rows) < fraud_rate
    # Make the rare label learnable enough for pipeline exercises without pretending to be realistic fraud modeling.
    fraud |= (amounts > np.quantile(amounts, 0.995)) & (categories == "travel") & (rng.random(rows) < 0.35)

    transactions = pd.DataFrame({
        "transaction_id": np.arange(rows, dtype=np.int64),
        "account_id": account_ids,
        "event_time": timestamps,
        "amount": amounts,
        "category": categories,
        "channel": rng.choice(["card", "ach", "cash", "etransfer"], rows, p=[0.62, 0.15, 0.08, 0.15]),
        "is_fraud": fraud.astype(np.int8),
    })

    customers.to_parquet(output / "customers.parquet", index=False)
    accounts.to_parquet(output / "accounts.parquet", index=False)
    transactions.to_parquet(output / "transactions.parquet", index=False)

    print({
        "rows": rows,
        "seed": seed,
        "fraud_rate_observed": float(transactions["is_fraud"].mean()),
        "output": str(output),
    })


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--fraud-rate", type=float, default=0.0075)
    parser.add_argument("--output", type=Path, default=Path("ray-learning/datasets/generated"))
    args = parser.parse_args()
    generate(args.rows, args.seed, args.fraud_rate, args.output)


if __name__ == "__main__":
    main()
