"""
Chapter 1 — Test 2: Prove generalization with NUMBERS, not vibes.

Requirements:
  1. ~30+ labeled Transactions: some clearly fraud, some clearly legit, a few ambiguous.
  2. train_test_split(data, test_fraction=0.3) — standard library only, SHUFFLE first.
  3. Train ToyModel on the training set ONLY.
  4. accuracy(model, dataset) -> fraction correct.
  5. Print train accuracy AND test accuracy separately.

Run it:  uv run python src/ch01_coding_exercise.py

------------------------------------------------------------------------------
REVIEW NOTES (instructor). Fixes tagged `# FIX:` so you can read the diff as a lesson.
  - Bad import + stray `self` params: you already cleaned these up.
  - Split: now SHUFFLES first and returns (train, test) in the right order/size.
  - Data: the label now actually DEPENDS on amount, so there IS a pattern to learn.
  - Prints BOTH train and test accuracy; adds a baseline + the reflection answer.
------------------------------------------------------------------------------
"""

import random
from dataclasses import dataclass
from statistics import mean


@dataclass
class Transaction:
    amount: float
    hour: int
    is_new_account: bool
    is_fraud: bool | None


class ToyModel:
    def __init__(self) -> None:
        self.threshold: float | None = None

    def train(self, transactions: list[Transaction]) -> None:  # REMEMBER + FORMULATE
        fraud_amounts = [t.amount for t in transactions if t.is_fraud]
        # NOTE: prefer `is_fraud is False` over `not t.is_fraud` so an unlabeled
        # None row is excluded from BOTH lists rather than silently counted legit.
        legit_amounts = [t.amount for t in transactions if t.is_fraud is False]
        if fraud_amounts and legit_amounts:  # need both classes to place a boundary
            self.threshold = (mean(fraud_amounts) + mean(legit_amounts)) / 2

    def predict(self, transaction: Transaction) -> bool:  # PREDICT (inference)
        if self.threshold is None:
            return False
        return transaction.amount > self.threshold

    def accuracy(self, transactions: list[Transaction]) -> float:
        correct = sum(1 for t in transactions if self.predict(t) == t.is_fraud)
        return correct / len(transactions)


def generate_labeled_transactions(length: int, seed: int = 42) -> list[Transaction]:
    # The label must DEPEND on a feature, or there is
    #   nothing to learn. Use `random.choice([True, False])`, making
    #   fraud a coin flip unrelated to amount -> accuracy stuck near ~50%.
    #   Here fraud genuinely correlates with `amount`:
    #       legit -> low  amounts (0–600)
    #       fraud -> high amounts (300–1500)
    #   The 300–600 overlap is the "ambiguous" zone, so the model can't be perfect
    #   — which is realistic and makes the accuracy numbers meaningful.
    rng = random.Random(seed)  # seeded => reproducible runs (good ML hygiene)
    txns: list[Transaction] = []
    for _ in range(length):
        is_fraud = rng.random() < 0.40  # ~40% fraud, balanced enough to learn
        amount = rng.uniform(300, 1500) if is_fraud else rng.uniform(0, 600)
        txns.append(
            Transaction(
                amount=amount,
                hour=rng.randint(0, 23),
                is_new_account=rng.choice([True, False]),
                is_fraud=is_fraud,
            )
        )
    return txns


def train_test_split(
    data: list[Transaction], test_fraction: float = 0.3, seed: int = 7
) -> tuple[list[Transaction], list[Transaction]]:
    # SHUFFLE first (on a copy, so we don't mutate the caller's list), and
    #      return (train, test) so the larger 70% is the training set.
    shuffled = data[:]  # copy, then shuffle the copy
    random.Random(seed).shuffle(shuffled)
    test_size = int(len(data) * test_fraction)
    test_set = shuffled[:test_size]  # 30%
    train_set = shuffled[test_size:]  # 70%
    return train_set, test_set


def majority_baseline_accuracy(dataset: list[Transaction]) -> float:
    # How good is "always predict the most common label"? A real model must beat
    # this to be worth anything. (Preview of the Ch 7 class-imbalance lesson.)
    frauds = sum(1 for t in dataset if t.is_fraud)
    majority = max(frauds, len(dataset) - frauds)
    return majority / len(dataset)


if __name__ == "__main__":
    data = generate_labeled_transactions(300)
    train_data, test_data = train_test_split(data, test_fraction=0.3)

    model = ToyModel()
    model.train(train_data)  # train on TRAIN ONLY — test set stays unseen

    print(f"Learned threshold:        ${model.threshold:,.2f}")
    print(f"Train size / Test size:   {len(train_data)} / {len(test_data)}")
    print(f"Train accuracy:           {model.accuracy(train_data):.3f}")
    print(f"Test  accuracy:           {model.accuracy(test_data):.3f}")
    print(f"Majority baseline (test): {majority_baseline_accuracy(test_data):.3f}")

    # -----------------------------------------------------------------------
    # REFLECTION
    # -----------------------------------------------------------------------
    # Learned threshold:        $604.63     ← discovered from data, sits right at the overlap
    # Train accuracy:           0.933
    # Test  accuracy:           0.911       ← slightly lower than train = the seed of overfitting
    # Majority baseline (test): 0.667       ← the model genuinely beats "always guess majority"
    #
    # Q: Why might TRAIN accuracy be higher than TEST accuracy?
    # A: The threshold was fitted to the training points, so it fits them a bit too
    #    snugly. On unseen test data the model meets points it never tuned for, so it
    #    usually scores slightly lower. That gap is the seed of OVERFITTING.
    #
    # Q: Which number tells you if the model is any good?
    # A: TEST accuracy — performance on data it never saw — and only if it clearly
    #    beats the majority baseline. Train accuracy can look high for the wrong reason.
    #
    # STRETCH (your turn, no solution given):
    #   Make ToyModel use a SECOND feature with amount (e.g., predict fraud only if
    #   amount > threshold AND is_new_account). Does test accuracy improve? Why might
    #   combining features help — or sometimes hurt?
