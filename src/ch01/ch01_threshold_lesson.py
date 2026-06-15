"""
Chapter 1 — DEEP DIVE: What IS a "threshold"?  (reverse-engineered from code)

Read this top-to-bottom, then run it:  uv run python src/ch01_threshold_lesson.py
The script PRINTS a picture so you can SEE the threshold doing its job.

==============================================================================
PART A — THE ANALOGY (understand it before the math)
==============================================================================

A threshold is a DIVIDING LINE on a single number, with a rule:
   "below the line -> guess NO,  above the line -> guess YES."

Analogy 1 — the amusement-park height bar 🎢
   A sign says "You must be THIS TALL to ride." That bar is a threshold on the
   feature `height`. Shorter than the bar -> can't ride. Taller -> can ride.
   The park didn't pull the height out of thin air — they set it from SAFETY
   DATA (who is safe vs unsafe on this ride). Same idea as our model learning
   the bar from labeled transaction data.

Analogy 2 — fever 🌡️
   Doctors call 38.0°C the threshold for "fever". 37.9 -> no fever. 38.1 -> fever.
   One number, one cutoff, a yes/no decision. That's a classifier with a threshold.

Analogy 3 — exam pass mark 📝
   Score >= 50 -> pass, otherwise fail. The "50" is the threshold.

In OUR fraud model the feature is `amount`, and the threshold is a dollar line:
   amount above the line -> guess FRAUD,  below the line -> guess LEGIT.

==============================================================================
PART B — REVERSE-ENGINEER THE CODE
==============================================================================

In ToyModel.train() the whole "learning" is ONE line:

    self.threshold = (mean(fraud_amounts) + mean(legit_amounts)) / 2

Read it in plain English:
   1. mean(legit_amounts) = the AVERAGE dollar amount of normal transactions.
        (Imagine the "center of gravity" of the legit cloud — say ~$300.)
   2. mean(fraud_amounts) = the AVERAGE dollar amount of fraud.
        (The center of gravity of the fraud cloud — say ~$900.)
   3. (a + b) / 2 = the MIDPOINT between those two centers — the fence placed
        exactly halfway between the two crowds — say ~$600.

That midpoint is the threshold. It's the model's best simple guess at "where
does legit end and fraud begin?" — and crucially, the number came FROM THE DATA,
not from a human typing 600.

Then ToyModel.predict() just asks which side of the fence you're on:

    return transaction.amount > self.threshold      # right of fence -> fraud

==============================================================================
PART C — WHY IT'S NOT PERFECT (the overlap)
==============================================================================

If every legit amount were below every fraud amount, one fence would be perfect.
But the real world OVERLAPS: some legit purchases are large ($550 TV) and some
fraud is small ($350 test charge). Those land on the "wrong" side of the fence
and get misclassified. That overlap is exactly why train/test accuracy was ~0.92
and not 1.00. No single straight fence can separate clouds that overlap — a key
motivation for the smarter models in later chapters.

Run the script and watch the histogram: you'll SEE the two crowds, the fence,
and the overlap where the mistakes happen.
"""

import random
from dataclasses import dataclass
from statistics import mean


@dataclass
class Transaction:
    amount: float
    is_fraud: bool


def make_data(n: int = 300, seed: int = 42) -> list[Transaction]:
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        fraud = rng.random() < 0.40
        amount = rng.uniform(300, 1500) if fraud else rng.uniform(0, 600)
        out.append(Transaction(amount, fraud))
    return out


def learn_threshold(data: list[Transaction]) -> float:
    fraud = [t.amount for t in data if t.is_fraud]
    legit = [t.amount for t in data if not t.is_fraud]
    return (mean(fraud) + mean(legit)) / 2  # midpoint of the two crowd-centers


def draw_picture(
    data: list[Transaction],
    threshold: float,
    lo: float = 0,
    hi: float = 1500,
    bins: int = 10,
) -> None:
    """ASCII histogram: each row is a $ range; o = legit, x = fraud."""
    width = (hi - lo) / bins
    print(f"\n  $ range          legit (o)            fraud (x)")
    print(f"  {'-' * 60}")
    for i in range(bins):
        b_lo, b_hi = lo + i * width, lo + (i + 1) * width
        legit = sum(1 for t in data if b_lo <= t.amount < b_hi and not t.is_fraud)
        fraud = sum(1 for t in data if b_lo <= t.amount < b_hi and t.is_fraud)
        fence = "  <===== THRESHOLD (the fence)" if b_lo <= threshold < b_hi else ""
        print(
            f"  ${b_lo:>4.0f}-{b_hi:<4.0f}  {'o' * legit:<20} {'x' * fraud:<14}{fence}"
        )
    print(f"  {'-' * 60}")


# =======================================================================
# YOUR EXERCISE (do this before Chapter 2) — no solution provided.
# =======================================================================
# Goal: discover that the midpoint is just ONE choice of fence, and that
# "training" is really a SEARCH for the BEST fence. This is the seed of
# gradient descent (Chapter 3).
#
#   1. Write accuracy_at(threshold, data) -> fraction correct, where a
#      prediction is `amount > threshold`.
#   2. Sweep candidate thresholds from $0 to $1500 in steps of $50. For each,
#      print:  threshold  ->  accuracy.
#   3. Find and print the BEST threshold (highest accuracy).
#   4. Compare it to the learned midpoint (${t:,.2f} above). Same? Close? Why?
#   5. In a comment, answer: you just searched by hand for the best fence.
#      What part of "machine learning" should do that search automatically?
#      (You're describing training / optimization — Chapter 3.)
#
# Put your work in a NEW function below and call it from here.


def accuracy_at(threshold, data) -> float:
    correct = sum(1 for x in data if (x.amount > threshold) == x.is_fraud)
    return correct / len(data)


if __name__ == "__main__":
    data = make_data()
    t = learn_threshold(data)

    legit_avg = mean([x.amount for x in data if not x.is_fraud])
    fraud_avg = mean([x.amount for x in data if x.is_fraud])
    print(f"Average legit amount: ${legit_avg:,.2f}   (center of the legit crowd)")
    print(f"Average fraud amount: ${fraud_avg:,.2f}   (center of the fraud crowd)")
    print(f"Threshold = midpoint: ${t:,.2f}   <- the fence, halfway between them")

    draw_picture(data, t)

    # Count the mistakes the fence makes, and WHERE they come from:
    false_alarms = sum(
        1 for x in data if not x.is_fraud and x.amount > t
    )  # legit flagged
    missed_fraud = sum(1 for x in data if x.is_fraud and x.amount <= t)  # fraud missed
    print(f"\n  Legit wrongly flagged (above fence): {false_alarms}")
    print(f"  Fraud wrongly missed  (below fence): {missed_fraud}")
    print(
        "  ^ both happen ONLY in the overlap zone. That's why no single fence is perfect."
    )

    # --- Step 2: sweep every candidate fence and print its accuracy ---------
    print("\n  threshold -> accuracy (the search, done by hand):")
    for threshold in range(0, 1501, 50):
        print(f"  {threshold:4d}  ->  {accuracy_at(threshold, data):.3f}")

    # --- Step 3: find the best fence AUTOMATICALLY (don't eyeball it) --------
    # max(iterable, key=fn) walks every candidate and keeps the one for which
    # fn(candidate) is largest. `key` tells max() WHAT to compare by — here, the
    # accuracy each threshold would achieve. This one line IS a brute-force search.
    best = max(range(0, 1501, 50), key=lambda th: accuracy_at(th, data))
    print(f"\n  Best threshold (searched):  ${best:>4}   (accuracy {accuracy_at(best, data):.3f})")
    print(f"  Midpoint shortcut (learn):  ${t:>7,.2f} (accuracy {accuracy_at(t, data):.3f})")

    # --- Step 4 & 5: what we learned ----------------------------------------
    # The search found $600 — essentially the SAME as the midpoint shortcut ($607).
    # That match is LUCK of a balanced, symmetric dataset. With the ~1% fraud rate
    # of the real world, the midpoint shortcut would drift far from the optimum.
    #
    # REFLECTION (Q5) — what should do this search automatically?
    #   The TRAINING / OPTIMIZATION step (e.g. gradient descent, Chapter 3).
    #   My hand sweep IS that search, done by brute force. `learn_threshold()` only
    #   takes a one-step shortcut — it does NOT truly search. Real training searches
    #   the parameter space efficiently and lands on the best value on purpose, even
    #   for millions of parameters where sweeping every option is impossible.
    #
    #   Guessing in one step  (learn_threshold)   vs
    #   Searching toward best  (training / gradient descent, Ch 3)
    #   ^ that distinction is the door into Chapter 3.
