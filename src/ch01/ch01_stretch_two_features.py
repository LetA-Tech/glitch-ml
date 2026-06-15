"""
Chapter 1 — STRETCH: does a SECOND feature help?  (your turn — no solution given)

So far the model decides with ONE feature: amount > threshold -> fraud.
Now you'll add a second feature, `is_new_account`, and find out when a second
feature HELPS, when it HURTS, and why.

==============================================================================
THE BIG IDEA YOU'RE TESTING
==============================================================================
A feature only helps a model if it carries SIGNAL about the label (i.e. it
correlates with fraud). A feature that's just noise can't help — and depending
on how you combine it, it can actively HURT. That's why this file gives you TWO
datasets:
  * make_data_noise_account  -> is_new_account is a coin flip (NO signal)
  * make_data_signal_account -> new accounts really ARE riskier (HAS signal)

You will run your two-feature model on BOTH and compare.

==============================================================================
YOUR TASKS
==============================================================================
  1. Implement TwoFeatureModel.predict(). Start with this rule:
         predict fraud  IF  amount > threshold  AND  is_new_account
     (train() is already done for you — it learns the amount threshold the same
      way as before.)
  2. Implement accuracy(model, data).
  3. Print, for BOTH datasets:
         - amount-only accuracy   (your single-feature model from before)
         - two-feature accuracy    (the new AND-rule model)
  4. Then EXPERIMENT: change the combine rule from AND to OR and re-run.
  5. Answer the questions at the bottom in comments.

HINT (think before you run): in make_data_noise_account, does is_new_account
tell you anything about fraud? If not, what do you PREDICT the AND-rule will do
to accuracy versus amount-only? Write your prediction down, THEN run it.
"""

# ============================================================================
# CONCEPT NOTES — keep these; they are the "why" behind this exercise.
# ============================================================================
#
# WHAT "SIGNAL" MEANS
#   A feature carries SIGNAL about the label when KNOWING it CHANGES your guess
#   about the label. If your fraud estimate is the same whether is_new_account is
#   True or False, the feature is noise. If knowing it shifts the estimate, it's
#   signal. Signal is the only thing a model can learn from.
#
# WHAT "RISKIER" MEANS  (this tripped me up — pin it down)
#   "New accounts are riskier" does NOT mean every new account is fraud. It means
#   the fraud RATE is higher in that group. Measured on our signal dataset:
#         new accounts:        ~53% fraud
#         established accounts: ~30% fraud
#   So learning is_new_account = True shifts the fraud estimate UP (~40% baseline
#   -> ~53%). That shift is the signal.
#
#   Analogy — car insurance: a 19-year-old driver is "riskier" than a 40-year-old,
#   not because every teenager crashes, but because the crash RATE is higher in
#   that group. The insurer uses age as a feature because knowing it moves the
#   probability. Same idea with is_new_account.
#
# CORRECTION TO A NATURAL MISTAKE
#   It is tempting to say "signal = big amount AND new account." Not quite.
#   is_new_account is informative ON ITS OWN, independent of amount. In fact the
#   signal generator deliberately puts SOME fraud at LOW amounts on new accounts:
#         new accounts:        fraud amount ~ uniform(50, 1500)   <- can be small!
#         established accounts: fraud amount ~ uniform(500, 1500)
#
# WHY A SECOND FEATURE CAN ADD VALUE  (the key insight)
#   The amount-only fence (~$600) MISSES low-amount fraud — those sneaky $200
#   charges sit below the fence. But many of them are on NEW accounts. So
#   is_new_account gives the model a SECOND WAY to catch fraud that amount alone
#   cannot see. A second feature helps when it catches the mistakes the first one
#   makes — two weak clues covering each other's blind spots.
#
# AND vs OR — how you COMBINE matters as much as which features you use
#   AND  (amount > thr AND is_new):  STRICTER. Predicts fraud less often.
#        -> fewer false alarms, but MORE missed fraud (must satisfy BOTH clues).
#   OR   (amount > thr OR is_new):   LOOSER. Predicts fraud more often.
#        -> catches more fraud (incl. low-amount new-account fraud), but MORE
#           false alarms (flags legit new-account purchases too).
#   There is no free lunch — you trade false alarms against missed fraud. Which
#   way you want to lean is a BUSINESS decision (Chapter 7: precision vs recall).
#
# WALK ONE TRANSACTION THROUGH BOTH RULES
#   tx = $200 fraud on a NEW account (amount below the ~$600 fence):
#     amount-only:  200 > 600? No  -> predicts LEGIT   -> MISS (wrong)
#     AND rule:     (200>600) AND new = False AND True = False -> LEGIT -> MISS
#     OR  rule:     (200>600) OR  new = False OR  True = True  -> FRAUD -> CATCH!
#   -> OR rescues this fraud; AND does not. That's the trade-off in one example.
#
# THE PRINCIPLE (your Q4 answer, in advance):
#   A second feature helps only when (a) it carries signal about the label, AND
#   (b) it is combined in a way that covers the first feature's blind spots.
# ============================================================================

import random
from dataclasses import dataclass
from statistics import mean


@dataclass
class Transaction:
    amount: float
    is_new_account: bool
    is_fraud: bool


# ---------------------------------------------------------------------------
# DATA GENERATORS (provided) — same amount pattern, different account signal.
# ---------------------------------------------------------------------------
def make_data_noise_account(n: int = 400, seed: int = 42) -> list[Transaction]:
    """is_new_account is a coin flip — pure noise, unrelated to fraud."""
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        is_fraud = rng.random() < 0.40
        amount = rng.uniform(300, 1500) if is_fraud else rng.uniform(0, 600)
        out.append(Transaction(amount, rng.choice([True, False]), is_fraud))
    return out


def make_data_signal_account(n: int = 400, seed: int = 42) -> list[Transaction]:
    """New accounts are genuinely riskier -> is_new_account carries SIGNAL.
    Crucially, some fraud on new accounts is LOW-amount (amount alone misses it)."""
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        is_new = rng.choice([True, False])
        # New accounts: more fraud, and fraud can be small (sneaky).
        # Established accounts: less fraud, and fraud tends to be large.
        if is_new:
            is_fraud = rng.random() < 0.55
            amount = rng.uniform(50, 1500) if is_fraud else rng.uniform(0, 600)
        else:
            is_fraud = rng.random() < 0.25
            amount = rng.uniform(500, 1500) if is_fraud else rng.uniform(0, 600)
        out.append(Transaction(amount, is_new, is_fraud))
    return out


def learn_threshold(data: list[Transaction]) -> float:
    fraud = [t.amount for t in data if t.is_fraud]
    legit = [t.amount for t in data if not t.is_fraud]
    return (mean(fraud) + mean(legit)) / 2


# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------
class AmountOnlyModel:
    def __init__(self) -> None:
        self.threshold = 0.0

    def train(self, data: list[Transaction]) -> None:
        self.threshold = learn_threshold(data)

    def predict(self, t: Transaction) -> bool:
        return t.amount > self.threshold


class TwoFeatureModel:
    def __init__(self) -> None:
        self.threshold = 0.0

    def train(self, data: list[Transaction]) -> None:
        self.threshold = learn_threshold(data)  # learns the amount fence as before

    def predict(self, t: Transaction) -> bool:
        # TODO (Task 1): combine the amount fence with is_new_account.
        #   Start with the AND rule:  amount > threshold AND is_new_account
        #   Later (Task 4) try the OR rule and compare.
        return t.amount > self.threshold and t.is_new_account

    def predict_with_or(self, t: Transaction) -> bool:
        return t.amount > self.threshold or t.is_new_account


def accuracy(model, data: list[Transaction]) -> float:
    # TODO (Task 2): fraction of predictions that match the true label.
    correct = 0
    for t in data:
        if model.predict(t) == t.is_fraud:
            correct += 1
    return correct / len(data)


def accuracy_with_or(model, data: list[Transaction]) -> float:
    correct = 0
    for t in data:
        if model.predict_with_or(t) == t.is_fraud:
            correct += 1
    return correct / len(data)


if __name__ == "__main__":
    # TODO (Task 3): for each dataset, train both models and print:
    #   dataset name | amount-only accuracy | two-feature accuracy
    #
    # Skeleton to fill in:
    # for name, data in [("noise", make_data_noise_account()),
    #                    ("signal", make_data_signal_account())]:
    #     ...
    amount_only_model = AmountOnlyModel()
    two_feature_model = TwoFeatureModel()
    for name, data in [
        ("noise", make_data_noise_account()),
        ("signal", make_data_signal_account()),
    ]:
        amount_only_model.train(data)
        two_feature_model.train(data)
        print(
            f"{name} | {accuracy(amount_only_model, data):.3f} | {accuracy(two_feature_model, data):.3f}"
        )

    # Experiment with OR-rule
    two_feature_model_with_or = TwoFeatureModel()
    for name, data in [
        ("noise", make_data_noise_account()),
        ("signal", make_data_signal_account()),
    ]:
        two_feature_model_with_or.train(data)
        print(f"{name} | {accuracy_with_or(two_feature_model_with_or, data):.3f}")

# ==========================================================================
# QUESTIONS TO ANSWER (in comments, after you see the numbers)
# ==========================================================================
# Q1. On the NOISE dataset, did the AND-rule two-feature model beat amount-only?
#     Why / why not?
# A: The AND-rule two-feature model did not beat amount-only on the NOISE dataset. (no signal only lost catches)
# Q2. On the SIGNAL dataset, did adding is_new_account help? Which frauds did it
#     newly catch (think about the LOW-amount fraud on new accounts)?
# A: Adding is_new_account didn't help on the SIGNAL dataset. The OR-rule two-feature model caught the LOW-amount fraud on new accounts.
# Q3. AND vs OR: how did switching the combine rule change false alarms vs missed
#     fraud? Which rule is "stricter" (predicts fraud less often)?
# A: AND stricter (predicts fraud less often) OR Looser (predicts fraud more often)
# Q4. State the principle in one sentence: a second feature helps only when ____.
# A: a second feature help only when it carries signal about the label and is combined in a way that fixes the first feature's mistakes without creating worse ones.
