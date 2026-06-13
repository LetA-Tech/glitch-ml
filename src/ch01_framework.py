"""
Chapter 1 - Remember / Formulate / Predict, made concrete.

Goal of this script: FEEL the differences between traditional programming and machine learning, using nothing but plain Python (no ML libraries yet)

"""

from dataclasses import dataclass
from fnmatch import filterfalse
from statistics import mean


# -----
# Our data point - a stripped-down version of the captone data contract
# ----
@dataclass
class Transaction:
    amount: float  # feature (input clue)
    hour: int  # feature (input clue)
    is_new_account: bool  # feature (input clue)
    is_fraud: bool | None  # label (known for training, None for prediction)


# ----
# Traditional programming
# A human write rule. the computer just executes it.
# This is not ML, even though it "detects" fraud
# ----
def rule_based_is_fraud(transaction: Transaction) -> bool:
    return (
        transaction.amount > 1000000
        or transaction.hour > 23
        or transaction.is_new_account
    )


# ----
# Machine learning
# We do not tell the machine the rule. We give it labeled data and let it learn.
# The model: REMEMBER (labeled examples) , it FORMULATES a rule by looking at the data (the output is the model), and we can use it to predict labels for new data.
# This is a toy version of ML - no libraries, just plain Python.
# The most important part is "How you split & evaluate data"; as it's master more than the algorithm.
# In the follow-up exercise, we'll use a library to split and evaluate data (training data set and test/evaluation data set).
# Our "learning" here is deliberately tiny: the model learns a single threshold on "amount"
# by averaging the amounts of fraud vs non-fraud transactions, and putting the boundary between them.
# This is a stand-in for what gradient descent will do properly.
# ----
class ToyModel:
    def __init__(self) -> None:
        self.threshold: float | None = None  # this is what training learns

    def train(
        self, data: list[Transaction]
    ) -> None:  # train & model --> remember + formulate
        fraud_amounts = [t.amount for t in data if t.is_fraud]
        non_fraud_amounts = [t.amount for t in data if not t.is_fraud]

        # Boundary halfway between the two groups' averages - learned, not given
        if fraud_amounts and non_fraud_amounts:
            self.threshold = (mean(fraud_amounts) + mean(non_fraud_amounts)) / 2

    def predict(
        self, transaction: Transaction
    ) -> bool:  # predict --> use the model to make a prediction (inference)
        if self.threshold is None:
            return False
        return transaction.amount > self.threshold


# ----
# You can use this model to predict whether a transaction is fraud or not, by calling `predict(transaction)`.
# ----
if __name__ == "__main__":
    # Labeled history the machine gets to REMEMBER.
    training_data = [
        Transaction(100.00, 13, False, is_fraud=False),
        Transaction(500.00, 14, True, is_fraud=False),
        Transaction(200.00, 3, False, is_fraud=True),
        Transaction(1000.00, 2, True, is_fraud=True),
        Transaction(50.00, 17, False, is_fraud=False),
    ]

    model = ToyModel()
    model.train(training_data)
    print(f"Learned threshold (discovered from data): ${model.threshold:,.2f}")

    # A new, UNSEEN transaction to predict - label unknown. This is "GENERALIZATION"
    new_tx = Transaction(5000.00, 18, True, is_fraud=None)
    print(f"Rule-based (human-written) says fraud? {rule_based_is_fraud(new_tx)}")
    print(f"Learned model says fraud?             {model.predict(new_tx)}")

    #  Added READING THE RESULT block explaining each line:
    # - $4,866.67 — midpoint math from fraud mean (9,666.67) + legit mean (66.67), discovered not typed
    # - Rule-based False — 7500 > 1,000,000 misses real fraud (arbitrary human cutoff)
    # - Learned True — 7500 > 4,866.67 catches it (data-driven boundary)
    # - Takeaway — same tx, opposite verdicts; model wins because its number came from examples
