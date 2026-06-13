# Interview Translation — Chapter 1

> How to talk about this chapter to a hiring manager, a senior engineer, and in product terms.
> Practice the 40-second answer out loud until it's natural.

---

## Concept: What is machine learning?

### Simple (to a hiring manager / non-technical)
> "Instead of me writing the rules, I show the computer lots of past examples with the right
> answers, and it figures out the rules itself — then applies them to new cases it's never seen."

### Technical (to a senior engineer)
> "We fit a model `f(features) → label` from a labeled training set, then run inference on unseen
> data. Success is measured by **generalization** — performance on held-out data — not by how
> well it fits the training set."

### The 40-second spoken answer (memorize this rhythm)
> "Traditional programming is: I write the rules, the computer produces answers. Machine learning
> inverts that — I give the computer data plus the correct answers, and it writes the rules itself,
> producing a model. That model's whole job is **generalization**: making correct predictions on
> data it never saw in training. So instead of hand-coding 'if merchant is Walmart, category is
> grocery' fifty thousand times, I show it labeled examples and it learns the mapping."

---

## Real-world example to drop in
Transaction categorization (Plaid-style): learn merchant/location/counterparty → category from
millions of labeled transactions, generalizing to merchants never seen before. Same machinery,
swap the label, and you get fraud detection (binary) instead of categorization (multi-class).

---

## Common follow-up questions + strong answers

**Q: How do you know the model actually learned and didn't just memorize?**
> Evaluate on a held-out test set the model never saw during training. If training accuracy is high
> but test accuracy is low, it memorized — that's overfitting.

**Q: When would you NOT use machine learning?**
> When a small set of stable, deterministic rules already solves it. ML adds data, training, and
> monitoring cost; if `if/else` is correct and won't drift, use it.

**Q: What's the difference between a feature, a label, and a model?**
> Feature = input clue. Label = the answer to predict. Model = the learned function mapping
> features to label — the output of training.

---

## Answer structure to reuse
**Claim → Mechanism → Trade-off → Example.**
(e.g., "ML inverts programming → machine derives rules from labeled data → costs data + risk of
overfitting → here's a categorization example.")

## Mistakes to avoid
- Calling a hand-written threshold "machine learning."
- Conflating *feature* and *label*.
- Talking about training accuracy as if it proves the model is good (it's about **generalization**).
