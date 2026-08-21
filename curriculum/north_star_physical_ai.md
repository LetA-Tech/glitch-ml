# North Star — Physical AI

> **Why I'm learning:** love of ML / AI / Data Engineering and a desire for deep, durable
> foundations — *not* to pass an interview. The destination is **Physical AI**: intelligence
> that perceives and acts in the physical world (robots, autonomous systems, embodied agents).
> Every concept we study is a brick in that foundation.

---

## 🎯 The dream — a companion robot ("a minimal Doraemon")

A warm, gadget-free companion for **children and the elderly**: a friend that **talks and listens**,
**plays games (chess)**, **walks alongside** you, and is genuinely good company. Not a toy, not a
gadget dispenser — a *presence*. This is the concrete reason for everything in this curriculum.

**What that companion must be able to do → and the foundation each needs:**

| Capability | What it really requires | Built from |
|------------|-------------------------|------------|
| **Talk & listen like a friend** | speech-to-text, dialogue, text-to-speech, reading emotion/tone | neural nets, NLP/LLMs, classification (Grokking ML Ch 5–10) |
| **Play chess / games** | look ahead, plan moves, search a game tree | **search & planning — minimax, MCTS** (Grokking AI Algorithms; Grokking Algorithms) |
| **Walk alongside** | balance, locomotion, follow a person, avoid obstacles | **reinforcement learning + control**, perception, path planning (Grokking ML Ch 2 RL → Grokking AI Algorithms) |
| **Know where things/people are** | sensor fusion, "is that a fall?", track the person | **probability / Bayes**, state estimation (Grokking Bayes; Grokking ML Ch 8) |
| **Remember & personalize** | recall the person, preferences, history over time | data engineering, storage, retrieval |
| **Be SAFE & private** | never harm a child/elder; protect intimate data | evaluation/reliability (Ch 7), privacy-by-design, robust systems |

> **Why this matters for how we learn:** a companion for *vulnerable* people makes **safety,
> reliability, and privacy first-class**, not afterthoughts. Expect me to stress cost-of-errors
> (Ch 7), uncertainty (Bayes), and trustworthy data handling throughout — those aren't side topics
> for this dream, they're the core.

The path is long, but each book is a real step toward it. We build the brain (ML/AI), the senses
(perception/Bayes), the decisions (search/RL), and the nervous system (data engineering) — then,
later, the body (robotics, control, sim-to-real).

---

## What "Physical AI" means

Software AI lives in data centers and answers questions. **Physical AI** is *embodied*: it senses
the real world, decides, and acts on it — and must do so under real-time, safety, and uncertainty
constraints that a chatbot never faces. Examples: autonomous vehicles, warehouse and humanoid
robots, drones, smart manufacturing, prosthetics. The loop is **perceive → estimate state →
decide/plan → act → observe the result**, forever, in the real world.

The hard parts that make it *physical*:
- **Perception** from noisy sensors (cameras, LiDAR, IMUs) — computer vision, sensor fusion.
- **State estimation under uncertainty** — where am I, what's around me (Bayes, Kalman filters, SLAM).
- **Decision & control** — choose actions that move the world toward a goal (RL, control theory, planning).
- **Real-time + safety** — must run fast, on edge hardware, and fail safely.
- **Sim-to-real** — train in simulation, deploy on hardware (the reality gap).

---

## The foundation map — how this curriculum builds toward it

| Foundation Physical AI needs | Where we build it now |
|------------------------------|------------------------|
| Learning from data + **optimization** (gradient descent) | *Grokking ML* Ch 3–4 — the engine behind training *every* policy/perceptron/network |
| **Classification / decision boundaries** | *Grokking ML* Ch 5–9 — "obstacle vs clear", "grasp vs no-grasp" are classifiers |
| **Probability & Bayesian reasoning** | *Grokking ML* Ch 8 + *Grokking Bayes* — sensor fusion, state estimation, Kalman/SLAM |
| **Neural networks / perception** | *Grokking ML* Ch 10 + *Grokking AI Algorithms* — vision, learned policies |
| **Search, planning, optimization, RL** | *Grokking AI Algorithms* — path planning (A*), control, robot decision-making |
| **Data structures & algorithms** | *Grokking DS* + *Grokking Algorithms* — graphs, trees, real-time efficiency on constrained hardware |
| **Data Engineering** (sensor pipelines, time-series, streaming, real-time) | the capstone's DE thread — directly transfers to telemetry from robots/vehicles |

> Translation habit: whenever we learn an ML idea, I'll also name its **Physical AI bridge** —
> e.g. *threshold → obstacle detection*, *gradient descent → training a control policy*,
> *naive Bayes → sensor fusion*, *reward/feedback → reinforcement learning for control*.

---

## The road after these books (future track, when foundations are solid)

1. **Math depth** — linear algebra, calculus, probability (the language of robotics & control).
2. **Reinforcement learning** (deep) — policies, value functions, model-based control.
3. **Computer vision** — detection, segmentation, depth, pose.
4. **Robotics** — kinematics/dynamics, control theory, ROS 2.
5. **Simulation & sim-to-real** — MuJoCo / Isaac Sim / Gazebo, domain randomization.
6. **Edge / embedded inference** — running models in real time on device.

We don't rush there. **Strong foundations first** — a shaky base makes Physical AI impossible;
a solid one makes it inevitable.

---

## How the current capstone (fraud detection) still serves the North Star

Fraud detection is a **real-time, streaming, uncertainty-laden decision system** — the same
*shape* as a robot's perception→decision loop, minus the actuators. It drills the transferable
muscles: optimization, classification under imbalance, cost-sensitive decisions, evaluation,
and data pipelines. When the foundations are in place we can re-anchor the capstone toward an
embodied/robotics problem (e.g., a simulated agent, sensor-anomaly detection, predictive
maintenance) — the skills carry straight over.
