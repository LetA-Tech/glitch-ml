# Module — Ray + Distributed Python Engineering

## Objective

Build production-grade understanding of Ray as a **distributed Python execution runtime**, not as a collection of APIs. The learner should finish able to design, implement, debug, measure, and operate Ray workloads with the judgment expected of a strong Senior Data Engineer.

The course uses two books as the primary source spine:

1. **Max Pumperla, Edward Oakes, Richard Liaw — _Learning Ray: Flexible Distributed Python for Machine Learning_**
2. **Holden Karau, Boris Lublinsky — _Scaling Python with Ray: Adventures in Cloud and Serverless Patterns_**

The books are not reproduced chapter by chapter. Repetition, narrative examples, obsolete API detail, and low-value filler are removed. The teaching material retains the engineering signal: runtime model, state, scheduling, data movement, failure semantics, production patterns, trade-offs, and exercises.

Current Ray documentation is used only in sections explicitly labeled **Current Ray update** so book-derived material and modern reconciliation remain distinguishable.

---

## Mastery outcomes

By the end of the module, you should be able to:

| Area | Senior-level capability |
|---|---|
| Distributed Python | Explain process boundaries, GIL implications, serialization, futures, async execution, state ownership, and data movement. |
| Ray Core | Design task/actor/object graphs without prematurely materializing results in the driver. |
| Scheduling | Reason about logical resources, locality, placement groups, gang scheduling, heterogeneous CPU/GPU nodes, and autoscaling. |
| Memory | Distinguish worker heap, object-store memory, reference lifetime, spilling, network transfer, and durable storage. |
| Reliability | Reason about application vs system failures, retries, actor recovery, lineage, ownership, idempotency, and side effects. |
| Ray Data | Design partitioned pipelines, control parallelism, understand blocks/shuffles/streaming execution, and decide when Spark is better. |
| Streaming | Use actors/Kafka patterns where appropriate and identify when Ray is the wrong stream-processing engine. |
| Train/Tune | Understand distributed training, trial parallelism, checkpointing, GPU allocation, scheduling, and HPO trade-offs. |
| Serve | Design scalable model-serving graphs with replicas, batching, resource isolation, autoscaling, and deployment composition. |
| Production | Deploy with Ray Jobs/KubeRay, observe runtime state, debug failures, size clusters, and reason about security/cost. |
| Architecture | Choose tasks vs actors vs Data vs external systems; explain failure domains and bottlenecks before implementation. |

---

## Course sequence

```mermaid
flowchart LR
    A[Day 1 mental model] --> B[Tasks + ObjectRefs]
    B --> C[Actors + state]
    C --> D[Scheduling + resources]
    D --> E[Object store + memory]
    E --> F[Fault tolerance]
    F --> G[Ray Data]
    G --> H[Streaming / event patterns]
    H --> I[Train + Tune + accelerators]
    I --> J[Serve]
    J --> K[Clusters + KubeRay + Jobs]
    K --> L[Observability + debugging]
    L --> M[Production design + capstone]
```

---

## Reading material

The authoritative teaching notebooks live in `class-sessions/ray/`.

| Order | Notebook | Core purpose |
|---:|---|---|
| 0 | `../class-sessions/day-01-ray-python-foundations.md` | First mental model and 15-minute entry reading. |
| 1 | `01-core-execution-and-python-runtime.md` | Python process model, Ray tasks, ObjectRefs, dependencies, dynamic execution. |
| 2 | `02-actors-state-and-concurrency.md` | Actor model, state ownership, persistence, actor scaling and concurrency. |
| 3 | `03-scheduling-resources-placement-autoscaling.md` | Logical resources, Raylets, placement groups, locality, heterogeneous clusters. |
| 4 | `04-object-store-memory-serialization-data-movement.md` | Object lifecycle, serialization, shared memory, spill, network movement, memory failure. |
| 5 | `05-fault-tolerance-recovery-and-idempotency.md` | Task/actor/object failure semantics and side-effect safety. |
| 6 | `06-ray-data-for-data-engineering.md` | Blocks, partitions, transforms, shuffles, pipeline design, Spark comparison. |
| 7 | `07-streaming-and-event-driven-patterns.md` | Kafka, ordering, actor state, backpressure, stream-processing limits. |
| 8 | `08-train-tune-and-accelerators.md` | Distributed training, HPO, checkpoints, GPUs, nested resource scheduling. |
| 9 | `09-ray-serve-and-online-inference.md` | Deployment/replica architecture, batching, autoscaling, serving graphs. |
| 10 | `10-clusters-kuberay-jobs-observability-debugging.md` | Production deployment and operational diagnosis. |
| 11 | `11-production-patterns-antipatterns-and-system-design.md` | Cross-cutting architecture patterns and senior design judgment. |
| 12 | `12-book-era-material-and-current-replacements.md` | AIR, DatasetPipeline, Workflows, old Tune/Train/Serve/cluster assumptions. |
| 13 | `13-exercise-bank-and-capstone.md` | Medium → hard exercises, chaos drills, design reviews, capstones. |
| Optional | `appendix-rllib-as-a-distributed-systems-case-study.md` | RLlib only as a useful example of nested distributed actors/tasks. |

---

## Source-to-course map

### _Learning Ray_

| Book chapter | Course treatment |
|---|---|
| Ch. 1 — Overview | Mental model, where Ray fits, Core/libraries/ecosystem. |
| Ch. 2 — Ray Core | Core execution, tasks, actors, objects, scheduler/runtime architecture. |
| Ch. 3 — First distributed application | Extract distributed decomposition lessons; RL-specific narrative minimized. |
| Ch. 4 — RLlib | Optional appendix; retained mainly for distributed-architecture lessons. |
| Ch. 5 — Tune | Train/Tune module; old API details reconciled separately. |
| Ch. 6 — Data | Ray Data module; DatasetPipeline marked historical. |
| Ch. 7 — Train | Train/Tune module; focus on data parallelism, worker coordination, checkpointing. |
| Ch. 8 — Serve | Serve module. |
| Ch. 9 — Clusters | Production/KubeRay module. |
| Ch. 10 — AIR | Architectural integration lessons retained; AIR branding/API treated as historical. |
| Ch. 11 — Ecosystem | Tool-selection and integration patterns retained, catalog-style filler removed. |

### _Scaling Python with Ray_

| Book chapter | Course treatment |
|---|---|
| Ch. 1–2 | Mental model and local runtime setup. |
| Ch. 3 — Remote Functions | Core execution, composition, waits/timeouts, task granularity. |
| Ch. 4 — Remote Actors | Actor/state/concurrency module. |
| Ch. 5 — Design Details | Scheduling, memory, serialization, resources, placement, fault tolerance. |
| Ch. 6 — Streaming | Streaming/event-driven module; Ray-specific historical streaming assumptions separated from durable concepts. |
| Ch. 7 — Microservices | Serve module; old Serve API surface separated from durable serving design. |
| Ch. 8 — Workflows | Historical/legacy appendix only; not recommended as a modern design target. |
| Ch. 9 — Advanced Data | Ray Data module; Spark/Dask comparison retained. |
| Ch. 10 — ML | Train/Tune integration patterns. |
| Ch. 11 — Accelerators | GPU/resource scheduling and CPU fallback patterns. |
| Ch. 12 — Enterprise | Security, cluster lifecycle, monitoring, production integration. |
| Appendix A | Actor/system design case-study lessons. |
| Appendix B | Deployment concepts only; version-specific commands are not memorization targets. |
| Appendix C | Debugging and profiling patterns. |

---

## What is deliberately removed

The teaching notes intentionally do **not** preserve:

- repeated introductory explanations;
- tutorial narration whose only purpose is typing along;
- long installation walkthroughs tied to old versions;
- catalog-like lists of integrations without architectural value;
- repeated API syntax once the concept is established;
- obsolete recommendations presented as current practice;
- examples whose ML/RL details distract from the distributed-systems lesson;
- benchmark numbers that are environment-specific and not reusable engineering knowledge.

The rule is: **preserve the mechanism, trade-off, invariant, failure mode, or reusable pattern; remove everything else.**

---

## Practice model

Every exercise uses the same loop:

```text
predict → implement → measure → break → observe → diagnose → fix → explain
```

An exercise is not complete because the program returned the expected value. It is complete only when you can explain:

1. which process executed each stage;
2. where the important data lived;
3. what serialization/network boundaries were crossed;
4. why the scheduler chose the observed placement;
5. what failure mode was possible;
6. whether retries were safe;
7. where backpressure existed;
8. what metric/log/state evidence supports the explanation.

---

## Mastery gate

You complete this module only when you can independently design a production Ray workload from a blank page and defend the following:

- why Ray is the correct tool rather than Spark/Flink/Kubernetes tasks/Celery/plain multiprocessing;
- the task/actor/object decomposition;
- state and durability boundaries;
- resource model and placement strategy;
- memory and data-movement model;
- retry/idempotency strategy;
- backpressure strategy;
- deployment topology;
- observability plan;
- failure-injection plan;
- cost and scaling behavior.

Recognition of API syntax does not count as mastery.
