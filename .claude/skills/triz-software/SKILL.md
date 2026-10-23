---
name: triz-software
description: Solve hard software, systems and agentic-AI design problems with TRIZ (Theory of Inventive Problem Solving) — by finding the contradiction behind the problem and resolving it instead of settling for a compromise. Use this skill whenever the user mentions TRIZ, ТРИЗ, ARIZ, АРИЗ, inventive principles, ideal final result / ИКР, or physical/technical contradictions, and also whenever they are stuck on a design trade-off ("we can have X or Y but not both", "every option makes something else worse", "we keep going back and forth between two designs"), ask for a non-obvious or breakthrough solution to an architecture, performance, database, distributed-system or LLM-agent design problem, or want to brainstorm systematically rather than freely. Do NOT use for ordinary bugs with a clear root cause — use normal debugging there.
---

# TRIZ for software problems

TRIZ treats a hard problem as a contradiction: improving one property of a system degrades another. Typical engineering answers pick a point on the trade-off curve. TRIZ instead looks for a way to remove the contradiction, so that both properties are satisfied. This skill adapts the classical TRIZ toolset (contradictions, Ideal Final Result, resources, separation principles, 40 inventive principles, trimming, evolution trends) to software.

The main value of TRIZ is discipline in **formulating** the problem. Most of the work is done before any solution is proposed. Resist the urge to jump to solutions — that is the single most common way to get a compromise instead of an invention.

## When TRIZ fits and when it does not

TRIZ pays off when the problem has a real conflict between requirements and the obvious solutions are all compromises. It is a poor fit for problems that are just unknown (a bug to localise, a missing fact to look up) or just laborious (lots of code to write). If the problem has no contradiction, say so and solve it directly — do not force the method.

## Workflow

Work through these steps in order. In a conversation, keep each step short; the user should see the reasoning, not a ritual. If information is missing (load profile, constraints, what has already been tried), ask once, compactly, then proceed with stated assumptions.

### 1. Describe the system and the problem

- **System**: what it is, its main useful function (one verb + object: "the planner chooses a join order", "the agent edits code").
- **Undesired effect**: what goes wrong, in observable terms.
- **Constraints**: what must not change (API, on-disk format, latency budget, compatibility).
- **System operator (9 screens)**: look at the subsystem, system and supersystem, each in the past, present and future. Often the best place to solve the problem is not where the symptom appears — e.g. the fix for a slow query may live in the statistics collector (subsystem, past) or in the application (supersystem).

### 2. Formulate the technical contradictions

Write two mirror statements:

- **TC1**: If <element> is <state A>, then <good property 1> improves, but <property 2> degrades.
- **TC2**: If <element> is <state anti-A>, then <property 2> improves, but <property 1> degrades.

Pick the one that serves the main useful function better and keep working with it. Make the conflict concrete with numbers or scenarios where possible.

Example (agentic coding): "If the agent's context contains the whole repository, it reasons with full knowledge, but cost rises and attention to the relevant part degrades. If the context is small, it is cheap and focused, but the agent misses dependencies."

### 3. Intensify the contradiction

Push the conflicting parameter to the extreme (infinite context, zero context, a million concurrent writers, a single writer). Extremes often make the shape of the solution visible and stop you from searching for a "reasonable middle".

### 4. Formulate the physical contradiction

Find the single element (the *operative zone* and *operative time*) whose property must be opposite:

- **PC**: <element> must be <P> in order to <satisfy property 1>, and must be <anti-P> in order to <satisfy property 2>.

Example: "The context must be large (to hold all relevant knowledge) and must be small (to stay cheap and focused)."

### 5. State the Ideal Final Result (IFR)

- **IFR**: <element> **itself**, without complicating the system and without harmful effects, provides <P> where/when it is needed and <anti-P> elsewhere.

The word "itself" is the point: prefer solutions where the existing system does the work, not a new component. The IFR is a direction, not a requirement — measure concepts by how close they get.

### 6. Inventory the resources

List what already exists and can be used for free. Software systems are rich in unused resources:

- **Data and metadata**: statistics, logs, histories (git, WAL, query logs), schemas, types, comments, previous results.
- **Time**: before the operation (precompute, plan), after it (deferred work, background), idle periods, the gap between request and response.
- **Space / structure**: unused bits or fields, slack in pages and buffers, hooks and extension points, neighbouring components.
- **Existing verifiers and oracles**: compilers, type checkers, test suites, linters, constraints, checksums, EXPLAIN output — feedback that costs nothing to produce.
- **Errors and harmful effects**: failures, misestimates, timeouts and retries are signals that can be turned into input.
- **Supersystem**: the user, the caller, the OS, the hardware, the network, other services.

### 7. Resolve the physical contradiction

Try each separation principle in turn; write down at least one concrete idea for each that makes sense.

- **Separation in time**: P at one moment, anti-P at another. → lazy loading, precomputation, phases, caching, compaction, re-planning during execution, progressive disclosure.
- **Separation in space**: P in one place, anti-P in another. → sharding, partitioning, isolation, subprocesses, sub-agents with their own context, read/write split.
- **Separation upon condition**: P under one condition, anti-P under another. → feature flags, thresholds, adaptive switching, guards, risk-based approval (full autonomy for reversible actions, confirmation for irreversible ones).
- **Separation between the whole and its parts**: the system as a whole has P while its parts have anti-P (or vice versa). → strong consistency per shard with eventual consistency globally; each request small but batches large; each agent narrow, the team broad.

If separation does not yield a good concept, move to the inventive principles.

### 8. Apply the inventive principles

Read `references/principles.md`. It maps the 40 classical principles to software analogies. Do not list all 40 — pick the 3–6 that plausibly fit this contradiction and generate a concrete idea from each. Principles that tend to be strong in software: 1 Segmentation, 10 Preliminary action, 13 The other way round, 15 Dynamics, 22 Blessing in disguise, 23 Feedback, 24 Intermediary, 25 Self-service, 35 Parameter changes, 40 Composite.

### 9. Try trimming

For each component involved, ask: can it be removed, with its useful function taken over by something that already exists? Trimming often beats adding. Before adding a new agent, service, cache or index, check whether an existing element (a test, a constraint, an output schema, the caller) can do its job.

### 10. Check against evolution trends

Briefly check where the system sits on its S-curve and which trend the concept follows. Useful trends for software:

- mono → bi → poly (one component → pair → many cooperating), and then back to a merged, simpler system;
- increasing dynamism and controllability (static → configurable → adaptive → self-tuning);
- transition to the supersystem (a function moves up a level: from application to database, from database to storage);
- displacement of the human (human does it → human approves → human monitors → no human);
- increasing ideality (more function with fewer parts).

A concept that goes against a strong trend is not wrong, but deserves a second look.

### 11. Generate, evaluate and choose

Produce at least three concepts that differ in mechanism, not just in parameters. For each, record the principle or separation it uses, how close it gets to the IFR, the resources it uses, and the **secondary problems** it creates (every solution creates new contradictions — name them). Then recommend one, and state what observation would prove the recommendation wrong.

## Output format

Use this structure for a full analysis. For a quick conversational answer, compress it to the contradiction, the IFR, and two or three concepts.

```
## Problem
System, main useful function, undesired effect, constraints.

## Contradictions
TC: ...
PC: ...

## Ideal Final Result
...

## Resources
Short list of the resources that actually matter here.

## Concepts
### 1. <name> — <principle / separation used>
Mechanism, distance to IFR, secondary problems.
### 2. ...
### 3. ...

## Recommendation
Which concept and why; what would falsify it; the next concrete step (prototype, benchmark, experiment).
```

## Pitfalls

- **Jumping to solutions.** If you notice you are proposing designs before writing the PC and IFR, stop and go back.
- **Accepting a compromise as the answer.** "Use a medium-sized context" is a point on the trade-off curve, not a resolution. Compromises are acceptable only after separation and principles have been tried, and should be labelled as such.
- **Mechanical principle listing.** Naming principles without a concrete mechanism is decoration. Each principle used must produce a specific design idea for this system.
- **Post-hoc labelling.** Mapping an existing solution onto a principle is useful for learning but is not problem solving. Be honest about which concepts were generated and which were recognised.
- **Wrong operative zone.** If the PC is vague ("the system must be fast and correct"), the element is too large. Narrow it until a single element has a single conflicting property.
- **Overclaiming.** Evidence that TRIZ speeds up software innovation is mostly anecdotal. Present it as a structured way to think, not as a guarantee.

## Worked example (condensed)

**Problem.** A query planner must choose a plan before execution, but cardinality estimates for complex predicates are often wrong, leading to catastrophic plans.

**TC.** If the planner relies on precomputed statistics, planning is cheap but estimates are wrong for correlated predicates. If it samples data at planning time, estimates improve but planning becomes expensive for every query.

**PC.** The cardinality must be known before execution (to choose the plan) and can only be known after execution (when rows are counted).

**IFR.** The plan itself, without extra planning cost, corrects its course once the real cardinality becomes visible.

**Concepts.**
1. *Separation in time* — adaptive execution: re-plan or switch operators at a checkpoint when actual rows diverge from the estimate. Secondary problem: wasted work and state transfer at the switch point.
2. *Blessing in disguise + feedback* — record misestimates from finished queries and feed them back into the estimator. Secondary problem: stale or misleading feedback after data changes.
3. *Separation upon condition* — robust plans: choose operators whose cost degrades gracefully (e.g. hash joins over nested loops) when estimate uncertainty is high. Secondary problem: lose the best plan when estimates are actually accurate.
