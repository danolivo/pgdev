# The 40 inventive principles — software analogies

Each line: classical principle → what it means in software → typical examples. Use these as prompts for ideas, not as answers. Pick the few that fit the contradiction at hand.

1. **Segmentation** — split into independent parts; make parts easy to separate; increase the degree of fragmentation. Examples: partitioning, sharding, microservices, chunked processing, sub-agents with isolated contexts.
2. **Taking out (extraction)** — separate the disturbing part, or extract only the needed part. Examples: move the hot path out of a generic routine, sidecars, read replicas for reporting, extracting configuration from code.
3. **Local quality** — make structure non-uniform; let each part work in the conditions that suit it best. Examples: per-table or per-column settings, specialised fast paths for common cases, JIT for hot loops only.
4. **Asymmetry** — replace a symmetrical design with an asymmetrical one. Examples: leader/follower roles, read/write split, different timeouts per direction, skewed hashing for hot keys.
5. **Merging (consolidation)** — combine identical or adjacent operations. Examples: batching, group commit, request coalescing, combining several small queries into one.
6. **Universality** — make one part perform several functions. Examples: one write-ahead log used for recovery, replication and change data capture; one binary playing several roles.
7. **Nested doll** — place one object inside another. Examples: savepoints inside transactions, layered caches, subqueries, containers inside VMs.
8. **Anti-weight** — compensate a load with a counter-force, often from the environment. Examples: backpressure, rate limiting, offloading work to clients or CDNs.
9. **Preliminary anti-action** — counter a known harmful effect in advance. Examples: input validation at the boundary, pre-armed circuit breakers, chaos testing before release.
10. **Preliminary action** — do the work, or part of it, before it is needed. Examples: indexes, materialised views, prepared statements, prefetching, cache warm-up, precomputed embeddings.
11. **Beforehand cushioning** — prepare emergency means in advance for unreliable parts. Examples: backups, fallbacks, retries with idempotency, kill switches, safe defaults.
12. **Equipotentiality** — avoid moving things "up and down"; keep them at the same level. Examples: process data where it lives, zero-copy, avoid crossing process or network boundaries, avoid repeated serialisation.
13. **The other way round** — invert the action, the roles, or the order. Examples: push instead of pull, inversion of control, test-first, ask the tool/oracle instead of the model, reverse indexes.
14. **Curvature** — go from linear to circular or rotating structures. Examples: ring buffers, circular logs, consistent-hashing rings, round-robin scheduling.
15. **Dynamics** — make characteristics adjustable, adaptive, or optimal at each stage. Examples: adaptive algorithms, auto-tuning, adaptive query execution, dynamic tool selection for agents.
16. **Partial or excessive action** — if exactly 100% is hard, do a bit less or a bit more. Examples: approximate answers, sampling, probabilistic sketches, speculative execution, over-provisioning.
17. **Another dimension** — move to another dimension, add levels, or tilt the structure. Examples: multi-level indexes, adding a time dimension (MVCC, versioning), turning a flat list into a tree, adding a metadata layer.
18. **Vibration** — use oscillation, frequency, resonance. Examples: heartbeats, polling with jitter, periodic checkpoints, aligning batch size with I/O cycles.
19. **Periodic action** — replace continuous action with periodic action, use the pauses. Examples: batch jobs, periodic maintenance, debouncing, doing background work in idle gaps.
20. **Continuity of useful action** — remove idle time; keep all parts working. Examples: pipelining, async I/O, streaming instead of materialising, keeping worker pools busy.
21. **Skipping (rushing through)** — perform harmful or risky stages very quickly. Examples: short critical sections, fast paths, fail-fast, lock elision.
22. **Blessing in disguise** — use harmful factors to obtain a positive effect. Examples: compiler and test errors as agent feedback, misestimates as training signal, failed requests as monitoring data, honeypots.
23. **Feedback** — introduce feedback or change its strength. Examples: monitoring loops, autoscaling, cardinality feedback, test-driven agent loops, evaluation-driven prompt changes.
24. **Intermediary** — use an intermediate object or process. Examples: proxies, adapters, queues, brokers, connection poolers, an orchestrator between agents and tools.
25. **Self-service** — make the object serve itself, use waste resources. Examples: self-healing, auto-maintenance, self-describing data, checksums that verify themselves, agents that write their own tests.
26. **Copying** — use cheap copies instead of the expensive or fragile original. Examples: snapshots, replicas, mocks, staging environments, copy-on-write, dry runs, simulations.
27. **Cheap short-lived objects** — replace an expensive durable object with many cheap disposable ones. Examples: ephemeral containers, throwaway sandboxes, short-lived tokens, arena allocators, disposable prototypes.
28. **Replace a mechanical system** — replace manual or physical interaction with informational fields. Examples: declarative configuration, events instead of polling, signals and notifications.
29. **Pneumatics and hydraulics** — use flexible, "fluid" elements instead of rigid ones. Examples: elastic buffers and queues that absorb bursts, elastic scaling.
30. **Flexible shells and thin films** — isolate with a thin flexible layer. Examples: thin wrappers and facades, sandboxes, namespaces, API gateways.
31. **Porous materials** — make the object porous; use the pores. Examples: sparse structures, slack space in pages (fill factor), gaps in identifiers, extension hooks.
32. **Colour changes** — change visibility or observability. Examples: tracing, tagging, structured logging, EXPLAIN output, highlighting diffs for reviewers.
33. **Homogeneity** — make interacting objects of the same material. Examples: one data format end to end, one protocol, the same language on client and server.
34. **Discarding and recovering** — discard parts that have done their job; restore consumed parts. Examples: garbage collection, TTLs, log truncation, rebuilding caches from the source of truth, context compaction.
35. **Parameter changes** — change state, concentration, flexibility, degree. Examples: consistency level, compression level, serialisation format, granularity of locks or tasks.
36. **Phase transitions** — use effects that appear when the state changes. Examples: hot/cold data tiering, compile-time vs run-time work, standby promotion, migration phases.
37. **Thermal expansion** — let properties change with load or conditions. Examples: autoscaling proportional to load, adaptive timeouts, buffers that grow under pressure.
38. **Strong oxidants** — intensify the environment to speed up a process. Examples: stress testing, fuzzing, aggressive optimisation levels for hot code.
39. **Inert atmosphere** — replace the environment with a neutral one. Examples: sandboxes, read-only modes, isolated test environments, deterministic replay.
40. **Composite materials** — combine heterogeneous parts. Examples: hybrid data structures, hybrid algorithms (introsort), polyglot persistence, an LLM combined with a symbolic verifier.
