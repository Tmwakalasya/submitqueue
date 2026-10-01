# Tango-backed conflict analysis for SubmitQueue

**Status:** research, not an implementation. **Updated:** October 1, 2026. **SubmitQueue base:** `origin/main@6c4b769c`. **Tango inspected:** `uber/tango@50b3695a9909f19b78a3b1b35c095c9a8331d9db`. **go-code measured:** `c5655f3f7e87f`.

## Question

A Tango-backed `conflict.Analyzer` stores one target signature per batch and compares a new batch against every in-flight batch. This research asks which signature representation still works as the number of in-flight batches `B` grows, measured at the **worst case**: a controller instance with nothing cached (after a restart, deploy or partition rebalance) must fetch and decode all `B` signatures for one admission. A process-local cache of the immutable signatures is possible even in a stateless controller and would remove most of this cost in steady state; it is an optimization of any design below, not a design of its own, and is deliberately excluded. Today go-code runs fewer than 100 in-flight batches; the goal is to choose a design for growth, and to find where each design stops working.

## Vocabulary

- **`B`**: number of in-flight batches the new batch is compared against.
- **`T`**: total affected targets across those `B` batches.
- **Cold admission**: one dependency-analysis call on a controller with nothing cached, which fetches and decodes all `B` signatures, then checks the candidate against them.
- **Signature**: the immutable per-`(batch, base)` record a design stores to represent a batch's affected targets.
- **TangoSnapshot** (baseline): the signature is the Tango changed-targets response for the batch's affected targets, including their direct dependencies and the response's ID→name metadata, stored as raw or gzip protobuf.
- **NameKey**: the signature is the sorted list of the batch's affected target labels.
- **ID64**: the signature is the sorted list of 64-bit hashes of those labels; a shared ID→label dictionary, stored separately, recovers names.

## Findings

Costs per affected target, fitted from the measured cases (B = 100 to 10,000 on the full go-code graph):

| Design | Stored (bytes/target) | Decoded heap (bytes/target) | Serial decode (µs/target) |
| --- | ---: | ---: | ---: |
| TangoSnapshot raw (gzip) | 535 (132) | 880 | 1.5 (4.1) |
| NameKey | 101 | 123 | 0.08 |
| ID64 | 8 | 8 | 0.005 |

- **TangoSnapshot as the conflict signature breaks on a single tail request, at any B.** The largest observed request (2.88M targets) is about 1.4 GiB raw and 2.4 GiB of heap by itself. At the 99th-percentile draw, 10 in-flight batches already exceed a 1 s cold load, and about 55 exceed 1 GiB of heap. At the median draw it holds below 1 s only up to about B ≈ 80–130, below 10 s up to about 570–890, and below 1 GiB of heap up to about 630. It is viable for today's go-code load (fewer than 100 in flight) only if a multi-second, multi-GiB cold start is acceptable whenever a tail request is in flight.
- **NameKey** is about 5× smaller than raw TangoSnapshot in stored bytes (1.3× smaller than gzip) and 7× smaller in heap. It holds 1 GiB of heap to about B ≈ 3,700 (median) or 1,100 (p99), and 10 s of cold load to about 1,450 (object store) or 3,700 (KV point reads).
- **ID64** is a further 12–15× smaller. Its heap stays below 1 GiB until about B ≈ 50,000. Its cold-load limit comes from round trips, not bytes: under the object-store profile, `ceil(B/32) × 150 ms` alone exceeds 1 s at B ≈ 200 and 10 s at B ≈ 2,100. Under KV point reads it holds 1 s to about B ≈ 2,100 (median) or 1,000 (p99), and 10 s to about 20,000.
- **The conflict rule needs the new-edge rule, and caching signatures across base advances is a trade-off.** Signatures computed once against the newest base miss conflicts unless they also inherit from structural dependencies; see [Conflict rule and base drift](#conflict-rule-and-base-drift).
- **The in-memory check is never the bottleneck.** At B = 10,000 it takes 32 ms (ID64) to 292 ms (TangoSnapshot) of CPU, against seconds to minutes of loading.

## What exists today

- SubmitQueue calls `conflict.Analyzer.Analyze(ctx, batch, inFlight)` from the dependency-analysis stage, whose messages are partitioned by queue and consumed in order ([`dependencyanalysis.go`](../../../submitqueue/orchestrator/controller/dependencyanalysis/dependencyanalysis.go)). Before the call, [`ListByStates`](../../../submitqueue/orchestrator/core/batch/list.go) hydrates every in-flight `Batch` by key at concurrency 16; the speculation stage repeats that listing ([`run.go`](../../../submitqueue/orchestrator/controller/speculate/run.go)). Signatures are not loaded today, so every design below adds its own reads on top of this.
- Tango's `GetChangedTargets` returns `NEW`/`DELETED`/`CHANGED` targets with old/new detail and the distance from a directly changed seed; `GetChangedTargetGraph` is unimplemented. Response IDs are local to one response and the ID→name metadata arrives last, so signatures must be built from resolved labels. Request `OutputConfig{MaxDistance: -1}` explicitly: an unset distance means `0`, not unlimited. See the [wire contract](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/proto/tango.proto) and [output filtering](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/controller/output_filter.go).
- Tango's compared-targets cache key omits the computation strategy and `SeedAttributes` policy ([cache keys](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/core/cachekey/cachekey.go)). Fix or isolate it before treating Tango as a conflict oracle.
- Tango applies GitHub PR URIs only ([request parser](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/core/workspace/request.go)); `phab://` queues need an adapter or the conservative `all` analyzer.

## Prior art: the production SubmitQueue

The legacy production SubmitQueue (`java-code`, `dev-platform/cicd/submitqueue/`) does not call Tango. Its Java `TargetAnalyzer` Thrift service diffs name→hash target maps plus direct edges between the speculation base and base plus the requests. `ChangesRetrieverImpl.processTargetStats` turns that diff into the `targetsChanged`/`targetsAdded`/`targetsRemoved` counts behind the Hive table used below. Its `ConflictAnalyzerImpl` is pairwise on one common base, and `FastTargetComparer` flags a conflict when any of three pairs intersects:

- A's changed targets with B's changed targets;
- the destinations of A's **new edges** with B's changed targets;
- the destinations of B's new edges with A's changed targets.

Removed edges are ignored. This replaced the "union graph" algorithm of [Keeping Master Green at Scale](https://dl.acm.org/doi/10.1145/3302424.3303970) (EuroSys 2019), which its Javadoc says needed too much memory and failed on cycles in the union of three graphs. A code comment there puts a target-analyzer RPC at about 3 minutes on average. Two things were not confirmed from source: whether that service's target hashes are transitive, and which entry point production calls. The new-edge rule is exactly the gap the next section finds in a plain affected-set overlap.

## Conflict rule and base drift

**Proposed premise:** compute each batch's changed targets once against the newest base at admission, cache the result, and never recompute it. Because every base advance is a landed batch that the queue itself analyzed, this should introduce no extra missed conflicts. **The premise is false as stated.** The [simulation](../../../tool/tangograph-eval/basedrift.go) tests it on 200,000 random small DAGs, each with up to three batches landing between admissions. Ground truth for a pair of batches allowed to run independently is a target affected by both in the first graph that contains both changes. Signatures follow Tango: the reverse closure, in the changed graph, of edited, rewired and new targets.

| Comparison signature | Missed / independent pairs, same base (pairs) | Missed / independent pairs, across a base advance (pairs) | False conflicts (pairs) | Mean signature size (targets) |
| --- | ---: | ---: | ---: | ---: |
| `affected` | 28,529 / 189,645 (15.04%) | 15,862 / 102,737 (15.44%) | 4,868 | 6.53 |
| `affected+added-deps` | 1,451 / 152,898 (0.95%) | 1,416 / 86,644 (1.63%) | 9,854 | 7.35 |
| `affected+added-deps+inherit-structural` | 0 / 135,393 (0.00%) | 0 / 62,040 (0.00%) | 49,319 | 11.09 |
| `affected+added-deps+inherit-all` | 0 / 131,053 (0.00%) | 0 / 56,248 (0.00%) | 59,608 | 12.43 |

Source: [simulation result](tango-eval/base-drift-simulation-20261001.json), 200,000 trials on 40-node random DAGs.

Two mechanisms produce the misses:

1. **A new dependency edge is invisible to the other change.** A makes `X` depend on `Y` and B edits `Y`. A's signature has `X` and its dependents; B's signature, computed on a graph where `X` does not yet depend on `Y`, does not. The fix is the production rule above: add each change's new-edge destinations (and the dependencies of its new targets) to its signature.
2. **A cached signature goes stale when a structural batch it depends on lands.** L makes `Z` depend on `X`, A edits `X`, and, with rule 1, A correctly depends on L. L lands. C now edits `Z` on the new base, where `Z` depends on `X`, but A's cached signature was computed before that edge existed and has no `Z`. C runs independently of A, and `Z` with both changes is never built. The same staleness also lets two batches that both depend on one structural batch miss each other on a shared base.

Inheritance closes both cases in every trial: a batch's comparison signature becomes its own signature united with the comparison signatures of the dependencies that add edges or targets, recorded once at admission. Any dependency path a landed structural batch created runs through one of its new edges. That edge's destination is in the batch's signature and its dependents are in its affected set, so inheriting the signature covers every target the path reaches. The cost is more false conflicts (about 5× in the toy model) and larger signatures (+51% mean), both growing along dependency chains of structural batches.

**Trade-off:** either accept a possible conflict undercount, which rule 1 alone shrinks from about 15% to about 1–2% of truly conflicting independent pairs in the toy model, or pay the false conflicts of inheritance. Recomputing signatures after every structural land is the third option, but it costs one Tango call per in-flight batch per structural land. The toy rates show only that misses exist; they do not estimate production rates. Commits that bypass the queue (direct pushes, reverts, bots) break the "landed batches only" premise under every rule.

## Workload: the real distribution of changed targets

The [summary query](tango-eval/hive-go-diff-targets-20260930.sql) and the [histogram query](tango-eval/hive-go-diff-target-histogram-20260930.sql) read `rawdata_user.kafka_hp_submitqueue_request_feature_event_nodedup` (`TIER_THREE`) for the `go` queue, September 1–29, 2026. They keep single-diff requests (`stackheight = 1`) and the latest event per diff, and use `targetschanged + targetsadded + targetsremoved` as the affected-target proxy. The checked-in [histogram](tango-eval/hive-go-diff-target-histogram-20260930.csv) holds one row per distinct count (4,953 rows) and reproduces the summary query's 58,876 diffs and 148,766,796 targets exactly. It contains no diff IDs.

| Statistic | Affected targets per request |
| --- | ---: |
| Requests | 58,876 |
| Zero targets | 5,942 (10.1%) |
| Median / p90 / p95 | 42 / 1,026 / 3,274 |
| p99 / p99.9 / max | 32,314 / 410,642 / 2,884,044 |
| Mean | 2,526.8 |
| Requests with ≥100,000 targets | 233 (0.40%), carrying 66.1% of all targets |

**One batch is one request** throughout; multi-request batches are not modeled. Each batch's target count is drawn from this histogram, and batch `i` always draws the same count, so a smaller `B` is a prefix of a larger one. Totals over many batches converge on `B × 2,527`, but at small `B` they are dominated by whether a tail request is in flight. Results are therefore reported at the 50th and 99th percentile of that total. Each drawn count is turned into a **closure-shaped** set on the complete go-code graph: the reverse-dependency closure of a random source file, widened with the closures of dependencies of targets already in the set until the count is reached. Labels, dependencies and set shape come from the real graph; which targets a real diff touches does not. The candidate batch is fixed at the mean, 2,527 targets.

## Design details

All three store an immutable signature per `(batch, base)` and differ in the data structure.

1. **TangoSnapshot (baseline): the Tango response itself.** Each batch stores a default-field `GetChangedTargets`-like response for its affected targets: optimized targets with their real direct dependencies plus the response-local ID→name and rule-type metadata, as raw protobuf or per-message gzip. This is the option to beat. The response is fetched anyway, and its dependency metadata is what conflict relaxation in the speculator could use, so if it is small enough, one artifact serves both. It is modeled, not captured: a real response also carries old/new detail and may be larger.
2. **NameKey: sorted canonical labels.** One blob of varint-length-prefixed UTF-8 labels per batch. Names are directly available to relaxation.
3. **ID64: sorted 64-bit target IDs plus an ID→name dictionary.** One blob of 8-byte IDs per batch. Conflict detection does not need the dictionary: a 64-bit collision only adds a false conflict, which is safe, and for 3 million labels the chance of any collision is about `n²/2⁶⁵ ≈ 2×10⁻⁷`. Relaxation may need names, so the dictionary stays in the design: an append-only store keyed by ID whose row can hold every label hashing to that ID. IDs are then a pure function of the label, with no read-before-write registration. The dictionary must be written before the batch is announced to speculation, but the conflict check never reads it.

## Measured results

The [benchmark](../../../tool/tangograph-eval/loadbench.go) reads the complete go-code Bazel graph (2,969,283 targets), builds the workload above, and for each `B` encodes and then actually decodes every signature in Go. It measures live heap after GC and times one candidate check against all `B` decoded batches. Raw numbers are in the [result JSON](tango-eval/go-code-load-histogram-20261001.json); the run took 13 min 48 s wall time and peaked at 35.2 GB (35,166,136 KiB) RSS for all cases together ([timing log](tango-eval/go-code-load-histogram-20261001-timing.txt)).

Measured cases use one fixed draw per `B` (the 'Total targets' column), not the median draw, so small-`B` rows reflect whichever tail requests that draw contains. Use the scale model below for percentiles.

### 1. Serialized size of all in-flight signatures

Application payload in MiB. The ID64 dictionary is the exact minimal size for the labels in all `B` batches and the candidate: an 8-byte ID, a varint length and the label per distinct target. It is stored, not fetched. Store overhead (keys, row headers, checksums) is excluded.

| `B` (batches) | Total targets (targets) | Largest batch (targets) | TangoSnapshot raw (MiB) | TangoSnapshot gzip (MiB) | NameKey (MiB) | ID64 signatures (MiB) | ID64 dictionary (MiB) | ID64 total (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 177,555 | 149,002 | 105.0 | 25.8 | 17.5 | 1.4 | 16.8 | 18.1 |
| 250 | 1,128,207 | 831,151 | 422.3 | 102.9 | 108.0 | 8.6 | 85.5 | 94.1 |
| 500 | 1,723,599 | 831,151 | 730.4 | 177.8 | 165.4 | 13.2 | 87.4 | 100.6 |
| 1,000 | 2,627,040 | 831,151 | 1,272.9 | 311.2 | 251.5 | 20.0 | 87.8 | 107.9 |
| 2,500 | 5,192,014 | 831,151 | 2,774.0 | 675.3 | 497.2 | 39.6 | 88.4 | 128.0 |
| 5,000 | 8,806,334 | 831,151 | 4,875.6 | 1,179.4 | 845.9 | 67.2 | 90.5 | 157.7 |
| 10,000 | 21,208,724 | 2,856,456 | 10,686.5 | 2,659.0 | 2,037.7 | 161.8 | 292.3 | 454.1 |

**Interpretation.** Stored size scales with total targets, not with `B`: about 535 bytes per target for raw TangoSnapshot, 132 gzipped, 101 for NameKey and 8 for ID64. At `B = 10,000` that is about 10.4 GiB, 2.6 GiB, 2.0 GiB and 162 MiB. A few tail requests dominate every total: in the `B = 100` row a single 149,002-target batch is 84% of all targets. The ID64 dictionary grows with *distinct* labels, not batches, so it plateaus near 90 MiB from `B = 250` to `5,000`. It jumps at `B = 10,000` when a 2.86M-target request registers most of the graph. It is bounded by the whole graph's labels, about 300 MiB, so ID64's total stays the smallest of the three.

### 2. Go heap with all signatures decoded

Live heap after GC, in MiB, for `B` decoded signatures plus the candidate. The graph, workload and serialized buffers are excluded.

| `B` (batches) | Total targets (targets) | Largest batch (targets) | TangoSnapshot (MiB) | NameKey (MiB) | ID64 (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 177,555 | 149,002 | 166.9 | 21.6 | 1.4 |
| 250 | 1,128,207 | 831,151 | 732.2 | 132.4 | 8.7 |
| 500 | 1,723,599 | 831,151 | 1,227.9 | 202.7 | 13.3 |
| 1,000 | 2,627,040 | 831,151 | 2,092.5 | 308.2 | 20.3 |
| 2,500 | 5,192,014 | 831,151 | 4,499.6 | 609.3 | 40.3 |
| 5,000 | 8,806,334 | 831,151 | 7,884.8 | 1,036.1 | 68.6 |
| 10,000 | 21,208,724 | 2,856,456 | 17,677.6 | 2,494.6 | 164.6 |

**Interpretation.** Decoded heap is what a cold controller must hold at once. TangoSnapshot needs about 880 bytes per target, 1.6× its raw bytes, because response-local ID→name maps and dependency lists are materialized. That is 2 GiB at `B = 1,000` and 17 GiB at `B = 10,000`. NameKey's 123 bytes per target (string headers plus label bytes) reaches 2.4 GiB at `B = 10,000`. ID64 stays at about 8 bytes per target: 165 MiB at `B = 10,000`. A single tail request (2.88M targets) costs about 2.4 GiB as TangoSnapshot, 340 MiB as NameKey and 22 MiB as ID64.

### 3. Fetch and decode latency

Time for one cold admission, in seconds: `ceil(B/P) × first byte + bytes / bandwidth + measured serial decode CPU`. These are planning assumptions, not measurements of a chosen backend. **Object store** means 150 ms to first byte, `P = 32` parallel GETs and 100 MiB/s per controller (S3 Standard-like, after [AWS guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)). **KV point read** means 5 ms, `P = 16` and 100 MiB/s, which suits signatures kept in a key-value or SQL store. Decode is measured on one goroutine; spreading it over cores would shorten it at the cost of concurrent heap.

| `B` (batches) | Store profile | TangoSnapshot raw (s) | TangoSnapshot gzip (s) | NameKey (s) | ID64 (s) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 100 | object-store | 1.93 | 1.79 | 0.79 | 0.61 |
| 100 | kv-point-read | 1.37 | 1.22 | 0.23 | 0.05 |
| 250 | object-store | 6.97 | 5.70 | 2.37 | 1.29 |
| 250 | kv-point-read | 5.85 | 4.58 | 1.25 | 0.17 |
| 500 | object-store | 12.24 | 10.22 | 4.17 | 2.54 |
| 500 | kv-point-read | 10.00 | 7.98 | 1.93 | 0.30 |
| 1,000 | object-store | 21.52 | 18.07 | 7.50 | 5.01 |
| 1,000 | kv-point-read | 17.04 | 13.59 | 3.01 | 0.53 |
| 2,500 | object-store | 47.71 | 40.70 | 17.17 | 12.28 |
| 2,500 | kv-point-read | 36.65 | 29.63 | 6.11 | 1.21 |
| 5,000 | object-store | 86.02 | 72.80 | 32.70 | 24.25 |
| 5,000 | kv-point-read | 64.04 | 50.82 | 10.72 | 2.26 |
| 10,000 | object-store | 186.73 | 161.50 | 69.21 | 48.69 |
| 10,000 | kv-point-read | 142.90 | 117.67 | 25.38 | 4.87 |

**Interpretation.**
- **TangoSnapshot and NameKey** are bound by bytes (transfer plus decode). Gzip makes TangoSnapshot slightly faster, despite costlier decompression, because at 100 MiB/s transfer outweighs decode. At `B = 1,000` TangoSnapshot takes 14–22 s and NameKey 3–7.5 s.
- **ID64** is bound by round trips. Behind the object store, `ceil(B/32) × 150 ms` is 47 of its 49 s at `B = 10,000`. Behind 5 ms point reads, the same signatures load in 4.9 s at `B = 10,000` and 0.5 s at `B = 1,000`. For ID64 the store's per-request latency and parallelism matter far more than the encoding.

### 4. Checking one candidate

Local CPU time for one candidate, in milliseconds, against all `B` decoded batches, stopping at the first shared target in each batch. "Matches" is the number of in-flight batches the candidate would depend on.

| `B` (batches) | Matches (batches) | TangoSnapshot (ms) | NameKey (ms) | ID64 (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 31 | 0.70 | 1.31 | 0.32 |
| 250 | 72 | 5.44 | 6.95 | 0.64 |
| 500 | 146 | 17.59 | 8.19 | 1.66 |
| 1,000 | 312 | 34.70 | 15.19 | 3.04 |
| 2,500 | 814 | 74.59 | 40.08 | 8.54 |
| 5,000 | 1,709 | 122.61 | 73.69 | 23.64 |
| 10,000 | 3,424 | 291.98 | 151.73 | 31.98 |

**Interpretation.** The check is never the bottleneck: at `B = 10,000` it is 32 ms (ID64) to 292 ms (TangoSnapshot), against 5–187 s of loading. ID64 is fastest because it compares fixed-width integers, while TangoSnapshot pays a map lookup per target. About a third of in-flight batches overlap the mean-sized candidate. That rate reflects the closure-shaped synthetic sets, not observed diffs, so the dependency list itself (`Ω(matches)`) can be as costly as the check.

## Where each design stops working

The [scale model](../../../tool/tangograph-eval/scale.go) fits each measured cost as a per-target ratio, `Σ cost / Σ T` over the measured cases (`T` is total targets across the batches). It does not use a two-term `a·B + b·T` fit: `B` and `T` rise together across the cases, so a per-batch term cannot be identified. Round trips are still charged per batch in the latency formula. It then draws `T` by Monte Carlo from the histogram, 400 trials per `B`. Below is the first `B` at which a budget is exceeded for the median draw and for the 99th-percentile draw. The budgets are illustrative and should be replaced by the admission SLO and controller memory limit once those are set. "—" means not exceeded up to `B = 100,000`. Beyond the largest measured `B` (10,000) these are extrapolations.

| Budget | TangoSnapshot raw (first `B`, median / p99) | TangoSnapshot gzip (first `B`, median / p99) | NameKey (first `B`, median / p99) | ID64 (first `B`, median / p99) |
| --- | ---: | ---: | ---: | ---: |
| Cold load > 1 s (object-store) | 81 / ≤10 | 94 / ≤10 | 161 / 34 | 196 / 133 |
| Cold load > 10 s (object-store) | 574 / 55 | 664 / 55 | 1,450 / 633 | 2,142 / 1,943 |
| Cold load > 60 s (object-store) | 3,014 / 1,030 | 3,489 / 1,381 | 8,397 / 6,908 | 12,406 / 12,406 |
| Cold load > 1 s (kv-point-read) | 120 / ≤10 | 133 / ≤10 | 450 / 55 | 2,142 / 1,030 |
| Cold load > 10 s (kv-point-read) | 732 / 55 | 890 / 55 | 3,664 / 1,136 | 20,208 / 17,457 |
| Cold load > 60 s (kv-point-read) | 3,664 / 1,136 | 4,676 / 1,678 | 21,219 / 15,834 | — / — |
| Decoded heap > 1 GiB | 633 / 55 | 633 / 55 | 3,664 / 1,136 | 53,619 / 42,012 |
| Decoded heap > 4 GiB | 2,249 / 450 | 2,249 / 450 | 14,362 / 9,721 | — / — |
| Decoded heap > 16 GiB | 8,397 / 4,453 | 8,397 / 4,453 | 56,300 / 46,318 | — / — |

**Interpretation.** At the 99th-percentile draw, TangoSnapshot exceeds 1 s with 10 batches and 1 GiB with about 55: one tail request in flight is enough. NameKey stays within 10 s and 1 GiB into the low thousands. ID64's heap is not a constraint below about 50,000 batches. Its load limit is set by the store: about 200 batches for 1 s behind an object store, about 2,000 behind KV point reads.

Representative modeled points (median draw / 99th-percentile draw):

| `B` (batches) | Design | Decoded heap (MiB) | Cold load, object store (s) | Cold load, KV point read (s) |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | TangoSnapshot raw | 1,683 / 6,171 | 18.1 / 53.7 | 13.7 / 49.2 |
| 1,000 | NameKey | 236 / 865 | 6.9 / 12.5 | 2.4 / 8.0 |
| 1,000 | ID64 | 16 / 57 | 5.0 / 5.4 | 0.5 / 0.9 |
| 5,000 | TangoSnapshot raw | 10,403 / 18,929 | 106.0 / 173.6 | 84.0 / 151.6 |
| 5,000 | NameKey | 1,458 / 2,653 | 36.5 / 47.1 | 14.5 / 25.1 |
| 5,000 | ID64 | 96 / 175 | 24.6 / 25.4 | 2.6 / 3.4 |
| 10,000 | TangoSnapshot raw | 20,681 / 34,992 | 210.9 / 324.4 | 167.1 / 280.5 |
| 10,000 | NameKey | 2,899 / 4,905 | 72.6 / 90.4 | 28.8 / 46.6 |
| 10,000 | ID64 | 191 / 324 | 49.0 / 50.3 | 5.1 / 6.5 |
| 20,000 | TangoSnapshot raw | 42,097 / 57,413 | 427.5 / 548.9 | 340.0 / 461.4 |
| 20,000 | NameKey | 5,900 / 8,047 | 146.0 / 165.0 | 58.5 / 77.5 |
| 20,000 | ID64 | 389 / 531 | 97.8 / 99.3 | 10.3 / 11.8 |
| 50,000 | TangoSnapshot raw | 106,413 / 131,715 | 1,078.1 / 1,278.6 | 859.2 / 1,059.8 |
| 50,000 | NameKey | 14,915 / 18,461 | 366.6 / 398.0 | 147.7 / 179.2 |
| 50,000 | ID64 | 985 / 1,219 | 244.8 / 247.2 | 25.9 / 28.4 |

## Recommendation

Use **ID64 signatures stored as one record per `(queue, batch)` in a low-latency key-value or SQL store**, not an object store. The latency profile matters more than the encoding: the same 8-byte IDs hit a 1 s cold load at B ≈ 200 behind 150 ms object GETs, but at B ≈ 2,000 behind 5 ms point reads.

Keep the ID→name dictionary so that conflict relaxation can recover labels. For relaxation that needs Tango's dependency metadata, store the TangoSnapshot response as a separate per-batch record and read it **only for the batches relaxation actually examines**, typically the matched pairs. It is then never loaded for all `B` batches on the conflict-check path. This keeps the baseline's metadata available while moving its size off the path that scales with `B`.

Add the warm-controller cache as an optimization once the store is chosen; it does not change the cold-start limits above. Beyond about B ≈ 20,000 (KV, 10 s) even ID64 cold loads become long. That is the point at which to revisit the persistent target→batch posting index, which reads postings for the candidate's targets instead of every in-flight signature.

Implementation shape:

1. Add `submitqueue/orchestrator/extension/conflict/tango/` implementing the shared `conflict.Analyzer`, with a Tango streaming client, a `changeset.Resolver`, a base-revision resolver and a key-oriented signature store injected at construction, and per-queue routing in `service/submitqueue/orchestrator/server/`.
2. Make one `GetChangedTargets` call per new batch against the newest base, with `MaxDistance: -1` and hashes, tags and attributes omitted. Treat a partial stream, unknown ID or cancellation as a failed analysis, never as an empty signature.
3. Form the signature from affected targets **plus new-edge destinations and dependencies of new targets**. Either record the comparison signature with inheritance from structural dependencies or accept the documented undercount; this is a policy decision.
4. Persist the signature (and, for ID64, the dictionary rows) before the `Creating → Created` promotion and before announcing to speculation. Keep each write key-local and idempotent on redelivery.
5. Shadow it against `all` and `pathoverlap`, recording per-batch `K`, in-flight `B`, cold-start frequency, fetch/decode latency, Tango latency, conflict pairs, and how often a land is structural.

## Full-graph footprint

For scale: the complete `//external:all-targets + deps(//...:all-targets)` query at `c5655f3f7e87f` produced the [measurement](tango-eval/go-code-full-20260928.json) below. No design here stores the full graph per batch. At 378.50 MiB, `B = 100` copies would already be 37 GiB.

| Property | Value |
| --- | ---: |
| Bazel `streamed_proto` input | 2,799,274,578 bytes |
| Targets (external) | 2,969,283 (430,553) |
| Dependency edges | 14,519,552 |
| Modeled default Tango graph protobuf (gzip) | 378.50 MiB (62.87 MiB) |
| Go heap, Tango-shaped ID graph | 1.099 GiB |
| Sorted 64-bit fingerprints, all targets | 22.65 MiB |

## Reproduce

Capture the graph from a go-code checkout; use only a complete stream.

```sh
bazel query --order_output=no --proto:locations --noproto:default_values --output=streamed_proto \
  '//external:all-targets + deps(//...:all-targets)' > /tmp/sq-go-code-full-20260928.streamed_proto
```

From this repository:

```sh
GOMEMLIMIT=300GiB go run ./tool/tangograph-eval -benchmark-load \
  -input /tmp/sq-go-code-full-20260928.streamed_proto -go-code-revision c5655f3f7e87f \
  -target-histogram doc/rfc/submitqueue/tango-eval/hive-go-diff-target-histogram-20260930.csv \
  -candidate-targets 2527 -benchmark-batches 100,250,500,1000,2500,5000,10000 > load.json
go run ./tool/tangograph-eval -simulate-base-drift 200000 > drift.json
go run ./tool/tangograph-eval -input /tmp/sq-go-code-full-20260928.streamed_proto \
  -query '//external:all-targets + deps(//...:all-targets)' -go-code-revision c5655f3f7e87f \
  -tango-revision 50b3695a9909f19b78a3b1b35c095c9a8331d9db > footprint.json
```

The raw Bazel output is not committed: it is 2.8 GB and contains internal labels. Its SHA-256 is `99f0cfd8e2541d381ca845dcaac01b4166cb451817ca998202932dbf3009c14f`.

## Limitations

- The Hive counts come from the legacy target analyzer, not Tango. They describe requests, not multi-request batches.
- Affected sets are closure-shaped but synthetic. Real match rates depend on which targets real diffs touch, which this data does not show.
- TangoSnapshot is modeled from the Bazel graph, not captured from a Tango server; old/new detail would make it larger.
- Store latencies are assumptions. No blob store, KV store or Tango RPC was benchmarked.
- The base-drift simulation runs on toy DAGs. It shows which rules can miss conflicts, not how often production would.
