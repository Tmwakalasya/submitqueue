# Tango conflict analysis for SubmitQueue

The [full technical report](doc/rfc/submitqueue/tango-conflict-analysis.md), [Go evaluator](tool/tangograph-eval/), and [measurement summaries](doc/rfc/submitqueue/tango-eval/) are in this repository on branch `research/tango-conflict-graphs`.

## Full go-code measurement — September 28, 2026

The complete Bazel query `//external:all-targets + deps(//...:all-targets)` succeeded on **attempt 3 of a 20-attempt maximum** against go-code commit `c5655f3f7e87f`. Attempts 1 and 2 failed on different external dependency downloads; the arguments were identical across all attempts. The checked-in [full-graph measurement](doc/rfc/submitqueue/tango-eval/go-code-full-20260928.json) and [attempt log](doc/rfc/submitqueue/tango-eval/go-code-full-20260928-attempts.tsv) replace the original whole-repo extrapolation.

| Measurement | Complete graph |
|---|---:|
| Actual Bazel query output | 2.607 GiB |
| Targets (including external) | **2,969,283** (430,553 external) |
| Dependency edges represented | 14,519,552 |
| Modeled default Tango-style protobuf, uncompressed | **378.50 MiB** across 94 messages |
| Modeled all-fields Tango-style protobuf, uncompressed | 595.55 MiB across 147 messages |
| Measured live Go heap for the evaluator's compact graph model | **1.099 GiB** |
| Raw sorted 64-bit values for *every* target (excluding the proposed name registry and postings) | 22.65 MiB |

The Bazel graph and Go evaluator were run on the full checkout. The protobuf and heap values **model** Tango's representation; no Tango service RPC, content-derived target hashing, or production network transfer was measured. The 2.8 GB raw Bazel output is not checked in because it contains internal target names; the technical report records its SHA-256 and explains the method and remaining limitations.

## Proposed approach

Use Tango's changed-target stream to produce a durable, compact impact signature per batch. Compare signatures at a common pinned base revision rather than transferring the whole monorepo graph into stateless SubmitQueue controllers. Fix or isolate Tango's incomplete compared-target cache key before using it as a conflict oracle. The technical report describes the integration constraints, measurements, and Go-first optimization plan.

## Thousands of in-flight batches — September 29, 2026

**A full signature fetch for every in-flight batch is not the scalable design.** The current dependency-analysis controller already loads every active `Batch` before invoking its conflict analyzer; doing another store read for each batch's full signature would add thousands of reads per admission. The [cross-check design](doc/rfc/submitqueue/tango-conflict-analysis.md#cross-checking-thousands-of-in-flight-batches) proposes a durable registered-target-ID→batch-ID mapping: look up postings for the *new* batch's affected targets, union their IDs, and validate the matched batches' current state. Complete index writes must succeed before a batch becomes `Created`; an incomplete index or incompatible graph epoch fails closed. A small, **complete** signature carried in the already-loaded `Batch` is a bounded interim option; truly avoiding the controller's initial all-batch hydration also requires changing that controller/analyzer boundary.

For **numeric 64-bit target IDs with name restoration**, the [ID algorithm](doc/rfc/submitqueue/tango-conflict-analysis.md#canonical-64-bit-target-ids-and-name-restoration) hashes each affected target's canonical label and registers it in a sparse, append-only ID→name store. Collision probes use conditional single-key writes. A hash cannot itself be decoded to a name; restoration requires one by-ID lookup, but no full graph-name index in memory.

## 100, 500 and 1,000 in-flight batches — September 30, 2026

**The figures in this section describe resident persistent-posting indexes and include posting writes in admission time. They do not estimate one stateless controller loading all in-flight signatures for a single read-only cross-check.** That requested comparison is under [Memory estimate](#memory-estimate--one-stateless-controller) and [Latency estimate](#latency-estimate--one-batch-versus-all-in-flight-batches).

The [benchmark](doc/rfc/submitqueue/tango-conflict-analysis.md#in-flight-benchmark-for-100-500-and-1000-batches) and its [aggregate results](doc/rfc/submitqueue/tango-eval/go-code-impact-benchmark-20260930.json) use real go-code target **names** with synthetic batches of 100 or 1,000 affected targets each; these are **not measured PR impact sets or Tango RPCs**. The `B=1,000, K=1,000` case measured **166.9 MiB** of live Go heap for 64-bit batch IDs plus the name registry and inverted postings, **173.8 MiB** for label strings plus inverted postings, and **944.4 MiB** for stored Tango-like target/metadata snapshots. The Tango-like protobuf model totals **633.3 MiB** uncompressed across those batches.

For an **illustrative**, not observed, backend with 5 ms per point read/write, concurrency 16, and 100 MiB/s transfer, the `B=1,000, K=1,000` admission model is **1.48 s** for a cold ID index, **0.95 s** for a label-string index, and **7.09 s** to load and scan uncompressed Tango-like snapshots (**3.02 s** if transferred under the benchmark's per-message gzip model, excluding decompression CPU). All include the current controller's all-batch hydration cost; the ID model includes name-registration and posting writes. At `B=100, K=1,000`, cold ID registration is instead slower than the modeled compressed Tango-like scan, so the lookup scheme should be chosen using *real* per-change `K`, store latency, and fan-out measurements—not from the eight-byte ID size alone.

The heap figures are for a **single Go process holding all synthetic records and indexes at once**, not a stateless controller's request heap or a measured database size. An index held in storage avoids loading it all into SubmitQueue; a Tango-like scan could also stream one batch at a time to lower request memory, while retaining its `B` reads and total transfer cost.

## Hive-informed target-count benchmark — September 30, 2026

The closest Hive source, [`rawdata_user.kafka_hp_submitqueue_request_feature_event_nodedup`](doc/rfc/submitqueue/tango-eval/hive-go-diff-targets-20260930.sql), reports changed/added/removed target counts for go-code SubmitQueue feature events. Across **58,876** unique single-diff changes from September 1–29, the latest-per-diff mean affected-target proxy is **2,526.78**. The benchmark uses **N = 2,527**, **2N = 5,054**, and **5N = 12,635**. This average is **heavily skewed**: the approximate median is **42**, while **0.40%** of diffs with at least 100,000 targets account for **66.1%** of all reported targets; [a second Hive query](doc/rfc/submitqueue/tango-eval/hive-go-diff-tail-20260930.sql) reproduced that tail contribution. These feature events are **not actual Tango RPC measurements**; the [Hive query](doc/rfc/submitqueue/tango-eval/hive-go-diff-targets-20260930.sql), [aggregate](doc/rfc/submitqueue/tango-eval/hive-go-diff-targets-20260930.json), and [nine-case results with a complete field dictionary](doc/rfc/submitqueue/tango-conflict-analysis.md#hive-derived-target-counts-n-2n-and-5n-september-30-2026) give the evidence and qualifications.

### Three distinctive designs for one comparison

- **ID64 — Registered-ID Signature Sweep:** Load one sorted `uint64` signature per batch, keep the persistent ID→name dictionary **outside** controller memory, and intersect the new candidate's sorted IDs against all `B` signatures.
- **NameKey — Canonical-Name Signature Sweep:** Load each batch's sorted, full target labels and intersect their strings against the candidate; there is no numeric registry.
- **TangoSnapshot — OptimizedGraph-Like Snapshot Sweep:** Load each stored Tango-shaped changed-target/metadata snapshot and scan it for candidate labels. These **K-target** snapshots are modeled, not actual `GetChangedTargets` responses or whole-monorepo graphs.

These are intentionally **cold, all-batch read-only sweeps** of `B` existing signatures for **one** incoming batch, not the persistent-posting lookup of `K` keys proposed for high-scale steady state. Here `B` is in-flight batches and `K` is targets *per batch*; Hive's `N` is an average *per single diff*, not the number of batches or a measured multi-diff batch size. The [new Go evaluator](tool/tangograph-eval/coldscan.go) and [nine-case cold-scan result](doc/rfc/submitqueue/tango-eval/go-code-cold-scan-20260930.json) measure ID64 and NameKey heap/intersection CPU; existing TangoSnapshot [N](doc/rfc/submitqueue/tango-eval/go-code-impact-hive-n-20260930.json), [2N](doc/rfc/submitqueue/tango-eval/go-code-impact-hive-2n-20260930.json), and [5N](doc/rfc/submitqueue/tango-eval/go-code-impact-hive-5n-20260930.json) results supply the identical-case modeled heap and protobuf sizes.

### Memory estimate — one stateless controller

| In-flight `B` | Targets/batch `K` | ID64 request heap | NameKey request heap | TangoSnapshot request heap |
|---:|---:|---:|---:|---:|
| 100 | N (2,527) | 2.0 MiB | 30.3 MiB | 262.8 MiB |
| 100 | 2N (5,054) | 3.9 MiB | 60.7 MiB | 527.1 MiB |
| 100 | 5N (12,635) | 10.3 MiB | 151.7 MiB | 1,250.9 MiB |
| 500 | N | 9.8 MiB | 150.5 MiB | 1,315.1 MiB |
| 500 | 2N | 19.6 MiB | 301.1 MiB | 2,627.5 MiB |
| 500 | 5N | 50.9 MiB | 752.8 MiB | 6,252.9 MiB |
| 1,000 | N | 19.6 MiB | 300.8 MiB | 2,629.9 MiB |
| 1,000 | 2N | 39.1 MiB | 601.6 MiB | 5,257.5 MiB |
| 1,000 | 5N | 101.7 MiB | 1,504.0 MiB | 12,502.1 MiB |

This is **incremental post-GC heap for the `B` loaded target signatures and one candidate**, not a persistent-index heap, total process RSS or database footprint; common `Batch` entities, decode buffers and ID registry storage are excluded. At `B=1,000, K=5N` the fetched payload model is **96.4 MiB ID64**, **1,230.7 MiB NameKey**, or **8,006.1 MiB TangoSnapshot raw** (**2,859.2 MiB** if optional per-message gzip were used). The [detailed report](doc/rfc/submitqueue/tango-conflict-analysis.md#memory-estimate--one-stateless-controller) has the fetched-byte table for all nine cases.

### Blob-storage fetch latency — signatures only

If **one signature per in-flight batch is stored as a separate blob outside the `Batch` table**, the following is the modeled latency to bring all `B` signatures into **one stateless controller**, **excluding** `Batch`-table reads, decoding and conflict comparison. Use a representative **S3 Standard-like** planning scenario: **150 ms per GET to first byte**, **32 parallel GETs**, and **100 MiB/s effective aggregate download throughput per controller**. The 150 ms is the midpoint of [AWS's 100–200 ms small-object first-byte guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html); parallel requests are [recommended for throughput](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html). The bandwidth and worker count are **assumptions**, not measured SubmitQueue or provider performance. `T_fetch = ceil(B/32) × 0.150 s + fetched MiB / (100 MiB/s)`.

| In-flight `B` | Targets/batch `K` | GET first-byte wave | ID64 blobs | NameKey blobs | TangoSnapshot raw blobs | TangoSnapshot gzip blobs |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | N | 0.60 s | 0.62 s | 0.85 s | 2.20 s | 1.17 s |
| 100 | 2N | 0.60 s | 0.64 s | 1.09 s | 3.81 s | 1.75 s |
| 100 | 5N | 0.60 s | 0.70 s | 1.83 s | 8.61 s | 3.46 s |
| 500 | N | 2.40 s | 2.50 s | 3.63 s | 10.38 s | 5.26 s |
| 500 | 2N | 2.40 s | 2.59 s | 4.86 s | 18.45 s | 8.13 s |
| 500 | 5N | 2.40 s | 2.88 s | 8.55 s | 42.45 s | 16.70 s |
| 1,000 | N | 4.80 s | 4.99 s | 7.26 s | 20.77 s | 10.51 s |
| 1,000 | 2N | 4.80 s | 5.19 s | 9.72 s | 36.90 s | 16.26 s |
| 1,000 | 5N | 4.80 s | 5.76 s | 17.11 s | 84.86 s | 33.39 s |

The GET wave dominates **many small ID64 blobs**; bytes dominate large TangoSnapshot transfers. These are additive planning estimates, not observed backend latency or an SLO; GET startup and transfers can overlap, while throttling/decompression/contending controllers can worsen them. See the [detailed method and column definitions](doc/rfc/submitqueue/tango-conflict-analysis.md#blob-storage-fetch-latency--signatures-only) and [calculated input-byte and latency artifact](doc/rfc/submitqueue/tango-eval/blob-fetch-latency-20260930.json). A persistent target→batch posting index avoids fetching all `B` signature blobs on the admission hot path.

### Latency estimate — one batch versus all in-flight batches

**The following earlier one-batch total uses a 5-ms primary-key signature store, *not* the blob GET assumptions above.** For this **one** read-only comparison, model (1) `ceil(B/16) × 5 ms` for the current controller's `B` batch-entity reads, (2) another `ceil(B/16) × 5 ms` for `B` separately stored signatures, (3) transfer of the table's raw or optional gzip bytes at **100 MiB/s aggregate**, and (4) measured in-process intersection/scan CPU. **ID64 IDs ready** excludes registry I/O; **ID64 register candidate** adds `K` registry checks and conditional creates of previously unseen candidate labels, using the earlier benchmark's count of registry entries. NameKey requires no ID registry. TangoSnapshot gzip omits decompression CPU. If blobs are used, **replace** the signature-read-plus-transfer term with the blob-fetch table; do **not** simply add the two tables.

| In-flight `B` | Targets/batch `K` | ID64: IDs ready | ID64: register candidate | NameKey | TangoSnapshot raw | TangoSnapshot gzip |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | N | 0.09 s | 1.60 s | 0.33 s | 1.68 s | 0.66 s |
| 100 | 2N | 0.11 s | 2.97 s | 0.57 s | 3.29 s | 1.23 s |
| 100 | 5N | 0.17 s | 6.49 s | 1.30 s | 8.08 s | 2.93 s |
| 500 | N | 0.42 s | 1.69 s | 1.60 s | 8.39 s | 3.26 s |
| 500 | 2N | 0.51 s | 2.68 s | 2.81 s | 16.43 s | 6.11 s |
| 500 | 5N | 0.80 s | 5.08 s | 6.49 s | 40.40 s | 14.65 s |
| 1,000 | N | 0.83 s | 1.92 s | 3.19 s | 16.78 s | 6.53 s |
| 1,000 | 2N | 1.02 s | 2.83 s | 5.62 s | 32.83 s | 12.20 s |
| 1,000 | 5N | 1.60 s | 5.58 s | 12.97 s | 80.76 s | 29.29 s |

**These are optimistic illustrative timings, not a production latency measurement or SLO.** They omit Tango computation for the candidate, storage/decode overhead, optional gzip decompression, list enumeration, retries and contention. The `2N`/`5N` uniformly sampled workload makes every batch intersect; this is **not** a production conflict-rate estimate. A *persistent posting-index* lookup is a different workflow: it does **not** load `B` target signatures and, with candidate IDs ready, has a modeled read floor of `R(B)+R(K)` plus posting bytes/CPU (**1.105 s** at `B=1,000, K=N`), before new-batch posting writes. If “full Tango response” instead means an **entire monorepo `GetTargetGraphResponse` per batch**, rather than the K-target TangoSnapshot, the measured whole-graph model implies **37/185/370 GiB** fetched for `B=100/500/1,000`, or **6.3/31.5/63.1 minutes of transfer alone** at 100 MiB/s; it is a separate, substantially larger scenario.
