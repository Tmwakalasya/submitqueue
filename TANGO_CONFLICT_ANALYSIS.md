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

The [benchmark](doc/rfc/submitqueue/tango-conflict-analysis.md#in-flight-benchmark-for-100-500-and-1000-batches) and its [aggregate results](doc/rfc/submitqueue/tango-eval/go-code-impact-benchmark-20260930.json) use real go-code target **names** with synthetic batches of 100 or 1,000 affected targets each; these are **not measured PR impact sets or Tango RPCs**. The `B=1,000, K=1,000` case measured **166.9 MiB** of live Go heap for 64-bit batch IDs plus the name registry and inverted postings, **173.8 MiB** for label strings plus inverted postings, and **944.4 MiB** for stored Tango-like target/metadata snapshots. The Tango-like protobuf model totals **633.3 MiB** uncompressed across those batches.

For an **illustrative**, not observed, backend with 5 ms per point read/write, concurrency 16, and 100 MiB/s transfer, the `B=1,000, K=1,000` admission model is **1.48 s** for a cold ID index, **0.95 s** for a label-string index, and **7.09 s** to load and scan uncompressed Tango-like snapshots (**3.02 s** if transferred under the benchmark's per-message gzip model, excluding decompression CPU). All include the current controller's all-batch hydration cost; the ID model includes name-registration and posting writes. At `B=100, K=1,000`, cold ID registration is instead slower than the modeled compressed Tango-like scan, so the lookup scheme should be chosen using *real* per-change `K`, store latency, and fan-out measurements—not from the eight-byte ID size alone.

The heap figures are for a **single Go process holding all synthetic records and indexes at once**, not a stateless controller's request heap or a measured database size. An index held in storage avoids loading it all into SubmitQueue; a Tango-like scan could also stream one batch at a time to lower request memory, while retaining its `B` reads and total transfer cost.
