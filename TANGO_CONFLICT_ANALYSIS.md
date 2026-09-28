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
| Sorted 64-bit fingerprints for *every* target | 22.65 MiB |

The Bazel graph and Go evaluator were run on the full checkout. The protobuf and heap values **model** Tango's representation; no Tango service RPC, content-derived target hashing, or production network transfer was measured. The 2.8 GB raw Bazel output is not checked in because it contains internal target names; the technical report records its SHA-256 and explains the method and remaining limitations.

## Proposed approach

Use Tango's changed-target stream to produce a durable, compact impact signature per batch. Compare signatures at a common pinned base revision rather than transferring the whole monorepo graph into stateless SubmitQueue controllers. Fix or isolate Tango's incomplete compared-target cache key before using it as a conflict oracle. The technical report describes the integration constraints, measurements, and Go-first optimization plan.
