# Tango conflict analysis for SubmitQueue

The [full technical report](doc/rfc/submitqueue/tango-conflict-analysis.md), [Go evaluator](tool/tangograph-eval/), and [aggregate measurements](doc/rfc/submitqueue/tango-eval/) live on branch `research/tango-conflict-graphs`. This summary has **four tables** for one new batch checked against all existing batches; prior persistent-posting experiments remain in the technical report and are **not** these all-batch cold scans.

## Scope and three designs

A complete Bazel query of go-code at commit `c5655f3f7e87f` produced **2,969,283** target nodes (including externals). The [full-graph measurement](doc/rfc/submitqueue/tango-eval/go-code-full-20260928.json) and [three-attempt retry log](doc/rfc/submitqueue/tango-eval/go-code-full-20260928-attempts.tsv) document the successful September 28, 2026 query. Separately, the [Hive analysis](doc/rfc/submitqueue/tango-eval/hive-go-diff-targets-20260930.sql) of September 1–29 go-code single-diff requests yielded a mean affected-target **proxy** of **`N=2,527`** per diff; benchmark scenarios set affected targets **per batch and candidate** to `K=N`, `2N=5,054`, and `5N=12,635`. `B=100`, `500`, or `1,000` is the number of in-flight batches. Neither `K` nor the simultaneously in-flight `B` values are measured production batch distributions.

- **ID64 — Registered-ID Signature Sweep:** each batch has sorted collision-checked 64-bit target IDs in one blob; a separate persistent, sparse ID→canonical-name dictionary restores labels. The controller loads **only the B signature blobs**, not the dictionary, to check conflicts when candidate IDs are already registered.
- **NameKey — Canonical-Name Signature Sweep:** each batch has one blob of sorted, length-prefixed canonical target-name strings; no ID dictionary.
- **TangoSnapshot — OptimizedGraph-Like Snapshot Sweep:** each batch has modeled default-field optimized targets plus response-local names/dependency metadata for its **K affected targets**, either raw protobuf or optional gzip. This is **not** a captured Tango `GetChangedTargets` response or the full 2.97-million-target monorepo graph; actual old/new target detail may be larger.

All figures are from the [nine-case Go serialize/decode/scan benchmark](doc/rfc/submitqueue/tango-eval/go-code-load-benchmark-20260930.json) and [four-stage calculation](doc/rfc/submitqueue/tango-eval/four-stage-estimates-20260930.json). Target labels and dependency degrees come from the full Bazel graph; changed sets and TangoSnapshot dependency neighbors are deterministic synthetic samples. The actual per-batch bytes are encoded and decoded by Go, but no blob service or Tango RPC was run.

## 1. Serialized size on disk for all in-flight batches

**MiB; minimum application payload for `B` signatures, including ID64's necessary sparse dictionary.** ID64 dictionary size is the exact sum over unique labels across `B` batches **and the incoming candidate** of `8-byte ID + unsigned-varint label length + UTF-8 label bytes`. It is persisted once for the queue/repository ID version rather than re-fetched for each check. ID64 total is blobs plus this dictionary; displayed components are rounded independently. NameKey stores each full name in every batch. TangoSnapshot values include per-message framing and response-local metadata; gzip is a separately modeled on-disk codec option. **ID64 and NameKey are uncompressed here**; compressing either would change the comparison and needs its own measurement.

| In-flight `B` | Targets/batch `K` | ID64 blobs | ID64 registry | ID64 total | NameKey | TangoSnapshot raw | TangoSnapshot gzip |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 2,527 | 1.9 | 25.5 | 27.5 | 24.6 | 159.6 | 57.1 |
| 100 | 5,054 | 3.9 | 48.6 | 52.4 | 49.2 | 321.2 | 114.7 |
| 100 | 12,635 | 9.6 | 105.5 | 115.2 | 123.1 | 800.9 | 286.0 |
| 500 | 2,527 | 9.6 | 104.8 | 114.4 | 123.0 | 798.1 | 285.7 |
| 500 | 5,054 | 19.3 | 168.4 | 187.7 | 246.1 | 1,604.9 | 573.1 |
| 500 | 12,635 | 48.2 | 244.7 | 292.9 | 615.4 | 4,004.6 | 1,430.1 |
| 1,000 | 2,527 | 19.3 | 168.3 | 187.6 | 246.1 | 1,596.7 | 571.5 |
| 1,000 | 5,054 | 38.6 | 230.4 | 269.0 | 492.3 | 3,209.8 | 1,146.3 |
| 1,000 | 12,635 | 96.4 | 264.8 | 361.2 | 1,230.7 | 8,006.2 | 2,859.2 |

These are **serialized payload sizes, not billed physical storage**. Real KV/SQL row indexes, blob keys/headers, base/policy epoch metadata, checksums, previously registered names, and an optionally persisted incoming candidate add space. Required common `Batch`-table records are excluded for every design. ID64's dictionary makes its disk advantage smaller than its eight-byte per-batch blobs suggest: at `B=100, K=N` its minimal **27.5 MiB** exceeds NameKey's **24.6 MiB**.

## 2. Go heap if all in-flight blobs are loaded

**Incremental live heap after GC, in MiB, for `B` *actually decoded* batch signatures plus the candidate's comparison set in one stateless Go process.** ID64's durable name dictionary is not loaded. These are **not** peak RSS or aggregate memory across concurrent controllers.

| In-flight `B` | Targets/batch `K` | ID64 | NameKey | TangoSnapshot |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 2,527 | 2.0 | 30.3 | 263.5 |
| 100 | 5,054 | 3.9 | 60.7 | 527.4 |
| 100 | 12,635 | 10.3 | 151.7 | 1,250.9 |
| 500 | 2,527 | 9.8 | 150.5 | 1,314.6 |
| 500 | 5,054 | 19.6 | 301.1 | 2,628.0 |
| 500 | 12,635 | 50.9 | 752.8 | 6,253.0 |
| 1,000 | 2,527 | 19.6 | 300.8 | 2,631.1 |
| 1,000 | 5,054 | 39.1 | 601.6 | 5,258.4 |
| 1,000 | 12,635 | 101.7 | 1,504.0 | 12,502.4 |

The benchmark excluded the shared 2.8-GB Bazel input/label pool, serialized network buffers, `Batch` entities, candidate Tango response, and allocator peaks. The complete evaluator reached **24,001,380 KiB peak RSS** while running all cases; that is neither one controller's steady heap nor the Tango service's memory.

## 3. Latency to fetch blobs and deserialize into Go

**Seconds; only the `B` separately stored blobs, not `Batch`-table hydration, Tango candidate computation, ID registration, or conflict checking.** A hypothetical S3 Standard-like store supplies **150 ms per GET to first byte**, **32 parallel GETs**, and an assumed **100 MiB/s effective aggregate download rate per controller**. AWS documents roughly 100–200 ms first-byte latency for small S3 objects and recommends parallel GETs; the worker count and bandwidth are planning assumptions, **not** measured or guaranteed performance of any chosen SubmitQueue backend. See the [AWS first-byte guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html) and [parallel request guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html).

`T_load = ceil(B/32) × 150 ms + batch-blob MiB / (100 MiB/s) + measured Go deserialization CPU for B blobs`. `GET wave` displays only the first term. The **TangoSnapshot gzip** column includes measured Go gzip decompression **and** protobuf parsing; raw includes protobuf parsing. ID64 dictionary bytes appear in disk size but are not downloaded for an **IDs-ready** check.

| In-flight `B` | Targets/batch `K` | GET wave | ID64 | NameKey | TangoSnapshot raw | TangoSnapshot gzip |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 2,527 | 0.60 | 0.62 | 0.86 | 2.49 | 2.64 |
| 100 | 5,054 | 0.60 | 0.64 | 1.13 | 4.42 | 4.63 |
| 100 | 12,635 | 0.60 | 0.70 | 1.92 | 10.15 | 10.73 |
| 500 | 2,527 | 2.40 | 2.50 | 3.73 | 11.85 | 12.32 |
| 500 | 5,054 | 2.40 | 2.60 | 5.04 | 21.45 | 22.39 |
| 500 | 12,635 | 2.40 | 2.90 | 9.06 | 50.28 | 52.74 |
| 1,000 | 2,527 | 4.80 | 5.00 | 7.46 | 23.69 | 24.49 |
| 1,000 | 5,054 | 4.80 | 5.20 | 10.13 | 42.73 | 44.37 |
| 1,000 | 12,635 | 4.80 | 5.80 | 18.30 | 99.96 | 104.04 |

The additive model does not overlap GETs, transfer and decode; real controllers may pipeline/parallelize them, at additional memory/concurrency cost. Codec CPU matters: at `B=1,000, K=5N`, TangoSnapshot transfers much less under gzip but took **70.65 s** to decompress and parse, versus **15.10 s** to parse raw protobuf locally, yielding **104.04 s gzip vs 99.96 s raw** in this model. These are neither measured blob p50/p99 nor admission SLOs; there may be retries, throttling, connection limits or additional metadata/decode work.

## 4. Latency to check one candidate against decoded in-flight batches

**Milliseconds of measured local Go CPU for one new batch's already canonicalized/sorted candidate set against all `B` already-decoded batches; no blob reads, disk transfer, Tango computation, candidate sorting, ID registration or durable posting writes.** TangoSnapshot's check is identical whether its stored blobs were raw or gzip; both decode into the same Go objects.

| In-flight `B` | Targets/batch `K` | Matched batches | ID64 | NameKey | TangoSnapshot |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 2,527 | 88 | 0.896 | 4.398 | 16.465 |
| 100 | 5,054 | 100 | 0.300 | 1.574 | 11.558 |
| 100 | 12,635 | 100 | 0.170 | 0.635 | 3.038 |
| 500 | 2,527 | 455 | 4.921 | 26.784 | 96.470 |
| 500 | 5,054 | 500 | 2.729 | 16.966 | 61.445 |
| 500 | 12,635 | 500 | 1.223 | 4.286 | 37.394 |
| 1,000 | 2,527 | 923 | 8.729 | 63.377 | 188.569 |
| 1,000 | 5,054 | 1,000 | 5.781 | 33.733 | 130.911 |
| 1,000 | 12,635 | 1,000 | 2.283 | 11.648 | 67.634 |

The synthetic labels are uniformly scattered, so at `K=2N` and `5N` **all** batches match. A match allows the loop to stop early *within that batch*, making some larger-`K` checks faster; this is not evidence that broad real changes are cheaper or that the modeled match rate predicts production. The result list can still contain `B` dependencies. The dominant cost in these scenarios is bringing and decoding B blobs, not this in-memory check.

## Reproduction and limits

From this SubmitQueue checkout, with the **previously captured complete** Bazel stream at the measured go-code commit, run `GOMEMLIMIT=28GiB go run ./tool/tangograph-eval -benchmark-load -input /tmp/sq-go-code-full-20260928.streamed_proto -go-code-revision c5655f3f7e87f -benchmark-batches 100,500,1000 -benchmark-targets 2527,5054,12635`. The [benchmark JSON](doc/rfc/submitqueue/tango-eval/go-code-load-benchmark-20260930.json), [calculation JSON](doc/rfc/submitqueue/tango-eval/four-stage-estimates-20260930.json), and [whole-process timing log](doc/rfc/submitqueue/tango-eval/go-code-load-20260930-timing.txt) retain all inputs and aggregate outputs without committing raw target names. The full technical report documents Tango's metadata-last response, epoch compatibility, sparse registry collision handling, stale-posting safety, and why a **persistent target→batch posting index** would avoid the B blob downloads under a different storage design.
