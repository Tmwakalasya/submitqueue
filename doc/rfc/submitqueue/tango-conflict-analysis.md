# Tango-backed conflict analysis for SubmitQueue

**Original analysis:** September 25, 2026. **Complete go-code graph measured:** September 28, 2026. **SubmitQueue base:** `origin/main@6c4b769c` on `research/tango-conflict-graphs`. **Tango inspected:** `uber/tango@50b3695a9909f19b78a3b1b35c095c9a8331d9db`. **go-code checkout measured:** `c5655f3f7e87f`; this checkout is not a fresh copy of go-code main.

## Recommendation

Implement a queue-scoped `conflict.Analyzer` backed by Tango's **`GetChangedTargets`**, not `GetTargetGraph`: compare the canonical labels of the affected targets of each batch, storing a small, durable per-batch impact signature and the baseline revision from which it was computed. Never load a monorepo target graph into a SubmitQueue controller, its queue payload, or its versioned `Batch`. Fetch and retain one signature per batch, then intersect the candidate's signature with each in-flight batch's signature. Initially support queues with canonical GitHub PR URIs and a known, common base SHA; **conservatively serialize** when graph coverage, identity, or base compatibility cannot be established.

This is a proposed design, **not an implementation of the Tango analyzer**. This branch contains an executable evaluator, tests, seven aggregate JSON measurements, the full-query attempts log, and this report. The [root-level report entry](../../../TANGO_CONFLICT_ANALYSIS.md) makes the findings easy to find in the submitqueue repository. No go-code branch or code modifications were necessary.

## What is available today

- SubmitQueue already calls `conflict.Analyzer.Analyze(ctx, batch, inFlight)` from the queue-partitioned dependency-analysis stage. The extension receives thin batch identities and can resolve request URIs through `changeset.Resolver`; its factory routing belongs in `service/submitqueue/orchestrator/server/`. The controller writes reverse indexes, claims requests, and atomically promotes the batch to `Created` with its final dependencies. No new controller-side graph materialization is required. See [`conflict.go`](../../../submitqueue/extension/conflict/conflict.go), [`dependencyanalysis.go`](../../../submitqueue/orchestrator/controller/dependencyanalysis/dependencyanalysis.go), and [`changeset.go`](../../../submitqueue/core/changeset/changeset.go).
- Tango exposes streaming `GetTargetGraph` and `GetChangedTargets`. `GetChangedTargetGraph` is explicitly **unimplemented**. Its `OptimizedTarget` carries integer IDs and packed dependency IDs, with the ID→label mapping in **metadata sent last**. `GetChangedTargets` includes `NEW`/`DELETED`/`CHANGED`, old/new targets, and distance from a direct-change seed. Metadata and IDs are **per response**, not stable between revisions or RPC calls. Compare resolved canonical labels, never unqualified numeric IDs. See Tango's [wire contract](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/proto/tango.proto).
- `OutputConfig{MaxDistance: -1, IncludeHashes: false, IncludeTags: false, IncludeAttributes: false}` retains the full transitive affected-target set while removing optional target detail. **Explicitly set `MaxDistance: -1`: supplying `OutputConfig` with an unset scalar means distance `0`, not unlimited.** Tango strips these fields at send time, so this saves client wire and decode costs but not all server-side comparison/materialization work. The implementation currently builds/caches the full diff before trimming distance, and filtering a diff does not currently prune its ID→name metadata by the distance filter. See Tango's [comparison controller](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/controller/getchangedtargets.go) and [output filtering](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/controller/output_filter.go).
- Tango already caches graphs by tree hash and comparisons by the pair of trees. The optional TGB format stores a compressed columnar graph and compares two cached graphs without fully decoding both; requesting `GetTargetGraph` still has to decode a TGB graph for streaming. Prefer TGB with shadow comparison during a measured rollout; do not invent a parallel graph format in SubmitQueue. See Tango's [TGB reader](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/core/storage/tgbgraph.go) and [TGB comparison path](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/controller/getchangedtargets.go).
- **Cache-key correctness needs work before treating Tango as a conflict oracle.** Tango's current compared-targets key includes repository ID, two tree hashes, and request-specific extra exclusion regexes, but **not the computation strategies or `SeedAttributes` policy**; its graph key has the strategy but not every repository configuration item affecting graph computation. An impact-summary policy version in SubmitQueue cannot repair a stale Tango result. First make the upstream keys include the computation/configuration identity, version/invalidate old cache entries, or use an isolated, immutable repository configuration and bypass incompatible comparison cache entries during rollout. See Tango's [cache-key construction](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/core/cachekey/cachekey.go) and [seed-attribute configuration](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/config/repository_config.go).
- Tango's native change applier currently accepts **GitHub PR URIs only**. SubmitQueue also accepts `phab://` changes and pluggable Git providers. A Phabricator queue needs a deliberately implemented Tango workspace adapter or a verified source-to-GitHub mapping; until then, use the configured conservative analyzer rather than pretend its changed-target set is empty. See Tango's [workspace request parser](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/core/workspace/request.go) and SubmitQueue's [Phabricator IDs](../../../platform/base/change/phabricator/change_id.go).

## Reproducible size experiment

The [`tool/tangograph-eval/`](../../../tool/tangograph-eval/) Go program reads Bazel's length-delimited `--output=streamed_proto` targets, retains the fields Tango maps into its graph, constructs a similarly shaped ID-mapped Go graph, measures **live Go heap after GC**, and encodes the modeled Tango `GetTargetGraphResponse` protobuf messages. It reports payload size without transport framing, measured per-message gzip/best-speed sizes **as an illustrative optional transport choice**, and a sorted eight-byte fingerprint array. It also samples 64 source-file nodes per subtree and compares one-hop with full reverse-dependency closure; these are **synthetic single-file changes**, not observed Tango diffs.

For the measured go-code checkout, Bazel is configured with `--noenable_bzlmod`; Tango's native runner would query `//external:all-targets + deps(//...:all-targets)` for that mode. Both the earlier subtree queries and the successful complete-repository query use Tango's `--order_output=no --proto:locations --noproto:default_values --output=streamed_proto` flags. The Bazel input files remain temporary rather than checking large internal target-name dumps into SubmitQueue. The seven checked-in JSON summaries under [`tango-eval/`](tango-eval/) record the query, checkout SHA, Tango SHA, node and edge counts, modeled protobuf payloads, and measured Go-model heap; the [attempt log](tango-eval/go-code-full-20260928-attempts.tsv) records the three identical full-query runs.

### Complete go-code target graph: observed September 28, 2026

The complete `//external:all-targets + deps(//...:all-targets)` query succeeded on **attempt 3 of a 20-attempt maximum**, with unchanged Bazel arguments and the same go-code commit on every attempt. Attempt 1 encountered an Artifactory HTTP 502 fetching a Go dependency; attempt 2 encountered a different external download stream error. The successful third query ran for **771.81 seconds**; all three attempts together ran from 20:42:54 to 20:58:05 UTC. The result is a **complete successful Bazel query for this checkout**, not a capture of Tango serving an RPC or computing content-derived target hashes. See the [full measurement](tango-eval/go-code-full-20260928.json) and [attempt log](tango-eval/go-code-full-20260928-attempts.tsv).

| Property | Complete result | What it measures |
|---|---:|---|
| Bazel `streamed_proto` input | 2,799,274,578 bytes (2.607 GiB) | Actual complete query output; SHA-256 `99f0cfd8e2541d381ca845dcaac01b4166cb451817ca998202932dbf3009c14f` |
| Target nodes | **2,969,283** | Actual parsed Bazel targets, including 430,553 external nodes (2,538,730 other nodes) |
| Target dependencies | 14,519,552 represented edges | 4,812 additional input references have no returned target and are not represented by the ID-mapped evaluator graph (0.033% of references) |
| Go heap: parsed Bazel-shaped graph | 2,465,486,896 bytes (2.296 GiB) | Live heap after GC; model retains the query fields the evaluator needs |
| Go heap: Tango-shaped ID graph | **1,180,570,080 bytes (1.099 GiB)** | Live heap after GC, including IDs, metadata dictionaries, modeled 40-character hashes, tags, and string attributes |
| Evaluator process peak RSS | 4,613,064 KiB (4.40 GiB) | Measured by `/usr/bin/time -v`; includes temporary allocations, runtime, and all modeling stages |
| Evaluator wall time | 49.73 seconds | Parse, model, encode and gzip the complete graph on this host; excludes the Bazel query |
| Default Tango-style graph protobuf | **396,890,158 bytes (378.50 MiB)** | Modeled uncompressed response, 94 size-bounded messages, including names/dependencies but excluding hashes/tags/attributes |
| All-fields Tango-style graph protobuf | 624,477,967 bytes (595.55 MiB) | Modeled uncompressed response, 147 messages, with synthetic 40-character hashes |
| Default protobuf under optional gzip | 65,922,174 bytes (62.87 MiB) | Measured gzip/best-speed encoding separately for each modeled response message; **not** a Tango RPC transport measurement |
| Sorted 64-bit target-label fingerprints | **23,754,264 bytes (22.65 MiB)** | Eight bytes per node if *all* targets change; a real per-batch impact set normally holds fewer |

The model also reports a hypothetical ID-and-name-only response of 319.71 MiB, **not a currently selectable Tango output mode**. These numbers replace the earlier whole-repo extrapolation as the best available *full-graph* evidence; they still do **not** measure the memory peak of Tango itself, actual content hashes, a real changed-target response, or compressed production traffic. The Bazel query output is not committed because its 2.8 GB payload contains internal labels; its digest, exact invocation, evaluator results, and failure/success provenance are retained here.

### Earlier observed closed subgraphs (September 25, 2026)

| Requested subtree | Raw Bazel stream | Closure nodes (external nodes) | Modeled default Tango wire | Modeled all-fields Tango wire | Modeled default wire with gzip | Live Go ID-graph heap |
|---|---:|---:|---:|---:|---:|---:|
| `devexp/code_merge` | 30.88 MB | 44,233 (38,899) | 3.91 MiB | 6.03 MiB | 0.76 MiB | 12.33 MiB |
| `devexp/buildkite` | 37.98 MB | 56,214 (46,090) | 5.00 MiB | 7.87 MiB | 0.99 MiB | 16.24 MiB |
| `marketplace/fulfillment` | 50.39 MB | 66,809 (43,938) | 6.28 MiB | 10.14 MiB | 1.24 MiB | 21.90 MiB |

`Default` means Tango's current default output fields: numeric ID, direct dependencies, rule type, root/external flags, and name/rule-type metadata. `All-fields` also includes tags, string attributes, and a **synthetic, label-derived 40-character SHA-1** as a stand-in for a target's actual content-derived hash; its *byte length* and incompressibility are modeled, **not the actual hash contents**. These are modeled serialized response bytes, **not captures of a running Tango service**. Both the observed closure and its many shared external targets differ among queries.

### Earlier extrapolated whole-repository range (superseded by the full query)

`git ls-files '*BUILD.bazel'` counted **317,209 tracked BUILD.bazel files** in go-code at the measured commit. The sampled requested subtrees have 84, 156, and 143 BUILD.bazel files respectively. Scaling each subtree's **own nodes and within-subtree edges** to that tracked-file count yields the following *scenario range*, **not a measurement or statistical confidence interval**:

| Extrapolated main-repository graph only | fulfillment sample | code_merge sample | buildkite sample |
|---|---:|---:|---:|
| Targets | 1.82 million | 1.95 million | 3.70 million |
| Default Tango protobuf, uncompressed | 203 MiB | 199 MiB | 386 MiB |
| Modeled all-fields Tango protobuf | 322 MiB | 351 MiB | 630 MiB |
| Illustrative gzip of default payload | 31 MiB | 34 MiB | 63 MiB |
| Live Go ID-mapped graph heap | 683 MiB | 846 MiB | 1,402 MiB |
| Packed, sorted 64-bit target-label fingerprints | 13.9 MiB | 14.9 MiB | 28.2 MiB |

**The table above is preserved as a record of the original estimate, not as current measured data.** Its subset-only projection omitted other main-repo subtrees, shared external targets, metadata, and edges. For example, of buildkite's 7,453 outgoing edges, 2,204 stay in the sample, 676 point to other main-repo targets, and 4,573 point to external targets. The complete successful query measured **2.97 million** nodes, **378.50 MiB** of modeled default protobuf, and **1.099 GiB** of modeled compact-graph Go heap: within the earlier broad scenarios, but based on the actual complete graph rather than linear scaling. Neither set of numbers is Tango server peak memory or an observed `GetChangedTargets` response.

The first broad devexp query on September 25 stopped on an external-module HTTP 502. The complete go-code query succeeded on September 28 after retries, so a **full Bazel graph and evaluator process RSS** are now measured. No Tango service call, real change pair, actual production wire compression, or Tango server RSS benchmark was performed.

### Conflict-policy sensitivity

For each **closed** subgraph, the evaluator selected 64 evenly distributed source-file nodes *inside the requested subtree* (2,016 pairs), followed reverse dependencies in the returned graph, and tested whether two synthetic single-file changes would share an affected target:

| Source subtree | Full closure: pairs overlapping | Distance ≤1: pairs overlapping | Median / 95th percentile full closure |
|---|---:|---:|---:|
| code_merge | 141 / 2,016 (7.0%) | 4 / 2,016 (0.2%) | 3 / 20 targets |
| buildkite | 225 / 2,016 (11.2%) | 36 / 2,016 (1.8%) | 4 / 68 targets |
| fulfillment | 171 / 2,016 (8.5%) | 4 / 2,016 (0.2%) | 2 / 84 targets |

Across the **complete graph**, 64 evenly spaced synthetic seeds sampled from 1,533,785 main-repo source-file nodes yielded 4 / 2,016 pairs with overlapping full closures and 0 / 2,016 with one-hop overlap (full-closure median 2, 95th percentile 225, maximum 1,615 targets). That whole-repo pair rate is lower because it compares files from widely separated parts of the monorepo; it does **not** negate the higher local overlap rates above or estimate the production PR mix.

This is a **structural sensitivity test**, not a conflict accuracy measurement: actual changed targets depend on changed file content and Tango's hashes. Capping to one hop changes the safety contract. For example, two different source files in two different libraries can both affect one integration test at distance two; one hop says they do not overlap even though their combined change has never been built. Use full closure for a conservative first implementation. An owner/one-hop mode belongs behind an explicit, measured policy decision or the separately designed controller-owned dependency relaxation, **not** behind a purportedly lossless compression switch. Even full closure is only conservative if Tango observes every build-affecting change; configure global build files, source hashing, and exclusions accordingly.

## Proposed SubmitQueue design

1. Add `submitqueue/orchestrator/extension/conflict/tango/` implementing the **existing shared** `submitqueue/extension/conflict.Analyzer`; the implementation is service-scoped because only the orchestrator resolves it. Inject a `changeset.Resolver`, an interface for Tango's streaming client, a queue-specific VCS/base-revision resolver, and a small key-oriented impact store at construction. Route each queue in `service/submitqueue/orchestrator/server/`, not in an extension factory. Do not expand controller inputs to include changes or graphs.
2. Resolve the candidate's and each in-flight batch's pinned request URIs, canonical repository remote, target branch **base SHA**, computation strategy, and analysis policy. Build Tango `first_revision={remote, base_sha, strategy: COMPUTATION_STRATEGY_UNSET}` and `second_revision={remote, base_sha, strategy: COMPUTATION_STRATEGY_UNSET, requests:[the pinned URIs in batch order]}` (or set `NATIVE` explicitly); the protobuf's zero-value `INVALID` strategy is not a valid default. Validate the same remote/base/policy for every compared signature, and recheck the relevant queue/branch version before promotion: the target branch can advance during a slow Tango call even though dependency messages are queue-partitioned. That recheck cannot be atomic with an external VCS update; continue relying on the landing service's final merge precondition rather than mixing incompatible signatures. Initial native Tango support is limited to compatible GitHub PR URIs; Git/Phabricator need an explicit adapter or conservative fallback. Verify Tango's application semantics against the queue's actual merge strategy before rolling out.
3. Make **at most one** `GetChangedTargets` request per missing batch signature, regardless of the number of in-flight peers; set `MaxDistance=-1` and omit hashes/tags/attributes. Buffer only the IDs of actual changed old/new targets until all metadata arrives, resolve their canonical labels, sort/dedupe, and form the impact signature. Do **not** include an unchanged target merely because its name appears in `direct_dependencies` metadata. Treat unknown mappings, premature EOF, cancellation, or a partial response as failed analysis, not an empty result.
4. Persist an **immutable, keyed summary** by `(queue, batch ID, base tree/revision, graph strategy, impact-policy version)`, with a small batch-keyed reference if retrieval needs one. Store compact sorted label fingerprints and a count/checksum or exact canonical labels as appropriate; a stable 64-bit hash collision can only cause a **false positive** overlap, not a false negative, provided the same canonicalization and hash version are used everywhere. Fingerprints need no shared mutable in-process dictionary; Tango response IDs are ephemeral. Use a simple two-pointer sorted-set intersection; avoid `map[string]Target` and `map[uint64]struct{}` for persisted signatures. Keep large payloads out of `entity.Batch`, request logs, and queue messages.
5. Create or retrieve the signature **before** the existing `Creating → Created` promotion and before the speculate notification; this follows the controller's existing persist-before-publish and retry-idempotency ordering. Creating records are not eligible as in-flight dependencies. On duplicate delivery, reuse a complete matching signature; never mark an incomplete one ready. Keep immutable blob creation and the versioned batch write as separate, retryable operations rather than a cross-entity transaction.
6. A newly advanced base, changed analysis policy, unsupported provider, or untracked build-affecting input invalidates comparability: recompute every relevant in-flight signature against one common base, or conservatively report those batches as conflicting until rebaselining succeeds. **Never** compare an old signature against a new revision merely because their target names happen to match. Return retryable failures through the existing error-classifier path; after a deliberate retry/availability policy, falling back to `all` preserves safety but reduces parallelism. Falling back to `none` does not.
7. Repair or isolate Tango's incomplete compared-targets cache key before enabling decisions, then shadow the proposed analyzer against `all`/`pathoverlap` and record response bytes, changed-target cardinality, fan-out, metadata bytes, base mismatch/rebase rate, Tango cache hits and latency, analyzer heap/RSS, conflict pairs, and conservative fallbacks. Test additions, deletions, renames, global BUILD/toolchain changes, two changes touching one target via different files, branch movement, and failed/partial streams before enabling it for GitHub queues.

## Where optimization belongs

- **In memory:** The cheapest graph is the graph SubmitQueue never materializes. Let Tango own cached graphs/TGB and stream a **per-batch affected-label signature** to SubmitQueue. A sorted `[]uint64` is eight bytes per affected target, with no Go map buckets or per-target pointers, and scales with **changed targets**, not every target in the repository. A counting Bloom filter can reject definitely disjoint sets, but positive matches still need the exact set; if a fingerprint collision is possible, conservatively treat it as an overlap. Roaring bitmaps are attractive only for IDs within one **stable, pinned** graph dictionary: raw Tango IDs cannot be compared across separate responses. For a huge diff, bound decoder buffers and optionally spill the ID list; do not assume "usually small" is a memory limit.
- **On the existing wire:** Use `GetChangedTargets` plus `OutputConfig` to drop hashes/tags/attributes and avoid `GetTargetGraph` altogether; no result graph belongs on an internal SubmitQueue topic. Tango's metadata-last ordering means the client must hold IDs until it can resolve them. Any optional gzip/zstd transport compression must be **negotiated and benchmarked**; the evaluator's gzip numbers are a possible bound on example payloads, not observed production throughput.
- **If Tango must evolve:** Add a **versioned conflict-impact API** returning only canonical affected labels or stable fingerprints and a base/policy identity, optionally metadata-first. Filter and prune its response *before* constructing all old/new target protos, allow bounded streaming/materialization, and cache by the complete revision pair **and policy**; never let a partial/filtered cache entry masquerade as a full diff. For very large changes, consider dictionary/prefix encoding for labels, partitioned chunks, and supported transport compression. Keep the existing `GetChangedTargets` contract unchanged for other clients.
- **Go versus Rust:** Implement the SubmitQueue adapter, sorted signatures, and wire measurements in **Go** first. Tango's existing Go TGB already offers compressed columnar storage and a native diff path, while the measured SubmitQueue-facing signature is small. Rust or a separate native process only merits investigation if a **full-repo Tango benchmark** shows graph decoding/diffing dominates CPU or memory after these changes; cgo/FFI plus serialization may add copies and deployment complexity without fixing metadata-last or per-request graph transfer. If justified, isolate a columnar codec/differ inside Tango with identical golden tests, not Rust state inside stateless SubmitQueue controllers.

## How to rerun

Run the **complete** query from `~/go-code` against a known checked-out revision (this checkout disables Bzlmod). On download failures, retry the **identical** command up to 20 times; do not treat a nonzero exit or a partial stream as a complete graph:

```sh
bazel query --order_output=no --proto:locations --noproto:default_values --output=streamed_proto \
  '//external:all-targets + deps(//...:all-targets)' \
  > /tmp/sq-go-code-full-20260928.streamed_proto
```

Run the Go evaluator from this SubmitQueue checkout without a subtree filter:

```sh
go run ./tool/tangograph-eval \
  -input /tmp/sq-go-code-full-20260928.streamed_proto \
  -query '//external:all-targets + deps(//...:all-targets)' \
  -go-code-revision c5655f3f7e87f \
  -tango-revision 50b3695a9909f19b78a3b1b35c095c9a8331d9db
go test ./tool/tangograph-eval
```

The **earlier subtree queries**, useful for comparing local and whole-repo behavior, can still be reproduced from `~/go-code`:

```sh
bazel query --order_output=no --proto:locations --noproto:default_values --output=streamed_proto \
  '//external:all-targets + deps(//src/code.uber.internal/devexp/code_merge/...:all-targets)' \
  > /tmp/sq-code-merge-tango.streamed_proto
```

Run the earlier subtree projection from this SubmitQueue branch, supplying the **counts corresponding to the actual snapshot**:

```sh
go run ./tool/tangograph-eval -input /tmp/sq-code-merge-tango.streamed_proto \
  -scope '//src/code.uber.internal/devexp/code_merge/' \
  -impact-source-prefix '//src/code.uber.internal/devexp/code_merge/' \
  -sample-build-files 84 -total-build-files 317209
go test ./tool/tangograph-eval
```

For the other earlier samples, substitute `//src/code.uber.internal/devexp/buildkite/` (156 BUILD files) or `//src/code.uber.internal/marketplace/fulfillment/` (143 BUILD files). Omit `-scope` and the BUILD-file counts to measure the closed subgraph returned by Bazel. Counts can be rechecked with `git ls-files '*BUILD.bazel'` and the corresponding subtree pathspec. Input `-` accepts a pipe, but an input file lets the same raw query be analyzed more than once. The evaluator intentionally commits **aggregate JSON and retry metadata only**: raw build-proto dumps are bulky and contain internal target names.

## Source of truth and limitations

The Tango source and schema cited above are pinned to the commit inspected, not hypothetical future endpoints. The output-configuration and TGB properties are from its implementation, and the Bazel query shape is from Tango's [native graph runner](https://github.com/uber/tango/blob/50b3695a9909f19b78a3b1b35c095c9a8331d9db/graphrunner/native.go). The successful complete-query and subtree summaries contain actual Bazel-query bytes and instrumented Go-model heap/protobuf sizes; the earlier projections, synthetic impacts, hash contents, compression in a real Tango deployment, and the final queue policy still need validation against a running Tango server and a representative **full-repository change workload**.
