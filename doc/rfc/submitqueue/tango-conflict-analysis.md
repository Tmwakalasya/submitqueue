# Tango-backed conflict analysis for SubmitQueue

**Original analysis:** September 25, 2026. **Complete go-code graph measured:** September 28, 2026. **In-flight scaling clarified:** September 29, 2026. **SubmitQueue base:** `origin/main@6c4b769c` on `research/tango-conflict-graphs`. **Tango inspected:** `uber/tango@50b3695a9909f19b78a3b1b35c095c9a8331d9db`. **go-code checkout measured:** `c5655f3f7e87f`; this checkout is not a fresh copy of go-code main.

## Recommendation

Implement a queue-scoped `conflict.Analyzer` backed by Tango's **`GetChangedTargets`**, not `GetTargetGraph`: compare canonical labels of the affected targets, storing a durable per-batch impact signature and the baseline revision from which it was computed. Never load a monorepo target graph into a SubmitQueue controller or queue payload. **Do not fetch every in-flight batch's full signature on each admission**: the previous proposal left that `O(inFlight)` storage-read cost unresolved. For thousands of in-flight batches, use a first-class, durable inverted mapping from registered 64-bit target IDs to batch IDs; consult only postings for the candidate's target IDs, verify the returned batches' authoritative state, and fail closed if index completeness cannot be proved. A bounded exact hint in each already-hydrated `Batch` is an interim option, not a substitute for fixing the controller's existing `O(inFlight)` reads when stricter scale is needed. Initially support queues with canonical GitHub PR URIs and a known, common base SHA; **conservatively serialize** when graph coverage, identity, or base compatibility cannot be established.

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
| Sorted 64-bit target-label fingerprints | **23,754,264 bytes (22.65 MiB)** | Eight raw bytes per node if *all* targets change; excludes the proposed sparse name registry and posting stores |

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

## Cross-checking thousands of in-flight batches

The concern is real: **streaming one stored signature at a time bounds Go heap but not storage reads or bytes transferred**. For 3,000 in-flight batches averaging 10,000 affected targets each, fetching every eight-byte-per-target signature transfers roughly **229 MiB per new batch**, plus 3,000 point reads. If every batch affected the measured entire graph, the signatures alone would total about **66 GiB**. Neither is a viable default hot path, and the full-graph measurements do not tell us the distribution of *actual* PR impact-set sizes.

The controller already pays a separate `O(B)` cost before invoking the analyzer: [`core/batch.ListByStates`](../../../submitqueue/orchestrator/core/batch/list.go) lists the relevant state memberships, then hydrates **every** candidate `Batch` with a per-key `BatchStore.Get`, at bounded concurrency 16. Passing those `B` batches into `Analyze` does **not** load their impact signatures today. Adding one signature read per batch would make this pre-existing cost materially worse. An inverted index removes those **extra signature reads** with the current method signature, but cannot make the *whole admission path* sublinear until the controller stops hydrating all `B` batches first.

| Cross-check with `B` in-flight batches and `K` candidate targets | Additional reads beyond the current batch hydration | Read payload or limiting factor |
|---|---:|---|
| Fetch every stored full signature | `B` | Eight bytes times the sum of all in-flight impact-set cardinalities, plus serialization |
| Complete, capped signatures in `Batch` | Zero | At most `B × cap × 8` extra raw bytes on the batch rows already read; broad batches must be conservative or checked separately |
| Target→batch inverted postings | `K` posting reads with the current API; after removing the pre-scan, also hydrate matched batches | Sum of the candidate targets' posting lengths; the current controller still does `B` hydration reads until its API changes |

### Canonical 64-bit target IDs and name restoration

A 64-bit number alone **cannot reversibly encode arbitrary target names**. Use one append-only, repository-scoped `TargetNameByID` mapping store keyed by `(repositoryID, hashVersion, uint64 ID)` and valued by the exact canonical Bazel label. This is an **on-demand dictionary of affected labels**, not a copy of all 2.97 million graph targets and not an in-process map. Its `Get` restores a name with one primary-key read. The `Epoch` in posting/impact keys still identifies the common base and analysis policy; it is **not** part of the target-name hash, so the same named target can retain its ID across epochs. Tango's per-response `int32` IDs are resolved through its ID→name metadata before registration, never persisted as canonical IDs.

1. Frame the byte input unambiguously as `"submitqueue-target-id/v1" || uvarint(len(repositoryID)) || repositoryID || uvarint(len(canonicalLabel)) || canonicalLabel || bigEndianUint32(probe)`. Use Tango's resolved **full label bytes**, including its external-repository prefix; do not case-fold, strip `@@repo`, or incorporate Tango's *content* hash.
2. Starting with `probe = 0`, calculate `id = bigEndianUint64(SHA256(input)[0:8])`; reserve `id = 0` as an invalid sentinel and try the next probe when it occurs. The hash version is part of the mapping key and the framed domain separator.
3. Read `(repositoryID, version, id)`. If the row has the **same** label, reuse its ID. If it has a different label, increment the probe and retry. If absent, conditionally `Create` the immutable `(id, label)` row; a concurrent create that loses retries the read/compare. This makes real 64-bit collisions distinct instead of accepting even a rare false conflict. Creation uses only one key at a time; no secondary index or cross-key transaction is required.
4. Persist the mapping row **before** any posting or `Created` batch that references the ID. To restore a name later, `Get(repositoryID, version, id)` and return its label; a missing row is corruption/incomplete state and fails closed. Do not expire a row while an impact record or posting can still refer to its ID.

Registration order affects which of two *colliding* labels gets the first candidate number; the **shared durable mapping makes the result canonical once assigned**, not a pure stateless function of the label. If an independently reproducible ID with no mapping store at all is mandatory, it cannot simultaneously guarantee unique 64-bit IDs *and* exact name restoration for arbitrary labels. For a change with an unmanageably large affected set, choose the explicit **broad** mode before registering millions of names, preserving a sparse dictionary and conservative conflict behavior. The Go evaluator's present FNV-1a `uint64` values model eight-byte storage only: production ID registration, SHA-256 probing, and the mapping store are **not implemented** here.

### Bounded interim path without an index

The already-hydrated `Batch` can carry a **small, complete** inline fingerprint set for batches whose actual impact fits an empirically chosen size cap (for example, at most 256 fingerprints = at most 2 KiB of raw IDs per batch). The candidate intersects its own sorted set with each inline set in memory, without any further store call. With 3,000 capped batches, the maximum additional raw fingerprint payload is about **5.9 MiB**, paid on the existing batch hydration rather than in 3,000 separate reads; serialized rows add overhead. A batch over the cap must carry an explicit **broad/unknown** marker, never a truncated set masquerading as exact: conservatively mark it conflicting with the candidate, or load its exact external signature only if measured false positives justify an extra read. A broad candidate depends on all in-flight batches. Store the marker/set with the batch's complete versioned snapshot before it becomes `Created`; adding the field entails entity and backend-schema changes. This preserves correctness and bounds resources but does **not** remove the controller's `B` batch reads or preserve high parallelism when many batches are broad.

### Indexed path when admission must scale beyond `B` reads

Build a **first-class mapping store** whose primary key is `(queue, graph/base epoch, registered target ID)` and whose value is an idempotent posting set of batch IDs; keep the immutable per-batch full signature keyed by `(queue, epoch, batchID)` as a rebuild source. This follows the repository's [key-value contract](../../../submitqueue/extension/storage/README.md#key-value-contract): the reverse lookup is a named mapping, **not** a hidden SQL secondary index or a multi-key-query requirement. For a candidate with `K` registered target IDs, perform `K` point reads with bounded concurrency, union the returned IDs, and verify their authoritative state and epoch. Under the **current** API, look IDs up in the already-hydrated `inFlight` batches; after removing that initial full scan, hydrate only returned IDs by key. Also read a separately keyed posting for broad batches. The name store resolves 64-bit hash collisions to **different** target IDs before posting; an unavailable or inconsistent registration is a fail-closed error, not an ambiguous match. The index-lookup cost becomes `O(K + returned posting entries)` rather than `O(B × stored-signature size)`, though inserting a newly admitted batch costs `O(K)` conditional posting writes. `K` and posting fan-out must be measured on real changes before choosing caps.

The index's **negative result is trustworthy only if every eligible batch is indexed**. For a new `Creating` batch, resolve its signature, idempotently add *all* target postings (or its broad posting), and only then perform the CAS promotion to `Created`; a crash part-way through leaves an ineligible Creating batch and harmless extra postings, repaired by redelivery. Any posting found later is checked against the current `Batch.State` so stale entries for terminal batches are false positives, not incorrect dependencies. Keep writes key-local with controller-computed old/new versions, retries, and no cross-entity transaction. The repository's [storage read-after-write contract](../../../submitqueue/extension/storage/README.md#read-after-write-consistency), queue-partitioned dependency analysis, and durable completion-before-promotion ordering are required for an absent posting to mean "no eligible batch"; an eventually stale posting read breaks that proof. During index rollout or an epoch rebuild, first backfill and verify **every** already-eligible batch, publish an explicit ready generation, and fail closed (or use the exhaustive path) until the generation is complete. An in-memory cache or a per-batch "indexed" flag by itself cannot prove multi-key posting completeness.

Hot targets may produce a posting list containing nearly every batch and a high-contention row. Return size then has a fundamental `Ω(B)` lower bound because the resulting dependency list itself contains `B` IDs; reading a bounded batch list or conservatively marking all as dependent is appropriate. Partition/compact posting lists only if measured hot-key latency requires it, without letting a partial partition look complete. Terminal postings can be removed *after* the authoritative batch state is terminal, using the durable full signature to issue idempotent per-key removals: a crash merely leaves stale positives, which state verification filters until a reconciler finishes cleanup. Epoch expiry is safe only after no active batch depends on that epoch. A candidate whose impact set would require millions of reads/writes should explicitly take the conservative **depends-on-all** path, not silently cap the set. For a sparse candidate, avoiding the controller's up-front [`ListByStates`](../../../submitqueue/orchestrator/core/batch/list.go) call requires a separate, explicit analyzer/controller contract change so the index supplies candidate IDs and only matches are hydrated; the current `Analyze(ctx, batch, inFlight)` contract still forces that initial `O(B)` enumeration.

### In-flight benchmark for 100, 500, and 1000 batches

The [benchmark code](../../../tool/tangograph-eval/bench.go) sampled **actual canonical main-repo labels** from the successful complete go-code Bazel query (`2,538,730` eligible labels, mean length `101.0` bytes). It generated deterministic synthetic batches with either `K=100` or `K=1,000` unique affected labels each, sampled uniformly from that pool, and used the same candidate and matches for all three representations. Dependency degree came from the query (capped at 32; resulting mean `4.97`). These are **not observed PR diffs or measured Tango RPC responses**: real changes cluster in the graph and may have different overlap and broad-target rates. The [measured results and modeled latency](tango-eval/go-code-impact-benchmark-20260930.json) and a [repeat run](tango-eval/go-code-impact-benchmark-20260930-repeat.json) contain aggregate data only, no internal target names.

The three benchmark representations are (1) sorted `[]uint64` per batch plus an in-memory stand-in for the **sparse ID→label registry and durable ID→batch postings**, including collision-checking SHA-256 ID registration; (2) complete sorted label-string sets plus **label-string→batch postings**, with no numeric registration; (3) one default-field `OptimizedTarget` per affected name plus per-response `Metadata.TargetIDMapping` containing changed targets **and direct dependencies**, scanned batch by batch. The Tango-like representation uses response-local `int32` IDs, omits hashes/tags/attributes according to the default `OutputConfig`, and models `GetTargetGraphResponse` framing; a real `GetChangedTargets` result can contain **both old and new targets**, making these Tango-like heap and wire numbers a lower-fidelity, potentially optimistic proxy. String payloads in the simulated per-batch metadata are separately allocated; indexed modes retain complete per-batch sets as rebuild sources.

| In flight `B` | Targets/batch `K` | Matches | ID index + registry live heap | Label-string index live heap | Tango-like snapshots live heap | Tango-like protobuf (gzip) |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 0 | 2.2 MiB | 2.0 MiB | 10.0 MiB | 6.4 MiB (2.3 MiB) |
| 100 | 1,000 | 40 | 20.1 MiB | 18.7 MiB | 94.3 MiB | 63.1 MiB (22.6 MiB) |
| 500 | 100 | 2 | 10.1 MiB | 9.4 MiB | 49.6 MiB | 31.6 MiB (11.5 MiB) |
| 500 | 1,000 | 161 | 100.1 MiB | 96.1 MiB | 472.2 MiB | 316.6 MiB (113.2 MiB) |
| 1,000 | 100 | 4 | 20.1 MiB | 18.8 MiB | 99.0 MiB | 63.0 MiB (23.0 MiB) |
| 1,000 | 1,000 | 317 | 166.9 MiB | 173.8 MiB | 944.4 MiB | 633.3 MiB (226.5 MiB) |

These **live Go heap** values are after GC, subtract the shared input-label pool and workload indices, and include the ID registry or label index as appropriate; the raw per-batch ID arrays alone are only `8 × B × K` bytes. At `B=1,000, K=1,000` that is **8.0 MB raw**, yet the ID representation uses **166.9 MiB** of Go heap because the sparse registry holds ~827,000 distinct label rows and postings use Go maps/slices. The label-string index is **173.8 MiB**, nearly the same despite labels averaging 101 bytes: **the inverted index changes the asymptotic lookup cost; merely choosing 64-bit IDs does not eliminate registry or index overhead**. The full Tango-like snapshots are much larger because per-batch metadata repeats target and dependency names. Storing a **complete 2.97-million-target graph per batch** would instead imply approximately **37/185/370 GiB** of default graph protobuf and **110/550/1,099 GiB** of modeled live ID-graph Go heap for 100/500/1,000 batches; that is arithmetic from the earlier full-graph measurement, **not an allocated benchmark case**.

**These heap totals are not per-controller RSS requirements.** The benchmark deliberately keeps *all* `B` synthetic impact records, posting lists and ID registry (or all Tango-like snapshots) in one Go process to compare resident representations; a deployed index and registry can live in persistent storage. A stateless indexed query only needs its `O(K)` candidate IDs, posting results, and `O(matches)` IDs in request memory, **plus** the existing controller's `O(B)` hydrated `Batch` values until that boundary changes. A Tango-like scan can also stream **one** stored snapshot at a time to bound request heap, but still performs `B` reads and transfers the table's aggregate bytes. Go maps, MySQL rows, and an object-store serialization have different overhead: no table entry is a measured persistent-storage size. The entire benchmark process reached roughly **3.62 GiB RSS** while parsing the 2.8 GB source graph and iterating the representations; its process peak is **not** one scenario's live impact-set heap.

Local **in-process lookup CPU** is much smaller than remote I/O: for `B=1,000, K=1,000`, recorded runs found roughly `0.04–0.05 ms` for 64-bit postings, `0.12–0.15 ms` for string postings, and `0.12–0.15 s` to scan all Tango-like snapshots. For `K=100`, 64-bit postings were roughly `1–2 µs` and strings `2–4 µs`, while the Tango-like scan rose from under 1 ms at `B=100` to about 18 ms at `B=1,000`. These are local Go measurements with resident in-memory structures, **not database or network timings**. Building the complete `B=1,000, K=1,000` local ID index and registering its ~827,000 distinct names took 1.58 seconds in aggregate, versus 1.28 seconds for a string index and 3.81 seconds to materialize all Tango-like snapshots; these are one-time *all-batch* construction timings without remote storage. Separate case build times are in the JSON artifact.

The following is a **latency model, not a benchmark of any storage backend**: assume independent primary-key reads/writes each take **5 ms**, at most **16** run concurrently, and Tango-like stored protobuf transfers at **100 MiB/s**. All modes include the current `ListByStates` hydration floor of `ceil(B/16) × 5 ms`; ID index cold admission additionally models `K` registry reads, conditional creates for unseen labels, `K` posting reads and `K` posting writes. The warm-ID column omits registry operations if IDs are already available; the label index models `K` reads and `K` writes. The Tango-like scan models `B` snapshot reads and the measured protobuf payload transfer (gzip column substitutes modeled compressed bytes but **excludes decompression CPU**). The model excludes the **common new-batch Tango computation**, candidate registration/serialization CPU, state transitions, connection-pool limits, hot-key contention, transaction retries, and storage protocol overhead.

| `B` | `K` | Common batch hydration | ID index, cold | ID index, warm | Label index | Tango-like scan, raw | Tango-like scan, gzip |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 35 ms | 175 ms | 105 ms | 105 ms | 134 ms | 94 ms |
| 100 | 1,000 | 35 ms | 1,280 ms | 665 ms | 665 ms | 712 ms | 306 ms |
| 500 | 100 | 160 ms | 300 ms | 230 ms | 230 ms | 645 ms | 445 ms |
| 500 | 1,000 | 160 ms | 1,365 ms | 790 ms | 790 ms | 3,550 ms | 1,516 ms |
| 1,000 | 100 | 315 ms | 450 ms | 385 ms | 385 ms | 1,278 ms | 878 ms |
| 1,000 | 1,000 | 315 ms | 1,475 ms | 945 ms | 945 ms | 7,086 ms | 3,018 ms |

**Decision implication:** retain the ID-based inverted index only if small `K`, hot-key fan-out, and backend point-read/write latency measured on *real* queue changes justify it. At `K≈B`, 64-bit registration and per-target posting writes can dominate admission even though the in-process lookup is fast; a direct canonical-label posting key avoids the registry phase and can have similar live heap. A Tango-like stored snapshot with compression can also be competitive for **small `B` and large `K`** under the assumed network model, but its memory/wire cost grows with every in-flight batch and a real two-sided changed-target payload may be larger. Broad changes still need an explicit fail-closed policy; do not infer an SLO or choose a static threshold from synthetic uniformly sampled labels.

### Hive-derived target counts: N, 2N and 5N (September 30, 2026)

The closest relevant **Hive** source found is [`rawdata_user.kafka_hp_submitqueue_request_feature_event_nodedup`](tango-eval/hive-go-diff-targets-20260930.sql), a TIER_THREE feature-event table with `msg.targetschanged`, `msg.targetsadded`, `msg.targetsremoved`, `msg.diffids`, `msg.queueid`, `msg.stackheight`, and `msg.basesha` (verified against uMetadata). There was **no relevant canonical uMetric definition**; the generic table recommendations for "target graph" described unrelated pricing/driver graphs, so this table was selected from a targeted SubmitQueue dataset search and verified by its nested field descriptions and actual rows. It measures **SubmitQueue feature-event counters**, not a direct Tango `GetChangedTargets` response. The [executed SQL](tango-eval/hive-go-diff-targets-20260930.sql) is QueryBuilder report `mcDHURW0X`, run `PEOkkp0n`, Data Central execution `6a86dc71-00db-4c93-a38d-b07cb221e51d`; the [aggregate evidence](tango-eval/hive-go-diff-targets-20260930.json) stores no individual diffs. The independent [broad-tail SQL](tango-eval/hive-go-diff-tail-20260930.sql) is QueryBuilder report `F1fC14EDp`, run `kMpN5D03F`, Data Central execution `216b3b36-a5b2-48c7-9463-20af6272b804`; it reproduced the latest-per-diff count and sum as well as the broad-diff contribution from the same source.

The SQL restricts `datestr` to **September 1–29, 2026**, `queueid='go'`, one diff ID and `stackheight=1`, complete non-null counts, and non-deleted rows. It takes the most recent event by `ts` for each immutable diff ID to avoid counting retries/remeasurements multiple times; all 62,957 eligible raw events have non-null timestamps and nonnegative counts, yielding **58,876 distinct diffs**. `targets_changed + targets_added + targets_removed` is an *affected-target proxy* for new, deleted and changed targets; the feature-event producer's category exclusivity was not independently verified. The resulting sum is **148,766,796**, arithmetic average **2,526.7816** affected targets per unique diff. Define integer **`N = round(2,526.7816) = 2,527`**, hence **`2N = 5,054`** and **`5N = 12,635`**. As a sensitivity check, deduplicating by `(diff ID, base SHA)` instead gives 60,614 observations and average **2,656.73** (+5.1%); the raw-event average **3,880.61** overweights repeats.

**The mean is not typical.** The deduplicated median is approximately **42**, p90 **1,047**, p95 **3,327**, and p99 around **33–35 thousand** (approximate percentile sketches vary slightly by query plan). Only **233 / 58,876 diffs (0.40%)** have at least 100,000 affected targets, yet they contribute **98,355,004 / 148,766,796 (66.1%)** of the total; excluding these broad cases drops the mean to **859.64**. At most **2,884,044** targets were reported for one diff, close to the measured whole-graph node count. A broad-diff fail-closed path is therefore essential; the three `N` workloads below deliberately stress arithmetic-mean cardinalities, **not** the median request.

| Hive source or SQL-derived field | Meaning in the executed N query |
|---|---|
| `datestr` | Hive date partition, restricted to September 1–29, 2026 to exclude the current partial day. |
| `msg.queueid` | SubmitQueue queue name; `go` selects changes in the go-code queue. |
| `msg.diffids`, `msg.diffids[1]` | IDs in the submitted stack; exactly one ID is required, and that ID is the per-code-change deduplication key. |
| `msg.stackheight` | Reported stack size; requiring `1` excludes stacked-request aggregates. |
| `msg.basesha` | Base commit used to measure target statistics; the alternative sensitivity groups by `(diff ID, base SHA)`. |
| `msg.targetschanged` | Producer-reported count of modified build targets; the largest component of the affected-target proxy. |
| `msg.targetsadded`, `msg.targetsremoved` | Producer-reported newly added and removed targets; both included in the proxy. |
| `ts` | Feature-event production timestamp; descending order selects the latest observed event for each diff ID. |
| `hadoop_isdeleted` | Soft-delete marker; only false or null records are considered. |
| `affected_targets` | Derived sum of the preceding three target-count fields, *not* a directly stored Tango RPC count. |
| `latest_event` | `ROW_NUMBER()` within each diff ID by descending timestamp/date; `1` selects its latest measurement. |

The executed Presto result contains two grains, which should not be confused: `raw_events` repeats diffs with multiple feature events, while `unique_diff_latest` is the one-observation-per-diff estimate used for `N`.

| Column in the Presto result | Meaning |
|---|---|
| `grain` | `raw_events` retains all qualifying rows; `unique_diff_latest` retains only the latest row for each single diff. |
| `observations` | Number of rows at the stated grain: 62,957 feature events or 58,876 unique diffs. |
| `sum_affected_targets` | Sum of `affected_targets` over the rows at this grain; 148,766,796 for unique diffs. |
| `avg_affected_targets` | Arithmetic mean of affected-target proxy per row; **2,526.7816** per unique diff, the source of `N`. |
| `avg_changed`, `avg_added`, `avg_removed` | Separate means of the three reported producer counters; for unique diffs they are 2,524.6627, 1.6947 and 0.4243 respectively. |
| `p50_p90_p95_p99_p999` | Array of approximate 50th, 90th, 95th, 99th and 99.9th percentiles of `affected_targets` at the stated grain. |
| `zero_target_observations` | Count of rows with zero reported affected targets; 5,942 among unique diffs. |
| `ge_1000`, `ge_10000`, `ge_100000` | Counts of rows with at least the specified affected-target threshold; unique-diff values are 5,988, 1,213 and 233. |
| `max_affected_targets` | Largest observed affected-target proxy for one row; 2,884,044. |

The [aggregate JSON](tango-eval/hive-go-diff-targets-20260930.json) keeps the chosen `unique_diff_latest` output and validation in a compact, reviewable artifact. Every field in that artifact is accounted for here:

| Field in Hive aggregate JSON | Definition / units |
|---|---|
| `source_table`, `data_tier` | Fully qualified source table and its uMetadata data-quality tier (`TIER_THREE`). |
| `period_utc_datestr`, `scope` | Inclusive Hive date partitions and the queue, stack and latest-per-diff filters used. |
| `affected_target_proxy` | Formula used to approximate conflict-relevant targets; not a measured Tango response. |
| `presto_report_id`, `presto_run_id`, `data_central_execution_uuid` | Identifiers of the executed mean/distribution query, sufficient to retrieve its results and execution metadata. |
| `raw_single_diff_event_rows`, `unique_diff_count` | Qualifying feature-event count and latest-per-diff count before and after deduplication. |
| `total_affected_targets`, `avg_affected_targets_per_unique_diff` | Deduplicated sum and arithmetic mean of the affected-target proxy. |
| `avg_changed`, `avg_added`, `avg_removed` | Deduplicated arithmetic means of each producer count, with the same units as target count per diff. |
| `approx_p50_p90_p95_p99_p999`, `zero_target_diffs` | Approximate affected-target percentiles, and count of unique diffs with zero affected targets. |
| `at_least_100000_target_diffs`, `affected_target_sum_from_at_least_100000_diffs`, `avg_below_100000` | Broad-diff count and contribution, plus arithmetic mean after excluding those diffs. |
| `broad_tail_cross_check` | Independent second Presto run with the same latest-per-diff filter; its `sql`, `presto_report_id`, `presto_run_id`, and `data_central_execution_uuid` locate that run, while `unique_diffs`, `total_affected_targets`, `broad_diffs`, `broad_affected_targets`, and `avg_below_100000` are its result columns. |
| `alternative_unique_diff_and_base_count`, `alternative_unique_diff_and_base_avg` | Sensitivity grouping by the pair `(diff ID, base SHA)` instead of diff ID alone; count and average in targets. |
| `null_event_timestamps`, `negative_counts` | Data-quality checks on the qualifying event rows before latest-event selection. |
| `diffs_with_repeated_events`, `diffs_with_multiple_base_shas`, `diffs_with_varying_affected_counts` | Counts of diffs with multiple feature events, multiple base revisions, and multiple reported affected-target values. |
| `integer_N_rounded`, `benchmark_targets` | Rounded mean `N` and its `N`, `2N` and `5N` integer benchmark target counts. |
| `caveat` | Reminder that event counters approximate affected targets and the mean is dominated by a heavy tail. |

The same [Go benchmark](../../../tool/tangograph-eval/bench.go) was run at each of the exact integer `N`, `2N`, and `5N` sizes against the previously measured full go-code target-name pool. Results are [N](tango-eval/go-code-impact-hive-n-20260930.json), [2N](tango-eval/go-code-impact-hive-2n-20260930.json), and [5N](tango-eval/go-code-impact-hive-5n-20260930.json). Each row is a **separate deterministic synthetic run**, not a Hive observation of that many simultaneous active batches. `N` is observed per **single diff**, not per multi-diff SubmitQueue batch: using `N`, `2N` or `5N` targets for **each** batch is a scenario assumption, not a measured batch-size distribution. As above, the Tango-like case models one optimized target and dependency/name metadata per affected label, not a full base graph or the potentially two-sided old/new response. Labels and capped dependency **degrees** come from the complete go-code Bazel graph, but benchmark dependency **neighbors** are sampled deterministically rather than representing its actual graph edges.

| In flight `B` | Scenario (`K` targets/batch) | Matched batches | Raw 64-bit arrays | ID index + registry heap | Label index heap | Tango-like snapshots heap | Tango-like protobuf (gzip) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | N (2,527) | 88 | 1.9 MiB | 61.2 MiB | 55.3 MiB | 262.8 MiB | 159.6 MiB (57.1 MiB) |
| 500 | N (2,527) | 455 | 9.6 MiB | 254.4 MiB | 254.4 MiB | 1,315.1 MiB | 798.1 MiB (285.7 MiB) |
| 1,000 | N (2,527) | 923 | 19.3 MiB | 333.9 MiB | 411.8 MiB | 2,629.9 MiB | 1,596.7 MiB (571.5 MiB) |
| 100 | 2N (5,054) | 100 | 3.9 MiB | 105.8 MiB | 99.0 MiB | 527.1 MiB | 321.2 MiB (114.7 MiB) |
| 500 | 2N (5,054) | 500 | 19.3 MiB | 334.0 MiB | 411.8 MiB | 2,627.5 MiB | 1,604.9 MiB (573.1 MiB) |
| 1,000 | 2N (5,054) | 1,000 | 38.6 MiB | 561.3 MiB | 821.6 MiB | 5,257.5 MiB | 3,209.8 MiB (1,146.3 MiB) |
| 100 | 5N (12,635) | 100 | 9.6 MiB | 255.5 MiB | 254.4 MiB | 1,250.9 MiB | 800.9 MiB (286.0 MiB) |
| 500 | 5N (12,635) | 500 | 48.2 MiB | 593.3 MiB | 978.2 MiB | 6,252.9 MiB | 4,004.6 MiB (1,430.1 MiB) |
| 1,000 | 5N (12,635) | 1,000 | 96.4 MiB | 693.9 MiB | 1,759.4 MiB | 12,502.1 MiB | 8,006.1 MiB (2,859.2 MiB) |

| Column in the nine-case memory/wire table | Definition / units |
|---|---|
| In flight `B` | Number of existing synthetic batches whose impact sets must be compared against the new candidate. |
| Scenario (`K` targets/batch) | Exact number of unique target names for each in-flight batch **and** the candidate; `N=2,527`, `2N=5,054`, `5N=12,635`. |
| Matched batches | Number of those `B` with at least one target name in common with the candidate in this synthetic run. |
| Raw 64-bit arrays | `B × K × 8` bytes of uncompressed, fixed-width IDs, shown in MiB (`2²⁰` bytes); **not end-to-end index wire bytes** (compression might reduce them), and excludes name registry, postings and representation overhead. |
| ID index + registry heap | Measured resident Go heap for `[]uint64` batch sets, sparse ID→name registry and ID→batch postings, net of shared inputs. |
| Label index heap | Measured resident Go heap for string-name batch sets and name→batch postings, net of shared inputs. |
| Tango-like snapshots heap | Measured resident Go heap for `B` modeled optimized-target/metadata batch snapshots, net of shared inputs. |
| Tango-like protobuf (gzip) | Total modeled uncompressed (best-speed gzip) serialized snapshot bytes across all `B` batches; it is **not** a measured Tango RPC or database transfer. |

**Memory interpretation:** heap is measured live Go heap for **all `B` representations resident in one process**, minus the shared raw label pool and workload indices; it is neither a deployed controller's request heap nor a database storage bill. At `B=1,000, K=5N`, the Tango-like test process peaked near **25.8 GiB RSS** while constructing and serializing its temporary snapshots, versus **12.2 GiB live snapshot heap** after GC. ID-index heap saturates more slowly at high `K` because the sparse name registry cannot exceed the sampled 2.54-million-label pool, while repeated per-batch names continue increasing the label-index and Tango-like sizes. These synthetic uniformly scattered sets match **92.3%** of batches even at `B=1,000, K=N` and **all** batches at `K=2N` or `5N`; real changes can be more clustered or less correlated. When every batch matches, the dependency output itself costs `Ω(B)` and broad-mode conservative serialization avoids thousands of posting operations.

Reusing the **illustrative** model above (5 ms/point read or write, concurrency 16, 100 MiB/s; not measured storage), the estimated admission times are:

| `B` | `K` | Current batch-hydration floor | ID index, cold | ID index, warm registry | Label index | Tango-like scan, raw | Tango-like scan, gzip |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | N | 35 ms | 3.12 s | 1.62 s | 1.62 s | 1.68 s | 0.66 s |
| 500 | N | 160 ms | 3.01 s | 1.74 s | 1.74 s | 8.39 s | 3.26 s |
| 1,000 | N | 315 ms | 2.98 s | 1.90 s | 1.90 s | 16.78 s | 6.53 s |
| 100 | 2N | 35 ms | 6.06 s | 3.20 s | 3.20 s | 3.29 s | 1.23 s |
| 500 | 2N | 160 ms | 5.49 s | 3.32 s | 3.32 s | 16.43 s | 6.11 s |
| 1,000 | 2N | 315 ms | 5.28 s | 3.48 s | 3.48 s | 32.83 s | 12.20 s |
| 100 | 5N | 35 ms | 14.26 s | 7.94 s | 7.94 s | 8.08 s | 2.93 s |
| 500 | 5N | 160 ms | 12.34 s | 8.06 s | 8.07 s | 40.40 s | 14.65 s |
| 1,000 | 5N | 315 ms | 12.20 s | 8.22 s | 8.22 s | 80.76 s | 29.29 s |

| Column in the nine-case latency table | Definition / units |
|---|---|
| `B`, `K` | Same existing-batch and per-batch target counts as the memory/wire table. |
| Current batch-hydration floor | `ceil(B/16) × 5 ms` for existing per-key batch reads, included in **all** subsequent times. |
| ID index, cold | Assumed registry check for every one of the `K` candidate names, conditional registration for unknown names, `K` posting reads, `K` posting writes, and measured local lookup CPU, plus hydration. |
| ID index, warm registry | Omits name-registry I/O if canonical IDs are already known, but still does `K` posting reads and writes, plus hydration and measured CPU. |
| Label index | `K` string-key posting reads and writes, hydration and measured local lookup CPU; no ID-name registration. |
| Tango-like scan, raw | `B` per-batch snapshot reads, modeled raw protobuf transfer at 100 MiB/s, hydration and measured local scan CPU. |
| Tango-like scan, gzip | Same with measured gzip/best-speed bytes, **excluding decompression CPU**. |

The modeled cold-ID latency falls slightly as `B` increases at fixed `K` because more candidate labels have already been registered, avoiding conditional creates; **do not interpret that as a faster actual queue with more batches**. Every mode excludes the common Tango computation for the new diff, posting hot-key contention, compression/decompression CPU, and service/database tail latency. `5N` should generally trigger an explicit, conservative broad policy rather than 12,635 target-registration and posting reads/writes per admission.

#### Benchmark result-field dictionary

The following table defines **every structural field** in the JSON benchmark artifacts. Fields nested under a mode appear only where applicable; zero-valued optional fields may be omitted. `B` and `K` describe synthetic benchmark workload sizes, **not a measured SubmitQueue population**.

| Field in JSON | Scope | Definition / units |
|---|---|---|
| `input` | top-level | Path of the successful full go-code Bazel `streamed_proto` used for label sampling; not committed because it contains internal labels. |
| `bazel_streamed_proto_input_bytes` | top-level | Byte length of that raw query file (`2,799,274,578`). |
| `go_code_revision`, `go_version` | top-level | Checked-out go-code commit and local Go runtime used for the benchmark. |
| `graph_targets`, `main_repo_labels` | top-level | Parsed complete graph node count and count of `//` main-repository labels eligible for synthetic changes. |
| `mean_label_bytes` | top-level | Arithmetic mean UTF-8 byte length of eligible target labels, **not** an in-memory string size (approximately 101). |
| `mean_capped_dependencies`, `max_dependencies_per_target` | top-level | Mean observed dependency degree after capping each node at the stated maximum (`32`); modeled neighbors are sampled and are **not** the actual graph edges. |
| `illustrative_latency_assumptions` | top-level | Namespace for **assumed**, not measured, backend timing inputs. |
| `point_read_or_write_rtt_ms` | assumptions | Assumed independent per-key read or write latency in milliseconds (`5`). |
| `max_concurrent_point_operations` | assumptions | Optimistic number of concurrent per-key storage operations (`16`). |
| `uncompressed_transfer_mib_per_second` | assumptions | Assumed storage throughput for raw or modeled gzip payloads in MiB/s (`100`). |
| `notes` | top-level | Workload, scope and non-production-measurement qualifications emitted by the benchmark code. |
| `cases` | top-level | Array containing one run for each requested pair of `B` and `K`. |
| `in_flight_batches` | case | `B`: synthetic batches already eligible for comparison. |
| `targets_per_batch`, `candidate_targets` | case | `K` affected labels in each synthetic batch and in the incoming candidate; generated unique within each batch. |
| `registered_64bit_index` | case/mode | Complete sorted 64-bit per-batch ID sets, an in-memory name registry and ID→batch posting lists; no real remote registry calls. |
| `canonical_label_index` | case/mode | Complete per-batch string-name sets and label→batch postings, without a 64-bit name registry. |
| `tango_optimized_target_scan` | case/mode | One default-field optimized target per changed name plus local ID→name metadata including dependency names; scans all stored batch snapshots. |
| `live_heap_bytes` | each mode | Incremental live Go heap **after GC** for all `B` mode records resident in one process, excluding the shared label pool and generated workload indices. |
| `build_milliseconds` | each mode | Local elapsed time to construct **all** `B` batches and their index/metadata, plus candidate registration where applicable; not the time for one admission. |
| `query_microseconds`, `query_alloc_bytes` | each mode | Average resident, in-memory candidate-lookup CPU time and Go allocated bytes **per lookup**; excludes fetching the stored data. |
| `matching_batches` | each mode | Number of synthetic batches sharing at least one candidate target name/ID; identical across the three modes within a case. |
| `posting_reads` | each mode | Modeled point reads for a lookup: `K` target postings for either index; **`B` batch-snapshot reads** in the Tango-like scan despite the generic JSON field name. |
| `registration_reads` | each mode | `K` modeled ID→name registry checks for the incoming candidate in the ID mode; `0` for the string and Tango-like modes. |
| `posting_keys` | indexed modes | Distinct registered IDs or label strings with in-flight postings, excluding candidate-only names that have no posting yet. |
| `name_registry_entries` | ID mode | Distinct label rows registered across all in-flight batches **and** candidate. Their difference from `posting_keys` estimates new candidate labels needing conditional creates. |
| `raw_impact_bytes` | ID mode | `8 × B × K` bytes of raw per-batch ID arrays; excludes Go slice/map overhead, name registry, postings and metadata. |
| `raw_label_bytes` | string or Tango-like modes | Sum of UTF-8 label bytes retained across per-batch string sets, or changed-target **and dependency** metadata respectively; excludes maps/slices and other fields. |
| `modeled_protobuf_bytes`, `modeled_gzip_bytes` | Tango-like mode | Sum of per-batch default-field `GetTargetGraphResponse`-shaped protobuf sizes and gzip/best-speed sizes. These are **not** observed Tango RPC bytes or actual two-sided `GetChangedTargets` messages. |
| `illustrative_remote_latency` | case | Namespace for the model using the input assumptions and locally measured lookup CPU; **not** backend timings or measured service SLOs. |
| `common_batch_hydration_ms` | latency model | `ceil(B / concurrency) × RTT` for the existing controller's per-key `BatchStore.Get` hydration, included in every mode. |
| `registered_64bit_index_ms` | latency model | Common hydration + `K` name-registry reads + conditional creates for new names + `K` posting reads + `K` posting writes + local index-lookup CPU. |
| `registered_64bit_index_warm_registry_ms` | latency model | Same index path, but optimistically omits all name-registry I/O when candidate IDs are already available. |
| `canonical_label_index_ms` | latency model | Common hydration + `K` label-posting reads + `K` posting writes + local lookup CPU. |
| `tango_optimized_target_scan_ms` | latency model | Common hydration + `B` snapshot reads + modeled raw protobuf transfer at assumed throughput + local scan CPU. |
| `tango_optimized_target_scan_gzip_ms` | latency model | Same, substituting modeled per-message gzip transfer; **excludes** gzip decompression CPU and backend compression costs. |

## Proposed SubmitQueue design

1. Add `submitqueue/orchestrator/extension/conflict/tango/` implementing the **existing shared** `submitqueue/extension/conflict.Analyzer`; the implementation is service-scoped because only the orchestrator resolves it. Inject a `changeset.Resolver`, an interface for Tango's streaming client, a queue-specific VCS/base-revision resolver, and a key-oriented impact store (plus a first-class posting store for indexed mode) at construction. Route each queue in `service/submitqueue/orchestrator/server/`, not in an extension factory. Do not expand controller inputs to include changes or graphs.
2. Resolve the candidate's and each in-flight batch's pinned request URIs, canonical repository remote, target branch **base SHA**, computation strategy, and analysis policy. Build Tango `first_revision={remote, base_sha, strategy: COMPUTATION_STRATEGY_UNSET}` and `second_revision={remote, base_sha, strategy: COMPUTATION_STRATEGY_UNSET, requests:[the pinned URIs in batch order]}` (or set `NATIVE` explicitly); the protobuf's zero-value `INVALID` strategy is not a valid default. Validate the same remote/base/policy for every compared signature, and recheck the relevant queue/branch version before promotion: the target branch can advance during a slow Tango call even though dependency messages are queue-partitioned. That recheck cannot be atomic with an external VCS update; continue relying on the landing service's final merge precondition rather than mixing incompatible signatures. Initial native Tango support is limited to compatible GitHub PR URIs; Git/Phabricator need an explicit adapter or conservative fallback. Verify Tango's application semantics against the queue's actual merge strategy before rolling out.
3. Make **at most one** `GetChangedTargets` request per missing batch signature, regardless of the number of in-flight peers; set `MaxDistance=-1` and omit hashes/tags/attributes. Buffer only the IDs of actual changed old/new targets until all metadata arrives, resolve their canonical labels, sort/dedupe, and form the impact signature. Do **not** include an unchanged target merely because its name appears in `direct_dependencies` metadata. Treat unknown mappings, premature EOF, cancellation, or a partial response as failed analysis, not an empty result.
4. Persist an **immutable, keyed summary** by `(queue, batch ID, base tree/revision, graph strategy, impact-policy and ID versions)`, with a small batch-keyed reference if retrieval needs one. Store complete sorted `uint64` registered target IDs and a count/checksum or exact canonical labels as appropriate. ID registration resolves hash collisions before comparison, and an ID can be restored to its label by one by-key read; Tango response IDs remain ephemeral. Use an indexed lookup (or the explicitly bounded inline-set interim path above), **not one full-signature load per in-flight batch**. Keep large payloads out of `entity.Batch`, request logs, and queue messages.
5. Create or retrieve the signature and complete all required posting writes **before** the existing `Creating → Created` promotion and before the speculate notification; this follows the controller's existing persist-before-publish and retry-idempotency ordering. Creating records are not eligible as in-flight dependencies. On duplicate delivery, reuse a complete matching signature and idempotently finish missing postings; never mark an incomplete one ready. Keep immutable blob creation, per-key index writes, and the versioned batch write as separate, retryable operations rather than a cross-entity transaction.
6. A newly advanced base, changed analysis policy, unsupported provider, or untracked build-affecting input invalidates comparability: recompute every relevant in-flight signature against one common base, or conservatively report those batches as conflicting until rebaselining succeeds. **Never** compare an old signature against a new revision merely because their target names happen to match. Return retryable failures through the existing error-classifier path; after a deliberate retry/availability policy, falling back to `all` preserves safety but reduces parallelism. Falling back to `none` does not.
7. Repair or isolate Tango's incomplete compared-targets cache key before enabling decisions, then shadow the proposed analyzer against `all`/`pathoverlap` and record response bytes, **real per-batch `K` and in-flight `B` distributions**, hot-posting fan-out, index reads/writes, broad fallbacks, base mismatch/rebase rate, Tango cache hits and latency, analyzer heap/RSS, and conflict pairs. Test additions, deletions, renames, global BUILD/toolchain changes, two changes touching one target via different files, branch movement, failed/partial streams, a crash halfway through index writes, redelivery, terminal stale postings, and index migration before enabling it for GitHub queues.

## Where optimization belongs

- **In memory:** The cheapest graph is the graph SubmitQueue never materializes. Let Tango own cached graphs/TGB and stream a **per-batch affected-label signature** to SubmitQueue. A sorted `[]uint64` of registered IDs is eight bytes per affected target, with no Go map buckets or per-target pointers, and scales with **changed targets**, not every target in the repository; it is a durable source for posting-index rebuilds, **not something to fetch for every in-flight batch on admission**. A counting Bloom filter can reject definitely disjoint sets, but positive matches still need the exact ID set. Roaring bitmaps are attractive only for IDs within one **stable, pinned** graph dictionary: raw Tango IDs cannot be compared across separate responses. For a huge diff, bound decoder buffers and optionally spill the ID list; do not assume "usually small" is a memory limit.
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

Run the synthetic in-flight benchmark against the same successful full-graph input. The local Go CPU/heap measurements and illustrative remote model are emitted together as JSON; the modeled RTT, operation concurrency, and transfer bandwidth are configurable without requerying Bazel:

```sh
go run ./tool/tangograph-eval -benchmark \
  -input /tmp/sq-go-code-full-20260928.streamed_proto \
  -go-code-revision c5655f3f7e87f \
  -benchmark-batches 100,500,1000 -benchmark-targets 100,1000 \
  -benchmark-rtt-ms 5 -benchmark-concurrency 16 \
  -benchmark-transfer-mib 100
```

The Hive-derived benchmark reruns the same input at the [verified integer target counts](tango-eval/hive-go-diff-targets-20260930.json) with separate processes to limit peak memory; the largest `5N, B=1,000` Tango-like case needs substantial memory:

```sh
for k in 2527 5054 12635; do
  GOMEMLIMIT=28GiB go run ./tool/tangograph-eval -benchmark \
    -input /tmp/sq-go-code-full-20260928.streamed_proto \
    -go-code-revision c5655f3f7e87f \
    -benchmark-batches 100,500,1000 -benchmark-targets "$k" \
    -benchmark-rtt-ms 5 -benchmark-concurrency 16 \
    -benchmark-transfer-mib 100 > "/tmp/sq-impact-${k}.json"
done
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
