// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

type benchLabel struct {
	name   string
	degree uint8
}

type benchWorkload struct {
	batches   [][]uint32
	candidate []uint32
	targets   int
}

type benchMode struct {
	HeapBytes            uint64  `json:"live_heap_bytes"`
	BuildMilliseconds    float64 `json:"build_milliseconds"`
	QueryMicroseconds    float64 `json:"query_microseconds"`
	QueryAllocBytes      uint64  `json:"query_alloc_bytes"`
	Matches              int     `json:"matching_batches"`
	PostingReads         int     `json:"posting_reads"`
	RegistryReads        int     `json:"registration_reads"`
	PostingKeys          int     `json:"posting_keys,omitempty"`
	RegistryEntries      int     `json:"name_registry_entries,omitempty"`
	RawImpactBytes       uint64  `json:"raw_impact_bytes,omitempty"`
	RawLabelBytes        uint64  `json:"raw_label_bytes,omitempty"`
	ModeledProtobufBytes uint64  `json:"modeled_protobuf_bytes,omitempty"`
	ModeledGzipBytes     uint64  `json:"modeled_gzip_bytes,omitempty"`
}

type benchAssumptions struct {
	RTTMilliseconds      float64 `json:"point_read_or_write_rtt_ms"`
	Concurrency          int     `json:"max_concurrent_point_operations"`
	TransferMiBPerSecond float64 `json:"uncompressed_transfer_mib_per_second"`
}

type benchLatencyModel struct {
	CommonBatchHydrationMs float64 `json:"common_batch_hydration_ms"`
	RegisteredIDIndexMs    float64 `json:"registered_64bit_index_ms"`
	RegisteredIDWarmMs     float64 `json:"registered_64bit_index_warm_registry_ms"`
	LabelStringIndexMs     float64 `json:"canonical_label_index_ms"`
	TangoLikeScanMs        float64 `json:"tango_optimized_target_scan_ms"`
	TangoLikeScanGzipMs    float64 `json:"tango_optimized_target_scan_gzip_ms"`
}

type benchCase struct {
	InFlight          int               `json:"in_flight_batches"`
	TargetsPerBatch   int               `json:"targets_per_batch"`
	CandidateTargets  int               `json:"candidate_targets"`
	RegisteredIDIndex benchMode         `json:"registered_64bit_index"`
	LabelStringIndex  benchMode         `json:"canonical_label_index"`
	TangoLikeScan     benchMode         `json:"tango_optimized_target_scan"`
	LatencyModel      benchLatencyModel `json:"illustrative_remote_latency"`
}

type benchReport struct {
	Input          string           `json:"input"`
	InputBytes     uint64           `json:"bazel_streamed_proto_input_bytes"`
	GoCodeRevision string           `json:"go_code_revision,omitempty"`
	GoVersion      string           `json:"go_version"`
	GraphTargets   uint64           `json:"graph_targets"`
	MainLabels     int              `json:"main_repo_labels"`
	MeanNameBytes  float64          `json:"mean_label_bytes"`
	MeanDepDegree  float64          `json:"mean_capped_dependencies"`
	MaxDepDegree   int              `json:"max_dependencies_per_target"`
	Assumptions    benchAssumptions `json:"illustrative_latency_assumptions"`
	Notes          []string         `json:"notes"`
	Cases          []benchCase      `json:"cases"`
}

type idModeData struct {
	batches   [][]uint64
	postings  map[uint64][]uint32
	registry  map[uint64]string
	candidate []uint64
}

type labelModeData struct {
	batches   [][]string
	postings  map[string][]uint32
	candidate []string
}

type tangoModeBatch struct {
	targets []optimizedTarget
	names   map[int32]string
}

type tangoModeData struct {
	batches       []tangoModeBatch
	candidateName map[string]struct{}
}

var benchResultSink int

func parseBenchmarkCounts(raw string) ([]int, error) {
	var counts []int
	for _, part := range strings.Split(raw, ",") {
		count, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || count <= 0 {
			return nil, fmt.Errorf("invalid positive benchmark count %q", part)
		}
		counts = append(counts, count)
	}
	return counts, nil
}

func benchmarkBatchImpacts(graph queryGraph, batchesText, targetsText string, assumptions benchAssumptions) (benchReport, error) {
	batchCounts, err := parseBenchmarkCounts(batchesText)
	if err != nil {
		return benchReport{}, err
	}
	targetCounts, err := parseBenchmarkCounts(targetsText)
	if err != nil {
		return benchReport{}, err
	}
	if assumptions.RTTMilliseconds <= 0 || assumptions.Concurrency <= 0 || assumptions.TransferMiBPerSecond <= 0 {
		return benchReport{}, fmt.Errorf("benchmark latency assumptions must be positive")
	}
	report := benchReport{
		GoVersion:    runtime.Version(),
		GraphTargets: graph.scanned,
		MaxDepDegree: 32,
		Assumptions:  assumptions,
		Notes: []string{
			"Deterministic synthetic impact sets sampled from actual main-repo Bazel labels; not observed PR diffs.",
			"Tango-like representation models one OptimizedTarget plus target/dependency metadata per affected label, with hashes/tags/attributes omitted as in default OutputConfig. An actual ChangedTarget can contain both old and new targets.",
			"Live heap and query time are local in-process Go measurements. Posting/registry reads and protobuf bytes are modeled; no remote storage or Tango RPC latency is measured.",
			"Heap excludes the shared input label pool and generated workload indices; it includes each mode's per-batch records, postings, and sparse registry.",
			"All synthetic records and indexes are resident in one Go process for measurement; reported heap is not a stateless controller's request heap or measured database size.",
			"Remote-latency model assumes independent parallel point operations without connection-pool, hot-row, transaction, or queue contention; it is not an observed backend latency.",
		},
	}
	var labels []benchLabel
	var totalName, totalDegree uint64
	for _, target := range graph.targets {
		if !strings.HasPrefix(target.name, "//") || strings.HasPrefix(target.name, "//external:") {
			continue
		}
		degree := min(len(target.deps), report.MaxDepDegree)
		labels = append(labels, benchLabel{name: target.name, degree: uint8(degree)})
		totalName += uint64(len(target.name))
		totalDegree += uint64(degree)
	}
	graph.targets = nil
	if len(labels) == 0 {
		return benchReport{}, fmt.Errorf("no main-repository target labels in input")
	}
	report.MainLabels = len(labels)
	report.MeanNameBytes = float64(totalName) / float64(len(labels))
	report.MeanDepDegree = float64(totalDegree) / float64(len(labels))
	for _, count := range batchCounts {
		for _, targets := range targetCounts {
			if targets > len(labels) {
				return benchReport{}, fmt.Errorf("%d targets per batch exceeds %d available labels", targets, len(labels))
			}
			workload := newBenchWorkload(len(labels), count, targets)
			entry := benchCase{InFlight: count, TargetsPerBatch: targets, CandidateTargets: len(workload.candidate)}
			entry.RegisteredIDIndex = benchmarkRegisteredIDs(labels, workload)
			runtime.GC()
			entry.LabelStringIndex = benchmarkCanonicalLabels(labels, workload)
			runtime.GC()
			entry.TangoLikeScan, err = benchmarkTangoLike(labels, workload)
			if err != nil {
				return benchReport{}, err
			}
			if entry.RegisteredIDIndex.Matches != entry.LabelStringIndex.Matches ||
				entry.RegisteredIDIndex.Matches != entry.TangoLikeScan.Matches {
				return benchReport{}, fmt.Errorf("lookup modes disagree on matches for batches=%d targets=%d", count, targets)
			}
			entry.LatencyModel = modelBenchLatency(entry, assumptions)
			report.Cases = append(report.Cases, entry)
			runtime.GC()
			runtime.KeepAlive(workload)
		}
	}
	runtime.KeepAlive(labels)
	return report, nil
}

func modelBenchLatency(entry benchCase, assumptions benchAssumptions) benchLatencyModel {
	pointWaveMs := func(keys int) float64 {
		return float64((keys+assumptions.Concurrency-1)/assumptions.Concurrency) * assumptions.RTTMilliseconds
	}
	batches := entry.InFlight
	candidateTargets := entry.CandidateTargets
	freshRegistrations := entry.RegisteredIDIndex.RegistryEntries - entry.RegisteredIDIndex.PostingKeys
	commonHydration := pointWaveMs(batches)
	idCPU := entry.RegisteredIDIndex.QueryMicroseconds / 1000
	labelCPU := entry.LabelStringIndex.QueryMicroseconds / 1000
	tangoCPU := entry.TangoLikeScan.QueryMicroseconds / 1000
	transferMs := float64(entry.TangoLikeScan.ModeledProtobufBytes) / (1 << 20) /
		assumptions.TransferMiBPerSecond * 1000
	gzipTransferMs := float64(entry.TangoLikeScan.ModeledGzipBytes) / (1 << 20) /
		assumptions.TransferMiBPerSecond * 1000
	return benchLatencyModel{
		CommonBatchHydrationMs: commonHydration,
		RegisteredIDIndexMs:    commonHydration + 3*pointWaveMs(candidateTargets) + pointWaveMs(freshRegistrations) + idCPU,
		RegisteredIDWarmMs:     commonHydration + 2*pointWaveMs(candidateTargets) + idCPU,
		LabelStringIndexMs:     commonHydration + 2*pointWaveMs(candidateTargets) + labelCPU,
		TangoLikeScanMs:        commonHydration + pointWaveMs(batches) + transferMs + tangoCPU,
		TangoLikeScanGzipMs:    commonHydration + pointWaveMs(batches) + gzipTransferMs + tangoCPU,
	}
}

func newBenchWorkload(poolSize, batchCount, targets int) benchWorkload {
	selectTargets := func(seed uint64) []uint32 {
		out := make([]uint32, 0, targets)
		seen := make(map[uint32]struct{}, targets)
		for j := 0; len(out) < targets; j++ {
			id := uint32(splitMix64(seed^uint64(j)*0x9e3779b97f4a7c15) % uint64(poolSize))
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		return out
	}
	workload := benchWorkload{batches: make([][]uint32, batchCount), targets: targets}
	for i := range workload.batches {
		workload.batches[i] = selectTargets(uint64(i + 1))
	}
	workload.candidate = selectTargets(0xd123456789abcdef)
	return workload
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

func registerTargetID(label string, registry map[uint64]string) uint64 {
	const domain, repository = "submitqueue-target-id/v1", "go-code"
	for probe := uint32(0); ; probe++ {
		payload := binary.AppendUvarint([]byte(domain), uint64(len(repository)))
		payload = append(payload, repository...)
		payload = binary.AppendUvarint(payload, uint64(len(label)))
		payload = append(payload, label...)
		payload = binary.BigEndian.AppendUint32(payload, probe)
		digest := sha256.Sum256(payload)
		id := binary.BigEndian.Uint64(digest[:8])
		if id == 0 {
			continue
		}
		if existing, ok := registry[id]; ok {
			if existing == label {
				return id
			}
			continue
		}
		registry[id] = strings.Clone(label)
		return id
	}
}

func liveHeap() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func benchmarkLookup(run func() int) (float64, uint64, int) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	iterations := 0
	totalMatches := 0
	for iterations < 2 || (time.Since(start) < 200*time.Millisecond && iterations < 1000) {
		totalMatches += run()
		iterations++
	}
	duration := time.Since(start)
	runtime.ReadMemStats(&after)
	benchResultSink += totalMatches
	return float64(duration.Microseconds()) / float64(iterations),
		(after.TotalAlloc - before.TotalAlloc) / uint64(iterations), totalMatches / iterations
}

func benchmarkRegisteredIDs(labels []benchLabel, workload benchWorkload) benchMode {
	baseline := liveHeap()
	start := time.Now()
	data := idModeData{
		batches:  make([][]uint64, len(workload.batches)),
		postings: make(map[uint64][]uint32),
		registry: make(map[uint64]string),
	}
	for batchIndex, indices := range workload.batches {
		ids := make([]uint64, 0, len(indices))
		for _, index := range indices {
			id := registerTargetID(labels[index].name, data.registry)
			ids = append(ids, id)
			data.postings[id] = append(data.postings[id], uint32(batchIndex))
		}
		slices.Sort(ids)
		data.batches[batchIndex] = ids
	}
	for _, index := range workload.candidate {
		data.candidate = append(data.candidate, registerTargetID(labels[index].name, data.registry))
	}
	result := benchMode{
		BuildMilliseconds: float64(time.Since(start).Microseconds()) / 1000,
		PostingKeys:       len(data.postings),
		RegistryEntries:   len(data.registry),
		PostingReads:      len(data.candidate),
		RegistryReads:     len(data.candidate),
		RawImpactBytes:    uint64(len(workload.batches) * workload.targets * 8),
	}
	result.HeapBytes = heapAboveBaseline(baseline, data)
	result.QueryMicroseconds, result.QueryAllocBytes, result.Matches = benchmarkLookup(func() int {
		seen := make([]bool, len(data.batches))
		matched := 0
		for _, id := range data.candidate {
			for _, batch := range data.postings[id] {
				if !seen[batch] {
					seen[batch] = true
					matched++
				}
			}
		}
		return matched
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result
}

func benchmarkCanonicalLabels(labels []benchLabel, workload benchWorkload) benchMode {
	baseline := liveHeap()
	start := time.Now()
	data := labelModeData{
		batches:  make([][]string, len(workload.batches)),
		postings: make(map[string][]uint32),
	}
	var rawBytes uint64
	for batchIndex, indices := range workload.batches {
		names := make([]string, 0, len(indices))
		for _, index := range indices {
			label := strings.Clone(labels[index].name)
			names = append(names, label)
			rawBytes += uint64(len(label))
			data.postings[label] = append(data.postings[label], uint32(batchIndex))
		}
		slices.Sort(names)
		data.batches[batchIndex] = names
	}
	for _, index := range workload.candidate {
		data.candidate = append(data.candidate, labels[index].name)
	}
	result := benchMode{
		BuildMilliseconds: float64(time.Since(start).Microseconds()) / 1000,
		PostingKeys:       len(data.postings),
		PostingReads:      len(data.candidate),
		RawLabelBytes:     rawBytes,
	}
	result.HeapBytes = heapAboveBaseline(baseline, data)
	result.QueryMicroseconds, result.QueryAllocBytes, result.Matches = benchmarkLookup(func() int {
		seen := make([]bool, len(data.batches))
		matched := 0
		for _, label := range data.candidate {
			for _, batch := range data.postings[label] {
				if !seen[batch] {
					seen[batch] = true
					matched++
				}
			}
		}
		return matched
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result
}

func benchmarkTangoLike(labels []benchLabel, workload benchWorkload) (benchMode, error) {
	baseline := liveHeap()
	start := time.Now()
	data := tangoModeData{
		batches:       make([]tangoModeBatch, len(workload.batches)),
		candidateName: make(map[string]struct{}, len(workload.candidate)),
	}
	for _, index := range workload.candidate {
		data.candidateName[labels[index].name] = struct{}{}
	}
	var rawLabelBytes uint64
	for batchIndex, indices := range workload.batches {
		targets := make([]optimizedTarget, 0, len(indices))
		names := make(map[int32]string, len(indices)*3)
		ids := make(map[uint32]int32, len(indices)*3)
		for j, index := range indices {
			id := int32(j + 1)
			ids[index] = id
			names[id] = strings.Clone(labels[index].name)
			rawLabelBytes += uint64(len(labels[index].name))
		}
		nextID := int32(len(indices) + 1)
		for _, index := range indices {
			target := optimizedTarget{id: ids[index], ruleType: 1}
			for j := range int(labels[index].degree) {
				depIndex := uint32(splitMix64(uint64(index)<<32|uint64(j)) % uint64(len(labels)))
				depID, exists := ids[depIndex]
				if !exists {
					depID = nextID
					nextID++
					ids[depIndex] = depID
					names[depID] = strings.Clone(labels[depIndex].name)
					rawLabelBytes += uint64(len(labels[depIndex].name))
				}
				target.directDependencies = append(target.directDependencies, depID)
			}
			targets = append(targets, target)
		}
		data.batches[batchIndex] = tangoModeBatch{targets: targets, names: names}
	}
	result := benchMode{
		BuildMilliseconds: float64(time.Since(start).Microseconds()) / 1000,
		PostingReads:      len(data.batches),
		RawLabelBytes:     rawLabelBytes,
	}
	result.HeapBytes = heapAboveBaseline(baseline, data)
	result.QueryMicroseconds, result.QueryAllocBytes, result.Matches = benchmarkLookup(func() int {
		matched := 0
		for _, batch := range data.batches {
			for _, target := range batch.targets {
				if _, found := data.candidateName[batch.names[target.id]]; found {
					matched++
					break
				}
			}
		}
		return matched
	})
	for _, batch := range data.batches {
		wire, err := measureProtoStream(compactGraph{
			targets: batch.targets,
			names:   batch.names,
			ruleTypes: map[int32]string{
				1: "go_library",
			},
		}, defaultPayload, 4_250_000)
		if err != nil {
			return benchMode{}, err
		}
		result.ModeledProtobufBytes += wire.Bytes
		result.ModeledGzipBytes += wire.GzipBytes
	}
	runtime.KeepAlive(data)
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result, nil
}
