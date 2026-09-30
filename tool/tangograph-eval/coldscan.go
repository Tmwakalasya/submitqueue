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
	"cmp"
	"fmt"
	"runtime"
	"slices"
	"strings"
)

type coldScanMode struct {
	LiveHeapBytes     uint64  `json:"live_heap_bytes"`
	SerializedBytes   uint64  `json:"serialized_bytes"`
	QueryMicroseconds float64 `json:"query_microseconds"`
	QueryAllocBytes   uint64  `json:"query_alloc_bytes"`
	MatchingBatches   int     `json:"matching_batches"`
}

type coldScanLatency struct {
	BatchHydrationMs   float64 `json:"batch_hydration_ms"`
	SignatureReadsMs   float64 `json:"signature_reads_ms"`
	IDTransferMs       float64 `json:"id_transfer_ms"`
	NameTransferMs     float64 `json:"name_transfer_ms"`
	IDTotalMs          float64 `json:"id_total_ms"`
	NameTotalMs        float64 `json:"name_total_ms"`
	IDRegistryReadMs   float64 `json:"optional_id_registry_read_ms"`
	IDRegistryCreateMs float64 `json:"optional_id_registry_create_ms"`
}

type coldScanCase struct {
	InFlight         int             `json:"in_flight_batches"`
	TargetsPerBatch  int             `json:"targets_per_batch"`
	CandidateTargets int             `json:"candidate_targets"`
	SortedIDs        coldScanMode    `json:"sorted_64bit_ids"`
	SortedNames      coldScanMode    `json:"sorted_target_names"`
	Latency          coldScanLatency `json:"illustrative_single_candidate_latency"`
}

type coldScanReport struct {
	Input          string           `json:"input"`
	InputBytes     uint64           `json:"bazel_streamed_proto_input_bytes"`
	GoCodeRevision string           `json:"go_code_revision,omitempty"`
	GoVersion      string           `json:"go_version"`
	GraphTargets   uint64           `json:"graph_targets"`
	MainLabels     int              `json:"main_repo_labels"`
	MeanLabelBytes float64          `json:"mean_label_bytes"`
	Assumptions    benchAssumptions `json:"illustrative_latency_assumptions"`
	Notes          []string         `json:"notes"`
	Cases          []coldScanCase   `json:"cases"`
}

// benchmarkColdStatelessScans measures a read-only, one-candidate full scan,
// not the persistent inverted-posting lookup measured by benchmarkBatchImpacts.
func benchmarkColdStatelessScans(graph queryGraph, batchesText, targetsText string, assumptions benchAssumptions) (coldScanReport, error) {
	batchCounts, err := parseBenchmarkCounts(batchesText)
	if err != nil {
		return coldScanReport{}, err
	}
	targetCounts, err := parseBenchmarkCounts(targetsText)
	if err != nil {
		return coldScanReport{}, err
	}
	if assumptions.RTTMilliseconds <= 0 || assumptions.Concurrency <= 0 || assumptions.TransferMiBPerSecond <= 0 {
		return coldScanReport{}, fmt.Errorf("cold-scan latency assumptions must be positive")
	}
	report := coldScanReport{
		GoVersion:    runtime.Version(),
		GraphTargets: graph.scanned,
		Assumptions:  assumptions,
		Notes: []string{
			"One candidate versus B in-flight batches, each with K synthetic uniformly sampled labels from the full go-code Bazel graph; neither batches nor target sets are observed production changes.",
			"Each read-only cold scan loads all B signatures into one stateless Go process and intersects their complete sorted sets with the candidate; no posting index or ID-name registry is loaded into request memory.",
			"64-bit ID assignment is collision-checked during benchmark preparation and is excluded from per-request heap and latency; candidate IDs must already be registered for the reported ID total.",
			"ID wire bytes are 8 bytes per stored target; name wire bytes are UTF-8 labels with one unsigned-varint length each. Both exclude per-batch framing, database protocol, metadata, and compression.",
			"Modeled time adds existing B BatchStore.Get calls, a second wave of B signature reads, raw-byte transfer at the assumed aggregate throughput, and measured resident in-process intersection CPU; it omits actual storage waits, serialization/decoding, Tango computation and controller scheduling.",
			"Optional ID registry reads and worst-case conditional creates are reported separately; they are not included in the read-only ID total.",
		},
	}
	var labels []string
	var totalNameBytes uint64
	for _, target := range graph.targets {
		if !strings.HasPrefix(target.name, "//") || strings.HasPrefix(target.name, "//external:") {
			continue
		}
		labels = append(labels, target.name)
		totalNameBytes += uint64(len(target.name))
	}
	graph.targets = nil
	if len(labels) == 0 {
		return coldScanReport{}, fmt.Errorf("no main-repository target labels in input")
	}
	report.MainLabels = len(labels)
	report.MeanLabelBytes = float64(totalNameBytes) / float64(len(labels))

	// Registration happens when writing signatures, not when a stateless
	// controller loads them; keep its dictionary outside the measured heap.
	idByLabel := make([]uint64, len(labels))
	registry := make(map[uint64]string)
	for i, label := range labels {
		idByLabel[i] = registerTargetID(label, registry)
	}
	registry = nil
	runtime.GC()

	for _, batches := range batchCounts {
		for _, targets := range targetCounts {
			if targets > len(labels) {
				return coldScanReport{}, fmt.Errorf("%d targets per batch exceeds %d available labels", targets, len(labels))
			}
			workload := newBenchWorkload(len(labels), batches, targets)
			entry := coldScanCase{InFlight: batches, TargetsPerBatch: targets, CandidateTargets: len(workload.candidate)}
			entry.SortedIDs = benchmarkColdSortedIDs(idByLabel, workload)
			runtime.GC()
			entry.SortedNames = benchmarkColdSortedNames(labels, workload)
			if entry.SortedIDs.MatchingBatches != entry.SortedNames.MatchingBatches {
				return coldScanReport{}, fmt.Errorf("cold-scan modes disagree on matches for batches=%d targets=%d", batches, targets)
			}
			entry.Latency = modelColdScanLatency(entry, assumptions)
			report.Cases = append(report.Cases, entry)
			runtime.GC()
			runtime.KeepAlive(workload)
		}
	}
	runtime.KeepAlive(idByLabel)
	runtime.KeepAlive(labels)
	return report, nil
}

func benchmarkColdSortedIDs(idByLabel []uint64, workload benchWorkload) coldScanMode {
	baseline := liveHeap()
	data := struct {
		batches   [][]uint64
		candidate []uint64
	}{
		batches: make([][]uint64, len(workload.batches)),
	}
	for batchIndex, indices := range workload.batches {
		ids := make([]uint64, len(indices))
		for i, index := range indices {
			ids[i] = idByLabel[index]
		}
		slices.Sort(ids)
		data.batches[batchIndex] = ids
	}
	data.candidate = make([]uint64, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = idByLabel[index]
	}
	slices.Sort(data.candidate)
	result := coldScanMode{
		SerializedBytes: uint64(len(workload.batches) * workload.targets * 8),
	}
	result.LiveHeapBytes = heapAboveBaseline(baseline, data)
	result.QueryMicroseconds, result.QueryAllocBytes, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(idByLabel)
	runtime.KeepAlive(workload)
	return result
}

func benchmarkColdSortedNames(labels []string, workload benchWorkload) coldScanMode {
	baseline := liveHeap()
	data := struct {
		batches   [][]string
		candidate []string
	}{
		batches: make([][]string, len(workload.batches)),
	}
	var serializedBytes uint64
	for batchIndex, indices := range workload.batches {
		names := make([]string, len(indices))
		for i, index := range indices {
			name := strings.Clone(labels[index])
			names[i] = name
			serializedBytes += uint64(len(name) + unsignedVarintBytes(uint64(len(name))))
		}
		slices.Sort(names)
		data.batches[batchIndex] = names
	}
	data.candidate = make([]string, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = strings.Clone(labels[index])
	}
	slices.Sort(data.candidate)
	result := coldScanMode{SerializedBytes: serializedBytes}
	result.LiveHeapBytes = heapAboveBaseline(baseline, data)
	result.QueryMicroseconds, result.QueryAllocBytes, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result
}

func unsignedVarintBytes(value uint64) int {
	size := 1
	for value >= 128 {
		value >>= 7
		size++
	}
	return size
}

func countIntersectingSortedSignatures[T cmp.Ordered](batches [][]T, candidate []T) int {
	matches := 0
	for _, batch := range batches {
		existingIndex, candidateIndex := 0, 0
		for existingIndex < len(batch) && candidateIndex < len(candidate) {
			if batch[existingIndex] == candidate[candidateIndex] {
				matches++
				break
			}
			if batch[existingIndex] < candidate[candidateIndex] {
				existingIndex++
			} else {
				candidateIndex++
			}
		}
	}
	return matches
}

func modelColdScanLatency(entry coldScanCase, assumptions benchAssumptions) coldScanLatency {
	pointWaveMs := func(keys int) float64 {
		return float64((keys+assumptions.Concurrency-1)/assumptions.Concurrency) * assumptions.RTTMilliseconds
	}
	transferMs := func(bytes uint64) float64 {
		return float64(bytes) / (1 << 20) / assumptions.TransferMiBPerSecond * 1000
	}
	hydration := pointWaveMs(entry.InFlight)
	reads := pointWaveMs(entry.InFlight)
	idTransfer := transferMs(entry.SortedIDs.SerializedBytes)
	nameTransfer := transferMs(entry.SortedNames.SerializedBytes)
	return coldScanLatency{
		BatchHydrationMs:   hydration,
		SignatureReadsMs:   reads,
		IDTransferMs:       idTransfer,
		NameTransferMs:     nameTransfer,
		IDTotalMs:          hydration + reads + idTransfer + entry.SortedIDs.QueryMicroseconds/1000,
		NameTotalMs:        hydration + reads + nameTransfer + entry.SortedNames.QueryMicroseconds/1000,
		IDRegistryReadMs:   pointWaveMs(entry.CandidateTargets),
		IDRegistryCreateMs: pointWaveMs(entry.CandidateTargets),
	}
}
