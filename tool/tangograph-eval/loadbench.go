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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

type loadBenchMode struct {
	BlobBytes               uint64  `json:"batch_blob_bytes"`
	MaxBlobBytes            uint64  `json:"max_single_batch_blob_bytes"`
	DecodedHeapBytes        uint64  `json:"decoded_go_heap_bytes"`
	DeserializeMilliseconds float64 `json:"deserialize_milliseconds"`
	CheckMicroseconds       float64 `json:"check_microseconds"`
	MatchingBatches         int     `json:"matching_batches"`
}

type loadBenchTango struct {
	RawBlobBytes      uint64  `json:"raw_batch_blob_bytes"`
	GzipBlobBytes     uint64  `json:"gzip_batch_blob_bytes"`
	DecodedHeapBytes  uint64  `json:"decoded_go_heap_bytes"`
	RawDeserializeMs  float64 `json:"raw_deserialize_milliseconds"`
	GzipDeserializeMs float64 `json:"gzip_deserialize_milliseconds"`
	CheckMicroseconds float64 `json:"check_microseconds"`
	MatchingBatches   int     `json:"matching_batches"`
}

type loadBenchCase struct {
	InFlight            int            `json:"in_flight_batches"`
	TotalTargets        uint64         `json:"total_batch_targets"`
	MedianBatchTargets  int            `json:"median_batch_targets"`
	MaxBatchTargets     int            `json:"max_batch_targets"`
	CandidateTargets    int            `json:"candidate_targets"`
	NameRegistryEntries int            `json:"id_registry_entries"`
	NameRegistryBytes   uint64         `json:"id_registry_minimal_bytes"`
	SortedIDs           loadBenchMode  `json:"id64"`
	SortedNames         loadBenchMode  `json:"namekey"`
	TangoSnapshot       loadBenchTango `json:"tango_snapshot"`
}

type loadBenchReport struct {
	Input             string          `json:"input"`
	InputBytes        uint64          `json:"bazel_streamed_proto_input_bytes"`
	GoCodeRevision    string          `json:"go_code_revision,omitempty"`
	GoVersion         string          `json:"go_version"`
	GraphTargets      uint64          `json:"graph_targets"`
	SourceSeeds       int             `json:"source_file_seeds"`
	HistogramRequests uint64          `json:"histogram_requests"`
	HistogramMean     float64         `json:"histogram_mean_targets"`
	Notes             []string        `json:"notes"`
	Cases             []loadBenchCase `json:"cases"`
	Scale             scaleReport     `json:"scale_model"`
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

func benchmarkLoadAndCheck(graph queryGraph, histogram targetHistogram, batchesText string, candidateTargets int, store []blobStoreProfile, scaleTrials int) (loadBenchReport, error) {
	batchCounts, err := parseBenchmarkCounts(batchesText)
	if err != nil {
		return loadBenchReport{}, err
	}
	report := loadBenchReport{
		GoVersion:         runtime.Version(),
		GraphTargets:      graph.scanned,
		HistogramRequests: histogram.requests(),
		HistogramMean:     histogram.mean(),
		Notes: []string{
			"One new batch versus B in-flight batches; one batch is one request. Each batch's target count is drawn from the Hive per-request histogram (batch i always draws the same count, so smaller B is a prefix of larger B).",
			"Affected sets are closure-shaped synthetic sets on the complete go-code graph: the reverse-dependency closure of a random source file, widened by climbing to a dependency until the drawn count is reached. They are not observed diffs.",
			"ID64 stores sorted uint64 target IDs; NameKey stores sorted varint-prefixed UTF-8 labels; TangoSnapshot stores default-field GetTargetGraphResponse-shaped protobuf for the affected targets with their real direct dependencies in the response metadata, raw or per-message gzip/best-speed.",
			"Go heap is measured after GC on all B decoded signatures plus the candidate, excluding the shared graph, workload indices, serialized buffers and ID64's dictionary held in storage.",
			"Deserialize time is serial single-goroutine Go CPU for all B blobs; check time is local Go CPU for one candidate against all B decoded batches.",
		},
	}
	if candidateTargets <= 0 || candidateTargets > len(graph.targets) {
		return loadBenchReport{}, fmt.Errorf("candidate targets %d must be between 1 and %d", candidateTargets, len(graph.targets))
	}
	closures := newClosureGraph(graph)
	graph.targets = nil
	report.SourceSeeds = len(closures.sources)
	runtime.GC()

	registeredIDs := make([]uint64, len(closures.names))
	registry := make(map[uint64]string, len(closures.names))
	for i, name := range closures.names {
		registeredIDs[i] = registerTargetID(name, registry)
	}
	registry = nil
	runtime.GC()

	generator := newAffectedSetGenerator(closures)
	maxBatches := slices.Max(batchCounts)
	sizes := sampledBatchTargets(histogram, maxBatches, len(closures.names))
	for _, batches := range batchCounts {
		workload := newSampledWorkload(generator, sizes[:batches], candidateTargets)
		sorted := slices.Sorted(slices.Values(sizes[:batches]))
		entry := loadBenchCase{
			InFlight:           batches,
			TotalTargets:       workload.totalTargets(),
			MedianBatchTargets: sorted[len(sorted)/2],
			MaxBatchTargets:    sorted[len(sorted)-1],
			CandidateTargets:   len(workload.candidate),
		}
		entry.NameRegistryEntries, entry.NameRegistryBytes = minimalIDRegistryBytes(closures.names, workload)
		entry.SortedIDs, err = benchmarkDecodedIDs(registeredIDs, workload)
		if err != nil {
			return loadBenchReport{}, err
		}
		runtime.GC()
		entry.SortedNames, err = benchmarkDecodedNames(closures.names, workload)
		if err != nil {
			return loadBenchReport{}, err
		}
		runtime.GC()
		entry.TangoSnapshot, err = benchmarkDecodedTango(closures, workload)
		if err != nil {
			return loadBenchReport{}, err
		}
		if entry.SortedIDs.MatchingBatches != entry.SortedNames.MatchingBatches ||
			entry.SortedIDs.MatchingBatches != entry.TangoSnapshot.MatchingBatches {
			return loadBenchReport{}, fmt.Errorf("decoded modes disagree on matches for batches=%d", batches)
		}
		report.Cases = append(report.Cases, entry)
		runtime.GC()
	}
	report.Scale = modelScale(report.Cases, histogram, store, scaleTrials)
	runtime.KeepAlive(registeredIDs)
	return report, nil
}

func minimalIDRegistryBytes(names []string, workload benchWorkload) (int, uint64) {
	seen := make([]bool, len(names))
	var entries int
	var bytes uint64
	record := func(index uint32) {
		if seen[index] {
			return
		}
		seen[index] = true
		entries++
		length := len(names[index])
		bytes += uint64(8 + unsignedVarintBytes(uint64(length)) + length)
	}
	for _, batch := range workload.batches {
		for _, index := range batch {
			record(index)
		}
	}
	for _, index := range workload.candidate {
		record(index)
	}
	return entries, bytes
}

func benchmarkDecodedIDs(registeredIDs []uint64, workload benchWorkload) (loadBenchMode, error) {
	baseline := liveHeap()
	data := struct {
		batches   [][]uint64
		candidate []uint64
	}{
		batches: make([][]uint64, len(workload.batches)),
	}
	var result loadBenchMode
	var decode time.Duration
	for i, indices := range workload.batches {
		blob := encodeIDSignature(indices, registeredIDs)
		result.BlobBytes += uint64(len(blob))
		result.MaxBlobBytes = max(result.MaxBlobBytes, uint64(len(blob)))
		start := time.Now()
		decoded, err := decodeIDSignature(blob)
		decode += time.Since(start)
		if err != nil {
			return loadBenchMode{}, err
		}
		data.batches[i] = decoded
	}
	result.DeserializeMilliseconds = milliseconds(decode)
	data.candidate = make([]uint64, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = registeredIDs[index]
	}
	slices.Sort(data.candidate)
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	return result, nil
}

func benchmarkDecodedNames(names []string, workload benchWorkload) (loadBenchMode, error) {
	baseline := liveHeap()
	data := struct {
		batches   [][]string
		candidate []string
	}{
		batches: make([][]string, len(workload.batches)),
	}
	var result loadBenchMode
	var decode time.Duration
	for i, indices := range workload.batches {
		blob := encodeNameSignature(indices, names)
		result.BlobBytes += uint64(len(blob))
		result.MaxBlobBytes = max(result.MaxBlobBytes, uint64(len(blob)))
		start := time.Now()
		decoded, err := decodeNameSignature(blob, len(indices))
		decode += time.Since(start)
		if err != nil {
			return loadBenchMode{}, err
		}
		data.batches[i] = decoded
	}
	result.DeserializeMilliseconds = milliseconds(decode)
	data.candidate = make([]string, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = strings.Clone(names[index])
	}
	slices.Sort(data.candidate)
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	return result, nil
}

func benchmarkDecodedTango(graph closureGraph, workload benchWorkload) (loadBenchTango, error) {
	baseline := liveHeap()
	data := struct {
		batches   []decodedTangoBatch
		candidate map[string]struct{}
	}{
		batches:   make([]decodedTangoBatch, len(workload.batches)),
		candidate: make(map[string]struct{}, len(workload.candidate)),
	}
	for _, index := range workload.candidate {
		data.candidate[graph.names[index]] = struct{}{}
	}
	var result loadBenchTango
	var rawDecode, gzipDecode time.Duration
	for i, indices := range workload.batches {
		var rawFrames, gzipFrames [][]byte
		err := streamProtoMessages(syntheticTangoResponse(graph, indices), defaultPayload, 4_250_000, func(frame []byte) error {
			compressed, err := gzipTangoFrame(frame)
			if err != nil {
				return err
			}
			rawFrames = append(rawFrames, frame)
			gzipFrames = append(gzipFrames, compressed)
			result.RawBlobBytes += uint64(len(frame) + unsignedVarintBytes(uint64(len(frame))))
			result.GzipBlobBytes += uint64(len(compressed) + unsignedVarintBytes(uint64(len(compressed))))
			return nil
		})
		if err != nil {
			return loadBenchTango{}, err
		}
		raw := newDecodedTangoBatch(len(indices))
		start := time.Now()
		for _, frame := range rawFrames {
			if err := decodeTangoFrame(frame, &raw); err != nil {
				return loadBenchTango{}, err
			}
		}
		if err := validateDecodedTangoBatch(raw, len(indices)); err != nil {
			return loadBenchTango{}, err
		}
		rawDecode += time.Since(start)
		runtime.KeepAlive(raw)
		raw = decodedTangoBatch{}

		decoded := newDecodedTangoBatch(len(indices))
		start = time.Now()
		for _, compressed := range gzipFrames {
			if err := decodeGzipTangoFrame(compressed, &decoded); err != nil {
				return loadBenchTango{}, err
			}
		}
		if err := validateDecodedTangoBatch(decoded, len(indices)); err != nil {
			return loadBenchTango{}, err
		}
		gzipDecode += time.Since(start)
		data.batches[i] = decoded
	}
	result.RawDeserializeMs = milliseconds(rawDecode)
	result.GzipDeserializeMs = milliseconds(gzipDecode)
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.MatchingBatches = benchmarkLookup(func() int {
		matches := 0
		for _, batch := range data.batches {
			for _, target := range batch.targets {
				if _, found := data.candidate[batch.names[target.id]]; found {
					matches++
					break
				}
			}
		}
		return matches
	})
	runtime.KeepAlive(data)
	return result, nil
}

// syntheticTangoResponse models a changed-targets response for one batch: one
// default-field optimized target per affected target, with its real direct
// dependencies, and response-local IDs for every named target.
func syntheticTangoResponse(graph closureGraph, indices []uint32) compactGraph {
	targets := make([]optimizedTarget, 0, len(indices))
	names := make(map[int32]string, len(indices)*2)
	ids := make(map[uint32]int32, len(indices)*2)
	idFor := func(index uint32) int32 {
		if id, ok := ids[index]; ok {
			return id
		}
		id := int32(len(ids) + 1)
		ids[index] = id
		names[id] = graph.names[index]
		return id
	}
	for _, index := range indices {
		idFor(index)
	}
	for _, index := range indices {
		target := optimizedTarget{id: ids[index], ruleType: 1}
		for _, dep := range graph.deps[index] {
			target.directDependencies = append(target.directDependencies, idFor(dep))
		}
		targets = append(targets, target)
	}
	return compactGraph{targets: targets, names: names, ruleTypes: map[int32]string{1: "go_library"}}
}

func milliseconds(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func unsignedVarintBytes(value uint64) int {
	n := 1
	for value >= 0x80 {
		value >>= 7
		n++
	}
	return n
}

// countIntersectingSortedSignatures counts batches sharing at least one element with
// the sorted candidate, stopping at the first shared element of each batch.
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

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

// registerTargetID derives a collision-checked 64-bit ID for a label against an
// in-memory stand-in for the durable ID-to-name dictionary.
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
		registry[id] = label
		return id
	}
}

func liveHeap() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// benchmarkLookup returns the mean microseconds per run and the matches of one run.
func benchmarkLookup(run func() int) (float64, int) {
	runtime.GC()
	start := time.Now()
	iterations := 0
	totalMatches := 0
	for iterations < 2 || (time.Since(start) < 200*time.Millisecond && iterations < 1000) {
		totalMatches += run()
		iterations++
	}
	duration := time.Since(start)
	benchResultSink += totalMatches
	return float64(duration.Nanoseconds()) / 1e3 / float64(iterations), totalMatches / iterations
}
