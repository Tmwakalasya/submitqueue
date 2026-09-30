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
	"fmt"
	"runtime"
	"slices"
	"strings"
	"time"
)

type loadBenchMode struct {
	BlobBytes               uint64  `json:"batch_blob_bytes"`
	DecodedHeapBytes        uint64  `json:"decoded_go_heap_bytes"`
	DeserializeMilliseconds float64 `json:"deserialize_milliseconds"`
	CheckMicroseconds       float64 `json:"check_microseconds"`
	CheckAllocBytes         uint64  `json:"check_alloc_bytes"`
	MatchingBatches         int     `json:"matching_batches"`
}

type loadBenchTango struct {
	RawBlobBytes      uint64  `json:"raw_batch_blob_bytes"`
	GzipBlobBytes     uint64  `json:"gzip_batch_blob_bytes"`
	DecodedHeapBytes  uint64  `json:"decoded_go_heap_bytes"`
	RawDeserializeMs  float64 `json:"raw_deserialize_milliseconds"`
	GzipDeserializeMs float64 `json:"gzip_deserialize_milliseconds"`
	CheckMicroseconds float64 `json:"check_microseconds"`
	CheckAllocBytes   uint64  `json:"check_alloc_bytes"`
	MatchingBatches   int     `json:"matching_batches"`
}

type loadBenchCase struct {
	InFlight            int            `json:"in_flight_batches"`
	TargetsPerBatch     int            `json:"targets_per_batch"`
	CandidateTargets    int            `json:"candidate_targets"`
	NameRegistryEntries int            `json:"id_registry_entries"`
	NameRegistryBytes   uint64         `json:"id_registry_minimal_bytes"`
	SortedIDs           loadBenchMode  `json:"id64"`
	SortedNames         loadBenchMode  `json:"namekey"`
	TangoSnapshot       loadBenchTango `json:"tango_snapshot"`
}

type loadBenchReport struct {
	Input          string          `json:"input"`
	InputBytes     uint64          `json:"bazel_streamed_proto_input_bytes"`
	GoCodeRevision string          `json:"go_code_revision,omitempty"`
	GoVersion      string          `json:"go_version"`
	GraphTargets   uint64          `json:"graph_targets"`
	MainLabels     int             `json:"main_repo_labels"`
	Notes          []string        `json:"notes"`
	Cases          []loadBenchCase `json:"cases"`
}

func benchmarkLoadAndCheck(graph queryGraph, batchesText, targetsText string) (loadBenchReport, error) {
	batchCounts, err := parseBenchmarkCounts(batchesText)
	if err != nil {
		return loadBenchReport{}, err
	}
	targetCounts, err := parseBenchmarkCounts(targetsText)
	if err != nil {
		return loadBenchReport{}, err
	}
	report := loadBenchReport{
		GoVersion:    runtime.Version(),
		GraphTargets: graph.scanned,
		Notes: []string{
			"One new batch versus B in-flight batches with K synthetic uniformly sampled affected targets in each; source labels and capped dependency degrees come from the complete go-code Bazel query, but changed sets and Tango dependency neighbors are synthetic.",
			"ID64 stores sorted fixed-width uint64 values; NameKey stores sorted UTF-8 target names prefixed by unsigned-varint lengths; TangoSnapshot stores framed default-field GetTargetGraphResponse-shaped protobuf messages, optionally gzip/best-speed per response message.",
			"ID64 includes a minimal sparse ID-to-name dictionary containing one eight-byte ID, unsigned-varint length, and exact canonical label per distinct target in all B batches and the candidate. This excludes real database row/key overhead and historically registered labels.",
			"Go heap is measured after GC on actual decoded B signatures plus candidate, excluding the shared source label pool, benchmark workload indices, temporary serialized buffers, and ID64's dictionary held in storage.",
			"Deserialize timing is local Go CPU to decode every B signature from the generated blob bytes, including validation; it excludes blob GET/transfer and concurrent decode scheduling. Tango gzip timing includes actual gzip decompression and the same protobuf parsing.",
			"Check timing is local Go CPU for one candidate intersecting all B already-decoded batches, independent of whether the TangoSnapshot blob was stored raw or gzip.",
			"Blob bytes exclude per-batch object-store framing, object keys, base-epoch/policy headers, checksums, metadata and the new candidate's optional persisted signature. TangoSnapshot is not an observed GetChangedTargets response; actual old/new detail can be larger.",
		},
	}
	var labels []benchLabel
	for _, target := range graph.targets {
		if strings.HasPrefix(target.name, "//") && !strings.HasPrefix(target.name, "//external:") {
			labels = append(labels, benchLabel{name: target.name, degree: uint8(min(32, len(target.deps)))})
		}
	}
	graph.targets = nil
	if len(labels) == 0 {
		return loadBenchReport{}, fmt.Errorf("no main-repository target labels in input")
	}
	report.MainLabels = len(labels)
	for _, count := range targetCounts {
		if count > len(labels) {
			return loadBenchReport{}, fmt.Errorf("%d targets per batch exceeds %d available labels", count, len(labels))
		}
	}

	// This dictionary is benchmark preparation, not part of any one request.
	registeredIDs := make([]uint64, len(labels))
	registry := make(map[uint64]string)
	for i, label := range labels {
		registeredIDs[i] = registerTargetID(label.name, registry)
	}
	registry = nil
	runtime.GC()

	for _, batches := range batchCounts {
		for _, targets := range targetCounts {
			workload := newBenchWorkload(len(labels), batches, targets)
			entry := loadBenchCase{
				InFlight:         batches,
				TargetsPerBatch:  targets,
				CandidateTargets: len(workload.candidate),
			}
			entry.NameRegistryEntries, entry.NameRegistryBytes = minimalIDRegistryBytes(labels, workload)
			entry.SortedIDs, err = benchmarkDecodedIDs(registeredIDs, workload)
			if err != nil {
				return loadBenchReport{}, err
			}
			runtime.GC()
			entry.SortedNames, err = benchmarkDecodedNames(labels, workload)
			if err != nil {
				return loadBenchReport{}, err
			}
			runtime.GC()
			entry.TangoSnapshot, err = benchmarkDecodedTango(labels, workload)
			if err != nil {
				return loadBenchReport{}, err
			}
			if entry.SortedIDs.MatchingBatches != entry.SortedNames.MatchingBatches ||
				entry.SortedIDs.MatchingBatches != entry.TangoSnapshot.MatchingBatches {
				return loadBenchReport{}, fmt.Errorf("decoded modes disagree on matches for batches=%d targets=%d", batches, targets)
			}
			report.Cases = append(report.Cases, entry)
			runtime.GC()
			runtime.KeepAlive(workload)
		}
	}
	runtime.KeepAlive(registeredIDs)
	runtime.KeepAlive(labels)
	return report, nil
}

func minimalIDRegistryBytes(labels []benchLabel, workload benchWorkload) (int, uint64) {
	seen := make([]bool, len(labels))
	var entries int
	var bytes uint64
	record := func(index uint32) {
		if seen[index] {
			return
		}
		seen[index] = true
		entries++
		length := len(labels[index].name)
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
	for i, indices := range workload.batches {
		blob := encodeIDSignature(indices, registeredIDs)
		result.BlobBytes += uint64(len(blob))
		start := time.Now()
		decoded, err := decodeIDSignature(blob)
		result.DeserializeMilliseconds += float64(time.Since(start).Microseconds()) / 1000
		if err != nil {
			return loadBenchMode{}, err
		}
		data.batches[i] = decoded
	}
	data.candidate = make([]uint64, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = registeredIDs[index]
	}
	slices.Sort(data.candidate)
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.CheckAllocBytes, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(workload)
	return result, nil
}

func benchmarkDecodedNames(labels []benchLabel, workload benchWorkload) (loadBenchMode, error) {
	baseline := liveHeap()
	data := struct {
		batches   [][]string
		candidate []string
	}{
		batches: make([][]string, len(workload.batches)),
	}
	var result loadBenchMode
	for i, indices := range workload.batches {
		blob := encodeNameSignature(indices, labels)
		result.BlobBytes += uint64(len(blob))
		start := time.Now()
		decoded, err := decodeNameSignature(blob, len(indices))
		result.DeserializeMilliseconds += float64(time.Since(start).Microseconds()) / 1000
		if err != nil {
			return loadBenchMode{}, err
		}
		data.batches[i] = decoded
	}
	data.candidate = make([]string, len(workload.candidate))
	for i, index := range workload.candidate {
		data.candidate[i] = strings.Clone(labels[index].name)
	}
	slices.Sort(data.candidate)
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.CheckAllocBytes, result.MatchingBatches = benchmarkLookup(func() int {
		return countIntersectingSortedSignatures(data.batches, data.candidate)
	})
	runtime.KeepAlive(data)
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result, nil
}

func benchmarkDecodedTango(labels []benchLabel, workload benchWorkload) (loadBenchTango, error) {
	baseline := liveHeap()
	data := struct {
		batches   []decodedTangoBatch
		candidate map[string]struct{}
	}{
		batches:   make([]decodedTangoBatch, len(workload.batches)),
		candidate: make(map[string]struct{}, len(workload.candidate)),
	}
	for _, index := range workload.candidate {
		data.candidate[labels[index].name] = struct{}{}
	}
	var result loadBenchTango
	for i, indices := range workload.batches {
		batch, _ := buildSyntheticTangoBatch(labels, indices)
		graph := compactGraph{
			targets:   batch.targets,
			names:     batch.names,
			ruleTypes: map[int32]string{1: "go_library"},
		}
		var rawFrames, gzipFrames [][]byte
		err := streamProtoMessages(graph, defaultPayload, 4_250_000, func(frame []byte) error {
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
		result.RawDeserializeMs += float64(time.Since(start).Microseconds()) / 1000

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
		result.GzipDeserializeMs += float64(time.Since(start).Microseconds()) / 1000
		data.batches[i] = decoded
		runtime.KeepAlive(raw)
	}
	result.DecodedHeapBytes = heapAboveBaseline(baseline, data)
	result.CheckMicroseconds, result.CheckAllocBytes, result.MatchingBatches = benchmarkLookup(func() int {
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
	runtime.KeepAlive(labels)
	runtime.KeepAlive(workload)
	return result, nil
}
