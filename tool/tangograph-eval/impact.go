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
	"math/bits"
	"slices"
	"strings"
)

type impactMeasurement struct {
	CandidateSources   uint64 `json:"eligible_main_repo_source_files"`
	SourceSeeds        uint64 `json:"sampled_source_file_seeds"`
	Pairs              uint64 `json:"source_seed_pairs"`
	FullOverlapPairs   uint64 `json:"full_reverse_closure_overlap_pairs"`
	DirectOverlapPairs uint64 `json:"one_hop_overlap_pairs"`
	MedianFullTargets  uint64 `json:"median_full_closure_targets"`
	P95FullTargets     uint64 `json:"p95_full_closure_targets"`
	MaxFullTargets     uint64 `json:"max_full_closure_targets"`
}

func measureSourceImpact(graph compactGraph, limit int, prefix string) impactMeasurement {
	var sources []int32
	for _, target := range graph.targets {
		if graph.ruleTypes[target.ruleType] == "source file" &&
			!target.external &&
			(prefix == "" || strings.HasPrefix(graph.names[target.id], prefix)) {
			sources = append(sources, target.id)
		}
	}
	if limit == 0 || len(sources) == 0 {
		return impactMeasurement{CandidateSources: uint64(len(sources))}
	}
	n := min(limit, len(sources))
	reverseDeps := make([][]int32, len(graph.targets)+1)
	for _, target := range graph.targets {
		for _, dep := range target.directDependencies {
			reverseDeps[dep] = append(reverseDeps[dep], target.id)
		}
	}
	full := make([][]uint64, n)
	direct := make([][]uint64, n)
	sizes := make([]int, 0, n)
	for i := range n {
		source := sources[i*len(sources)/n]
		full[i] = reverseClosure(reverseDeps, source, false)
		direct[i] = reverseClosure(reverseDeps, source, true)
		size := 0
		for _, word := range full[i] {
			size += bits.OnesCount64(word)
		}
		sizes = append(sizes, size)
	}
	slices.Sort(sizes)
	result := impactMeasurement{
		CandidateSources:  uint64(len(sources)),
		SourceSeeds:       uint64(n),
		MedianFullTargets: uint64(sizes[n/2]),
		P95FullTargets:    uint64(sizes[(n-1)*95/100]),
		MaxFullTargets:    uint64(sizes[n-1]),
	}
	for i := range n {
		for j := i + 1; j < n; j++ {
			result.Pairs++
			if intersectBitsets(full[i], full[j]) {
				result.FullOverlapPairs++
			}
			if intersectBitsets(direct[i], direct[j]) {
				result.DirectOverlapPairs++
			}
		}
	}
	return result
}

func reverseClosure(reverseDeps [][]int32, start int32, oneHop bool) []uint64 {
	found := make([]uint64, (len(reverseDeps)+63)/64)
	set := func(id int32) bool {
		word, mask := id/64, uint64(1)<<(id%64)
		if found[word]&mask != 0 {
			return false
		}
		found[word] |= mask
		return true
	}
	set(start)
	if oneHop {
		for _, id := range reverseDeps[start] {
			set(id)
		}
		return found
	}
	queue := []int32{start}
	for head := 0; head < len(queue); head++ {
		for _, id := range reverseDeps[queue[head]] {
			if set(id) {
				queue = append(queue, id)
			}
		}
	}
	return found
}

func intersectBitsets(a, b []uint64) bool {
	for i := range a {
		if a[i]&b[i] != 0 {
			return true
		}
	}
	return false
}

func intersectSortedFingerprints(a, b []uint64) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			return true
		}
	}
	return false
}
