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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountIntersectingSortedSignatures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		batches   [][]uint64
		candidate []uint64
		want      int
	}{
		{name: "overlap", batches: [][]uint64{{1, 3}, {2, 4}, {3, 5}}, candidate: []uint64{3, 7}, want: 2},
		{name: "disjoint", batches: [][]uint64{{1, 3}, {2, 4}}, candidate: []uint64{5, 7}},
		{name: "empty", batches: [][]uint64{{}, {1}}, candidate: []uint64{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, countIntersectingSortedSignatures(tt.batches, tt.candidate))
		})
	}
	assert.Equal(t, 1, countIntersectingSortedSignatures(
		[][]string{{"//app:a"}, {"//app:b"}},
		[]string{"//app:b"},
	))
}

func TestUnsignedVarintBytes(t *testing.T) {
	for _, tt := range []struct {
		value uint64
		want  int
	}{
		{value: 0, want: 1},
		{value: 127, want: 1},
		{value: 128, want: 2},
		{value: 16384, want: 3},
	} {
		assert.Equal(t, tt.want, unsignedVarintBytes(tt.value))
	}
}

func TestBenchmarkColdStatelessScans(t *testing.T) {
	graph := queryGraph{scanned: 25}
	for i := range 25 {
		graph.targets = append(graph.targets, queryTarget{name: "//app:target" + string(rune('a'+i))})
	}
	result, err := benchmarkColdStatelessScans(graph, "2,5", "3", benchAssumptions{
		RTTMilliseconds:      5,
		Concurrency:          4,
		TransferMiBPerSecond: 100,
	})
	require.NoError(t, err)
	require.Len(t, result.Cases, 2)
	for _, entry := range result.Cases {
		assert.Equal(t, uint64(entry.InFlight*entry.TargetsPerBatch*8), entry.SortedIDs.SerializedBytes)
		assert.Greater(t, entry.SortedNames.SerializedBytes, entry.SortedIDs.SerializedBytes)
		assert.Equal(t, entry.SortedIDs.MatchingBatches, entry.SortedNames.MatchingBatches)
		assert.Positive(t, entry.SortedIDs.LiveHeapBytes)
		assert.Positive(t, entry.SortedNames.LiveHeapBytes)
		assert.Greater(t, entry.Latency.IDTotalMs, entry.Latency.BatchHydrationMs+entry.Latency.SignatureReadsMs)
		assert.Greater(t, entry.Latency.NameTotalMs, entry.Latency.IDTotalMs)
	}
}

func TestBenchmarkColdStatelessScansRejectsInvalidInputs(t *testing.T) {
	graph := queryGraph{targets: []queryTarget{{name: "//app:a"}}, scanned: 1}
	for _, tt := range []struct {
		name        string
		batches     string
		targets     string
		assumptions benchAssumptions
	}{
		{name: "invalid batch count", batches: "0", targets: "1", assumptions: benchAssumptions{RTTMilliseconds: 5, Concurrency: 16, TransferMiBPerSecond: 100}},
		{name: "invalid target count", batches: "1", targets: "0", assumptions: benchAssumptions{RTTMilliseconds: 5, Concurrency: 16, TransferMiBPerSecond: 100}},
		{name: "invalid throughput", batches: "1", targets: "1", assumptions: benchAssumptions{RTTMilliseconds: 5, Concurrency: 16}},
		{name: "too many targets", batches: "1", targets: "2", assumptions: benchAssumptions{RTTMilliseconds: 5, Concurrency: 16, TransferMiBPerSecond: 100}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := benchmarkColdStatelessScans(graph, tt.batches, tt.targets, tt.assumptions)
			require.Error(t, err)
		})
	}
}
