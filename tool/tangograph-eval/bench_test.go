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

func TestParseBenchmarkCounts(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  []int
		valid bool
	}{
		{name: "two counts", input: "100, 500", want: []int{100, 500}, valid: true},
		{name: "zero", input: "100,0"},
		{name: "non-numeric", input: "one"},
		{name: "empty", input: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			counts, err := parseBenchmarkCounts(tt.input)
			if tt.valid {
				require.NoError(t, err)
				assert.Equal(t, tt.want, counts)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestRegisteredTargetIDMatchesName(t *testing.T) {
	names := make(map[uint64]string)
	one := registerTargetID("//app:one", names)
	other := registerTargetID("//app:other", names)
	assert.NotZero(t, one)
	assert.NotEqual(t, one, other)
	assert.Equal(t, one, registerTargetID("//app:one", names))
	assert.Equal(t, "//app:one", names[one])
	assert.Len(t, names, 2)
}

func TestBenchWorkloadIsReproducibleAndUnique(t *testing.T) {
	first := newBenchWorkload(100, 3, 20)
	second := newBenchWorkload(100, 3, 20)
	assert.Equal(t, first.batches, second.batches)
	assert.Equal(t, first.candidate, second.candidate)
	for _, batch := range first.batches {
		seen := make(map[uint32]bool)
		for _, index := range batch {
			assert.False(t, seen[index])
			seen[index] = true
		}
	}
}

func TestBenchImpactModesAgreeOnMatches(t *testing.T) {
	graph := queryGraph{scanned: 10}
	for i := range 10 {
		graph.targets = append(graph.targets, queryTarget{
			name: "//app:target" + string(rune('a'+i)),
			deps: []string{"//app:source"},
		})
	}
	result, err := benchmarkBatchImpacts(graph, "2", "2", benchAssumptions{
		RTTMilliseconds:      5,
		Concurrency:          16,
		TransferMiBPerSecond: 100,
	})
	require.NoError(t, err)
	require.Len(t, result.Cases, 1)
	assert.Equal(t, result.Cases[0].RegisteredIDIndex.Matches, result.Cases[0].TangoLikeScan.Matches)
	assert.Equal(t, result.Cases[0].LabelStringIndex.Matches, result.Cases[0].TangoLikeScan.Matches)
	assert.Positive(t, result.Cases[0].TangoLikeScan.ModeledProtobufBytes)
	assert.Positive(t, result.Cases[0].TangoLikeScan.ModeledGzipBytes)
	assert.Positive(t, result.Cases[0].LatencyModel.CommonBatchHydrationMs)
}
