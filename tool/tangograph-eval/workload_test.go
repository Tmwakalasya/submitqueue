// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTargetHistogram(t *testing.T) {
	h, err := readTargetHistogram(strings.NewReader("affected_targets,diffs\n0,2\n10,1\n100,1\n"))
	require.NoError(t, err)
	assert.Equal(t, uint64(4), h.requests())
	assert.InDelta(t, 27.5, h.mean(), 1e-9)
	counts := map[int]int{}
	for i := range uint64(4000) {
		counts[h.sample(splitMix64(i))]++
	}
	assert.InDelta(t, 2000, counts[0], 200)
	assert.InDelta(t, 1000, counts[10], 150)
	assert.InDelta(t, 1000, counts[100], 150)

	for _, tt := range []struct {
		name  string
		input string
	}{
		{name: "no rows", input: "affected_targets,diffs\n"},
		{name: "negative value", input: "a,b\n-1,1\n"},
		{name: "zero count", input: "a,b\n1,0\n"},
		{name: "unsorted", input: "a,b\n5,1\n4,1\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readTargetHistogram(strings.NewReader(tt.input))
			require.Error(t, err)
		})
	}
}

func TestAffectedSetIsUniqueAndClosureShaped(t *testing.T) {
	gen := newAffectedSetGenerator(newClosureGraph(testChainGraph(25)))
	set := gen.affectedSet(5, 7)
	require.Len(t, set, 5)
	assert.Equal(t, set, gen.affectedSet(5, 7))
	seen := map[uint32]bool{}
	for _, index := range set {
		assert.False(t, seen[index])
		seen[index] = true
	}
	// In a chain every node's dependents are the higher-numbered nodes, so a closure
	// grown from the only source file is the first k nodes.
	assert.Equal(t, []uint32{0, 1, 2, 3, 4}, set)
	assert.Len(t, gen.affectedSet(25, 9), 25)
}

func TestSampledBatchTargetsIsPrefixStable(t *testing.T) {
	h := targetHistogram{values: []int{1, 50, 900}, cumulative: []uint64{5, 8, 9}}
	small := sampledBatchTargets(h, 10, 100)
	large := sampledBatchTargets(h, 20, 100)
	assert.Equal(t, small, large[:10])
	for _, k := range large {
		assert.LessOrEqual(t, k, 100)
	}
}
