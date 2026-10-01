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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignatureCodecsRoundTrip(t *testing.T) {
	t.Run("sorted numeric IDs", func(t *testing.T) {
		encoded := encodeIDSignature([]uint32{2, 0, 1}, []uint64{30, 10, 20})
		require.Len(t, encoded, 24)
		got, err := decodeIDSignature(encoded)
		require.NoError(t, err)
		assert.Equal(t, []uint64{10, 20, 30}, got)
		_, err = decodeIDSignature(encoded[:23])
		require.Error(t, err)
	})
	t.Run("sorted full names", func(t *testing.T) {
		names := []string{"//app:one", "//app:two", "//other:three"}
		encoded := encodeNameSignature([]uint32{2, 0, 1}, names)
		got, err := decodeNameSignature(encoded, 3)
		require.NoError(t, err)
		assert.Equal(t, []string{"//app:one", "//app:two", "//other:three"}, got)
		_, err = decodeNameSignature(encoded[:len(encoded)-1], 3)
		require.Error(t, err)
		_, err = decodeNameSignature(encoded, 2)
		require.Error(t, err)
	})
}

func TestTangoResponseFrameRoundTrip(t *testing.T) {
	graph := compactGraph{
		targets:   []optimizedTarget{{id: 1, directDependencies: []int32{2}, ruleType: 1}},
		names:     map[int32]string{1: "//app:one", 2: "//app:dependency"},
		ruleTypes: map[int32]string{1: "go_library"},
	}
	var frames [][]byte
	require.NoError(t, streamProtoMessages(graph, defaultPayload, 128, func(frame []byte) error {
		frames = append(frames, frame)
		return nil
	}))
	require.NotEmpty(t, frames)
	raw := newDecodedTangoBatch(1)
	compressed := newDecodedTangoBatch(1)
	for _, frame := range frames {
		require.NoError(t, decodeTangoFrame(frame, &raw))
		gzipFrame, err := gzipTangoFrame(frame)
		require.NoError(t, err)
		require.NoError(t, decodeGzipTangoFrame(gzipFrame, &compressed))
	}
	require.NoError(t, validateDecodedTangoBatch(raw, 1))
	require.NoError(t, validateDecodedTangoBatch(compressed, 1))
	assert.Equal(t, raw, compressed)
	assert.Equal(t, int32(2), raw.targets[0].directDependencies[0])
	assert.Equal(t, "//app:one", raw.names[1])
	assert.Equal(t, "go_library", raw.ruleTypes[1])
	require.Error(t, decodeTangoFrame([]byte{0xff}, &raw))
	require.Error(t, decodeGzipTangoFrame([]byte{0xff}, &compressed))
}

func testChainGraph(n int) queryGraph {
	graph := queryGraph{scanned: uint64(n)}
	graph.targets = append(graph.targets, queryTarget{name: "//app:root.go", ruleType: "source file"})
	for i := 1; i < n; i++ {
		graph.targets = append(graph.targets, queryTarget{
			name:     fmt.Sprintf("//app:t%02d", i),
			ruleType: "go_library",
			deps:     []string{graph.targets[i-1].name},
		})
	}
	return graph
}

func TestBenchmarkLoadAndCheck(t *testing.T) {
	histogram := targetHistogram{values: []int{0, 3, 7}, cumulative: []uint64{1, 3, 4}}
	result, err := benchmarkLoadAndCheck(testChainGraph(25), histogram, "2,5", 4, defaultBlobStoreProfiles, 4)
	require.NoError(t, err)
	require.Len(t, result.Cases, 2)
	for _, entry := range result.Cases {
		assert.Equal(t, entry.TotalTargets*8, entry.SortedIDs.BlobBytes)
		assert.GreaterOrEqual(t, entry.SortedNames.BlobBytes, entry.SortedIDs.BlobBytes)
		assert.Positive(t, entry.TangoSnapshot.RawBlobBytes)
		assert.Positive(t, entry.NameRegistryBytes)
		assert.Equal(t, entry.SortedIDs.MatchingBatches, entry.SortedNames.MatchingBatches)
		assert.Equal(t, entry.SortedIDs.MatchingBatches, entry.TangoSnapshot.MatchingBatches)
		assert.Equal(t, 4, entry.CandidateTargets)
	}
	assert.NotEmpty(t, result.Scale.Points)
	assert.NotEmpty(t, result.Scale.Crossovers)
}

func TestMinimalIDRegistryBytesDeduplicatesBatchAndCandidateLabels(t *testing.T) {
	names := []string{"//a:one", "//b:two"}
	workload := benchWorkload{
		batches:   [][]uint32{{0, 1}, {0}},
		candidate: []uint32{0, 1},
	}
	count, bytes := minimalIDRegistryBytes(names, workload)
	assert.Equal(t, 2, count)
	assert.Equal(t, uint64(2*(8+1+len("//a:one"))), bytes)
}

func TestBenchmarkLoadAndCheckRejectsInvalidWorkload(t *testing.T) {
	histogram := targetHistogram{values: []int{1}, cumulative: []uint64{1}}
	for _, tt := range []struct {
		name      string
		batches   string
		candidate int
	}{
		{name: "invalid batches", batches: "0", candidate: 1},
		{name: "invalid candidate", batches: "1", candidate: 0},
		{name: "candidate larger than graph", batches: "1", candidate: 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := benchmarkLoadAndCheck(testChainGraph(25), histogram, tt.batches, tt.candidate, defaultBlobStoreProfiles, 4)
			require.Error(t, err)
		})
	}
}

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
