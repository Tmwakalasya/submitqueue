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
		labels := []benchLabel{{name: "//app:one"}, {name: "//app:two"}, {name: "//other:three"}}
		encoded := encodeNameSignature([]uint32{2, 0, 1}, labels)
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

func TestBenchmarkLoadAndCheck(t *testing.T) {
	graph := queryGraph{scanned: 25}
	for i := range 25 {
		graph.targets = append(graph.targets, queryTarget{
			name: "//app:target" + string(rune('a'+i)),
			deps: []string{"//app:source"},
		})
	}
	result, err := benchmarkLoadAndCheck(graph, "2,5", "3")
	require.NoError(t, err)
	require.Len(t, result.Cases, 2)
	for _, entry := range result.Cases {
		assert.Equal(t, uint64(entry.InFlight*entry.TargetsPerBatch*8), entry.SortedIDs.BlobBytes)
		assert.Greater(t, entry.SortedNames.BlobBytes, entry.SortedIDs.BlobBytes)
		assert.Positive(t, entry.TangoSnapshot.RawBlobBytes)
		assert.Positive(t, entry.TangoSnapshot.GzipBlobBytes)
		assert.Positive(t, entry.NameRegistryBytes)
		assert.Positive(t, entry.SortedIDs.DecodedHeapBytes)
		assert.Positive(t, entry.SortedNames.DecodedHeapBytes)
		assert.Positive(t, entry.TangoSnapshot.DecodedHeapBytes)
		assert.Equal(t, entry.SortedIDs.MatchingBatches, entry.SortedNames.MatchingBatches)
		assert.Equal(t, entry.SortedIDs.MatchingBatches, entry.TangoSnapshot.MatchingBatches)
	}
}

func TestMinimalIDRegistryBytesDeduplicatesBatchAndCandidateLabels(t *testing.T) {
	labels := []benchLabel{{name: "//a:one"}, {name: "//b:two"}}
	workload := benchWorkload{
		batches:   [][]uint32{{0, 1}, {0}},
		candidate: []uint32{0, 1},
	}
	count, bytes := minimalIDRegistryBytes(labels, workload)
	assert.Equal(t, 2, count)
	assert.Equal(t, uint64(2*(8+1+len("//a:one"))), bytes)
}

func TestBenchmarkLoadAndCheckRejectsInvalidWorkload(t *testing.T) {
	graph := queryGraph{targets: []queryTarget{{name: "//app:one"}}, scanned: 1}
	for _, tt := range []struct {
		name    string
		batches string
		targets string
	}{
		{name: "invalid batches", batches: "0", targets: "1"},
		{name: "invalid target count", batches: "1", targets: "0"},
		{name: "too many targets", batches: "1", targets: "2"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := benchmarkLoadAndCheck(graph, tt.batches, tt.targets)
			require.Error(t, err)
		})
	}
}
