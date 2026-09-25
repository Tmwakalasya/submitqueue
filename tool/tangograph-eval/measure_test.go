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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeOptimizedTarget(t *testing.T) {
	target := optimizedTarget{
		id:                 1,
		hash:               "abc",
		directDependencies: []int32{2},
		ruleType:           1,
		tags:               []int32{4},
		root:               true,
		external:           true,
		attributes:         map[int32]int32{3: 5},
	}
	assert.Equal(t, []byte{0x08, 0x01}, encodeOptimizedTarget(target, namesOnlyPayload))
	assert.Equal(t, []byte{0x08, 0x01, 0x1a, 0x01, 0x02, 0x28, 0x01, 0x30, 0x01, 0x38, 0x01},
		encodeOptimizedTarget(target, defaultPayload))
	assert.Equal(t, []byte{
		0x08, 0x01, 0x12, 0x03, 'a', 'b', 'c', 0x1a, 0x01, 0x02,
		0x22, 0x01, 0x04, 0x28, 0x01, 0x30, 0x01, 0x38, 0x01,
		0x42, 0x04, 0x08, 0x03, 0x10, 0x05,
	}, encodeOptimizedTarget(target, allFieldsPayload))
}

func TestCompactTargetsAndWire(t *testing.T) {
	graph, err := compactTargets([]queryTarget{
		{name: "//app:lib", ruleType: "go_library", deps: []string{"//app:file.go", "@@repo//:outside"},
			tags: []string{"manual"}, attributes: []stringAttribute{{name: "note", value: "hi"}}},
		{name: "//app:file.go", ruleType: "source file"},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), graph.presentEdges)
	assert.Equal(t, uint64(1), graph.missingEdges)
	assert.Equal(t, uint64(1), graph.uniquePackage)
	assert.True(t, graph.targets[1].root)
	assert.Len(t, graph.tags, 1)
	assert.Len(t, graph.attrValues, 1)

	for _, tt := range []struct {
		name string
		mode payloadMode
	}{
		{"default", defaultPayload},
		{"all fields", allFieldsPayload},
		{"names only", namesOnlyPayload},
	} {
		t.Run(tt.name, func(t *testing.T) {
			measured, err := measureProtoStream(graph, tt.mode, 128)
			require.NoError(t, err)
			assert.Positive(t, measured.Bytes)
			assert.Positive(t, measured.GzipBytes)
			assert.Equal(t, uint64(2), measured.Messages)
			assert.LessOrEqual(t, measured.MaxBytes, uint64(128))
		})
	}
	allFields, err := measureProtoStream(graph, allFieldsPayload, 128)
	require.NoError(t, err)
	defaultFields, err := measureProtoStream(graph, defaultPayload, 128)
	require.NoError(t, err)
	namesOnly, err := measureProtoStream(graph, namesOnlyPayload, 128)
	require.NoError(t, err)
	assert.Greater(t, allFields.Bytes, defaultFields.Bytes)
	assert.Greater(t, defaultFields.Bytes, namesOnly.Bytes)
}

func TestProtoStreamChunksWithinBudget(t *testing.T) {
	var targets []queryTarget
	for i := range 100 {
		targets = append(targets, queryTarget{name: fmt.Sprintf("//code/module:target%03d", i), ruleType: "go_library"})
	}
	graph, err := compactTargets(targets)
	require.NoError(t, err)
	measurement, err := measureProtoStream(graph, allFieldsPayload, 128)
	require.NoError(t, err)
	assert.Greater(t, measurement.Messages, uint64(2))
	assert.LessOrEqual(t, measurement.MaxBytes, uint64(128))
}

func TestRunReportsScopeAndProjection(t *testing.T) {
	in := streamedTargets(
		bazelRule("//app:lib", "go_library", "//app:file.go", "@@ext//:dep"),
		bazelFile(2, "//app:file.go"),
		bazelRule("@@ext//:dep", "cc_library"),
	)
	input := t.TempDir() + "/query.bin"
	require.NoError(t, os.WriteFile(input, in, 0o600))
	var out, errOut bytes.Buffer
	require.NoError(t, run([]string{
		"-input", input,
		"-scope", "//app:",
		"-sample-build-files", "1",
		"-total-build-files", "3",
	}, &out, &errOut))
	var result report
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	assert.Equal(t, uint64(len(in)), result.InputBytes)
	assert.Equal(t, uint64(3), result.Graph.ScannedTargets)
	assert.Equal(t, uint64(2), result.Graph.Targets)
	assert.Equal(t, uint64(1), result.Graph.IncludedEdges)
	assert.Equal(t, uint64(1), result.Graph.ExcludedEdges)
	require.NotNil(t, result.Projected)
	assert.Equal(t, uint64(6), result.Projected.Targets)
}
