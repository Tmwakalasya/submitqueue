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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func streamedTargets(targets ...[]byte) []byte {
	var out []byte
	for _, target := range targets {
		out = binary.AppendUvarint(out, uint64(len(target)))
		out = append(out, target...)
	}
	return out
}

func bazelRule(name, ruleType string, deps ...string) []byte {
	rule := appendStringField(nil, 1, name)
	rule = appendStringField(rule, 2, ruleType)
	for _, dep := range deps {
		rule = appendStringField(rule, 5, dep)
	}
	target := appendVarintField(nil, 1, 1)
	return appendBytesField(target, 2, rule)
}

func bazelFile(kind uint64, name string) []byte {
	inner := appendStringField(nil, 1, name)
	target := appendVarintField(nil, 1, kind)
	return appendBytesField(target, protowire.Number(kind+1), inner)
}

func TestReadStreamedProto(t *testing.T) {
	attrs := appendStringField(nil, 1, "name")
	attrs = appendVarintField(attrs, 2, 2)
	attrs = appendStringField(attrs, 5, "lib")
	tags := appendVarintField(nil, 2, 5)
	tags = appendStringField(tags, 6, "manual")
	tags = appendStringField(tags, 1, "tags")
	tags = appendStringField(tags, 6, "large")

	rule := appendStringField(nil, 1, "//app:lib")
	rule = appendStringField(rule, 2, "go_library")
	rule = appendBytesField(rule, 4, attrs)
	rule = appendBytesField(rule, 4, tags)
	rule = appendStringField(rule, 5, "//app:file.go")
	rule = appendStringField(rule, 5, "//app:app/resources") // Tango ignores these pseudo-labels.
	in := streamedTargets(
		appendBytesField(appendVarintField(nil, 1, 1), 2, rule),
		bazelFile(2, "//app:file.go"),
		bazelRule("@@external//:lib", "go_library"),
		bazelRule("//external:repo", "http_archive"),
	)
	for _, tt := range []struct {
		name  string
		scope string
		count int
	}{
		{"full graph", "", 4},
		{"app subtree", "//app:", 2},
		{"no matches", "//nope:", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			graph, err := readStreamedProto(bytes.NewReader(in), tt.scope)
			require.NoError(t, err)
			assert.Equal(t, uint64(4), graph.scanned)
			require.Len(t, graph.targets, tt.count)
			if tt.count == 2 {
				assert.Equal(t, []string{"//app:file.go"}, graph.targets[0].deps)
				assert.Equal(t, []string{"manual", "large"}, graph.targets[0].tags)
				assert.Equal(t, []stringAttribute{{name: "name", value: "lib"}}, graph.targets[0].attributes)
			} else if tt.scope == "" {
				assert.Equal(t, "external rule", graph.targets[3].ruleType)
			}
		})
	}
}

func TestReadStreamedProtoRejectsCorruption(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"short target body", []byte{5, 0x08, 0x01}},
		{"short size varint", []byte{0x80}},
		{"oversized target", binary.AppendUvarint(nil, maxTargetMessageBytes+1)},
		{"malformed protobuf", streamedTargets([]byte{0xff})},
		{"unknown Bazel target", streamedTargets(appendVarintField(nil, 1, 5))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readStreamedProto(bytes.NewReader(tt.data), "")
			require.Error(t, err)
		})
	}
}

func TestGeneratedFileReferencesGeneratingRule(t *testing.T) {
	file := appendStringField(nil, 1, "//app:generated.go")
	file = appendStringField(file, 2, "//app:generator")
	target := appendBytesField(appendVarintField(nil, 1, 3), 4, file)
	graph, err := readStreamedProto(bytes.NewReader(streamedTargets(target)), "")
	require.NoError(t, err)
	require.Len(t, graph.targets, 1)
	assert.Equal(t, "generated file", graph.targets[0].ruleType)
	assert.Equal(t, []string{"//app:generator"}, graph.targets[0].deps)
}
