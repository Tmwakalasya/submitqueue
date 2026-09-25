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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// The Bazel query field numbers are from src/main/protobuf/build.proto.
// Parsing only the fields Tango's ResultToTargetGraph consumes avoids pulling
// Bazel's build-proto Go dependency into SubmitQueue.
type queryTarget struct {
	name       string
	ruleType   string
	deps       []string
	tags       []string
	attributes []stringAttribute
}

type stringAttribute struct {
	name  string
	value string
}

type queryGraph struct {
	targets []queryTarget
	scanned uint64
}

const maxTargetMessageBytes = 64 << 20

func readStreamedProto(r io.Reader, scope string) (queryGraph, error) {
	reader := bufio.NewReaderSize(r, 1<<20)
	var graph queryGraph
	for {
		if _, err := reader.Peek(1); err == io.EOF {
			return graph, nil
		} else if err != nil {
			return queryGraph{}, fmt.Errorf("peek at next target: %w", err)
		}
		size, err := binary.ReadUvarint(reader)
		if err != nil {
			return queryGraph{}, fmt.Errorf("read target size: %w", err)
		}
		if size > maxTargetMessageBytes {
			return queryGraph{}, fmt.Errorf("target message size %d exceeds %d", size, maxTargetMessageBytes)
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(reader, payload); err != nil {
			return queryGraph{}, fmt.Errorf("read target %d: %w", graph.scanned+1, err)
		}
		target, err := parseTarget(payload)
		if err != nil {
			return queryGraph{}, fmt.Errorf("decode target %d: %w", graph.scanned+1, err)
		}
		graph.scanned++
		if scope == "" || strings.HasPrefix(target.name, scope) {
			graph.targets = append(graph.targets, target)
		}
	}
}

func parseTarget(data []byte) (queryTarget, error) {
	var kind uint64
	var rule, source, generated, group []byte
	err := walkFields(data, func(field protowire.Number, wireType protowire.Type, value []byte, number uint64) error {
		switch field {
		case 1:
			kind = number
		case 2:
			rule = value
		case 3:
			source = value
		case 4:
			generated = value
		case 5:
			group = value
		}
		return nil
	})
	if err != nil {
		return queryTarget{}, err
	}
	switch kind {
	case 1:
		return parseRule(rule)
	case 2:
		return parseNamedTarget(source, "source file")
	case 3:
		return parseGeneratedFile(generated)
	case 4:
		return parseNamedTarget(group, "package group")
	default:
		return queryTarget{}, fmt.Errorf("unsupported Bazel target type %d", kind)
	}
}

func parseNamedTarget(data []byte, ruleType string) (queryTarget, error) {
	var target queryTarget
	target.ruleType = ruleType
	err := walkFields(data, func(field protowire.Number, _ protowire.Type, value []byte, _ uint64) error {
		if field == 1 {
			target.name = string(value)
		}
		return nil
	})
	if err != nil {
		return queryTarget{}, err
	}
	if target.name == "" {
		return queryTarget{}, fmt.Errorf("%s without a name", ruleType)
	}
	return target, nil
}

func parseGeneratedFile(data []byte) (queryTarget, error) {
	target, err := parseNamedTarget(data, "generated file")
	if err != nil {
		return queryTarget{}, err
	}
	err = walkFields(data, func(field protowire.Number, _ protowire.Type, value []byte, _ uint64) error {
		if field == 2 {
			target.deps = append(target.deps, string(value))
		}
		return nil
	})
	return target, err
}

func parseRule(data []byte) (queryTarget, error) {
	var target queryTarget
	err := walkFields(data, func(field protowire.Number, _ protowire.Type, value []byte, _ uint64) error {
		switch field {
		case 1:
			target.name = string(value)
		case 2:
			target.ruleType = string(value)
		case 4:
			attribute, tags, err := parseAttribute(value)
			if err != nil {
				return err
			}
			if attribute.name != "" {
				target.attributes = append(target.attributes, attribute)
			}
			if tags != nil {
				target.tags = tags
			}
		case 5:
			dep := string(value)
			if !isDoubledPackageLabel(dep) {
				target.deps = append(target.deps, dep)
			}
		}
		return nil
	})
	if err != nil {
		return queryTarget{}, err
	}
	if target.name == "" || target.ruleType == "" {
		return queryTarget{}, fmt.Errorf("Bazel rule without a name or rule class")
	}
	if strings.HasPrefix(target.name, "//external:") {
		target.ruleType = "external rule"
		target.tags = nil
		target.attributes = nil
	}
	return target, nil
}

func isDoubledPackageLabel(label string) bool {
	workspace := strings.Index(label, "//")
	if workspace < 0 {
		return false
	}
	parts := strings.SplitN(label[workspace+2:], ":", 2)
	return len(parts) == 2 && parts[0] != "" && strings.HasPrefix(parts[1], parts[0]+"/")
}

func parseAttribute(data []byte) (stringAttribute, []string, error) {
	var name, stringValue string
	var kind uint64
	var hasStringValue bool
	err := walkFields(data, func(field protowire.Number, _ protowire.Type, value []byte, number uint64) error {
		switch field {
		case 1:
			name = string(value)
		case 2:
			kind = number
		case 5:
			stringValue = string(value)
			hasStringValue = true
		}
		return nil
	})
	if err != nil {
		return stringAttribute{}, nil, err
	}
	if kind == 2 && hasStringValue {
		return stringAttribute{name: name, value: stringValue}, nil, nil
	}
	if kind == 5 && name == "tags" {
		var tags []string
		err := walkFields(data, func(field protowire.Number, _ protowire.Type, value []byte, _ uint64) error {
			if field == 6 {
				tags = append(tags, string(value))
			}
			return nil
		})
		return stringAttribute{}, tags, err
	}
	return stringAttribute{}, nil, nil
}

func walkFields(data []byte, visit func(protowire.Number, protowire.Type, []byte, uint64) error) error {
	for len(data) != 0 {
		field, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return protowire.ParseError(tagBytes)
		}
		data = data[tagBytes:]
		var value []byte
		var number uint64
		var consumed int
		switch wireType {
		case protowire.BytesType:
			value, consumed = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			number, consumed = protowire.ConsumeVarint(data)
		default:
			consumed = protowire.ConsumeFieldValue(field, wireType, data)
		}
		if consumed < 0 {
			return protowire.ParseError(consumed)
		}
		if err := visit(field, wireType, value, number); err != nil {
			return err
		}
		data = data[consumed:]
	}
	return nil
}
