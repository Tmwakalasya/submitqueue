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
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
)

type decodedTangoBatch struct {
	targets   []optimizedTarget
	names     map[int32]string
	ruleTypes map[int32]string
}

func encodeIDSignature(indices []uint32, registeredIDs []uint64) []byte {
	ids := make([]uint64, len(indices))
	for i, index := range indices {
		ids[i] = registeredIDs[index]
	}
	slices.Sort(ids)
	blob := make([]byte, len(ids)*8)
	for i, id := range ids {
		binary.LittleEndian.PutUint64(blob[i*8:], id)
	}
	return blob
}

func decodeIDSignature(blob []byte) ([]uint64, error) {
	if len(blob)%8 != 0 {
		return nil, fmt.Errorf("ID signature length %d is not divisible by 8", len(blob))
	}
	ids := make([]uint64, len(blob)/8)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(blob[i*8:])
		if i > 0 && ids[i] < ids[i-1] {
			return nil, fmt.Errorf("ID signature is not sorted at index %d", i)
		}
	}
	return ids, nil
}

func encodeNameSignature(indices []uint32, names []string) []byte {
	sorted := make([]string, len(indices))
	size := 0
	for i, index := range indices {
		sorted[i] = names[index]
		size += len(sorted[i]) + unsignedVarintBytes(uint64(len(sorted[i])))
	}
	slices.Sort(sorted)
	blob := make([]byte, 0, size)
	for _, name := range sorted {
		blob = binary.AppendUvarint(blob, uint64(len(name)))
		blob = append(blob, name...)
	}
	return blob
}

func decodeNameSignature(blob []byte, targetCount int) ([]string, error) {
	names := make([]string, 0, targetCount)
	for len(blob) > 0 {
		size, prefix := binary.Uvarint(blob)
		if prefix <= 0 || size > uint64(len(blob[prefix:])) {
			return nil, fmt.Errorf("invalid length-prefixed target name")
		}
		name := string(blob[prefix : prefix+int(size)])
		if len(names) > 0 && name < names[len(names)-1] {
			return nil, fmt.Errorf("target-name signature is not sorted")
		}
		names = append(names, name)
		blob = blob[prefix+int(size):]
	}
	if len(names) != targetCount {
		return nil, fmt.Errorf("got %d target names, expected %d", len(names), targetCount)
	}
	return names, nil
}

func newDecodedTangoBatch(targetCount int) decodedTangoBatch {
	return decodedTangoBatch{
		targets:   make([]optimizedTarget, 0, targetCount),
		names:     make(map[int32]string, targetCount*3),
		ruleTypes: make(map[int32]string, 1),
	}
}

func decodeTangoFrame(frame []byte, batch *decodedTangoBatch) error {
	return walkFields(frame, func(field protowire.Number, _ protowire.Type, value []byte, _ uint64) error {
		switch field {
		case 1:
			return walkFields(value, func(entry protowire.Number, _ protowire.Type, target []byte, _ uint64) error {
				if entry != 1 {
					return nil
				}
				decoded, err := decodeOptimizedTarget(target)
				if err != nil {
					return err
				}
				batch.targets = append(batch.targets, decoded)
				return nil
			})
		case 2:
			return walkFields(value, func(mapping protowire.Number, _ protowire.Type, entry []byte, _ uint64) error {
				if mapping != 1 && mapping != 2 {
					return nil
				}
				id, name, err := decodeTangoMapping(entry)
				if err != nil {
					return err
				}
				if mapping == 1 {
					batch.names[id] = name
				} else {
					batch.ruleTypes[id] = name
				}
				return nil
			})
		}
		return nil
	})
}

func decodeOptimizedTarget(encoded []byte) (optimizedTarget, error) {
	var target optimizedTarget
	err := walkFields(encoded, func(field protowire.Number, _ protowire.Type, value []byte, number uint64) error {
		switch field {
		case 1:
			if number == 0 || number > math.MaxInt32 {
				return fmt.Errorf("invalid target ID %d", number)
			}
			target.id = int32(number)
		case 3:
			for len(value) > 0 {
				dependency, n := protowire.ConsumeVarint(value)
				if n < 0 {
					return protowire.ParseError(n)
				}
				if dependency == 0 || dependency > math.MaxInt32 {
					return fmt.Errorf("invalid dependency ID %d", dependency)
				}
				target.directDependencies = append(target.directDependencies, int32(dependency))
				value = value[n:]
			}
		case 5:
			if number > math.MaxInt32 {
				return fmt.Errorf("invalid rule type ID %d", number)
			}
			target.ruleType = int32(number)
		case 6:
			target.root = number != 0
		case 7:
			target.external = number != 0
		}
		return nil
	})
	if err != nil {
		return optimizedTarget{}, err
	}
	if target.id == 0 {
		return optimizedTarget{}, fmt.Errorf("optimized target has no ID")
	}
	return target, nil
}

func decodeTangoMapping(encoded []byte) (int32, string, error) {
	var id int32
	var name string
	err := walkFields(encoded, func(field protowire.Number, _ protowire.Type, value []byte, number uint64) error {
		switch field {
		case 1:
			if number == 0 || number > math.MaxInt32 {
				return fmt.Errorf("invalid metadata ID %d", number)
			}
			id = int32(number)
		case 2:
			name = string(value)
		}
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	if id == 0 || name == "" {
		return 0, "", fmt.Errorf("incomplete target metadata mapping")
	}
	return id, name, nil
}

func validateDecodedTangoBatch(batch decodedTangoBatch, targetCount int) error {
	if len(batch.targets) != targetCount {
		return fmt.Errorf("got %d targets, expected %d", len(batch.targets), targetCount)
	}
	for _, target := range batch.targets {
		if _, exists := batch.names[target.id]; !exists {
			return fmt.Errorf("target %d has no response-local name mapping", target.id)
		}
		if _, exists := batch.ruleTypes[target.ruleType]; !exists {
			return fmt.Errorf("target %d has no response-local rule type", target.id)
		}
	}
	return nil
}

func gzipTangoFrame(frame []byte) ([]byte, error) {
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(frame); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func decodeGzipTangoFrame(compressed []byte, batch *decodedTangoBatch) error {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return err
	}
	frame, readErr := io.ReadAll(io.LimitReader(reader, 4_250_001))
	closeErr := reader.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(frame) > 4_250_000 {
		return fmt.Errorf("decoded Tango frame exceeds limit")
	}
	return decodeTangoFrame(frame, batch)
}
