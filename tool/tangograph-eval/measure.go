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
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

type optimizedTarget struct {
	id                 int32
	hash               string
	directDependencies []int32
	ruleType           int32
	tags               []int32
	root               bool
	external           bool
	attributes         map[int32]int32
}

type compactGraph struct {
	targets       []optimizedTarget
	names         map[int32]string
	ruleTypes     map[int32]string
	tags          map[int32]string
	attrNames     map[int32]string
	attrValues    map[int32]string
	missingEdges  uint64
	missingMain   uint64
	missingExtern uint64
	presentEdges  uint64
	uniquePackage uint64
	rules         uint64
	sources       uint64
	generated     uint64
	packageGroups uint64
	external      uint64
}

type dictionary struct {
	ids   map[string]int32
	names map[int32]string
}

func newDictionary(capacity int) dictionary {
	return dictionary{
		ids:   make(map[string]int32, capacity),
		names: make(map[int32]string, capacity),
	}
}

func (d dictionary) id(name string) int32 {
	if n, ok := d.ids[name]; ok {
		return n
	}
	n := int32(len(d.ids) + 1)
	d.ids[name] = n
	d.names[n] = name
	return n
}

func compactTargets(nodes []queryTarget) (compactGraph, error) {
	if len(nodes) > math.MaxInt32 {
		return compactGraph{}, fmt.Errorf("cannot assign int32 IDs to %d targets", len(nodes))
	}
	slices.SortFunc(nodes, func(a, b queryTarget) int { return strings.Compare(a.name, b.name) })
	names := newDictionary(len(nodes))
	for _, node := range nodes {
		if _, exists := names.ids[node.name]; exists {
			return compactGraph{}, fmt.Errorf("duplicate Bazel target %q", node.name)
		}
		names.id(node.name)
	}
	ruleTypes := newDictionary(64)
	tags := newDictionary(64)
	attrNames := newDictionary(64)
	attrValues := newDictionary(256)
	graph := compactGraph{targets: make([]optimizedTarget, 0, len(nodes))}
	referenced := make([]bool, len(nodes)+1)
	packages := make(map[string]struct{})
	for _, node := range nodes {
		t := optimizedTarget{
			id:       names.ids[node.name],
			ruleType: ruleTypes.id(node.ruleType),
			external: strings.HasPrefix(node.name, "@") || strings.HasPrefix(node.name, "//external:"),
		}
		switch node.ruleType {
		case "source file":
			graph.sources++
		case "generated file":
			graph.generated++
		case "package group":
			graph.packageGroups++
		default:
			graph.rules++
		}
		if t.external {
			graph.external++
		}
		if colon := strings.IndexByte(node.name, ':'); colon >= 0 {
			packages[node.name[:colon]] = struct{}{}
		}
		for _, dependency := range node.deps {
			if id, exists := names.ids[dependency]; exists {
				t.directDependencies = append(t.directDependencies, id)
				referenced[id] = true
				graph.presentEdges++
			} else {
				graph.missingEdges++
				if strings.HasPrefix(dependency, "@") {
					graph.missingExtern++
				} else {
					graph.missingMain++
				}
			}
		}
		for _, tag := range node.tags {
			t.tags = append(t.tags, tags.id(tag))
		}
		for _, attribute := range node.attributes {
			if t.attributes == nil {
				t.attributes = make(map[int32]int32, len(node.attributes))
			}
			t.attributes[attrNames.id(attribute.name)] = attrValues.id(attribute.value)
		}
		// The upstream hash is content-derived; a label SHA-1 is used only
		// to model its 40-byte hex-encoded, near-incompressible wire shape.
		digest := sha1.Sum([]byte(node.name))
		t.hash = hex.EncodeToString(digest[:])
		graph.targets = append(graph.targets, t)
	}
	for i := range graph.targets {
		t := &graph.targets[i]
		t.root = !referenced[t.id] &&
			nodes[i].ruleType != "source file" &&
			nodes[i].ruleType != "package group"
	}
	graph.names = names.names
	graph.ruleTypes = ruleTypes.names
	graph.tags = tags.names
	graph.attrNames = attrNames.names
	graph.attrValues = attrValues.names
	graph.uniquePackage = uint64(len(packages))
	return graph, nil
}

type wireMeasurement struct {
	Bytes     uint64 `json:"protobuf_bytes"`
	GzipBytes uint64 `json:"gzip_best_speed_bytes"`
	Messages  uint64 `json:"messages"`
	MaxBytes  uint64 `json:"largest_message_bytes"`
}

type graphMeasurement struct {
	ScannedTargets    uint64            `json:"bazel_query_targets_scanned"`
	Targets           uint64            `json:"targets"`
	RuleTargets       uint64            `json:"rule_targets"`
	SourceFiles       uint64            `json:"source_files"`
	GeneratedFiles    uint64            `json:"generated_files"`
	PackageGroups     uint64            `json:"package_groups"`
	ExternalTargets   uint64            `json:"external_targets"`
	UniquePackages    uint64            `json:"unique_packages_in_query"`
	IncludedEdges     uint64            `json:"included_edges"`
	ExcludedEdges     uint64            `json:"edges_to_targets_outside_scope"`
	ExcludedMainEdges uint64            `json:"excluded_main_repo_edges"`
	ExcludedExternal  uint64            `json:"excluded_external_repo_edges"`
	HeapNamed         uint64            `json:"go_heap_parsed_graph_bytes"`
	HeapCompact       uint64            `json:"go_heap_compact_graph_bytes"`
	HeapFingerprints  uint64            `json:"go_heap_sorted_fingerprints_bytes"`
	FingerprintBytes  uint64            `json:"sorted_fingerprints_raw_bytes"`
	TangoDefault      wireMeasurement   `json:"tango_default"`
	TangoAllFields    wireMeasurement   `json:"tango_all_fields_model"`
	TangoNamesOnly    wireMeasurement   `json:"tango_names_only_model"`
	Impact            impactMeasurement `json:"synthetic_source_change_overlap"`
}

type projectedMeasurement struct {
	TotalBuildFiles         uint64  `json:"total_tracked_build_files"`
	SampledBuildFiles       uint64  `json:"sampled_tracked_build_files"`
	ScalingFactor           float64 `json:"linear_scaling_factor"`
	Targets                 uint64  `json:"estimated_targets"`
	IncludedEdges           uint64  `json:"estimated_in_sample_edges"`
	ExcludedMainEdges       uint64  `json:"estimated_cross_scope_main_repo_edges"`
	ExcludedExternalEdges   uint64  `json:"estimated_cross_scope_external_edges"`
	DefaultProtoBytes       uint64  `json:"estimated_default_proto_bytes"`
	AllFieldsProtoBytes     uint64  `json:"estimated_all_fields_proto_bytes"`
	NamesOnlyProtoBytes     uint64  `json:"estimated_names_only_proto_bytes"`
	DefaultGzipBytes        uint64  `json:"estimated_default_gzip_bytes"`
	CompactGraphHeapBytes   uint64  `json:"estimated_compact_graph_go_heap_bytes"`
	FingerprintHeapBytes    uint64  `json:"estimated_fingerprint_go_heap_bytes"`
	FingerprintStorageBytes uint64  `json:"estimated_fingerprint_storage_bytes"`
}

func projectGraph(measured graphMeasurement, total, sampled uint64) projectedMeasurement {
	scale := float64(total) / float64(sampled)
	scaled := func(n uint64) uint64 { return uint64(math.Round(float64(n) * scale)) }
	return projectedMeasurement{
		TotalBuildFiles:         total,
		SampledBuildFiles:       sampled,
		ScalingFactor:           scale,
		Targets:                 scaled(measured.Targets),
		IncludedEdges:           scaled(measured.IncludedEdges),
		ExcludedMainEdges:       scaled(measured.ExcludedMainEdges),
		ExcludedExternalEdges:   scaled(measured.ExcludedExternal),
		DefaultProtoBytes:       scaled(measured.TangoDefault.Bytes),
		AllFieldsProtoBytes:     scaled(measured.TangoAllFields.Bytes),
		NamesOnlyProtoBytes:     scaled(measured.TangoNamesOnly.Bytes),
		DefaultGzipBytes:        scaled(measured.TangoDefault.GzipBytes),
		CompactGraphHeapBytes:   scaled(measured.HeapCompact),
		FingerprintHeapBytes:    scaled(measured.HeapFingerprints),
		FingerprintStorageBytes: scaled(measured.FingerprintBytes),
	}
}

// measureGraph counts the modeled protobuf frames, including the response
// wrappers, and measures live Go heap at each stage with intervening GCs.
func measureGraph(graph queryGraph, baseline uint64, maxMessageBytes, impactSeeds int, impactPrefix string) (graphMeasurement, error) {
	measured := graphMeasurement{
		ScannedTargets: graph.scanned,
		Targets:        uint64(len(graph.targets)),
		HeapNamed:      heapAboveBaseline(baseline, graph.targets),
	}
	compact, err := compactTargets(graph.targets)
	if err != nil {
		return graphMeasurement{}, err
	}
	measured.RuleTargets = compact.rules
	measured.SourceFiles = compact.sources
	measured.GeneratedFiles = compact.generated
	measured.PackageGroups = compact.packageGroups
	measured.ExternalTargets = compact.external
	measured.UniquePackages = compact.uniquePackage
	measured.IncludedEdges = compact.presentEdges
	measured.ExcludedEdges = compact.missingEdges
	measured.ExcludedMainEdges = compact.missingMain
	measured.ExcludedExternal = compact.missingExtern
	measured.Impact = measureSourceImpact(compact, impactSeeds, impactPrefix)

	graph.targets = nil
	measured.HeapCompact = heapAboveBaseline(baseline, compact)
	for _, mode := range []payloadMode{defaultPayload, allFieldsPayload, namesOnlyPayload} {
		wire, err := measureProtoStream(compact, mode, maxMessageBytes)
		if err != nil {
			return graphMeasurement{}, err
		}
		switch mode {
		case defaultPayload:
			measured.TangoDefault = wire
		case allFieldsPayload:
			measured.TangoAllFields = wire
		case namesOnlyPayload:
			measured.TangoNamesOnly = wire
		}
	}

	fingerprints := make([]uint64, 0, len(compact.names))
	for _, label := range compact.names {
		fingerprints = append(fingerprints, fingerprint(label))
	}
	sort.Slice(fingerprints, func(i, j int) bool { return fingerprints[i] < fingerprints[j] })
	measured.FingerprintBytes = uint64(len(fingerprints) * 8)
	compact = compactGraph{}
	measured.HeapFingerprints = heapAboveBaseline(baseline, fingerprints)
	return measured, nil
}

func fingerprint(label string) uint64 {
	const offset, prime = uint64(14695981039346656037), uint64(1099511628211)
	hash := offset
	for i := range len(label) {
		hash = (hash ^ uint64(label[i])) * prime
	}
	return hash
}

func heapAboveBaseline(baseline uint64, reachable any) uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	runtime.KeepAlive(reachable)
	if stats.HeapAlloc <= baseline {
		return 0
	}
	return stats.HeapAlloc - baseline
}

type payloadMode uint8

const (
	defaultPayload payloadMode = iota
	allFieldsPayload
	namesOnlyPayload
)

type wireCounter struct{ bytes uint64 }

func (w *wireCounter) Write(p []byte) (int, error) {
	w.bytes += uint64(len(p))
	return len(p), nil
}

func (w *wireMeasurement) addMessage(data []byte) error {
	w.Bytes += uint64(len(data))
	w.Messages++
	w.MaxBytes = max(w.MaxBytes, uint64(len(data)))
	var compressed wireCounter
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	w.GzipBytes += compressed.bytes
	return nil
}

func measureProtoStream(graph compactGraph, mode payloadMode, maxMessageBytes int) (wireMeasurement, error) {
	var measured wireMeasurement
	var targets []byte
	sendTargets := func() error {
		response := appendBytesField(nil, 1, targets)
		targets = nil
		return measured.addMessage(response)
	}
	for _, target := range graph.targets {
		encoded := encodeOptimizedTarget(target, mode)
		entry := appendBytesField(nil, 1, encoded)
		if len(targets) != 0 && len(targets)+len(entry)+10 > maxMessageBytes {
			if err := sendTargets(); err != nil {
				return wireMeasurement{}, err
			}
		}
		targets = append(targets, entry...)
	}
	if err := sendTargets(); err != nil {
		return wireMeasurement{}, err
	}
	var meta []byte
	sendMetadata := func() error {
		response := appendBytesField(nil, 2, meta)
		meta = nil
		return measured.addMessage(response)
	}
	addMapping := func(field protowire.Number, mapping map[int32]string) error {
		for id := int32(1); id <= int32(len(mapping)); id++ {
			entry := appendVarintField(nil, 1, uint64(id))
			entry = appendStringField(entry, 2, mapping[id])
			mapField := appendBytesField(nil, field, entry)
			if len(meta) != 0 && len(meta)+len(mapField)+10 > maxMessageBytes {
				if err := sendMetadata(); err != nil {
					return err
				}
			}
			meta = append(meta, mapField...)
		}
		return nil
	}
	if err := addMapping(1, graph.names); err != nil {
		return wireMeasurement{}, err
	}
	if mode != namesOnlyPayload {
		if err := addMapping(2, graph.ruleTypes); err != nil {
			return wireMeasurement{}, err
		}
	}
	if mode == allFieldsPayload {
		for _, mapping := range []struct {
			field protowire.Number
			items map[int32]string
		}{{3, graph.tags}, {4, graph.attrNames}, {5, graph.attrValues}} {
			if err := addMapping(mapping.field, mapping.items); err != nil {
				return wireMeasurement{}, err
			}
		}
	}
	if err := sendMetadata(); err != nil {
		return wireMeasurement{}, err
	}
	return measured, nil
}

func encodeOptimizedTarget(target optimizedTarget, mode payloadMode) []byte {
	b := appendVarintField(nil, 1, uint64(target.id))
	if mode == namesOnlyPayload {
		return b
	}
	if mode == allFieldsPayload {
		b = appendStringField(b, 2, target.hash)
	}
	if len(target.directDependencies) != 0 {
		var packed []byte
		for _, dep := range target.directDependencies {
			packed = protowire.AppendVarint(packed, uint64(dep))
		}
		b = appendBytesField(b, 3, packed)
	}
	if mode == allFieldsPayload && len(target.tags) != 0 {
		var packed []byte
		for _, tag := range target.tags {
			packed = protowire.AppendVarint(packed, uint64(tag))
		}
		b = appendBytesField(b, 4, packed)
	}
	b = appendVarintField(b, 5, uint64(target.ruleType))
	if target.root {
		b = appendVarintField(b, 6, 1)
	}
	if target.external {
		b = appendVarintField(b, 7, 1)
	}
	if mode == allFieldsPayload {
		for name, value := range target.attributes {
			entry := appendVarintField(nil, 1, uint64(name))
			entry = appendVarintField(entry, 2, uint64(value))
			b = appendBytesField(b, 8, entry)
		}
	}
	return b
}

func appendVarintField(b []byte, field protowire.Number, value uint64) []byte {
	b = protowire.AppendTag(b, field, protowire.VarintType)
	return protowire.AppendVarint(b, value)
}

func appendStringField(b []byte, field protowire.Number, value string) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	return protowire.AppendString(b, value)
}

func appendBytesField(b []byte, field protowire.Number, value []byte) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	return protowire.AppendBytes(b, value)
}

var _ io.Writer = (*wireCounter)(nil)
