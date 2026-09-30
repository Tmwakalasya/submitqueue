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

// tangograph-eval estimates the size of Tango's graph responses from a Bazel
// streamed_proto query, without running Tango or calculating target hashes.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
)

type report struct {
	Input                   string                `json:"input"`
	InputBytes              uint64                `json:"bazel_streamed_proto_input_bytes"`
	ScopePrefix             string                `json:"scope_prefix,omitempty"`
	Query                   string                `json:"bazel_query,omitempty"`
	GoCodeRevision          string                `json:"go_code_revision,omitempty"`
	TangoRevision           string                `json:"tango_revision,omitempty"`
	GoVersion               string                `json:"go_version"`
	MaxMessageBytes         int                   `json:"max_protobuf_message_bytes"`
	ImpactSourcePrefix      string                `json:"impact_source_prefix,omitempty"`
	Graph                   graphMeasurement      `json:"sample"`
	Projected               *projectedMeasurement `json:"linear_projection,omitempty"`
	ProjectionQualification string                `json:"projection_qualification,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "tangograph-eval:", err)
		os.Exit(1)
	}
}

func run(args []string, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("tangograph-eval", flag.ContinueOnError)
	flags.SetOutput(errOut)
	input := flags.String("input", "-", "Bazel --output=streamed_proto file, or - for stdin")
	scope := flags.String("scope", "", "keep only Bazel labels with this prefix (for projection by subtree)")
	query := flags.String("query", "", "Bazel query expression for the report provenance")
	revision := flags.String("go-code-revision", "", "go-code git commit for the report provenance")
	tangoRevision := flags.String("tango-revision", "", "Tango git commit whose wire schema is modeled")
	sampledFiles := flags.Uint64("sample-build-files", 0, "tracked BUILD.bazel files in -scope")
	totalFiles := flags.Uint64("total-build-files", 0, "tracked BUILD.bazel files in the full repo")
	maxMessageBytes := flags.Int("max-message-bytes", 4_250_000, "limit for each modeled Tango protobuf message")
	impactSeeds := flags.Int("impact-seeds", 64, "number of evenly distributed source-file nodes to test for reverse-closure overlap (0 to skip)")
	impactPrefix := flags.String("impact-source-prefix", "", "select source-file change seeds under this label prefix (default: all main-repo source files)")
	benchmark := flags.Bool("benchmark", false, "run synthetic per-batch impact benchmarks on labels sampled from the Bazel input")
	benchmarkColdScan := flags.Bool("benchmark-cold-scan", false, "measure one-candidate stateless full scans of stored ID and name signatures")
	benchmarkTargets := flags.String("benchmark-targets", "100,1000", "comma-separated affected targets per synthetic batch")
	benchmarkBatches := flags.String("benchmark-batches", "100,500,1000", "comma-separated in-flight batch counts")
	benchmarkRTT := flags.Float64("benchmark-rtt-ms", 5, "illustrative per-key storage read/write RTT in milliseconds")
	benchmarkConcurrency := flags.Int("benchmark-concurrency", 16, "illustrative concurrent per-key storage operations")
	benchmarkTransfer := flags.Float64("benchmark-transfer-mib", 100, "illustrative uncompressed storage throughput in MiB/s")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *maxMessageBytes < 128 {
		return fmt.Errorf("-max-message-bytes must be at least 128")
	}
	if *benchmark && *benchmarkColdScan {
		return fmt.Errorf("-benchmark and -benchmark-cold-scan cannot be used together")
	}
	if *impactSeeds < 0 || *impactSeeds > 256 {
		return fmt.Errorf("-impact-seeds must be between 0 and 256")
	}
	if *impactPrefix == "" && *scope != "" {
		*impactPrefix = *scope
	}
	if (*sampledFiles == 0) != (*totalFiles == 0) || *sampledFiles > *totalFiles {
		return fmt.Errorf("both BUILD file counts must be positive, and the sample count cannot exceed the total")
	}
	if (*sampledFiles != 0 || *totalFiles != 0) && *scope == "" {
		return fmt.Errorf("a linear projection requires -scope")
	}
	source := io.Reader(os.Stdin)
	if *input != "-" {
		f, err := os.Open(*input)
		if err != nil {
			return err
		}
		defer f.Close()
		source = f
	}
	counter := &countingReader{source: source}
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	graph, err := readStreamedProto(counter, *scope)
	if err != nil {
		return err
	}
	if *benchmark {
		measured, err := benchmarkBatchImpacts(graph, *benchmarkBatches, *benchmarkTargets, benchAssumptions{
			RTTMilliseconds:      *benchmarkRTT,
			Concurrency:          *benchmarkConcurrency,
			TransferMiBPerSecond: *benchmarkTransfer,
		})
		if err != nil {
			return err
		}
		measured.Input = *input
		measured.InputBytes = counter.bytes
		measured.GoCodeRevision = *revision
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(measured)
	}
	if *benchmarkColdScan {
		measured, err := benchmarkColdStatelessScans(graph, *benchmarkBatches, *benchmarkTargets, benchAssumptions{
			RTTMilliseconds:      *benchmarkRTT,
			Concurrency:          *benchmarkConcurrency,
			TransferMiBPerSecond: *benchmarkTransfer,
		})
		if err != nil {
			return err
		}
		measured.Input = *input
		measured.InputBytes = counter.bytes
		measured.GoCodeRevision = *revision
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(measured)
	}
	measured, err := measureGraph(graph, baseline.HeapAlloc, *maxMessageBytes, *impactSeeds, *impactPrefix)
	if err != nil {
		return err
	}
	result := report{
		Input:              *input,
		InputBytes:         counter.bytes,
		ScopePrefix:        *scope,
		Query:              *query,
		GoCodeRevision:     *revision,
		TangoRevision:      *tangoRevision,
		GoVersion:          runtime.Version(),
		MaxMessageBytes:    *maxMessageBytes,
		ImpactSourcePrefix: *impactPrefix,
		Graph:              measured,
	}
	if *sampledFiles != 0 {
		projection := projectGraph(measured, *totalFiles, *sampledFiles)
		result.Projected = &projection
		result.ProjectionQualification = "Linear extrapolation of an observed subset, not a full Bazel or Tango run; excludes cross-scope dependencies and may misestimate external repositories, label lengths, tags, hash cycles, and BUILD files Bazel ignores."
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

type countingReader struct {
	source io.Reader
	bytes  uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.source.Read(p)
	c.bytes += uint64(n)
	return n, err
}
