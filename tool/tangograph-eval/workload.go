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
	"encoding/csv"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// targetHistogram is the empirical distribution of affected targets per request:
// values[i] targets were observed for counts[i] requests.
type targetHistogram struct {
	values     []int
	cumulative []uint64
}

func readTargetHistogram(r io.Reader) (targetHistogram, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return targetHistogram{}, err
	}
	if len(rows) < 2 {
		return targetHistogram{}, fmt.Errorf("histogram has no rows")
	}
	var h targetHistogram
	var total uint64
	for i, row := range rows[1:] {
		if len(row) != 2 {
			return targetHistogram{}, fmt.Errorf("histogram row %d has %d columns, expected 2", i+1, len(row))
		}
		value, err := strconv.Atoi(strings.TrimSpace(row[0]))
		if err != nil || value < 0 {
			return targetHistogram{}, fmt.Errorf("invalid target count %q in row %d", row[0], i+1)
		}
		count, err := strconv.ParseUint(strings.TrimSpace(row[1]), 10, 64)
		if err != nil || count == 0 {
			return targetHistogram{}, fmt.Errorf("invalid request count %q in row %d", row[1], i+1)
		}
		if len(h.values) > 0 && value <= h.values[len(h.values)-1] {
			return targetHistogram{}, fmt.Errorf("histogram values must be strictly increasing at row %d", i+1)
		}
		total += count
		h.values = append(h.values, value)
		h.cumulative = append(h.cumulative, total)
	}
	return h, nil
}

func (h targetHistogram) requests() uint64 { return h.cumulative[len(h.cumulative)-1] }

func (h targetHistogram) mean() float64 {
	var sum, previous uint64
	for i, value := range h.values {
		sum += uint64(value) * (h.cumulative[i] - previous)
		previous = h.cumulative[i]
	}
	return float64(sum) / float64(h.requests())
}

// sample maps a uniform 64-bit value to a target count, weighting each value by its
// observed request count.
func (h targetHistogram) sample(random uint64) int {
	rank := random % h.requests()
	i, _ := slices.BinarySearchFunc(h.cumulative, rank, func(c, r uint64) int {
		if c <= r {
			return -1
		}
		return 1
	})
	return h.values[i]
}

// closureGraph is the target graph reduced to what workload generation and the
// TangoSnapshot model need: names, real direct dependencies, and reverse edges.
type closureGraph struct {
	names   []string
	deps    [][]uint32
	reverse [][]uint32
	sources []uint32
}

func newClosureGraph(graph queryGraph) closureGraph {
	index := make(map[string]uint32, len(graph.targets))
	g := closureGraph{
		names: make([]string, len(graph.targets)),
		deps:  make([][]uint32, len(graph.targets)),
	}
	for i, target := range graph.targets {
		index[target.name] = uint32(i)
		g.names[i] = target.name
	}
	reverseDegree := make([]uint32, len(graph.targets))
	for i, target := range graph.targets {
		for _, dep := range target.deps {
			if d, ok := index[dep]; ok && d != uint32(i) {
				g.deps[i] = append(g.deps[i], d)
				reverseDegree[d]++
			}
		}
		if target.ruleType == "source file" && strings.HasPrefix(target.name, "//") {
			g.sources = append(g.sources, uint32(i))
		}
	}
	g.reverse = make([][]uint32, len(graph.targets))
	for i, degree := range reverseDegree {
		g.reverse[i] = make([]uint32, 0, degree)
	}
	for i, deps := range g.deps {
		for _, d := range deps {
			g.reverse[d] = append(g.reverse[d], uint32(i))
		}
	}
	return g
}

// affectedSetGenerator draws closure-shaped affected sets: the reverse-dependency
// closure of a random source file, widened by adding the closures of dependencies of
// targets already in the set until k targets are collected. Widening models a change
// to more widely used targets in one region rather than k unrelated targets.
type affectedSetGenerator struct {
	graph closureGraph
	mark  []uint32
	gen   uint32
}

func newAffectedSetGenerator(graph closureGraph) *affectedSetGenerator {
	return &affectedSetGenerator{graph: graph, mark: make([]uint32, len(graph.names))}
}

func (a *affectedSetGenerator) affectedSet(k int, seed uint64) []uint32 {
	a.gen++
	out := make([]uint32, 0, k)
	state := seed
	random := func(n int) int {
		state++
		return int(splitMix64(state) % uint64(n))
	}
	visit := func(start uint32) {
		if a.mark[start] == a.gen || len(out) == k {
			return
		}
		a.mark[start] = a.gen
		out = append(out, start)
		for head := len(out) - 1; head < len(out) && len(out) < k; head++ {
			for _, r := range a.graph.reverse[out[head]] {
				if a.mark[r] != a.gen {
					a.mark[r] = a.gen
					out = append(out, r)
					if len(out) == k {
						return
					}
				}
			}
		}
	}
	misses := 0
	for len(out) < k {
		var node uint32
		if misses < 64 && len(a.graph.sources) > 0 {
			node = a.graph.sources[random(len(a.graph.sources))]
		} else {
			node = uint32(random(len(a.graph.names)))
		}
		if a.mark[node] == a.gen {
			misses++
			continue
		}
		visit(node)
		// Widen from inside the set: a dependency of a member has a reverse closure
		// containing that member, so the set grows around one region of the graph.
		for stalls := 0; len(out) < k && stalls < 64; {
			member := out[random(len(out))]
			deps := a.graph.deps[member]
			if len(deps) == 0 {
				stalls++
				continue
			}
			before := len(out)
			visit(deps[random(len(deps))])
			if len(out) == before {
				stalls++
			} else {
				stalls = 0
			}
		}
	}
	return out
}

type benchWorkload struct {
	batches   [][]uint32
	candidate []uint32
}

func (w benchWorkload) totalTargets() uint64 {
	var total uint64
	for _, batch := range w.batches {
		total += uint64(len(batch))
	}
	return total
}

// sampledBatchTargets draws batch i's size from seed i, so a smaller B is a prefix of
// a larger B and every case shares the same leading batches.
func sampledBatchTargets(h targetHistogram, batches, limit int) []int {
	out := make([]int, batches)
	for i := range out {
		out[i] = min(limit, h.sample(splitMix64(0x5eed0000+uint64(i))))
	}
	return out
}

func newSampledWorkload(gen *affectedSetGenerator, sizes []int, candidateTargets int) benchWorkload {
	w := benchWorkload{batches: make([][]uint32, len(sizes))}
	for i, size := range sizes {
		w.batches[i] = gen.affectedSet(size, uint64(i+1)<<20)
	}
	w.candidate = gen.affectedSet(candidateTargets, 0xd123456789abcdef)
	return w
}
