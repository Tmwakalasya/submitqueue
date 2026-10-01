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
	"math"
	"slices"
)

// The scale model extends the measured cases to larger B. Every measured cost is fitted
// as perTarget·T, where T is the total affected targets across the B batches; T is then drawn by Monte Carlo from the request histogram, so tail requests
// enter at their observed frequency rather than as a mean.

// blobStoreProfile is an assumed, not measured, per-controller fetch model.
type blobStoreProfile struct {
	Name         string  `json:"name"`
	FirstByteMs  float64 `json:"first_byte_ms"`
	Parallel     int     `json:"parallel_gets"`
	MiBPerSecond float64 `json:"mib_per_second"`
}

var defaultBlobStoreProfiles = []blobStoreProfile{
	{Name: "object-store", FirstByteMs: 150, Parallel: 32, MiBPerSecond: 100},
	{Name: "kv-point-read", FirstByteMs: 5, Parallel: 16, MiBPerSecond: 100},
}

type linearFit struct {
	PerBatch  float64 `json:"per_batch"`
	PerTarget float64 `json:"per_target"`
}

func (f linearFit) at(batches int, targets float64) float64 {
	return f.PerBatch*float64(batches) + f.PerTarget*targets
}

// fitPerTarget is the ratio estimator Σy/ΣT over the measured cases. A two-term fit
// with a per-batch coefficient is rejected: B and T rise together across cases, so the
// split is unidentifiable and attributed hundreds of KiB to batches whose median is 42
// targets. Per-batch round trips are charged separately in loadSeconds.
func fitPerTarget(targets, y []float64) linearFit {
	var sumTargets, sumY float64
	for i := range y {
		sumTargets += targets[i]
		sumY += y[i]
	}
	if sumTargets == 0 {
		return linearFit{}
	}
	return linearFit{PerTarget: sumY / sumTargets}
}

type designCost struct {
	bytes, heap, decodeMs, checkUs linearFit
}

type scaleDesign struct {
	Name          string    `json:"name"`
	BlobBytes     linearFit `json:"blob_bytes"`
	HeapBytes     linearFit `json:"decoded_heap_bytes"`
	DeserializeMs linearFit `json:"deserialize_milliseconds"`
	CheckUs       linearFit `json:"check_microseconds"`
	cost          designCost
}

type scaleDesignPoint struct {
	Name        string       `json:"name"`
	BlobMiB     [2]float64   `json:"blob_mib_p50_p99"`
	HeapMiB     [2]float64   `json:"heap_mib_p50_p99"`
	LoadSeconds [][2]float64 `json:"load_seconds_p50_p99_by_profile"`
	CheckMs     [2]float64   `json:"check_ms_p50_p99"`
}

type scalePoint struct {
	InFlight int                `json:"in_flight_batches"`
	Targets  [2]float64         `json:"total_targets_p50_p99"`
	Designs  []scaleDesignPoint `json:"designs"`
}

type scaleBudget struct {
	Metric string  `json:"metric"`
	Limit  float64 `json:"limit"`
}

type scaleCrossover struct {
	Design   string  `json:"design"`
	Metric   string  `json:"metric"`
	Profile  string  `json:"profile,omitempty"`
	Limit    float64 `json:"limit"`
	P50Batch int     `json:"first_in_flight_exceeding_at_p50"`
	P99Batch int     `json:"first_in_flight_exceeding_at_p99"`
}

type scaleReport struct {
	Profiles   []blobStoreProfile `json:"blob_store_profiles"`
	Trials     int                `json:"monte_carlo_trials"`
	Designs    []scaleDesign      `json:"fitted_designs"`
	Points     []scalePoint       `json:"points"`
	Crossovers []scaleCrossover   `json:"crossovers"`
}

var scaleDisplayBatches = []int{100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000}

var scaleBudgets = []scaleBudget{
	{Metric: "load_seconds", Limit: 1},
	{Metric: "load_seconds", Limit: 10},
	{Metric: "load_seconds", Limit: 60},
	{Metric: "heap_mib", Limit: 1024},
	{Metric: "heap_mib", Limit: 4096},
	{Metric: "heap_mib", Limit: 16384},
}

func fittedScaleDesigns(cases []loadBenchCase) []scaleDesign {
	targets := make([]float64, len(cases))
	for i, c := range cases {
		targets[i] = float64(c.TotalTargets)
	}
	fit := func(value func(loadBenchCase) float64) linearFit {
		y := make([]float64, len(cases))
		for i, c := range cases {
			y[i] = value(c)
		}
		return fitPerTarget(targets, y)
	}
	id := designCost{
		bytes:    fit(func(c loadBenchCase) float64 { return float64(c.SortedIDs.BlobBytes) }),
		heap:     fit(func(c loadBenchCase) float64 { return float64(c.SortedIDs.DecodedHeapBytes) }),
		decodeMs: fit(func(c loadBenchCase) float64 { return c.SortedIDs.DeserializeMilliseconds }),
		checkUs:  fit(func(c loadBenchCase) float64 { return c.SortedIDs.CheckMicroseconds }),
	}
	names := designCost{
		bytes:    fit(func(c loadBenchCase) float64 { return float64(c.SortedNames.BlobBytes) }),
		heap:     fit(func(c loadBenchCase) float64 { return float64(c.SortedNames.DecodedHeapBytes) }),
		decodeMs: fit(func(c loadBenchCase) float64 { return c.SortedNames.DeserializeMilliseconds }),
		checkUs:  fit(func(c loadBenchCase) float64 { return c.SortedNames.CheckMicroseconds }),
	}
	tangoHeap := fit(func(c loadBenchCase) float64 { return float64(c.TangoSnapshot.DecodedHeapBytes) })
	tangoCheck := fit(func(c loadBenchCase) float64 { return c.TangoSnapshot.CheckMicroseconds })
	tangoRaw := designCost{
		bytes:    fit(func(c loadBenchCase) float64 { return float64(c.TangoSnapshot.RawBlobBytes) }),
		heap:     tangoHeap,
		decodeMs: fit(func(c loadBenchCase) float64 { return c.TangoSnapshot.RawDeserializeMs }),
		checkUs:  tangoCheck,
	}
	tangoGzip := designCost{
		bytes:    fit(func(c loadBenchCase) float64 { return float64(c.TangoSnapshot.GzipBlobBytes) }),
		heap:     tangoHeap,
		decodeMs: fit(func(c loadBenchCase) float64 { return c.TangoSnapshot.GzipDeserializeMs }),
		checkUs:  tangoCheck,
	}
	var designs []scaleDesign
	for _, d := range []struct {
		name string
		cost designCost
	}{
		{"TangoSnapshot raw", tangoRaw},
		{"TangoSnapshot gzip", tangoGzip},
		{"NameKey", names},
		{"ID64", id},
	} {
		designs = append(designs, scaleDesign{
			Name:          d.name,
			BlobBytes:     d.cost.bytes,
			HeapBytes:     d.cost.heap,
			DeserializeMs: d.cost.decodeMs,
			CheckUs:       d.cost.checkUs,
			cost:          d.cost,
		})
	}
	return designs
}

func loadSeconds(d designCost, profile blobStoreProfile, batches int, targets float64) float64 {
	return math.Ceil(float64(batches)/float64(profile.Parallel))*profile.FirstByteMs/1000 +
		d.bytes.at(batches, targets)/(1<<20)/profile.MiBPerSecond + d.decodeMs.at(batches, targets)/1000
}

// sampledTotalTargets returns the 50th and 99th percentile of T over trials draws of
// batches requests each.
func sampledTotalTargets(h targetHistogram, batches, trials int, limit int, seed uint64) [2]float64 {
	totals := make([]float64, trials)
	state := seed
	for trial := range totals {
		var total int
		for range batches {
			state++
			total += min(limit, h.sample(splitMix64(state)))
		}
		totals[trial] = float64(total)
	}
	slices.Sort(totals)
	return [2]float64{totals[trials/2], totals[trials*99/100]}
}

func modelScale(cases []loadBenchCase, h targetHistogram, profiles []blobStoreProfile, trials int) scaleReport {
	const graphLimit = math.MaxInt
	report := scaleReport{Profiles: profiles, Trials: trials, Designs: fittedScaleDesigns(cases)}
	if len(cases) == 0 {
		return report
	}
	for _, batches := range scaleDisplayBatches {
		targets := sampledTotalTargets(h, batches, trials, graphLimit, uint64(batches))
		point := scalePoint{InFlight: batches, Targets: targets}
		for _, d := range report.Designs {
			dp := scaleDesignPoint{Name: d.Name}
			for q := range 2 {
				dp.BlobMiB[q] = d.cost.bytes.at(batches, targets[q]) / (1 << 20)
				dp.HeapMiB[q] = d.cost.heap.at(batches, targets[q]) / (1 << 20)
				dp.CheckMs[q] = d.cost.checkUs.at(batches, targets[q]) / 1000
			}
			for _, profile := range profiles {
				dp.LoadSeconds = append(dp.LoadSeconds, [2]float64{
					loadSeconds(d.cost, profile, batches, targets[0]),
					loadSeconds(d.cost, profile, batches, targets[1]),
				})
			}
			point.Designs = append(point.Designs, dp)
		}
		report.Points = append(report.Points, point)
	}

	var grid []int
	for b := 10.0; b <= 100_000; b *= 1.05 {
		if n := int(math.Round(b)); len(grid) == 0 || n != grid[len(grid)-1] {
			grid = append(grid, n)
		}
	}
	gridTargets := make([][2]float64, len(grid))
	for i, batches := range grid {
		gridTargets[i] = sampledTotalTargets(h, batches, trials, graphLimit, 0xc0ffee+uint64(batches))
	}
	first := func(exceeds func(batches int, targets float64) bool, q int) int {
		for i, batches := range grid {
			if exceeds(batches, gridTargets[i][q]) {
				return batches
			}
		}
		return 0
	}
	for _, d := range report.Designs {
		for _, budget := range scaleBudgets {
			if budget.Metric == "heap_mib" {
				exceeds := func(batches int, targets float64) bool {
					return d.cost.heap.at(batches, targets)/(1<<20) > budget.Limit
				}
				report.Crossovers = append(report.Crossovers, scaleCrossover{
					Design: d.Name, Metric: budget.Metric, Limit: budget.Limit,
					P50Batch: first(exceeds, 0), P99Batch: first(exceeds, 1),
				})
				continue
			}
			for _, profile := range profiles {
				exceeds := func(batches int, targets float64) bool {
					return loadSeconds(d.cost, profile, batches, targets) > budget.Limit
				}
				report.Crossovers = append(report.Crossovers, scaleCrossover{
					Design: d.Name, Metric: budget.Metric, Profile: profile.Name, Limit: budget.Limit,
					P50Batch: first(exceeds, 0), P99Batch: first(exceeds, 1),
				})
			}
		}
	}
	return report
}
