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

import "slices"

// The base-drift simulation checks, on small random DAGs, whether affected-target
// signatures computed once against the newest base at admission (and never recomputed
// as landed batches advance the base) can miss a conflict. Ground truth for a pair of
// changes that were allowed to run independently is "some target is affected by both
// in the first graph that contains both changes".

// driftRule selects how a batch's comparison signature is formed.
type driftRule struct {
	Name string `json:"name"`
	// AddedDeps adds the targets that the change's new or rewired targets newly depend on.
	AddedDeps bool `json:"added_deps"`
	// Inherit unions the comparison signatures of the batch's dependencies at admission.
	Inherit bool `json:"inherit_dependency_signatures"`
	// StructuralOnly limits inheritance to dependencies that add edges or targets.
	StructuralOnly bool `json:"inherit_structural_only"`
}

type driftRuleResult struct {
	Rule driftRule `json:"rule"`
	// SameBasePairs counts independent (in-flight, in-flight) pairs admitted on one base.
	SameBasePairs int `json:"same_base_independent_pairs"`
	// SameBaseMissed counts those pairs whose changes truly share an affected target.
	SameBaseMissed int `json:"same_base_missed_conflicts"`
	// DriftPairs counts independent pairs whose signatures were computed on different bases.
	DriftPairs int `json:"cross_base_independent_pairs"`
	// DriftMissed counts those pairs whose changes truly share an affected target.
	DriftMissed int `json:"cross_base_missed_conflicts"`
	// FalseConflicts counts serialized pairs whose changes share no affected target.
	FalseConflicts int `json:"false_conflicts"`
	// MeanSignature is the mean comparison-signature size in targets.
	MeanSignature float64 `json:"mean_comparison_signature_targets"`
}

type driftReport struct {
	Trials     int               `json:"trials"`
	BaseNodes  int               `json:"base_nodes"`
	Notes      []string          `json:"notes"`
	RuleResult []driftRuleResult `json:"rules"`
}

var driftRules = []driftRule{
	{Name: "affected"},
	{Name: "affected+added-deps", AddedDeps: true},
	{Name: "affected+added-deps+inherit-structural", AddedDeps: true, Inherit: true, StructuralOnly: true},
	{Name: "affected+added-deps+inherit-all", AddedDeps: true, Inherit: true},
}

type driftEdge struct{ from, to int32 }

type driftChange struct {
	touched []int32
	added   []driftEdge
	removed []driftEdge
	// newNodes maps each new target (an ID unique to this change) to its dependencies.
	newNodes map[int32][]int32
}

func (c driftChange) structural() bool { return len(c.added) > 0 || len(c.newNodes) > 0 }

func (c driftChange) direct() []int32 {
	out := slices.Clone(c.touched)
	for _, e := range c.added {
		out = append(out, e.from)
	}
	for _, e := range c.removed {
		out = append(out, e.from)
	}
	for id := range c.newNodes {
		out = append(out, id)
	}
	return out
}

func (c driftChange) addedDeps() []int32 {
	var out []int32
	for _, e := range c.added {
		out = append(out, e.to)
	}
	for _, deps := range c.newNodes {
		out = append(out, deps...)
	}
	return out
}

// driftGraph maps each present target to its direct dependencies.
type driftGraph map[int32][]int32

func (g driftGraph) apply(c driftChange) driftGraph {
	out := make(driftGraph, len(g)+len(c.newNodes))
	for id, deps := range g {
		out[id] = slices.Clone(deps)
	}
	for id, deps := range c.newNodes {
		out[id] = slices.Clone(deps)
	}
	for _, e := range c.removed {
		out[e.from] = slices.DeleteFunc(out[e.from], func(d int32) bool { return d == e.to })
	}
	for _, e := range c.added {
		if !slices.Contains(out[e.from], e.to) {
			out[e.from] = append(out[e.from], e.to)
		}
	}
	return out
}

func (g driftGraph) affected(seeds []int32) map[int32]bool {
	reverse := make(map[int32][]int32, len(g))
	for id, deps := range g {
		for _, d := range deps {
			reverse[d] = append(reverse[d], id)
		}
	}
	found := make(map[int32]bool)
	var queue []int32
	for _, s := range seeds {
		if _, present := g[s]; present && !found[s] {
			found[s] = true
			queue = append(queue, s)
		}
	}
	for head := 0; head < len(queue); head++ {
		for _, r := range reverse[queue[head]] {
			if !found[r] {
				found[r] = true
				queue = append(queue, r)
			}
		}
	}
	return found
}

func intersects(a, b map[int32]bool) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	for id := range a {
		if b[id] {
			return true
		}
	}
	return false
}

type driftRand struct{ state uint64 }

func (r *driftRand) intn(n int) int {
	r.state++
	return int(splitMix64(r.state) % uint64(n))
}

func (r *driftRand) chance(percent int) bool { return r.intn(100) < percent }

func randomDriftBase(r *driftRand, nodes int) driftGraph {
	g := make(driftGraph, nodes)
	for i := range int32(nodes) {
		g[i] = nil
		for range r.intn(3) {
			if i > 0 {
				d := int32(r.intn(int(i)))
				if !slices.Contains(g[i], d) {
					g[i] = append(g[i], d)
				}
			}
		}
	}
	return g
}

// randomDriftChange edits targets of base; edges always point to lower IDs, so every
// combination of changes stays acyclic. New targets get IDs from firstNewID upward.
func randomDriftChange(r *driftRand, base driftGraph, nodes int, firstNewID int32) driftChange {
	c := driftChange{newNodes: map[int32][]int32{}}
	for range 1 + r.intn(2) {
		c.touched = append(c.touched, int32(r.intn(nodes)))
	}
	if r.chance(50) {
		from := int32(1 + r.intn(nodes-1))
		c.added = append(c.added, driftEdge{from: from, to: int32(r.intn(int(from)))})
	}
	if r.chance(20) {
		from := int32(r.intn(nodes))
		if deps := base[from]; len(deps) > 0 {
			c.removed = append(c.removed, driftEdge{from: from, to: deps[r.intn(len(deps))]})
		}
	}
	if r.chance(30) {
		var deps []int32
		for range 1 + r.intn(2) {
			deps = append(deps, int32(r.intn(nodes)))
		}
		c.newNodes[firstNewID] = deps
	}
	return c
}

type driftBatch struct {
	change     driftChange
	signature  map[int32]bool
	dependsOn  []int
	comparison map[int32]bool
}

func driftSignature(base driftGraph, c driftChange, rule driftRule) map[int32]bool {
	sig := base.apply(c).affected(c.direct())
	if rule.AddedDeps {
		for _, d := range c.addedDeps() {
			sig[d] = true
		}
	}
	return sig
}

// admitDriftBatch compares the batch against every in-flight batch and records the
// comparison signature it will be checked with for the rest of its life.
func admitDriftBatch(base driftGraph, c driftChange, inFlight []driftBatch, rule driftRule) driftBatch {
	b := driftBatch{change: c, signature: driftSignature(base, c, rule)}
	b.comparison = b.signature
	for i, other := range inFlight {
		if intersects(b.signature, other.comparison) {
			b.dependsOn = append(b.dependsOn, i)
		}
	}
	if rule.Inherit {
		b.comparison = make(map[int32]bool, len(b.signature))
		for id := range b.signature {
			b.comparison[id] = true
		}
		for _, i := range b.dependsOn {
			if rule.StructuralOnly && !inFlight[i].change.structural() {
				continue
			}
			for id := range inFlight[i].comparison {
				b.comparison[id] = true
			}
		}
	}
	return b
}

func ordered(batches []driftBatch, later, earlier int) bool {
	for _, d := range batches[later].dependsOn {
		if d == earlier || ordered(batches, d, earlier) {
			return true
		}
	}
	return false
}

func trulyConflict(g driftGraph, x, y driftChange) bool {
	combined := g.apply(x).apply(y)
	return intersects(combined.affected(x.direct()), combined.affected(y.direct()))
}

// simulateBaseDrift admits 1–3 "landing" batches and batch A on base b0, lands every
// landing batch (A may depend on them; they never depend on A), then admits C on the
// advanced base and checks whether C was correctly ordered after A.
func simulateBaseDrift(trials, nodes int, seed uint64) driftReport {
	report := driftReport{
		Trials:    trials,
		BaseNodes: nodes,
		Notes: []string{
			"Toy random DAGs, not go-code: rates show whether a rule can miss conflicts, not how often production would.",
			"Signatures follow Tango's GetChangedTargets: reverse closure, in the changed graph, of edited targets, rewired targets and new targets.",
			"A pair is independent when neither batch transitively depends on the other; truth is a shared affected target in the first graph containing both changes.",
		},
	}
	for _, rule := range driftRules {
		result := driftRuleResult{Rule: rule}
		r := &driftRand{state: seed}
		var signatureTargets, signatures int
		for range trials {
			base := randomDriftBase(r, nodes)
			landing := 1 + r.intn(3)
			nextNew := int32(nodes)
			var changes []driftChange
			for range landing + 2 {
				changes = append(changes, randomDriftChange(r, base, nodes, nextNew))
				nextNew++
			}
			var batches []driftBatch
			for _, c := range changes[:landing+1] {
				batches = append(batches, admitDriftBatch(base, c, batches, rule))
			}
			a := landing
			advanced := base
			for _, c := range changes[:landing] {
				advanced = advanced.apply(c)
			}
			c := admitDriftBatch(advanced, changes[landing+1], []driftBatch{batches[a]}, rule)

			for i := range landing {
				if slices.Contains(batches[a].dependsOn, i) && !trulyConflict(advanced, changes[i], changes[a]) {
					result.FalseConflicts++
				}
				if ordered(batches, a, i) {
					continue
				}
				result.SameBasePairs++
				if trulyConflict(advanced, changes[i], changes[a]) {
					result.SameBaseMissed++
				}
			}
			if len(c.dependsOn) == 0 {
				result.DriftPairs++
				if trulyConflict(advanced, changes[a], c.change) {
					result.DriftMissed++
				}
			} else if !trulyConflict(advanced, changes[a], c.change) {
				result.FalseConflicts++
			}
			for _, b := range append(batches, c) {
				signatureTargets += len(b.comparison)
				signatures++
			}
		}
		result.MeanSignature = float64(signatureTargets) / float64(signatures)
		report.RuleResult = append(report.RuleResult, result)
	}
	return report
}
