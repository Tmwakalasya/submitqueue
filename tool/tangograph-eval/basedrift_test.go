// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
)

func TestBaseDriftRules(t *testing.T) {
	report := simulateBaseDrift(4000, 40, 1)
	byName := map[string]driftRuleResult{}
	for _, result := range report.RuleResult {
		byName[result.Rule.Name] = result
	}
	t.Run("affected-target overlap misses conflicts on one base and across bases", func(t *testing.T) {
		assert.Positive(t, byName["affected"].SameBaseMissed)
		assert.Positive(t, byName["affected"].DriftMissed)
	})
	t.Run("added dependencies alone still miss conflicts", func(t *testing.T) {
		result := byName["affected+added-deps"]
		assert.Positive(t, result.SameBaseMissed+result.DriftMissed)
	})
	for _, name := range []string{"affected+added-deps+inherit-structural", "affected+added-deps+inherit-all"} {
		t.Run(name+" misses nothing", func(t *testing.T) {
			assert.Zero(t, byName[name].SameBaseMissed)
			assert.Zero(t, byName[name].DriftMissed)
			assert.Positive(t, byName[name].DriftPairs)
		})
	}
}

func TestDriftGraphApplyIsPure(t *testing.T) {
	base := driftGraph{0: nil, 1: {0}}
	changed := base.apply(driftChange{added: []driftEdge{{from: 1, to: 0}}, removed: []driftEdge{{from: 1, to: 0}}, newNodes: map[int32][]int32{2: {1}}})
	assert.Equal(t, driftGraph{0: nil, 1: {0}}, base)
	assert.Equal(t, []int32{0}, changed[1])
	assert.Equal(t, map[int32]bool{0: true, 1: true, 2: true}, changed.affected([]int32{0}))
}
