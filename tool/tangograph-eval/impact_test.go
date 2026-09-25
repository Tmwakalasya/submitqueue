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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceImpactMayMissSharedTransitiveTargetWhenCapped(t *testing.T) {
	graph, err := compactTargets([]queryTarget{
		{name: "//app:a.go", ruleType: "source file"},
		{name: "//app:b.go", ruleType: "source file"},
		{name: "//app:a", ruleType: "go_library", deps: []string{"//app:a.go"}},
		{name: "//app:b", ruleType: "go_library", deps: []string{"//app:b.go"}},
		{name: "//app:integration_test", ruleType: "go_test", deps: []string{"//app:a", "//app:b"}},
	})
	require.NoError(t, err)
	impact := measureSourceImpact(graph, 2, "//app:")
	assert.Equal(t, uint64(2), impact.SourceSeeds)
	assert.Equal(t, uint64(2), impact.CandidateSources)
	assert.Equal(t, uint64(1), impact.Pairs)
	assert.Equal(t, uint64(1), impact.FullOverlapPairs)
	assert.Zero(t, impact.DirectOverlapPairs)
	assert.Equal(t, uint64(3), impact.MaxFullTargets)
}

func TestIntersectSortedFingerprints(t *testing.T) {
	one := []uint64{fingerprint("//app:one"), fingerprint("//app:two")}
	two := []uint64{fingerprint("//app:three"), fingerprint("//app:two")}
	disjoint := []uint64{fingerprint("//app:four")}
	slices.Sort(one)
	slices.Sort(two)
	slices.Sort(disjoint)
	assert.True(t, intersectSortedFingerprints(one, two))
	assert.False(t, intersectSortedFingerprints(one, disjoint))
	assert.False(t, intersectSortedFingerprints(one, nil))
}
