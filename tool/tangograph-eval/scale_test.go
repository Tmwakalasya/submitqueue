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

func TestFitPerTarget(t *testing.T) {
	fit := fitPerTarget([]float64{100, 300}, []float64{200, 600})
	assert.Zero(t, fit.PerBatch)
	assert.InDelta(t, 2, fit.PerTarget, 1e-9)
	assert.Equal(t, linearFit{}, fitPerTarget([]float64{0}, []float64{5}))
}

func TestLoadSeconds(t *testing.T) {
	cost := designCost{bytes: linearFit{PerTarget: 1 << 20}, decodeMs: linearFit{PerTarget: 100}}
	profile := blobStoreProfile{FirstByteMs: 100, Parallel: 10, MiBPerSecond: 1}
	assert.InDelta(t, 3*0.1+5+0.5, loadSeconds(cost, profile, 21, 5), 1e-9)
}
