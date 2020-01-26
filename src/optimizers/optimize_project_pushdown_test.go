// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"testing"

	"planners"

	"github.com/stretchr/testify/assert"
)

func TestOptimizeProjectPushDown(t *testing.T) {
	plan := planners.NewMapPlan(
		planners.NewScanPlan("tables", "system"),
		planners.NewProjectPlan(
			planners.NewMapPlan(
				planners.NewVariablePlan("name"),
			),
		),
	)

	plan = Optimize(plan, DefaultOptimizers).(*planners.MapPlan)

	expect := plan.SubPlans[1]
	actual := plan.SubPlans[0].(*planners.ScanPlan).Project
	assert.Equal(t, expect, actual)
}
