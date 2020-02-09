// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"testing"

	"planners"

	"github.com/stretchr/testify/assert"
)

func TestOptimizePredicatePushDown(t *testing.T) {
	plan := planners.NewMapPlan(
		planners.NewScanPlan("tables", "system"),
		planners.NewProjectPlan(
			planners.NewMapPlan(
				planners.NewVariablePlan("name"),
			),
		),
		planners.NewFilterPlan(
			planners.NewBinaryExpressionPlan(
				"=",
				planners.NewVariablePlan("name"),
				planners.NewConstantPlan("db2"),
			),
		),
	)

	plan = Optimize(plan, DefaultOptimizers).(*planners.MapPlan)

	expect := plan.SubPlans[2]
	actual := plan.SubPlans[0].(*planners.ScanPlan).Filter
	assert.Equal(t, expect, actual)
}
