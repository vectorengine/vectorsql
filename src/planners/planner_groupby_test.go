// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByPlan(t *testing.T) {
	groupbys := NewMapPlan(
		NewVariablePlan("x"),
		NewVariablePlan("y"),
	)
	aggregators := NewMapPlan(
		NewVariablePlan("c"),
		NewVariablePlan("d"),
	)

	plan := NewGroupByPlan(aggregators, groupbys)
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := `{
    "Name": "GroupByPlan",
    "GroupBys": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "VariablePlan",
                "Value": "x"
            },
            {
                "Name": "VariablePlan",
                "Value": "y"
            }
        ]
    },
    "Aggregators": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "VariablePlan",
                "Value": "c"
            },
            {
                "Name": "VariablePlan",
                "Value": "d"
            }
        ]
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
