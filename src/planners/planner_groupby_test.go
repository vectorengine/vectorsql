// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByPlan(t *testing.T) {
	plans := NewMapPlan(
		NewVariablePlan("c"),
		NewVariablePlan("d"),
	)

	plan := NewGroupByPlan(plans)
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := `{
    "Name": "GroupByPlan",
    "SubPlan": {
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
