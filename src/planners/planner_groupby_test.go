// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByPlan(t *testing.T) {
	projects := NewMapPlan(
		NewFunctionExpressionPlan(
			"COUNT",
			NewVariablePlan("c"),
		),
		NewVariablePlan("d"),
	)

	groupbys := NewMapPlan(
		NewVariablePlan("c"),
		NewVariablePlan("d"),
	)

	plan := NewGroupByPlan(projects, groupbys)
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := `{
    "Name": "GroupByPlan",
    "Projects": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "FunctionExpressionPlan",
                "FuncName": "COUNT",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "c"
                    }
                ]
            },
            {
                "Name": "VariablePlan",
                "Value": "d"
            }
        ]
    },
    "GroupBys": {
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
