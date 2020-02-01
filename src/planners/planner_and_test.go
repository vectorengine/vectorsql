// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAndPlan(t *testing.T) {
	plan := NewAndPlan(
		NewBooleanExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("x"),
		),
		NewBooleanExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("y"),
		),
	)
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := `{
    "Name": "AndPlan",
    "FuncName": "AND",
    "Left": {
        "Name": "BooleanExpressionPlan",
        "Args": [
            {
                "Name": "VariablePlan",
                "Value": "name"
            },
            {
                "Name": "ConstantPlan",
                "Value": "x"
            }
        ],
        "FuncName": "="
    },
    "Right": {
        "Name": "BooleanExpressionPlan",
        "Args": [
            {
                "Name": "VariablePlan",
                "Value": "name"
            },
            {
                "Name": "ConstantPlan",
                "Value": "y"
            }
        ],
        "FuncName": "="
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
