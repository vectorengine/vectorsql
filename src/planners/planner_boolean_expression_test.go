// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBooleanExpressionPlan(t *testing.T) {
	plan := NewBooleanExpressionPlan(">",
		NewVariablePlan("a"),
		NewConstantPlan(2),
	)
	err := plan.Build()
	assert.Nil(t, err)

	_ = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})

	expect := `{
    "Name": "BooleanExpressionPlan",
    "Args": [
        {
            "Name": "VariablePlan",
            "Value": "a"
        },
        {
            "Name": "ConstantPlan",
            "Value": 2
        }
    ],
    "FuncName": "\u003e"
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
