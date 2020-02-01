// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionExpressionPlan(t *testing.T) {
	plan := NewFunctionExpressionPlan("+",
		NewConstantPlan(1),
		NewConstantPlan(2),
	)
	err := plan.Build()
	assert.Nil(t, err)

	_ = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})

	expect := `{
    "Name": "FunctionExpressionPlan",
    "FuncName": "+",
    "Args": [
        {
            "Name": "ConstantPlan",
            "Value": 1
        },
        {
            "Name": "ConstantPlan",
            "Value": 2
        }
    ]
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
