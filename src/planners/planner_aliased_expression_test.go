// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAliasedExpressionPlan(t *testing.T) {
	plan := NewAliasedExpressionPlan("add", NewBinaryExpressionPlan("+",
		NewVariablePlan("a"),
		NewConstantPlan(2),
	))
	err := plan.Build()
	assert.Nil(t, err)

	_ = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})

	expect := `{
    "Name": "AliasedExpressionPlan",
    "As": "add",
    "Expr": {
        "Name": "BinaryExpressionPlan",
        "FuncName": "+",
        "Left": {
            "Name": "VariablePlan",
            "Value": "a"
        },
        "Right": {
            "Name": "ConstantPlan",
            "Value": 2
        }
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
