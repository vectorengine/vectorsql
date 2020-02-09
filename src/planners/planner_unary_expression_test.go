// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnaryExpressionPlan(t *testing.T) {
	plan := NewUnaryExpressionPlan("SUM",
		NewVariablePlan("a"),
	)
	err := plan.Build()
	assert.Nil(t, err)

	_ = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})

	expect := `{
    "Name": "UnaryExpressionPlan",
    "FuncName": "SUM",
    "Expr": {
        "Name": "VariablePlan",
        "Value": "a"
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
