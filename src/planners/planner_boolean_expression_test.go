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
	t.Logf("%v", plan.Name())

	_ = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})

	expect := "BooleanExpressionNode=(Func=[>], Args=[[VariableNode=[$a] ConstantNode=<2>]])"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
