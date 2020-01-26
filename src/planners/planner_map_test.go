// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapPlan(t *testing.T) {
	plans := []IPlan{
		NewBooleanExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("db1"),
		),
		NewBooleanExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("db2"),
		)}

	plan := NewMapPlan(plans...)
	err := plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := "BooleanExpressionNode=(Func=[=], Args=[[VariableNode=[$name] ConstantNode=<db1>]]), BooleanExpressionNode=(Func=[=], Args=[[VariableNode=[$name] ConstantNode=<db2>]])"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
