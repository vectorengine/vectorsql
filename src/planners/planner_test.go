// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanner(t *testing.T) {
	tests := []struct {
		name      string
		plan      IPlan
		expect    string
		errString string
	}{
		{
			name:   "planner-pass",
			plan:   NewVariablePlan("a"),
			expect: "a",
		},
		{
			name:   "planner-pass",
			plan:   NewConstantPlan(1),
			expect: "int:1 ",
		},
		{
			name: "planner-pass",
			plan: NewAliasedExpressionPlan("as",
				NewVariablePlan("b")),
			expect: "as",
		},
		{
			name:   "planner-pass",
			plan:   NewUnaryExpressionPlan("SUM", NewConstantPlan(1)),
			expect: "SUM(int:1 )",
		},
		{
			name:   "planner-pass",
			plan:   NewBinaryExpressionPlan("+", NewConstantPlan(1), NewConstantPlan(2)),
			expect: "(int:1 +int:2 )",
		},
		{
			name: "planner-pass",
			plan: NewFunctionExpressionPlan("if",
				NewBinaryExpressionPlan(">", NewConstantPlan(1), NewConstantPlan(2)),
				NewConstantPlan(1),
				NewConstantPlan(2)),
			expect: "IF([(int:1 >int:2 ) int:1  int:2 ])",
		},
		{
			name: "planner-pass",
			plan: NewMapPlan(
				NewVariablePlan("a"),
			),
			errString: "Unsupported expression plan:*planners.MapPlan",
		},
	}

	for _, test := range tests {
		plan := test.plan
		exprs, err := BuildExpression(plan)
		if test.errString == "" {
			assert.Nil(t, err)
		} else {
			assert.Equal(t, test.errString, err.Error())
			continue
		}
		expect := test.expect
		actual := exprs.String()
		assert.Equal(t, expect, actual)
	}
}
