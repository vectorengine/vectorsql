// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTVFPlan(t *testing.T) {
	tests := []struct {
		name   string
		plan   IPlan
		expect string
	}{
		{
			name: "simple",
			plan: NewTableValuedFunctionPlan(
				"rangetable",
				NewMapPlan(
					NewFunctionExpressionPlan("",
						NewVariablePlan("rows"),
						NewConstantPlan(1000),
					),
					NewFunctionExpressionPlan("",
						NewVariablePlan("c1"),
						NewConstantPlan("UInt32"),
					),
					NewFunctionExpressionPlan("",
						NewVariablePlan("c2"),
						NewConstantPlan("String"),
					),
				),
			),
			expect: "\n->TableValuedFunctionNode\t--> (Func=[rangetable], Args=[FuncExpressionNode=(Func=[], Args=[[VariableNode=[$rows] ConstantNode=<1000>]]), FuncExpressionNode=(Func=[], Args=[[VariableNode=[$c1] ConstantNode=<UInt32>]]), FuncExpressionNode=(Func=[], Args=[[VariableNode=[$c2] ConstantNode=<String>]])])",
		},
	}

	for _, test := range tests {
		plan := test.plan
		err := plan.Walk(func(plan IPlan) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
