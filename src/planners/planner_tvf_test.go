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
			expect: `{
    "Name": "TableValuedFunctionPlan",
    "As": "",
    "FuncName": "rangetable",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "FunctionExpressionPlan",
                "FuncName": "",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "rows"
                    },
                    {
                        "Name": "ConstantPlan",
                        "Value": 1000
                    }
                ]
            },
            {
                "Name": "FunctionExpressionPlan",
                "FuncName": "",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "c1"
                    },
                    {
                        "Name": "ConstantPlan",
                        "Value": "UInt32"
                    }
                ]
            },
            {
                "Name": "FunctionExpressionPlan",
                "FuncName": "",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "c2"
                    },
                    {
                        "Name": "ConstantPlan",
                        "Value": "String"
                    }
                ]
            }
        ]
    }
}`,
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
