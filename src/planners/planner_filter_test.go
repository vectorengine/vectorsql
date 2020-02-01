// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterPlan(t *testing.T) {
	tests := []struct {
		name   string
		plan   IPlan
		expect string
	}{
		{
			name: "simple",
			plan: NewBooleanExpressionPlan(
				"OR",
				NewBooleanExpressionPlan(
					"=",
					NewVariablePlan("name"),
					NewConstantPlan("db1"),
				),
				NewBooleanExpressionPlan(
					"=",
					NewVariablePlan("name"),
					NewConstantPlan("db2"),
				),
			),
			expect: `{
    "Name": "FilterPlan",
    "SubPlan": {
        "Name": "BooleanExpressionPlan",
        "Args": [
            {
                "Name": "BooleanExpressionPlan",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "name"
                    },
                    {
                        "Name": "ConstantPlan",
                        "Value": "db1"
                    }
                ],
                "FuncName": "="
            },
            {
                "Name": "BooleanExpressionPlan",
                "Args": [
                    {
                        "Name": "VariablePlan",
                        "Value": "name"
                    },
                    {
                        "Name": "ConstantPlan",
                        "Value": "db2"
                    }
                ],
                "FuncName": "="
            }
        ],
        "FuncName": "OR"
    }
}`,
		},
	}

	for _, test := range tests {
		plan := NewFilterPlan(test.plan)
		err := plan.Build()
		assert.Nil(t, err)

		err = plan.Walk(func(plan IPlan) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
