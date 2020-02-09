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
			plan: NewBinaryExpressionPlan(
				"OR",
				NewBinaryExpressionPlan(
					"=",
					NewVariablePlan("name"),
					NewConstantPlan("db1"),
				),
				NewBinaryExpressionPlan(
					"=",
					NewVariablePlan("name"),
					NewConstantPlan("db2"),
				),
			),
			expect: `{
    "Name": "FilterPlan",
    "SubPlan": {
        "Name": "BinaryExpressionPlan",
        "FuncName": "OR",
        "Left": {
            "Name": "BinaryExpressionPlan",
            "FuncName": "=",
            "Left": {
                "Name": "VariablePlan",
                "Value": "name"
            },
            "Right": {
                "Name": "ConstantPlan",
                "Value": "db1"
            }
        },
        "Right": {
            "Name": "BinaryExpressionPlan",
            "FuncName": "=",
            "Left": {
                "Name": "VariablePlan",
                "Value": "name"
            },
            "Right": {
                "Name": "ConstantPlan",
                "Value": "db2"
            }
        }
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
