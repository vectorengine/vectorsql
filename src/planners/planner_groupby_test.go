// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByPlan(t *testing.T) {
	tests := []struct {
		name      string
		plan      IPlan
		expect    string
		errString string
	}{
		{
			name: "groupby-sum-fail",
			plan: NewGroupByPlan(
				NewMapPlan(
					NewBinaryExpressionPlan(
						"=",
						NewVariablePlan("name"),
						NewConstantPlan("db1"),
					),
				),
				NewMapPlan(
					NewUnaryExpressionPlan(
						"SUM",
						NewVariablePlan("name")),
				),
			),
			errString: "Unsupported aggregate expression in GroupBy",
		},
		{
			name: "groupby-sum-pass",
			plan: NewGroupByPlan(
				NewMapPlan(
					NewBinaryExpressionPlan(
						"=",
						NewVariablePlan("name"),
						NewConstantPlan("db1"),
					),
				),
				NewMapPlan(
					NewVariablePlan("name")),
			),
			expect: `{
    "Name": "GroupByPlan",
    "HasAggregate": false,
    "Projects": {
        "Name": "MapPlan",
        "SubPlans": [
            {
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
            }
        ]
    },
    "GroupBys": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "VariablePlan",
                "Value": "name"
            }
        ]
    }
}`,
		},
	}

	for _, test := range tests {
		plan := test.plan
		err := plan.Build()
		if test.errString == "" {
			assert.Nil(t, err)
		} else {
			assert.Equal(t, test.errString, err.Error())
			continue
		}

		err = plan.Walk(func(plan IPlan) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
