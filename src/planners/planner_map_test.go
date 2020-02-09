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
		NewBinaryExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("db1"),
		),
		NewBinaryExpressionPlan(
			"=",
			NewVariablePlan("name"),
			NewConstantPlan("db2"),
		)}

	plan := NewMapPlan(plans...)
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := `{
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
        },
        {
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
    ]
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)

	subplans := plan.AsPlans()
	assert.NotNil(t, subplans)
}
