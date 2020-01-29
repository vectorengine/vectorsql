// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderByPlan(t *testing.T) {
	tests := []struct {
		name   string
		plan   IPlan
		expect string
	}{
		{
			name: "simple",
			plan: NewOrderByPlan(
				[]IPlan{
					NewVariablePlan("c1"),
					NewVariablePlan("c2"),
					NewVariablePlan("c2"),
				},
				[]string{
					"asc",
					"desc",
					"asc",
				},
			),
			expect: "OrderByNode[(field:VariableNode=[$c1], direction:asc)(field:VariableNode=[$c2], direction:desc)(field:VariableNode=[$c2], direction:asc)]",
		},
	}

	for _, test := range tests {
		plan := test.plan
		err := plan.Build()
		assert.Nil(t, err)
		t.Logf("%v", plan.Name())

		err = plan.Walk(func(plan IPlan) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
