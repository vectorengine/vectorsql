// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectionPlan(t *testing.T) {
	tests := []struct {
		name      string
		plan      *SelectionPlan
		expect    SelectionMode
		errString string
	}{
		{
			name: "normal-pass",
			plan: NewSelectionPlan(
				NewMapPlan(
					NewVariablePlan("name"),
					NewVariablePlan("age"),
				),
				NewMapPlan(),
			),
			expect: NormalSelection,
		},
		{
			name: "aggregate-pass",
			plan: NewSelectionPlan(
				NewMapPlan(
					NewVariablePlan("name"),
					NewUnaryExpressionPlan("sum", NewVariablePlan("name")),
				),
				NewMapPlan(),
			),
			expect: AggregateSelection,
		},
		{
			name: "groupby-pass",
			plan: NewSelectionPlan(
				NewMapPlan(
					NewVariablePlan("name"),
					NewUnaryExpressionPlan("sum", NewVariablePlan("name")),
				),
				NewMapPlan(
					NewVariablePlan("name")),
			),
			expect: GroupBySelection,
		},
		{
			name: "groupby-with-aggregate-fail",
			plan: NewSelectionPlan(
				NewMapPlan(
					NewVariablePlan("name"),
					NewUnaryExpressionPlan("sum", NewVariablePlan("name")),
				),
				NewMapPlan(
					NewUnaryExpressionPlan(
						"SUM",
						NewVariablePlan("name")),
				),
			),
			errString: "Unsupported aggregate expression in GroupBy",
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
		_ = plan.String()
		assert.Nil(t, err)
		expect := test.expect
		actual := plan.SelectionMode
		assert.Equal(t, expect, actual)
	}
}
