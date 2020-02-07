// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimitPlan(t *testing.T) {
	tests := []struct {
		name   string
		plan   IPlan
		expect string
	}{
		{
			name: "simple",
			plan: NewLimitPlan(
				NewConstantPlan(1),
				NewConstantPlan(2),
			),
			expect: `{
    "Name": "LimitPlan",
    "OffsetPlan": {
        "Name": "ConstantPlan",
        "Value": 1
    },
    "RowcountPlan": {
        "Name": "ConstantPlan",
        "Value": 2
    }
}`,
		},
	}

	for _, test := range tests {
		plan := test.plan
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
