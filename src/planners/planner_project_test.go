// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectPlan(t *testing.T) {
	plans := NewMapPlan(
		NewVariablePlan("name"),
		NewVariablePlan("age"),
	)

	plan := NewProjectPlan(plans)
	err := plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := "\n->ProjectNode\t--> (VariableNode=[$name], VariableNode=[$age])"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
