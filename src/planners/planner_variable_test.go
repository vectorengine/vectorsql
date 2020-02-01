// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVariablePlan(t *testing.T) {
	plan := NewVariablePlan("v1")
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := `{
	"Name": "VariablePlan",
	"Value": "v1"
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
