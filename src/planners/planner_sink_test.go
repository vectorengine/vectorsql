// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSinkPlan(t *testing.T) {
	plan := NewSinkPlan()
	err := plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := `{
    "Name": "SinkPlan"
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
