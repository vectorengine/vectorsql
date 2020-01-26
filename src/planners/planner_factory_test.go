// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactory(t *testing.T) {
	query := "use db1"
	plan, err := PlanFactory(query)
	assert.Nil(t, err)

	expect := "UseNode(AST: use db1\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
