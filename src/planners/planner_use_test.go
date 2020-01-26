// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"parsers"
	"parsers/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestUsePlan(t *testing.T) {
	query := "use db1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewUsePlan(statement.(*sqlparser.Use))
	err = plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := "UseNode(AST: use db1\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
