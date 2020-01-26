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

func TestShowTablesPlan(t *testing.T) {
	query := "show tables"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewShowTablesPlan(statement.(*sqlparser.Show))
	err = plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(func(plan IPlan) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)

	expect := "ShowTablesNode(AST: show tables\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
