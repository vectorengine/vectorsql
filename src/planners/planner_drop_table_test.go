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

func TestDropTablePlan(t *testing.T) {
	query := "drop table t1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewDropTablePlan(statement.(*sqlparser.DDL))
	err = plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := "DropTableNode(AST: drop table t1\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}

func TestDropTablePlanError(t *testing.T) {
	query := "create table t1(a UInt32)"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewDropTablePlan(statement.(*sqlparser.DDL))
	err = plan.Build()
	expect := "DropTable Plan must be 'Drop' operation, got:TABLE_CREATE"
	actual := err.Error()
	assert.Equal(t, expect, actual)
}
