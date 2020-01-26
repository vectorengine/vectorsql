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

func TestCreateTablePlan(t *testing.T) {
	query := "create table t1(a UInt32)"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewCreateTablePlan(statement.(*sqlparser.DDL))
	err = plan.Build()
	assert.Nil(t, err)
	t.Logf("%v", plan.Name())

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := "CreateTableNode(AST: create table t1 (\n\ta UInt32\n)\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}

func TestCreateTablePlanError(t *testing.T) {
	query := "drop table t1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewCreateTablePlan(statement.(*sqlparser.DDL))
	err = plan.Build()
	expect := "CreateTable Plan must be 'Create' operation, got:TABLE_DROP"
	actual := err.Error()
	assert.Equal(t, expect, actual)
}
