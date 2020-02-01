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

func TestCreateDatabasePlan(t *testing.T) {
	query := "create database db1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewCreateDatabasePlan(statement.(*sqlparser.DBDDL))
	err = plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := "CreateDatabaseNode(AST: &{Action:create DBName:db1 IfExists:false Collate: Charset: Options:<nil> StatementBase:{}}\n)"
	actual := plan.String()
	assert.Equal(t, expect, actual)
}

func TestCreateDatabasePlanError(t *testing.T) {
	query := "drop database db1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewCreateDatabasePlan(statement.(*sqlparser.DBDDL))
	err = plan.Build()
	expect := "CreateDatabase Plan must be 'Create' operation, got:DATABASE_DROP"
	actual := err.Error()
	assert.Equal(t, expect, actual)
}
