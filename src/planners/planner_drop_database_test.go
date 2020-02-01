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

func TestDropDatabasePlan(t *testing.T) {
	query := "drop database db1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewDropDatabasePlan(statement.(*sqlparser.DBDDL))
	err = plan.Build()
	assert.Nil(t, err)

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := `{
    "Name": "DropDatabasePlan",
    "Ast": {
        "Action": "drop",
        "DBName": "db1",
        "IfExists": false,
        "Collate": "",
        "Charset": "",
        "Options": null
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}

func TestDropDatabasePlanError(t *testing.T) {
	query := "create database db1"
	statement, err := parsers.Parse(query)
	assert.Nil(t, err)

	plan := NewDropDatabasePlan(statement.(*sqlparser.DBDDL))
	err = plan.Build()
	expect := "DropDatabase Plan must be 'Drop' operation, got:DATABASE_CREATE"
	actual := err.Error()
	assert.Equal(t, expect, actual)
}
