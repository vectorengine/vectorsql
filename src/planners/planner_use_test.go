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

	err = plan.Walk(nil)
	assert.Nil(t, err)

	expect := `{
    "Name": "UsePlan",
    "Ast": {
        "DBName": "db1"
    }
}`
	actual := plan.String()
	assert.Equal(t, expect, actual)
}
