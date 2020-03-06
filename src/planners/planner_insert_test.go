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

func TestInsertPlan(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect string
	}{
		{
			name:  "simple",
			query: "insert into t1",
			expect: `{
    "Name": "InsertPlan",
    "Schema": "",
    "Table": "t1",
    "Format": ""
}`,
		},
		{
			name:  "simple",
			query: "insert into test.t1 values",
			expect: `{
    "Name": "InsertPlan",
    "Schema": "test",
    "Table": "t1",
    "Format": ""
}`,
		},
		{
			name:  "simple",
			query: "insert into test.t1 FORMAT xx values",
			expect: `{
    "Name": "InsertPlan",
    "Schema": "test",
    "Table": "t1",
    "Format": "xx"
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := parsers.Parse(test.query)
			assert.Nil(t, err)

			plan := NewInsertPlan(statement.(*sqlparser.Insert))
			err = plan.Build()
			assert.Nil(t, err)
			actual := plan.String()
			assert.Equal(t, test.expect, actual)
		})
	}
}
