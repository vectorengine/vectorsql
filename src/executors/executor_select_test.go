// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"mocks"
	"testing"

	"columns"
	"datablocks"
	"datatypes"
	"parsers"
	"planners"

	"parsers/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestSelectExecutor(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect *datablocks.DataBlock
	}{
		{
			name:  "simple",
			query: "select name from system.databases where name='db1'",
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "engine", DataType: datatypes.NewStringDataType()},
					{Name: "data_path", DataType: datatypes.NewStringDataType()},
					{Name: "metadata_path", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{},
			),
		},
		{
			name:  "tvf-range",
			query: "SELECT * FROM range(1, 5)",
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{3},
				[]interface{}{4},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()

		statement, err := parsers.Parse(test.query)
		assert.Nil(t, err)

		plan := planners.NewSelectPlan(statement.(*sqlparser.Select))
		err = plan.Build()
		assert.Nil(t, err)

		ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
		executor := NewSelectExecutor(ctx, plan)
		transform, err := executor.Execute()
		assert.Nil(t, err)

		for x := range transform.In().Recv() {
			expect := test.expect
			actual := x.(*datablocks.DataBlock)
			assert.Equal(t, expect, actual)
		}
	}
}
