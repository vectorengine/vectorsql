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
			query: "select name, `engine`,data_path, metadata_path from system.databases where name='db1'",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "engine", DataType: datatypes.NewStringDataType()},
					{Name: "data_path", DataType: datatypes.NewStringDataType()},
					{Name: "metadata_path", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{},
				[]interface{}{},
				[]interface{}{},
				[]interface{}{},
			),
		},

		{
			name:  "tvf-rangetable",
			query: "SELECT i FROM rangetable(rows->5, i->'Int32')",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{0},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{3},
				[]interface{}{4},
			),
		},
		{
			name:  "filter",
			query: "SELECT i FROM rangetable(rows->5, i->'Int32') WHERE i>2",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{3},
				[]interface{}{4},
			),
		},
		{
			name:  "orderby",
			query: "SELECT i FROM rangetable(rows->5, i->'Int32') WHERE i>2 order by i desc",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{4},
				[]interface{}{3},
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
			assert.True(t, mocks.DataBlockEqual(expect, actual))
		}
	}
}
