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
			name:  "simple-pass",
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
			name:  "tvf-rangetable-pass",
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
			name:  "filter-pass",
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
			name:  "orderby-pass",
			query: "SELECT i FROM rangetable(rows->5, i->'Int32') WHERE i>2 order by i desc",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{4},
				[]interface{}{3},
			),
		},
		{
			name:  "system.numbers-pass",
			query: "SELECT number,(number+1) FROM system.numbers limit 3",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "number", DataType: datatypes.NewUInt64DataType()},
					{Name: "(number+int:1 )", DataType: datatypes.NewInt64DataType()},
				},
				[]interface{}{0, 1},
				[]interface{}{1, 2},
				[]interface{}{2, 3},
			),
		},
		{
			name:  "simple-pass",
			query: "SELECT server,sum(response_time) as time FROM logmock(rows->15) order by time desc",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "server", DataType: datatypes.NewStringDataType()},
					{Name: "time", DataType: datatypes.NewInt64DataType()},
				},
				[]interface{}{"192.168.0.2", 170},
			),
		},
		{
			name: "aggregate-pass",
			query: `SELECT 
    COUNT(server) as count,
    SUM(IF(status != 200, 1, 0)) AS errors, 
    SUM(IF(status = 200, 1, 0)) AS success, 
    errors / COUNT(server) AS error_rate, 
    success / COUNT(server) AS success_rate, 
    SUM(response_time) / COUNT(server) AS load_avg, 
    MIN(response_time), 
    MAX(response_time), 
    server
FROM logmock(rows -> 15)
GROUP BY server
HAVING errors > 0
ORDER BY 
    server ASC, 
    load_avg DESC`,
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "count", DataType: datatypes.NewInt64DataType()},
					{Name: "errors", DataType: datatypes.NewInt64DataType()},
					{Name: "success", DataType: datatypes.NewInt64DataType()},
					{Name: "error_rate", DataType: datatypes.NewFloat64DataType()},
					{Name: "success_rate", DataType: datatypes.NewFloat64DataType()},
					{Name: "load_avg", DataType: datatypes.NewFloat64DataType()},
					{Name: "MIN(response_time)", DataType: datatypes.NewInt64DataType()},
					{Name: "MAX(response_time)", DataType: datatypes.NewInt64DataType()},
					{Name: "server", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{9, 3, 6, 0.3333333333333333, 0.6666666666666666, 11.444444444444445, 10, 13, "192.168.0.1"},
				[]interface{}{6, 2, 4, 0.3333333333333333, 0.6666666666666666, 11.166666666666666, 10, 14, "192.168.0.2"},
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
